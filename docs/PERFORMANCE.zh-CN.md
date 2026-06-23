# 性能与长时间端到端测试报告

English version: [PERFORMANCE.md](PERFORMANCE.md)

本文记录 `go-kv` 最新一轮生产式验证结果。这些测试不只衡量吞吐量，还会覆盖 Raft leader 切换、节点重启、快照、日志压缩、LSM flush、LSM compaction、客户端重试和最终数据一致性。

## 最新验证

| 项目 | 值 |
|---|---|
| 日期 | 2026-06-23 |
| 机器 | macOS Darwin 25.5.0, Apple Silicon |
| Go | 1.25.5 |
| 传输层 | 长时间 E2E 使用 gRPC；聚焦集成测试额外覆盖 TCP |
| 存储 | Raft 日志和状态机均使用 LSM |
| 日志级别 | `GO_KV_LOG_LEVEL=warn` |

## 已通过的命令

| 目的 | 命令 | 结果 |
|---|---|---|
| LSM/WAL recovery 回归 | `GO_KV_LOG_LEVEL=warn go test ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=10m` | 通过 |
| LSM compaction 调度回归 | `GO_KV_LOG_LEVEL=warn go test ./engine/lsm/sstable -run '^(TestCreateNewSSTableSkipsCompactionWhenBelowThreshold|TestSSTableManagerOpenFilesSnapshotReleasesManagerLock)$' -count=10 -timeout=2m` | 通过 |
| SSTable 包稳定性循环 | `GO_KV_LOG_LEVEL=warn go test ./engine/lsm/sstable -count=100 -timeout=5m` | 114.758s 通过 |
| LSM/storage race 门禁 | `GO_KV_LOG_LEVEL=warn go test -race ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=12m` | 通过 |
| LSM-backed Raft log 物理压缩回归 | `GO_KV_LOG_LEVEL=warn go test ./pkg/storage/lsm -run '^(TestStorageAdapterCompactLogDeletesPhysicalLogKeys|TestStorageAdapter_Snapshot|TestStorageAdapter_CompactBeyondLastIndexFromSnapshot|TestStorageAdapter_LogEntries|TestStorageAdapter_ReappendAfterTruncateSurvivesFlushCompactionAndRestart)$' -count=1 -timeout=5m` | 2.753s 通过 |
| 物理日志 tombstone 后的 LSM 包回归 | `GO_KV_LOG_LEVEL=warn go test ./pkg/storage/lsm ./engine/lsm/... -count=1 -timeout=12m` | 通过 |
| 物理日志 tombstone 后的 LSM/storage race 门禁 | `GO_KV_LOG_LEVEL=warn go test -race ./pkg/storage/lsm ./engine/lsm/... -count=1 -timeout=12m` | 通过；最慢 package `engine/lsm/database` 33.343s |
| 物理日志 tombstone 后的快照/重启集群循环 | `GO_KV_LOG_LEVEL=warn go test ./tests -run '^(TestCluster_TakeSnapshot|TestCluster_InstallSnapshot|TestCluster_FullClusterRestart)$' -count=3 -timeout=12m` | 301.753s 通过 |
| 确定性的 waitForAppliedLog timeout 重查回归 | `GO_KV_LOG_LEVEL=warn go test -race ./raft -run '^TestWaitForAppliedLogRechecksLastAppliedOnTimeout$' -count=100 -timeout=3m` | 11.291s 通过 |
| 确定性测试修复后的 Raft package race 门禁 | `GO_KV_LOG_LEVEL=warn go test -race ./raft -count=1 -timeout=8m` | 14.295s 通过 |
| Race 负载下 leader 发现回归 | `GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_MembershipChange$/^grpc_simplefile$' -count=5 -timeout=12m` | 71.951s 通过 |
| Leader 发现修复后的完整 membership-change 矩阵 | `GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_MembershipChange$' -count=1 -timeout=15m` | 72.375s 通过 |
| Race 负载下 network partition leader 检测回归 | `GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_NetworkPartition$/^tcp_inmemory$' -count=5 -timeout=12m` | 61.821s 通过 |
| 候选节点限定 leader discovery 后的完整 network-partition 矩阵 | `GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_NetworkPartition$' -count=1 -timeout=15m` | 68.575s 通过 |
| 聚焦集群回归循环 | `GO_KV_LOG_LEVEL=warn go test ./tests -run '^(TestCluster_ConcurrentClientRequests|TestCluster_TakeSnapshot|TestCluster_InstallSnapshot|TestCluster_FullClusterRestart|TestCluster_LeaderFailover)$' -count=3 -timeout=12m` | 527.110s 通过 |
| 全量 short 单元/集成门禁 | `GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m` | 通过；修复 #122、#123、#124 后最新 `tests` 包 1005.048s |
| 单个 10 分钟写入密集触发场景 | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=20m ./tests -run '^TestLongRunning_10Min_WriteHeavy$' -count=1` | 异步 compaction 调度修复后 613.049s 通过 |
| #121 后 10 分钟重启/快照一致性场景 | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=25m ./tests -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' -count=1` | 611.921s 通过，失败操作 0 |
| 全量长时间 E2E 回归 | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' -count=1` | 3657.132s 通过 |

现在 short 模式行为是明确的：10 分钟 E2E 在 `testing.Short()` 下会跳过。这样 `go test -short ./...` 可以继续作为 PR 覆盖率入口，而真实 10 分钟场景必须显式运行。

## 最终 10 分钟 E2E 结果

所有场景都开启了 race detector。一次写入只有在 Raft leader 提交日志并应用到状态机之后才算成功。包含重启的场景会在客户端停止后等待最终 cluster barrier，然后再比较所有节点数据。

| 场景 | 总操作数 | 失败操作 | 吞吐量 | P50 | P95 | P99 | Leader 切换 | 快照节点数 | 最大快照 index | 一致性 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Comprehensive | 655,382 | 0 | 1,092.30 ops/s | 3.457458ms | 12.997042ms | 40.629916ms | 4 | 3 | 482,995 | 通过，1,998 个 key |
| WriteHeavy | 512,565 | 0 | 854.27 ops/s | 3.519625ms | 14.7735ms | 44.532042ms | 16 | 3 | 507,421 | 通过，2,000 个 key |
| MixedWithFailures | 948,673 | 0 | 1,581.12 ops/s | 1.426167ms | 4.114709ms | 11.605334ms | 1 | 3 | 659,881 | 通过，3,600 个 node-key 检查 |
| ConsistencyWithRestartsAndSnapshots | 1,299,458 | 0 | 2,165.76 ops/s | 1.554667ms | 3.969875ms | 8.658667ms | 5 | 3 | 908,296 | 通过，3,600 个 node-key 检查 |
| ReadHeavy | 50,159,859 | 0 | 83,599.76 ops/s | 17.25us | 331.375us | 897.375us | 0 | 0 | 0 | 通过，2,000 个 key |
| DeleteStress | 714,117 | 0 | 1,190.19 ops/s | 2.473625ms | 9.325959ms | 25.642416ms | 13 | 3 | 695,088 | 通过，3,600 个 node-key 检查 |

最新全量运行修复了此前由
[issue #113](https://github.com/xmh1011/go-kv/issues/113)、
[issue #116](https://github.com/xmh1011/go-kv/issues/116) 和
[issue #117](https://github.com/xmh1011/go-kv/issues/117) 跟踪的
ReadIndex 与 apply-timeout 回归。对写入稳定性最关键的变化是：
SSTable compaction 不再同步运行在 Raft apply 前台路径里。MemTable flush
仍会先发布持久化的 Level-0 SSTable，然后把 compaction 合并到后台 worker
里执行；测试或关闭流程可以通过 `WaitForCompactions()` 等待后台任务收敛。
Issue #119 进一步收紧了这个路径：低于阈值的 flush 不再启动无实际 merge 工作的
compaction worker，普通小 flush 不会额外创建 goroutine，也不会为 no-op compaction
竞争 `Manager.mu`。Issue #121 进一步收紧 snapshot 驱动的 Raft log compaction：
`CompactLog` 现在会先 tombstone 已压缩的物理 `log:<index>` key，再推进逻辑窗口，
让长时间运行节点可以通过普通 LSM compaction 回收旧日志 payload。Issue #122
修复了 `TestWaitForAppliedLogRechecksLastAppliedOnTimeout` 在 race 模式下的测试前置
条件：测试现在先等待 apply waiter 注册，再设置 `lastApplied`，因此验证的是 timeout
分支重查逻辑，而不是依赖很短的调度竞态。Issue #123 修复了集成测试 leader-discovery
helper：membership 测试在 race 模式下会先扫描本地 leader 候选，再发 ReadIndex probe，
避免对所有 follower 串行执行缓慢 probe。Issue #124 在 network-partition 测试的
majority partition 内复用了同一个 helper，移除了另一份固定 sleep 的手写 probe loop。

## #109 后的重启/快照聚焦重放

修复 SSTable 重写 metadata reset 后，重新在 race detector 下跑了重启/快照触发场景：

| 项目 | 值 |
|---|---:|
| 时长 | 10m0s |
| 总操作数 | 1,392,428 |
| 失败操作 | 0 |
| 吞吐量 | 2,320.71 ops/s |
| P50 | 1.696541ms |
| P95 | 3.19325ms |
| P99 | 7.467875ms |
| Leader 切换 | 84 |
| 快照节点数 | 3 |
| 最大快照 index | 974,207 |
| 最终 barrier | 通过 |
| 严格一致性 | 通过，3,600 个 node-key 检查 |

这次聚焦重放不替代上面的六场景全量回归。它是 #109 SSTable metadata 修复的最新
定向证据，因为该问题影响 snapshot 和 restart 路径会使用到的 LSM 文件布局边界。

## #111 后的重启/快照聚焦重放

修复 LSM snapshot 路径验证后，再次在 race detector 下跑了相同的重启/快照触发场景：

| 项目 | 值 |
|---|---:|
| 时长 | 10m0s |
| 总操作数 | 1,104,337 |
| 失败操作 | 0 |
| 吞吐量 | 1,840.56 ops/s |
| P50 | 1.579459ms |
| P95 | 5.621625ms |
| P99 | 16.051875ms |
| Leader 切换 | 65 |
| 快照节点数 | 3 |
| 最大快照 index | 759,795 |
| 最终 barrier | 通过 |
| 严格一致性 | 通过，3,600 个 node-key 检查 |

这是 snapshot apply 路径验证修复的最新定向证据。长测覆盖正常 snapshot
导出/安装和重启行为；新增单测直接覆盖畸形 snapshot manifest。

## #121 后的物理日志压缩聚焦验证

Issue #121 没有表现为用户可见的一致性失败，因为 `firstIndex` 推进后，`GetEntry`
已经会正确隐藏被压缩的 index。真正的问题在更底层：旧的物理 `log:<index>` key
仍留在 LSM tree 中，而且没有 tombstone，普通 LSM compaction 无法回收这些 payload
字节。

因此这次聚焦验证同时检查两个层次：

| 检查 | 信号 |
|---|---|
| 直接物理 key 回归 | `TestStorageAdapterCompactLogDeletesPhysicalLogKeys` 确认已压缩 key 在 raw LSM lookup 路径返回 `nil`，而第一个保留 key 仍存在。 |
| Snapshot/log adapter 兼容性 | 现有 snapshot、compact-beyond-last-index、log-entry、truncate/reappend 测试在新增 tombstone 写入后仍通过。 |
| LSM 包回归 | `./pkg/storage/lsm ./engine/lsm/...` 通过，覆盖 flush、compaction、WAL recovery、SSTable read 和接近 restart 的存储行为。 |
| Snapshot/restart 集成 | `TestCluster_TakeSnapshot`、`TestCluster_InstallSnapshot`、`TestCluster_FullClusterRestart` 使用 `-count=3` 通过，证明新增物理删除不破坏 snapshot 创建、snapshot install 或持久化重启恢复。 |

这组验证有意和吞吐指标分开。该修复会为每个已压缩 Raft log key 多写一个 tombstone，
这是正确性取舍：只有先确保逻辑日志窗口和物理 keyspace 一致，后续 range deletion
或批量删除优化才是安全的。

这次 #121 后的 10 分钟重启/快照 E2E 定向重放结果：

| 项目 | 值 |
|---|---:|
| 时长 | 10m0s |
| 总操作数 | 518,144 |
| 失败操作 | 0 |
| 吞吐量 | 863.57 ops/s |
| P50 | 2.64ms |
| P95 | 16.902334ms |
| P99 | 40.707ms |
| Leader 切换 | 5 |
| 重启次数 | 3 |
| 快照节点数 | 3 |
| 最大快照 index | 363,305 |
| 最终 barrier | 通过 |
| 严格一致性 | 通过，3,600 个 node-key 检查 |

这次重放直接覆盖修复后的 `CompactLog` 路径，因为 snapshot 会推进 Raft log 窗口，
restart 又会强制重新打开已持久化的 LSM 状态。

## 正确性门禁

当前长时间 E2E 会检查：

- 最新成功运行中失败操作数为 0；
- 客户端停止后等待 final cluster barrier，避免遗留已发出的请求；
- 重启类场景结束后逐节点比较数据；
- 显式校验指标，一致性失败不能被成功请求数掩盖；
- 无效 Raft log 编码不能静默回退；
- short 模式跳过长时间 E2E，而不是运行缩短版伪长测。

## 如何复现

排查单个问题时先跑触发场景：

```bash
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=25m ./tests \
  -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' \
  -count=1
```

代码或测试逻辑修复后，再跑全量长时间 E2E 回归：

```bash
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests \
  -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' \
  -count=1
```

PR 覆盖率和 Codecov 使用：

```bash
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=25m
make test
```

## 当前性能判断

最新结果说明写密集负载已经稳定，但仍显著慢于读密集负载。这符合当前架构预期，因为每次写入都要经过：

1. 进入 leader 日志；
2. 通过 LSM-backed Raft log 持久化；
3. 复制到多数派；
4. 等待提交；
5. 应用到 LSM-backed 状态机；
6. 对客户端返回可见结果。

最新性能修复已经把 LSM compaction 移出了前台 flush 路径，并且让后台调度受阈值门禁控制。后续有价值的性能优化方向包括 batch 大小、follower 追赶、Raft log adapter 写放大降低，以及后台 compaction worker 的背压指标。任何优化都不应削弱上面的正确性门禁。

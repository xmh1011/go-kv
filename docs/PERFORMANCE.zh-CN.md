# 性能与长时间端到端测试报告

English version: [PERFORMANCE.md](PERFORMANCE.md)

本文记录 `go-kv` 最新一轮生产式验证结果。这些测试不只衡量吞吐量，还会覆盖 Raft leader 切换、节点重启、快照、日志压缩、LSM flush、LSM compaction、客户端重试和最终数据一致性。

## 最新验证

| 项目 | 值 |
|---|---|
| 日期 | 2026-06-22 |
| 机器 | macOS Darwin 26.3.1, Apple Silicon |
| Go | 1.25.5 |
| 传输层 | 长时间 E2E 使用 gRPC；聚焦集成测试额外覆盖 TCP |
| 存储 | Raft 日志和状态机均使用 LSM |
| 日志级别 | `GO_KV_LOG_LEVEL=warn` |

## 已通过的命令

| 目的 | 命令 | 结果 |
|---|---|---|
| LSM/WAL recovery 回归 | `GO_KV_LOG_LEVEL=warn go test ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=5m` | 通过 |
| 全量 short 单元/集成门禁 | `GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=25m` | 通过；最新 `tests` 包 776.861s |
| 单个 10 分钟重启/快照触发场景 | `GO_KV_LOG_LEVEL=warn go test -race -v ./tests -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' -count=1 -timeout=25m` | 最新 post-#111 运行 607.722s 通过 |
| 全量长时间 E2E 回归 | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' -count=1` | 最新运行 3684.450s 通过 |

现在 short 模式行为是明确的：10 分钟 E2E 在 `testing.Short()` 下会跳过。这样 `go test -short ./...` 可以继续作为 PR 覆盖率入口，而真实 10 分钟场景必须显式运行。

## 最终 10 分钟 E2E 结果

所有场景都开启了 race detector。一次写入只有在 Raft leader 提交日志并应用到状态机之后才算成功。包含重启的场景会在客户端停止后等待最终 cluster barrier，然后再比较所有节点数据。

| 场景 | 总操作数 | 失败操作 | 吞吐量 | P50 | P95 | P99 | Leader 切换 | 快照节点数 | 最大快照 index | 一致性 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Comprehensive | 502,428 | 0 | 837.38 ops/s | 3.5285ms | 25.564834ms | 66.73125ms | 23 | 3 | 371,551 | 通过，1,994 个 key |
| WriteHeavy | 326,298 | 0 | 543.83 ops/s | 3.841292ms | 24.972667ms | 70.632083ms | 35 | 3 | 326,489 | 通过，2,000 个 key |
| MixedWithFailures | 464,357 | 0 | 773.93 ops/s | 2.070792ms | 13.270042ms | 35.819375ms | 31 | 3 | 311,698 | 通过，3,600 个 node-key 检查 |
| ConsistencyWithRestartsAndSnapshots | 681,847 | 0 | 1,136.41 ops/s | 2.518375ms | 10.924792ms | 28.547208ms | 41 | 3 | 462,404 | 通过，3,600 个 node-key 检查 |
| ReadHeavy | 16,729,989 | 0 | 27,883.31 ops/s | 29.5us | 467.625us | 2.115667ms | 0 | 0 | 0 | 通过，2,000 个 key |
| DeleteStress | 539,492 | 0 | 899.15 ops/s | 2.93875ms | 11.89325ms | 29.039792ms | 58 | 3 | 536,558 | 通过，3,600 个 node-key 检查 |

最新全量运行保持了全部正确性门禁，但也暴露了性能和 ReadIndex 可用性信号，已在
[issue #113](https://github.com/xmh1011/go-kv/issues/113) 跟踪。多个场景出现
ReadIndex timeout warning，但失败操作仍然是 0：

```text
[ReadIndex] Node 3 timed out waiting for heartbeat quorum.
[ReadIndex] Node 3 timed out waiting for lastApplied to reach 234016 (current: 233759)
[ReadIndex] Node 2 timed out waiting for heartbeat quorum.
[ReadIndex] Node 1 timed out waiting for heartbeat quorum.
```

请求流最终仍然是 0 失败，并且最终一致性通过。这个日志是有价值的性能信号：
写入密集 compaction、snapshot 压力或 ReadIndex heartbeat 调度可能会短暂拖慢
线性一致读。和上一版报告相比，WriteHeavy 与 ReadHeavy 吞吐明显下降，因此 #113
应作为后续性能 bug 跟进，而不是正确性失败。

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

后续有价值的性能优化方向包括 batch 大小、follower 追赶、LSM compaction 调度，以及 Raft log adapter 的写放大降低。任何优化都不应削弱上面的正确性门禁。

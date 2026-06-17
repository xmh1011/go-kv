# 性能与长时间端到端测试报告

English version: [PERFORMANCE.md](PERFORMANCE.md)

本文记录 `go-kv` 最新一轮生产式验证结果。这些测试不只衡量吞吐量，还会覆盖 Raft leader 切换、节点重启、快照、日志压缩、LSM flush、LSM compaction、客户端重试和最终数据一致性。

## 最新验证

| 项目 | 值 |
|---|---|
| 日期 | 2026-06-17 |
| 机器 | macOS Darwin 26.3.1, Apple Silicon |
| Go | 1.25.5 |
| 传输层 | 长时间 E2E 使用 gRPC；聚焦集成测试额外覆盖 TCP |
| 存储 | Raft 日志和状态机均使用 LSM |
| 日志级别 | `GO_KV_LOG_LEVEL=warn` |

## 已通过的命令

| 目的 | 命令 | 结果 |
|---|---|---|
| TCP/LSM 重启回归 | `GO_KV_LOG_LEVEL=warn go test -v ./tests -run '^TestCluster_Persistence_Restart$/^tcp_lsm$' -count=10 -timeout=3m` | 40.689s 通过 |
| 全量 short 单元/集成门禁 | `GO_KV_LOG_LEVEL=warn go test -short ./... -count=1 -timeout=15m` | 772.798s 通过 |
| 单个 10 分钟触发场景 | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=25m ./tests/long_running_e2e_test.go -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' -count=1` | 608.749s 通过 |
| 全量长时间 E2E 回归 | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests/long_running_e2e_test.go -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' -count=1` | 3688.472s 通过 |

现在 short 模式行为是明确的：10 分钟 E2E 在 `testing.Short()` 下会跳过。这样 `go test -short ./...` 可以继续作为 PR 覆盖率入口，而真实 10 分钟场景必须显式运行。

## 最终 10 分钟 E2E 结果

所有场景都开启了 race detector。一次写入只有在 Raft leader 提交日志并应用到状态机之后才算成功。包含重启的场景会在客户端停止后等待最终 cluster barrier，然后再比较所有节点数据。

| 场景 | 总操作数 | 失败操作 | 吞吐量 | P50 | P95 | P99 | Leader 切换 | 快照节点数 | 最大快照 index | 一致性 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Comprehensive | 1,187,811 | 0 | 1,979.68 ops/s | 2.079625ms | 4.845958ms | 11.733209ms | 33 | 3 | 891,649 | 通过，1,996 个 key |
| WriteHeavy | 711,886 | 0 | 1,186.48 ops/s | 1.9855ms | 5.132084ms | 11.577292ms | 57 | 3 | 706,490 | 通过，2,000 个 key |
| MixedWithFailures | 998,188 | 0 | 1,663.65 ops/s | 1.276875ms | 3.291916ms | 7.843792ms | 39 | 3 | 698,422 | 通过，3,600 个 node-key 检查 |
| ConsistencyWithRestartsAndSnapshots | 1,304,041 | 0 | 2,173.40 ops/s | 1.468917ms | 3.905167ms | 8.690958ms | 72 | 3 | 908,358 | 通过，3,600 个 node-key 检查 |
| ReadHeavy | 61,771,824 | 0 | 102,953.04 ops/s | 15.542us | 327.333us | 681.125us | 0 | 0 | 0 | 通过，2,000 个 key |
| DeleteStress | 714,646 | 0 | 1,191.08 ops/s | 2.107375ms | 5.763834ms | 13.462959ms | 50 | 3 | 714,968 | 通过，3,600 个 node-key 检查 |

`MixedWithFailures` 中出现过一次新的 LSM compaction 恢复防线日志：

```text
[Compaction] Pruning stale SSTable metadata for missing file .../node-2/lsm_raftlog/sst/1-level/515.sst at level 1
```

这是修复后的预期路径。内存元数据中残留了已经不存在的文件，compaction 会剪掉这个过期目录项，而不是让整个存储引擎失败。该场景继续运行并通过了严格最终一致性检查。

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
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=25m ./tests/long_running_e2e_test.go \
  -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' \
  -count=1
```

代码或测试逻辑修复后，再跑全量长时间 E2E 回归：

```bash
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests/long_running_e2e_test.go \
  -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' \
  -count=1
```

PR 覆盖率和 Codecov 使用：

```bash
GO_KV_LOG_LEVEL=warn go test -short ./... -count=1 -timeout=15m
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

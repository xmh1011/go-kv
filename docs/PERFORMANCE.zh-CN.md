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
| 全量 short 单元/集成门禁 | `GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=25m` | 通过；`tests` 包 779.477s |
| 单个 10 分钟重启/快照触发场景 | `GO_KV_LOG_LEVEL=warn go test -race -v ./tests -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' -count=1 -timeout=25m` | 606.884s 通过 |
| 全量长时间 E2E 回归 | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' -count=1` | 3672.630s 通过 |

现在 short 模式行为是明确的：10 分钟 E2E 在 `testing.Short()` 下会跳过。这样 `go test -short ./...` 可以继续作为 PR 覆盖率入口，而真实 10 分钟场景必须显式运行。

## 最终 10 分钟 E2E 结果

所有场景都开启了 race detector。一次写入只有在 Raft leader 提交日志并应用到状态机之后才算成功。包含重启的场景会在客户端停止后等待最终 cluster barrier，然后再比较所有节点数据。

| 场景 | 总操作数 | 失败操作 | 吞吐量 | P50 | P95 | P99 | Leader 切换 | 快照节点数 | 最大快照 index | 一致性 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Comprehensive | 741,998 | 0 | 1,236.66 ops/s | 2.947709ms | 13.335041ms | 29.132709ms | 34 | 3 | 557,264 | 通过，1,994 个 key |
| WriteHeavy | 651,910 | 0 | 1,086.52 ops/s | 2.319708ms | 6.559708ms | 14.27275ms | 76 | 3 | 634,129 | 通过，2,000 个 key |
| MixedWithFailures | 667,024 | 0 | 1,111.71 ops/s | 1.45125ms | 6.477083ms | 19.660041ms | 34 | 3 | 467,029 | 通过，3,600 个 node-key 检查 |
| ConsistencyWithRestartsAndSnapshots | 649,820 | 0 | 1,083.03 ops/s | 2.218459ms | 12.986833ms | 35.574416ms | 44 | 3 | 445,972 | 通过，3,600 个 node-key 检查 |
| ReadHeavy | 29,051,853 | 0 | 48,419.75 ops/s | 24.667us | 472.458us | 1.695083ms | 0 | 0 | 0 | 通过，2,000 个 key |
| DeleteStress | 397,732 | 0 | 662.89 ops/s | 4.177875ms | 20.314792ms | 58.492375ms | 30 | 3 | 397,665 | 通过，3,600 个 node-key 检查 |

`Comprehensive` 在 snapshot 和 compaction 压力下出现过一次 ReadIndex 等待日志：

```text
[ReadIndex] Node 1 timed out waiting for lastApplied to reach 446053 (current: 445844)
```

请求流最终仍然是 0 失败，并且最终一致性通过。这个日志是有价值的性能信号：
写入密集 compaction 或 snapshot 压力会短暂拖慢 `lastApplied` 追赶，但最新运行没有
暴露数据丢失或节点分歧。

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

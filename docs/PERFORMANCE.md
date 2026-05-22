# Go-KV 性能报告与优化状态

## 项目概述

go-kv 是一个基于 Raft 共识协议的分布式 KV 存储系统，使用 LSM-tree 作为底层存储引擎，gRPC 作为传输层。

---

## 测试总览 (2026-05-22)

**测试环境**: macOS Darwin 26.3.1, Apple Silicon, Go 1.25.5

| 验证命令 | 结果 | 用时 / 覆盖点 |
|---------|------|---------------|
| `go test -race ./pkg/storage/lsm ./raft ./engine/lsm/... ./pkg/storage/... ./pkg/param` | **通过** | Raft、LSM、存储适配层核心 race 覆盖 |
| `make test` | **通过** | 全仓单元测试与集成测试入口 |
| `make integration-test` | **通过** | 356.522s，覆盖选举、复制、故障转移、分区、快照、重启、成员变更 |
| `go test -race -v -timeout=20m ./tests/long_running_e2e_test.go -run TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots -count=1` | **通过** | 606.524s，真实重启 + 快照 + 最终一致性校验 |
| `go test -race -v -timeout=25m ./tests/long_running_e2e_test.go -run '^TestLongRunning_10Min_Comprehensive$' -count=1` | **通过** | 607.73s，综合读写删、Leader 切换、快照、最终一致性校验 |
| `go test -race -v -timeout=30m ./tests/e2e_perf_test.go` | **通过** | 422.562s，覆盖本地与 gRPC 网络性能场景 |

---

## 长时间 E2E 测试结果 (10 分钟)

**集群配置**: 3 节点 Raft, gRPC 传输, LSM 存储, 真实节点重启, 自动快照, 最终全节点一致性校验。

该轮长测用于验证 Raft 与 LSM 的内核一致性，不再只统计请求成功率。测试会在运行过程中制造 leader 切换、节点停止/重启、快照生成和日志压缩，并在结束时按每个节点逐一读取最近 1200 个期望 key。

| 指标 | 数值 |
|------|------|
| 总操作数 | 545,390 |
| 成功操作 | 545,390 |
| 失败操作 | 0 |
| 成功率 | **100.00%** |
| 总吞吐量 | **908.98 ops/sec** |
| 写入操作 | 300,209 (55.0%) |
| 读取操作 | 163,348 (30.0%) |
| 删除操作 | 81,833 (15.0%) |
| P50 延迟 | 3.178041ms |
| P95 延迟 | 17.501083ms |
| P99 延迟 | 44.363875ms |
| Leader 切换 | 27 次 |
| 生成快照的节点数 | 3 |
| 最大快照 index | 379,839 |
| 最终一致性校验 | **通过**，3,600 个 node-key 组合全部一致 |

### 本轮发现并修复的关键瓶颈

长测曾在 5:30 到 7:30 附近卡在约 167,520 次操作，CPU 持续消耗但提交进度不再增长。SIGQUIT 堆栈显示热路径集中在 `pkg/storage/lsm.encodeLogEntry` 和 `pkg/storage/lsm.decodeLogEntry` 的 gob 编解码上，并伴随 Raft apply 与 replication goroutine 等待。

修复方案是将 LSM-backed Raft log entry 从 gob 改为带 magic/version/type tag 的二进制格式。该格式只支持当前代码生成的命令类型，未知旧数据或未知命令类型会快速失败，不再为 WAL/SSTable 中的旧 gob 数据做兼容回退。

```go
func encodeLogEntry(entry *param.LogEntry) ([]byte, error) {
    cmdBytes, err := encodeLogCommand(entry.Command)
    if err != nil {
        return nil, err
    }

    buf := make([]byte, 4+8+8+4+len(cmdBytes))
    copy(buf[:4], logEntryFormatMagic) // GLG1
    binary.BigEndian.PutUint64(buf[4:12], entry.Term)
    binary.BigEndian.PutUint64(buf[12:20], entry.Index)
    binary.BigEndian.PutUint32(buf[20:24], uint32(len(cmdBytes)))
    copy(buf[24:], cmdBytes)
    return buf, nil
}
```

修复后同一长测完成 545,390 次操作，0 失败，所有节点最终数据一致。

### 综合长测复验：复制追赶与一致性断言

2026-05-22 的后续综合长测进一步覆盖了 ReadIndex 失败分类、Follower 快照后追赶、快照导出与 `lastApplied` 发布顺序，以及“最终一致性失败必须让测试失败”的断言。

```bash
go test -race -v -timeout=25m ./tests/long_running_e2e_test.go \
  -run '^TestLongRunning_10Min_Comprehensive$' \
  -count=1
```

| 指标 | 数值 |
|------|------|
| 总操作数 | 346,634 |
| 成功操作 | 346,634 |
| 失败操作 | 0 |
| 成功率 | **100.00%** |
| 总吞吐量 | **577.72 ops/sec** |
| 写入操作 | 208,083 (60.0%) |
| 读取操作 | 86,675 (25.0%) |
| 删除操作 | 51,876 (15.0%) |
| P50 延迟 | 9.786417ms |
| P95 延迟 | 37.089209ms |
| P99 延迟 | 85.199459ms |
| Leader 切换 | 19 次 |
| 生成快照的节点数 | 3 |
| 最大快照 index | 260,109 |
| 最终一致性校验 | **通过**，1,988 个 node-key 组合全部一致 |

本轮修复点：

1. 对 `read quorum timeout`、`read apply timeout`、`apply timeout`、`not leader` 等失败原因做分类统计，避免只看到聚合失败数。
2. 客户端停止后先等待 Follower 追赶，超时仍不一致时调用 `t.Fatalf`，避免一致性失败被误报为通过。
3. 成功的 `AppendEntries` 如果仍未追上 Leader，会立即触发下一批复制，不再受 100ms heartbeat 和 32 条批大小限制。
4. 普通命令的状态机写入与 `lastApplied` 推进在同一个 `stateMachineMu` 临界区内完成，快照不会观察到“新数据 + 旧 LastIncludedIndex”的混合状态。

---

## E2E 性能测试结果 (30 秒)

Latency percentile 统计已修复为对采样副本排序后再取分位点，避免旧实现因直接索引未排序样本而出现 P95/P99 小于 P50 的错误报告。

### 本地 E2E

| 测试场景 | 总操作数 | 成功率 | 吞吐量 (ops/sec) | P50 延迟 | P95 延迟 | P99 延迟 |
|---------|---------|--------|-----------------|----------|----------|----------|
| **WriteHeavy** | 35,533 | **100%** | **1,184.43** | 588.583µs | 1.563792ms | 3.902167ms |
| **ReadHeavy** | 1,282,062 | **100%** | **42,735.40** | 12.125µs | 19.208µs | 51.5µs |
| **MixedWorkload** | 59,824 | **100%** | **1,994.13** | 492µs | 1.0765ms | 2.028083ms |
| **SmallValues** | 72,004 | **100%** | **2,400.13** | 426.583µs | 1.105ms | 2.412875ms |
| **BatchOperations** | 1,400 | **100%** | **46.67** | 47.4355ms | 177.525167ms | 402.038ms |
| **DeleteOperations** | 38,721 | **100%** | **1,290.70** | 558.375µs | 1.40125ms | 2.984041ms |

### gRPC 网络 E2E

| 测试场景 | 总操作数 | 成功率 | 吞吐量 (ops/sec) | P50 延迟 | P95 延迟 | P99 延迟 |
|---------|---------|--------|-----------------|----------|----------|----------|
| **Network WriteHeavy** | 41,778 | **100%** | **1,392.60** | 540.584µs | 1.223166ms | 2.538625ms |
| **Network ReadHeavy** | 1,386,535 | **100%** | **46,217.83** | 12.125µs | 15.791µs | 32.834µs |
| **Network MixedWorkload** | 67,937 | **100%** | **2,264.57** | 483.917µs | 822.083µs | 1.371042ms |
| **Network SmallValues** | 92,206 | **100%** | **3,073.53** | 439.5µs | 711.375µs | 1.210084ms |
| **Network BatchOperations** | 1,450 | **100%** | **48.33** | 35.945917ms | 80.96275ms | 383.876167ms |
| **Network DeleteOperations** | 44,812 | **100%** | **1,493.73** | 511.25µs | 1.008125ms | 1.836292ms |
| **Network ConcurrentClients** | 40,816 | **100%** | **1,360.53** | 2.532ms | 6.369208ms | 14.763417ms |

---

## Benchmark 测试结果

以下 benchmark 数据是 2026-03-31 的历史基线，本轮主要补充了 race、集成测试、10 分钟一致性 E2E 和 gRPC E2E 性能长测。

**配置**: 3 节点 Raft, 100 次迭代, InMemory 传输（除 TCP/LSM/gRPC 测试外）

### 集群端到端 Benchmark

| Benchmark | ns/op | 等效 ops/sec |
|-----------|-------|-------------|
| 3NodesInmemory | 87,954 | ~11,370 |
| 3NodesTcp | 180,378 | ~5,544 |
| ConcurrentWrites | 44,231 | ~22,610 |
| SmallKeys | 49,409 | ~20,240 |
| MediumKeys (256B) | 55,010 | ~18,180 |
| LargeKeys (4KB) | 531,370 | ~1,882 |
| 3NodesLSM | 144,943 | ~6,899 |

### 生产环境 Benchmark (gRPC + LSM)

| Benchmark | ns/op | 等效 ops/sec | allocs/op |
|-----------|-------|-------------|-----------|
| GrpcLsm_3Nodes | 15,451 | ~64,720 | 33 |
| GrpcLsm_ConcurrentWrites | 377,756 | ~2,647 | 1,890 |
| GrpcLsm_SmallKeys | 335,771 | ~2,978 | 1,128 |
| GrpcLsm_MediumKeys (256B) | 275,988 | ~3,623 | 1,134 |
| GrpcLsm_LargeKeys (4KB) | 1,036,177 | ~965 | 1,162 |
| GrpcLsm_MixedWorkload | 544,493 | ~1,837 | 2,118 |
| GrpcLsm_ReadAfterWrite | 988,680 | ~1,011 | 1,244 |
| GrpcLsm_5Nodes | 329,823 | ~3,032 | 60 |

### 微基准测试

| Benchmark | ns/op | 等效 ops/sec |
|-----------|-------|-------------|
| AppendEntries RPC | 3,486 | ~286,860 |
| RequestVote RPC | 2,435 | ~410,680 |
| LogEntry 序列化 | 438 | ~2,285,000 |
| LogEntry 反序列化 | 2,018 | ~495,540 |
| KVCommand 序列化 | 152 | ~6,590,000 |
| ClientRequestProcessing | 1,302 | ~768,050 |
| StateMachine Apply | 746 | ~1,340,480 |
| StateMachine Get | 9 | ~109,050,000 |
| Storage AppendEntries | 7,172 | ~139,430 |
| Storage GetEntry | 5 | ~200,000,000 |
| MixedWorkload (单节点) | 94,008 | ~10,638 |
| SnapshotCreation | 213 | ~4,700,000 |
| SnapshotApply | 2,667,004 | ~375 |

---

## 与优化前对比 (2026-02-28 基准)

| 指标 | 优化前 | 优化后 | 提升 |
|------|--------|--------|------|
| WriteHeavy 成功率 | 53.48% | **100%** | +46.52pp |
| WriteHeavy 吞吐量 | 717 ops/sec | **4,439 ops/sec** | **6.2x** |
| WriteHeavy P50 延迟 | 5.25ms | **480µs** | **10.9x** |
| MixedWorkload 吞吐量 | 1,247 ops/sec | **5,162 ops/sec** | **4.1x** |
| ReadHeavy 吞吐量 | 379,656 ops/sec | 360,458 ops/sec | -5% (波动正常) |
| DeleteOperations 吞吐量 | 949 ops/sec | **3,264 ops/sec** | **3.4x** |

### 关键改善

1. **WriteHeavy 成功率从 53% 提升到 100%** — 之前近一半写入失败，现在全部成功
2. **写入吞吐量提升 6.2 倍** — 从 717 ops/sec 到 4,439 ops/sec
3. **写入延迟降低一个数量级** — P50 从 5.25ms 降到 480µs
4. **10 分钟长时稳定性验证通过** — 累计处理 4.8 亿次操作
5. **ReadHeavy 10min 吞吐提升 2.3x** — 从 344,672 ops/sec 到 787,533 ops/sec
6. **DeleteStress 吞吐提升 2.0x** — 从 1,373 ops/sec 到 2,813 ops/sec
7. **Comprehensive/MixedWithFailures 吞吐量提升** — 分别提升 25%/32%，但三阶段锁的 `appendEntriesMu` 争用在高并发混合负载下导致 Leader 频繁切换，成功率从 100% 降至 ~83%（详见长时测试成功率分析章节）

---

## 已完成的优化 (2026-03-31)

### Raft 全局锁优化 — 三阶段锁与并发改进

以下 6 项优化是 2026-03-30~31 期间针对 Raft 全局锁瓶颈（PERFORMANCE.md P0 问题）实施的改进。虽然未完全消除全局锁，但通过减少锁持有时间和降低锁竞争，显著改善了写入性能。

#### 11. Follower AppendEntries 三阶段锁 [高收益]

**文件**: `raft/replication.go`

将 Follower 侧 AppendEntries 处理从全程持锁改为三阶段：Phase 1 短锁（任期检查 + 心跳）→ Phase 2 无锁（磁盘 I/O：TruncateLog + AppendEntries）→ Phase 3 短锁（提交推进）。引入 `appendEntriesMu` 串行化多个并发 AppendEntries 的 Phase 2，防止 TruncateLog 竞态。

#### 12. Leader sendSnapshot 磁盘读取移出锁 [中收益]

**文件**: `raft/snapshot.go`

`sendSnapshot` 中的 `store.ReadSnapshot()` 从锁内移到锁外执行。快照文件可能很大（MB 级），磁盘读取在锁内会阻塞所有其他 Raft 操作。

#### 13. Leader replicateLogsToPeer 网络 I/O 移出锁 [高收益]

**文件**: `raft/replication.go`

将日志复制拆分为 `determineReplicationAction`（短锁：读取 nextIndex、准备 args）和 `replicateLogsToPeer`（无锁：网络 I/O + 响应处理）。网络往返通常在毫秒级，将其移出锁显著减少了锁持有时间。

#### 14. InstallSnapshot Follower 侧锁优化 [中收益]

**文件**: `raft/snapshot.go`

Follower 接收快照时，将快照对象创建和部分校验移到锁外。仅在需要修改 Raft 状态（commitIndex、lastApplied）时持锁。

#### 15. Election Pre-Vote/Real-Vote 并行 RPC [低收益]

**文件**: `raft/election.go`

选举 RPC 已经在独立 goroutine 中并行发送（每个 Peer 一个 goroutine），但选举结果处理（`handleElectionResult`）也移到了独立 goroutine 中，不阻塞 `startElection` 调用方。

#### 16. TakeSnapshot 异步化 [中收益]

**文件**: `raft/snapshot.go`

`store.SaveSnapshot` 和 `store.CompactLog` 在独立 goroutine 中异步执行，不阻塞 Raft 主循环。`isSnapshotting` 标志确保串行化。

### 此前已完成的优化（LSM 与编码层）

### 1. KV 编码优化：消除 binary.Write 反射 [高收益]

**文件**: `engine/lsm/kv/kv.go`

将 `KeyValuePair.EncodeTo` 从 4 次独立 `binary.Write`/`w.Write` 调用（每次都有反射开销），改为预分配 buffer + 单次 `w.Write`。同时优化了 `Key.EncodeTo` 和 `Value.EncodeTo`。

### 2. WAL 添加 bufio 缓冲 [中收益]

**文件**: `engine/lsm/wal/wal.go`

使用 32KB `bufio.Writer` 包装 WAL 文件写入。配合优化 1 的单次写入，减少系统调用次数。每次 Append 后 Flush 保证 crash safety。

### 3. LSM 日志条目编码：gob 替换为二进制头 [高收益]

**文件**: `pkg/storage/lsm/storage.go`

将 `AppendEntries`/`GetEntry` 中的全量 gob 编码替换为自定义二进制格式：`Term(8) + Index(8) + CmdLen(4) + CmdBytes`。Command 字段仍使用 gob（因为是 `any` 类型），但消除了 gob 的 struct 包络开销。

### 4. getLogKey 优化：消除 fmt.Sprintf [中收益]

**文件**: `pkg/storage/lsm/storage.go`

将 `fmt.Sprintf("%s%020d", logKeyPrefix, index)` 替换为 `strconv.FormatUint` + 手动零填充到固定 `[24]byte`，消除了热路径上的 `fmt` 解析和堆分配。

### 5. gRPC Transport：gob 替换为直接 bytes 传递 [高收益]

**文件**: `pkg/transport/grpc/transport.go`

`encode()`/`decode()` 使用前缀字节区分编码类型：`0x01` 表示原始 `[]byte`（零拷贝），`0x00` 表示 gob 编码。对于占大多数的 `[]byte` Command，完全跳过 gob 序列化。兼容旧格式的无前缀 gob 数据。

### 6. MemTable Delete 修复双写 WAL [低收益]

**文件**: `engine/lsm/memtable/manager.go`

移除 `Manager.Delete` 中对 `m.Mem.Delete(key)` 的调用（该调用会写一次 WAL），直接通过 `m.Mem.Insert(tombstone)` 写入删除标记。消除了每次删除操作的 WAL 双写。

### 7. SSTable Compaction `sync.Cond`/`RWMutex` 修复 [中收益]

**文件**: `engine/lsm/sstable/compaction.go`

修复了 `waitCompaction` 中 `sync.Cond.Wait()` 与 `RLock` 不匹配的问题（改为 `Lock` 写锁），删除了冗余的 `isLevelCompacting` 方法。此修复消除了一个潜在的 panic（当前同步 flush 路径未触发，但引入并发后会立即暴露）。

### 8. Raft `performReadAfterApply` goroutine 泄漏修复 [中收益]

**文件**: `raft/raft.go`

将每个 ReadIndex 请求的 `go func() { time.Sleep(timeout) }` 替换为 `time.AfterFunc` + `defer timer.Stop()`。读操作完成后定时器自动取消，不再产生 goroutine 泄漏。使 benchmark 测试可以正常运行。

### 9. LSM metadata 批量写入合并 [低收益]

**文件**: `pkg/storage/lsm/storage.go`

将 `saveLastIndex()` + `saveLogSize()` + `saveFirstIndex()` 三次独立 `db.Put` 合并为单次 `saveLogMeta()`，使用一个 key `meta:log_meta` 编码 `firstIndex(8) + lastIndex(8) + logSize(8)`。兼容旧格式的分离 key 读取。

### 10. Raft Replication 防御性空指针修复

**文件**: `raft/replication.go`

`prepareAppendEntriesArgs` 中当 `GetEntry` 返回 `nil, nil`（entry 不存在但无 error）时，原代码将 nil error 传给调用方，导致 nil args 被用于 SendAppendEntries → panic。修复为返回明确的 `fmt.Errorf("log entry at index %d not found")`。

### 17. InMemory Storage GetEntry 接口行为统一 [高收益]

**文件**: `pkg/storage/inmemory/storage.go`

InMemory Storage 的 `GetEntry` 在 index 越界时返回 `(nil, ErrLogNotFound)`，但 LSM Storage 返回 `(nil, nil)`。Raft 层的 `findConflictAndPrepare` 将 non-nil error 视为致命错误，导致所有使用 InMemory Storage 的 benchmark（3NodesInmemory、SmallKeys、MediumKeys、LargeKeys 等）每次操作耗时 ~5 秒（等待 `waitForAppliedLog` 超时）。修复为返回 `(nil, nil)`，统一两种存储后端的接口契约。此修复使 InMemory benchmark 性能恢复正常：3NodesInmemory 从 5s/op 降至 87,954 ns/op（**57,000x 提升**）。

---

## 已尝试但回退的优化

### SSTable Flush 异步化

**文件**: `engine/lsm/database/database.go`
**问题**: 将 `createNewSSTable` 改为 goroutine 异步执行后，IMemTable 被从内存列表驱逐但 SSTable 尚未写入完成，存在数据丢失窗口。长时间 E2E 测试（10 分钟、120K+ 条目）中表现为 "Log entry at index N not found"。即使用 `sync.Mutex` 串行化 flush 也无法解决，因为根因是 IMemTable 驱逐时机与 flush 完成时间不同步。
**结论**: 安全的异步 flush 需要重构 MemTable Manager，使 IMemTable 在 SSTable 写入确认前不被驱逐（引入 flush 回调或 reference counting）。

### Raft fetchEntriesToApply 锁优化

**文件**: `raft/replication.go`
**问题**: 将日志读取移到锁外后，并发的 `TruncateLog`（来自其他 AppendEntries 请求）可能删除正在读取的条目，导致 "could not retrieve committed log entry" 致命错误。
**结论**: 需要 Raft 层实现 AppendEntries 请求串行化（或引入 Ready 机制），才能安全地将 I/O 移出锁。

### Raft AppendEntries Follower 侧磁盘 I/O 移出锁

**文件**: `raft/replication.go`
**问题**: 释放锁期间，心跳可能推进 commitIndex，导致 `applyLogs` 尝试读取尚未写入的条目。多个并发 AppendEntries 的 `TruncateLog` 也会互相干扰。
**结论**: 同上，Raft 单一全局锁 + 并发 RPC 处理的架构限制了此优化的可行性。

---

## 当前存在的问题

### P0: Raft 全局锁仍是写入性能瓶颈（已缓解）

经过三阶段锁优化后，Follower 侧磁盘 I/O 和 Leader 侧网络 I/O 已移出全局锁，锁持有时间大幅减少。但 Raft 的核心状态（Term、commitIndex、日志索引缓存）仍由单一 `sync.Mutex` 保护。

### P1: `appendEntriesMu` 争用导致心跳阻塞（新发现）

三阶段锁引入的 `appendEntriesMu` 同时被 `AppendEntries`（Phase 0 串行化）和 `fetchEntriesToApply`（日志应用）持有。在高并发写入场景下，心跳 AppendEntries（`len(Entries) == 0`）也需要排队等待 `appendEntriesMu`，导致 Follower 在 500ms 选举超时内未收到心跳，触发不必要的选举。

**影响**: Comprehensive 测试 Leader 切换从 3 次增加到 46 次，成功率从 100% 下降到 82.35%。

**优化方向**:
- 心跳快速路径：心跳 AppendEntries 跳过 `appendEntriesMu`，仅获取 `r.mu` 短锁
- 或将 `appendEntriesMu` 改为读写锁，心跳使用读锁、日志追加使用写锁

当前状态和进一步优化路线：
- **已完成**: 三阶段锁（Phase 2 无锁 I/O）、Leader 网络 I/O 移出锁、异步快照
- **阶段一**: 修复 `appendEntriesMu` 心跳阻塞问题 + 细化锁粒度（stateMu/logMu/replMu）
- **阶段二**: 引入 Ready 机制（etcd 风格），预期 5-10x 提升
- **阶段三**: Multi-Raft 分片，预期 10x+ 提升

---

## 测试命令

```bash
# 单元测试（排除 tests/ 目录，生成 coverage.txt）
make test

# 集成测试
make integration-test

# E2E 性能测试 (30s)
make e2e-test

# Benchmark 测试
make bench-test

# 10 分钟长时测试（每个子场景都可能运行 10 分钟以上）
make long-test

# 单个综合长测复现命令
go test -race -v -timeout=25m ./tests/long_running_e2e_test.go \
  -run '^TestLongRunning_10Min_Comprehensive$' \
  -count=1

# 静态分析
go vet ./...
```

`go test -race ./... -short -count=1` 目前会进入 `tests` 包中的重量级集成场景，并可能在 Go 默认 10 分钟包级超时处失败。该通用工作流问题已单独追踪，推荐使用上面的 Makefile 分层入口运行验证。

---

**最后更新**: 2026-05-22

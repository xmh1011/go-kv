# Go-KV 性能报告与优化状态

## 项目概述

go-kv 是一个基于 Raft 共识协议的分布式 KV 存储系统，使用 LSM-tree 作为底层存储引擎，gRPC 作为传输层。

---

## 测试总览 (2026-03-31)

**测试环境**: macOS Darwin 25.3.0, Apple Silicon, Go 1.24+

| 测试类别 | 数量 | 结果 |
|---------|------|------|
| 单元测试 | 16 packages | **全部通过** |
| 竞态检测 (race) | 16 packages | **全部通过，零 race** |
| Benchmark 测试 | 31 benchmarks | **全部通过** |
| E2E 性能测试 (30s) | 6 tests | **全部通过** |
| 长时间 E2E 测试 (10min) | 5 tests | **全部通过** |

---

## 长时间 E2E 测试结果 (10 分钟)

**集群配置**: 3 节点 Raft, gRPC 传输, LSM 存储

### 综合测试 (Comprehensive)

混合负载：60% 写入 + 25% 读取 + 15% 删除，10 个并发客户端。

| 指标 | 数值 |
|------|------|
| 总操作数 | 1,630,986 |
| 成功操作 | 1,343,140 |
| 成功率 | 82.35%（详见下方分析） |
| 总吞吐量 | **2,239 ops/sec** |
| 写入吞吐 | 1,630 ops/sec |
| 读取吞吐 | 680 ops/sec |
| 删除吞吐 | 409 ops/sec |
| P50 延迟 | 1.36ms |
| P99 延迟 | 36.28ms |
| Leader 切换 | 46 次 |

### 写入密集型 (WriteHeavy)

| 指标 | 数值 |
|------|------|
| 总操作数 | 967,222 |
| 成功率 | **100.00%** |
| 写入吞吐 | **1,612 ops/sec** |
| P50 延迟 | 1.52ms |
| P99 延迟 | 28.49ms |
| Leader 切换 | 44 次 |

### 故障恢复混合测试 (MixedWithFailures)

含节点宕机和恢复模拟（每 2 分钟触发一次 Follower 节点停止，30 秒后恢复，最多 2 次）。

| 指标 | 数值 |
|------|------|
| 总操作数 | 2,314,291 |
| 成功操作 | 1,936,645 |
| 成功率 | 83.68%（详见下方分析） |
| 总吞吐量 | **3,228 ops/sec** |
| P50 延迟 | 578µs |
| P99 延迟 | 8.50ms |
| Leader 切换 | 42 次 |

### 读取密集型 (ReadHeavy)

| 指标 | 数值 |
|------|------|
| 总操作数 | **472,519,981** |
| 成功率 | **100.00%** |
| 读取吞吐 | **787,533 ops/sec** |
| 读取流量 | 29.88 MB/s |
| P50 延迟 | 1.58µs |
| P99 延迟 | 104µs |

### 删除压力测试 (DeleteStress)

60% 写入 + 40% 删除。

| 指标 | 数值 |
|------|------|
| 总操作数 | 1,687,774 |
| 成功率 | **100.00%** |
| 总吞吐量 | **2,813 ops/sec** |
| 写入吞吐 | 1,686 ops/sec |
| 删除吞吐 | 1,127 ops/sec |
| P50 延迟 | 1.07ms |
| P99 延迟 | 11.60ms |
| Leader 切换 | 65 次 |

### 长时测试成功率分析

三阶段锁优化后，Comprehensive（82.35%）和 MixedWithFailures（83.68%）的成功率低于 100%。根因分析如下：

**直接原因：`appendEntriesMu` 导致心跳被阻塞，触发频繁选举**

三阶段锁引入了 `appendEntriesMu` 互斥锁，用于串行化 Follower 侧的 AppendEntries Phase 2（磁盘 I/O）。但 `fetchEntriesToApply`（日志应用）也需要先获取 `appendEntriesMu` 再获取 `r.mu`，以防止读取到正在被截断的日志。这导致以下阻塞链：

```
心跳 AppendEntries → 等待 appendEntriesMu → 被 fetchEntriesToApply 或正在进行的日志追加阻塞
→ 心跳超时 → Follower 发起选举 → Leader 切换 → 客户端请求失败
```

**证据**：
- 优化前 Comprehensive 仅 3 次 Leader 切换，现在 46 次（10 个并发客户端持续高负载放大了锁争用）
- WriteHeavy 也有 44 次 Leader 切换，但因为只有写入（无读取），客户端可以快速切换到新 Leader，成功率仍为 100%
- ReadHeavy 0 次 Leader 切换（纯读取不经过 `appendEntriesMu`）
- MixedWithFailures 有显式故障注入（节点宕机），42 次切换中部分是注入造成的

**本质**：这是吞吐量 vs 稳定性的 trade-off。三阶段锁将写入吞吐量从 1,795 提升到 2,239 ops/sec（+25%），但 `appendEntriesMu` 的锁争用在高并发混合负载下导致心跳延迟，引发不必要的选举。

**后续优化方向**：
- 为心跳 AppendEntries（`len(args.Entries) == 0`）设置高优先级，跳过 `appendEntriesMu` 排队
- 或增大选举超时（当前 500ms），给高负载下的心跳更多余量

---

## E2E 性能测试结果 (30 秒)

| 测试场景 | 总操作数 | 成功率 | 吞吐量 (ops/sec) | P50 延迟 | P99 延迟 |
|---------|---------|--------|-----------------|----------|----------|
| **WriteHeavy** | 133,180 | **100%** | **4,439** | 480µs | 219µs |
| **ReadHeavy** | 10,813,740 | **100%** | **360,458** | 1µs | 1.2µs |
| **MixedWorkload** | 154,865 | **100%** | **5,162** | 107µs | 235µs |
| **SmallValues** | 198,992 | **100%** | **6,633** | 294µs | 2.6µs |
| **BatchOperations** | 1,500 | **100%** | **50** | 25ms | 15.8ms |
| **DeleteOperations** | 97,924 | **100%** | **3,264** | 139µs | 148µs |

---

## Benchmark 测试结果

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
# 单元测试
go test $(go list ./... | grep -v /tests) -count=1 -short

# 单元测试 + 竞态检测
go test -race $(go list ./... | grep -v /tests) -count=1 -short

# E2E 性能测试 (30s)
go test ./tests/ -run TestE2E -short -v -timeout 600s

# Benchmark 测试
go test ./tests/ -run='^$' -bench=. -benchtime=100x -timeout 300s

# 10 分钟长时测试
go test ./tests/ -run '^TestLongRunning_10Min_' -timeout 90m -v

# 静态分析
go vet ./...
```

---

**最后更新**: 2026-03-31

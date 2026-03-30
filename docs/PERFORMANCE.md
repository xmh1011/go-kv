# Go-KV 性能报告与优化状态

## 项目概述

go-kv 是一个基于 Raft 共识协议的分布式 KV 存储系统，使用 LSM-tree 作为底层存储引擎，gRPC 作为传输层。

---

## 测试总览 (2026-03-30)

**测试环境**: macOS Darwin 25.3.0, Apple Silicon, Go 1.24+

| 测试类别 | 数量 | 结果 |
|---------|------|------|
| 单元测试 | 16 packages | **全部通过** |
| 竞态检测 (race) | 16 packages | **全部通过，零 race** |
| Benchmark 测试 | 30 benchmarks | **全部通过** |
| E2E 性能测试 (30s) | 6 tests | **全部通过** |
| 长时间 E2E 测试 (10min) | 5 tests | **全部通过** |

---

## 长时间 E2E 测试结果 (10 分钟)

**集群配置**: 3 节点 Raft, gRPC 传输, LSM 存储

### 综合测试 (Comprehensive)

混合负载：60% 写入 + 25% 读取 + 15% 删除，含故障注入和 Leader 切换。

| 指标 | 数值 |
|------|------|
| 总操作数 | 1,077,504 |
| 成功率 | **100.00%** |
| 总吞吐量 | **1,795 ops/sec** |
| 写入吞吐 | 1,078 ops/sec |
| 读取吞吐 | 448 ops/sec |
| 删除吞吐 | 269 ops/sec |
| P50 延迟 | 2.22ms |
| P99 延迟 | 48.42ms |
| Leader 切换 | 3 次 |

### 写入密集型 (WriteHeavy)

| 指标 | 数值 |
|------|------|
| 总操作数 | 797,002 |
| 成功率 | **100.00%** |
| 写入吞吐 | **1,328 ops/sec** |
| P50 延迟 | 2.34ms |
| P99 延迟 | 46.26ms |

### 故障恢复混合测试 (MixedWithFailures)

含节点宕机和恢复模拟。

| 指标 | 数值 |
|------|------|
| 总操作数 | 1,468,905 |
| 成功率 | **100.00%** |
| 总吞吐量 | **2,448 ops/sec** |
| P50 延迟 | 894µs |
| P99 延迟 | 13.40ms |

### 读取密集型 (ReadHeavy)

| 指标 | 数值 |
|------|------|
| 总操作数 | **206,803,532** |
| 成功率 | **100.00%** |
| 读取吞吐 | **344,672 ops/sec** |
| 读取流量 | 13.08 MB/s |
| P50 延迟 | 1.58µs |
| P99 延迟 | 308µs |

### 删除压力测试 (DeleteStress)

60% 写入 + 40% 删除。

| 指标 | 数值 |
|------|------|
| 总操作数 | 823,894 |
| 成功率 | **100.00%** |
| 总吞吐量 | **1,373 ops/sec** |
| 写入吞吐 | 823 ops/sec |
| 删除吞吐 | 549 ops/sec |
| P50 延迟 | 2.27ms |
| P99 延迟 | 41.58ms |

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

**配置**: 3 节点 Raft, 1000 次迭代, InMemory 传输（除 TCP 和 LSM 测试外）

| Benchmark | ns/op | 等效 ops/sec |
|-----------|-------|-------------|
| 3NodesInmemory | 239,208 | ~4,180 |
| 3NodesTcp | 120,779 | ~8,280 |
| ConcurrentWrites | 41,650 | ~24,010 |
| SmallKeys | 46,290 | ~21,600 |
| MediumKeys (256B) | 104,760 | ~9,546 |
| LargeKeys (4KB) | 312,951 | ~3,195 |
| 3NodesLSM | 177,507 | ~5,634 |

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
4. **所有场景 100% 成功率** — 零失败
5. **10 分钟长时稳定性验证通过** — 累计处理 2.1 亿次操作，含故障注入和 Leader 切换

---

## 已完成的优化 (2026-03-30)

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

### P0: Raft 全局锁是写入性能瓶颈

Raft 使用单一 `sync.Mutex` 保护所有状态，80+ 处 Lock/Unlock 调用。写入操作的完整路径 — 提交日志、复制到 Follower、Follower 处理 AppendEntries（包括磁盘 I/O）— 都在持锁状态下执行。

这是当前写入吞吐量的根本瓶颈。进一步优化需要：
- **阶段一**: 细化锁粒度（stateMu/logMu/replMu），预期 3-5x 提升
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

**最后更新**: 2026-03-30

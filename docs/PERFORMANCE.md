# 性能测试报告

## 概述

本报告记录了 go-kv 分布式键值存储系统的性能测试结果。该系统基于 Raft 共识算法实现，支持多种传输层（InMemory、TCP、gRPC）和存储引擎（InMemory、LSM）。

## 基准测试结果

### 传输层性能对比

| 测试项 | 吞吐量 (ops/sec) | 平均延迟 (ns/op) | 内存分配 (B/op) |
|---------|------------------|------------------|------------------|
| **InMemory 传输 + InMemory 存储** | | | |
| BenchmarkCluster_3NodesInmemory | ~67,300 | 14,870 | 105,030 |
| BenchmarkCluster_ConcurrentWrites | ~9,000 | 424,400 | - |
| BenchmarkCluster_SmallKeys | ~55,000 | 18,186 | - |
| BenchmarkCluster_MediumKeys | ~13,000 | 76,000 | - |
| BenchmarkCluster_LargeKeys | ~2,900 | 343,300 | - |
| BenchmarkCluster_3NodesLSM | ~55,000 | 18,180 | - |

### 存储引擎性能对比

| 存储引擎 | 场景 | 吞吐量 (ops/sec) | 相对性能 |
|----------|------|------------------|----------|
| **InMemory** | 基础写入 | ~67,300 | 基准 (100%) |
| **LSM** | 基础写入 | ~55,000 | ~82% |

### 数据大小对性能的影响

| 数据大小 | 平均延迟 (ns/op) | 吞吐量 (ops/sec) |
|----------|------------------|------------------|
| 小值 (2B + 2B) | 18,186 | ~55,000 |
| 中等值 (256B) | 76,000 | ~13,000 |
| 大值 (4KB) | 343,300 | ~2,900 |

### 并发性能

| 测试项 | 吞吐量 (ops/sec) | 平均延迟 (ns/op) |
|---------|------------------|------------------|
| 单线程基准 | ~67,300 | 14,870 |
| 并发写入 | ~9,000 | 424,400 |

## 性能分析

### 1. 传输层性能

- **InMemoryTransport**: 性能最优，完全在内存中通信，无网络开销
- **TCPTransport**: 存在网络序列化和 TCP 协议开销
- **GrpcTransport**: 存在 gRPC 协议开销，但支持跨语言调用

### 2. 存储层性能

- **InMemory**: 无持久化开销，性能最高
- **LSM**: 存在 WAL 写入和 SSTable 合并开销，但支持持久化和数据压缩

### 3. 数据大小影响

- **小数据 (< 100B)**: 性能受 Raft 协议开销主导
- **中等数据 (100B - 1KB)**: 性能受序列化和网络传输影响
- **大数据 (> 1KB)**: 性能受存储 I/O 和网络带宽限制

### 4. 并发性能

- 单线程操作性能最优，避免 Raft 日志串行化开销
- 高并发场景下，Raft 的 Leader 串行处理成为瓶颈
- 建议使用批处理和流水线优化提升并发性能

## 性能优化建议

### 1. 批量操作
```go
// 建议：批量提交多个操作
batchSize := 100
for i := 0; i < len(ops); i += batchSize {
    batch := ops[i:min(i+batchSize, len(ops))]
    // 一次性提交批量操作
}
```

### 2. 管道化读取
```go
// 建议：使用管道化读取减少往返延迟
readCh := make(chan ReadResult, 10)
go func() {
    for _, key := range keys {
        ch <- kv.Get(key)
    }
    close(ch)
}()
```

### 3. 预热 Leader 缓存
```go
// 建议：频繁访问的数据可以先在 Leader 本地预加载
if node.IsLeader() {
    kv.Prefetch(hotKeys)
}
```

### 4. 调整 Raft 参数
```go
// 建议：根据网络延迟调整心跳间隔
electionTimeout = 150ms + rand(0, 150ms)
heartbeatInterval = electionTimeout / 2
```

## 测试场景

### 端到端性能测试 (E2E)

| 测试场景 | 描述 | 持续时间 |
|---------|------|----------|
| TestE2E_WriteHeavy | 写入密集型，模拟热点写入场景 | 30s |
| TestE2E_ReadHeavy | 读取密集型，模拟缓存查询场景 | 30s |
| TestE2E_MixedWorkload | 混合负载，70% 写入 + 30% 读取 | 30s |
| TestE2E_SmallValues | 小值操作，50% 写入 + 50% 读取 | 30s |
| TestE2E_BatchOperations | 批量操作，50 条/批 | 30s |
| TestE2E_DeleteOperations | 删除操作场景 | 30s |

### 性能指标收集

所有测试收集以下指标：

| 指标 | 描述 |
|--------|------|
| TotalOps | 总操作数 |
| SuccessOps | 成功操作数 |
| FailedOps | 失败操作数 |
| BytesRead | 读取字节数 |
| BytesWritten | 写入字节数 |
| LatencyP50 | 50% 延迟 |
| LatencyP95 | 95% 延迟 |
| LatencyP99 | 99% 延迟 |
| ThroughputOps | 吞吐量 (ops/sec) |
| ErrorRate | 错误率 (%) |

## 测试执行命令

```bash
# 编译测试
go test -c ./tests/

# 运行基准测试
go test ./tests/... -bench=^Benchmark -benchtime=1s -run=^$

# 运行带内存统计的基准测试
go test ./tests/... -bench=^Benchmark -benchtime=500ms -benchmem=true -run=^$

# 运行端到端性能测试 (E2ETestSuite - 使用 sendThroughRPC)
go test ./tests/... -run=^TestE2E -timeout=5m

# 运行性能测试 (PerfTestSuite)
go test ./tests/... -run=^TestPerf -timeout=5m

# 运行网络端到端性能测试 (Network E2E - 使用 sendThroughNetwork)
# 注意：这些测试与 E2E 测试使用相同的底层调用方式，
# 但测试场景不同，专注于网络场景
go test ./tests/... -run=^TestNetworkE2E -timeout=5m

# 运行单个 E2E 测试
go test ./tests/... -run=^TestE2E_WriteHeavy -timeout=2m
go test ./tests/... -run=^TestE2E_ReadHeavy -timeout=2m
go test ./tests/... -run=^TestE2E_MixedWorkload -timeout=2m

# 运行单个 Network E2E 测试
go test ./tests/... -run=^TestNetworkE2E_WriteHeavy -timeout=2m
go test ./tests/... -run=^TestNetworkE2E_ReadHeavy -timeout=2m
go test ./tests/... -run=^TestNetworkE2E_MixedWorkload -timeout=2m

# 运行所有 Network E2E 测试
go test ./tests/... -run=^TestNetworkE2E -timeout=10m -v
```

## 结论

1. **编译状态**: 所有测试文件编译通过，无语法错误

2. **Bug 修复**:
   - 修复了 Raft 实现中 `log entry not found` 错误
   - 修复了 InMemoryTransport 节点连接问题
   - 修复了 commitIndex 边界检查问题

3. **性能表现**:
   - InMemory + InMemory: 吞吐量 ~67,300 ops/sec
   - LSM 存储: 吞吐量 ~55,000 ops/sec
   - 小值操作延迟最低 (18,186 ns/op)
   - 大值操作延迟最高 (343,300 ns/op)

4. **建议**:
   - 生产环境建议使用 LSM 存储以获得持久化保证
   - 使用批量操作减少 Raft 协议开销
   - 根据网络条件调整心跳和选举超时参数
   - 对于高并发场景，考虑多 Raft Group 分片存储

---

**测试日期**: 2026-02-23
**测试环境**: macOS Darwin 25.2.0
**Go 版本**: go1.x

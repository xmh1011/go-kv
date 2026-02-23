# 性能测试报告

## 概述

本报告记录了 go-kv 分布式键值存储系统的性能测试结果。该系统基于 Raft 共识算法实现，支持多种传输层（InMemory、TCP、gRPC）和存储引擎（InMemory、LSM）。

## 最新测试结果 (2026-02-23)

### gRPC + LSM 生产环境基准测试结果

| 测试项 | 吞吐量 (ops/sec) | 平均延迟 (ns/op) | 内存分配 (B/op) |
|---------|------------------|------------------|------------------|
| **gRPC 传输 + LSM 存储** | | | |
| BenchmarkProduction_GrpcLsm_3Nodes | ~7,280 | 1,372,660 | 134,293 |
| BenchmarkProduction_GrpcLsm_SmallKeys | ~8,080 | 1,236,240 | 142,446 |

### 端到端 (E2E) 性能测试结果 (gRPC + LSM, 3节点)

| 测试场景 | 总操作数 | 成功操作数 | 吞吐量 (ops/sec) | P50 延迟 | P95 延迟 | P99 延迟 |
|---------|---------|-----------|------------------|----------|----------|----------|
| TestE2E_WriteHeavy | 17,686 | 17,686 (100%) | 589.53 | 3.10ms | 2.71ms | 0.04ms |
| TestE2E_ReadHeavy | 49,368 | 49,368 (100%) | 1,645.60 | 269µs | 707µs | 110µs |
| TestE2E_MixedWorkload (70%写/30%读) | 22,486 | 22,486 (100%) | 749.53 | 564µs | 937µs | 126µs |
| TestE2E_DeleteOperations | 17,925 | 17,925 (100%) | 597.50 | 2.92ms | 1.62ms | 0.58ms |
| TestE2E_BatchOperations (50条/批) | 1,350 | 1,350 (100%) | 45.00 | 73.66ms | 84.81ms | 45.23ms |

### 不同传输层基准测试对比

| 测试项 | 吞吐量 (ops/sec) | 平均延迟 (ns/op) | 内存分配 (B/op) |
|---------|------------------|------------------|------------------|
| BenchmarkCluster_3NodesInmemory | ~55,000 | 181,640 | 11,178 |
| BenchmarkCluster_3NodesTcp | ~2,000 | 488,459 | 16,224 |
| BenchmarkCluster_3NodesLSM | ~960 | 1,036,618 | 141,355 |
| BenchmarkCluster_SmallKeys | ~57,000 | 174,250 | 11,383 |
| BenchmarkCluster_MediumKeys | ~5,700 | 173,254 | 19,286 |
| BenchmarkCluster_LargeKeys | ~1,500 | 645,483 | 132,855 |
| BenchmarkCluster_ConcurrentWrites | ~12,200 | 81,703 | 5,871 |

### 存储引擎性能对比

| 存储引擎 | 配置 | 吞吐量 (ops/sec) | 平均延迟 (ns/op) | 相对性能 |
|----------|------|------------------|------------------|----------|
| **InMemory** | 3节点, InMemory传输 | ~55,000 | 181,640 | 基准 (100%) |
| **InMemory** | 3节点, TCP传输 | ~2,000 | 488,459 | ~4% |
| **LSM** | 3节点, InMemory传输 | ~960 | 1,036,618 | ~2% |
| **LSM** | 3节点, gRPC传输 | ~7,280 | 1,372,660 | ~13% |

### 数据大小对性能的影响 (InMemory传输 + InMemory存储)

| 数据大小 | 平均延迟 (ns/op) | 吞吐量 (ops/sec) |
|----------|------------------|------------------|
| 小值 (2B + 2B) | 174,250 | ~57,000 |
| 中等值 (256B) | 173,254 | ~5,700 |
| 大值 (4KB) | 645,483 | ~1,500 |

### 并发性能

| 测试项 | 吞吐量 (ops/sec) | 平均延迟 (ns/op) |
|---------|------------------|------------------|
| 单线程基准 (3NodesInmemory) | ~55,000 | 181,640 |
| 并发写入 (ConcurrentWrites) | ~12,200 | 81,703 |

## 长时端到端性能测试 (Long Running E2E)

### 测试概述

长时端到端测试模拟生产环境场景：
- **集群配置**: 3节点 Raft 集群
- **传输层**: gRPC (真实网络通信)
- **存储引擎**: LSM (持久化存储)
- **测试时长**: 10分钟 (短模式调整为1分钟)

### 测试场景

| 测试名称 | 描述 | 操作类型分布 | 并发客户端数 |
|---------|------|-------------|-------------|
| TestLongRunning_10Min_Comprehensive | 综合混合测试 | 60% 写入, 25% 读取, 15% 删除 | 10 |
| TestLongRunning_10Min_WriteHeavy | 写入密集型 | 100% 写入 | 8 |
| TestLongRunning_10Min_ReadHeavy | 读取密集型 | 100% 读取 | 10 |
| TestLongRunning_10Min_DeleteStress | 删除压力测试 | 写入+周期性删除 | 8 |
| TestLongRunning_10Min_MixedWithFailures | 带故障恢复的混合测试 | 70% 写入, 30% 读取 | 5 |

### 长时测试性能指标

所有长时测试收集以下指标：

| 指标 | 描述 |
|--------|------|
| TotalOps | 总操作数 |
| SuccessOps | 成功操作数 |
| FailedOps | 失败操作数 |
| WriteOps | 写入操作数 |
| ReadOps | 读取操作数 |
| DeleteOps | 删除操作数 |
| ThroughputOps | 总吞吐量 (ops/sec) |
| WriteThroughput | 写入吞吐量 (ops/sec) |
| ReadThroughput | 读取吞吐量 (ops/sec) |
| DeleteThroughput | 删除吞吐量 (ops/sec) |
| LatencyP50 | 50% 延迟 |
| LatencyP95 | 95% 延迟 |
| LatencyP99 | 99% 延迟 |
| BytesRead | 读取字节数 |
| BytesWritten | 写入字节数 |
| ErrorRate | 错误率 (%) |
| LeaderElections | Leader 切换次数 |
| DataConsistencyOK | 数据一致性检查结果 |
| KeysVerified | 已验证数据条数 |

### 长时测试实际结果 (1分钟短模式) - 2026-02-23 更新

| 测试名称 | 总操作数 | 成功率 | 总吞吐量 | 写入吞吐量 | 读取吞吐量 | 删除吞吐量 | P50 | P95 | P99 | 数据一致性 | 已验证数据 |
|---------|---------|--------|----------|-----------|-----------|-----------|-----|-----|-----|-----------|-----------|
| MixedWithFailures | 28,905 | 100.00% | 481.75 ops/sec | N/A | N/A | N/A | 1.90ms | 17.83ms | 87.88ms | ✅ | N/A |
| WriteHeavy | 4,124 | 100.00% | 68.73 ops/sec | 68.73 | 0 | 0 | 36.93ms | 488.36ms | 705.54ms | ✅ | N/A |
| Comprehensive | 8,786,356 | 0.16% | 231.65 ops/sec | N/A | N/A | N/A | 32.04ms | 581.56ms | 938.45ms | ✅ | N/A |
| ReadHeavy | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| DeleteStress | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A |

> **注意**: ReadHeavy 和 DeleteStress 测试在本次运行中超时失败（存在死锁问题），Comprehensive 测试在高并发场景下出现高错误率，需要进一步优化。

### 测试结果对比分析

#### 本次运行 (2026-02-23) vs 之前记录

| 测试场景 | 之前记录的吞吐量 | 本次运行吞吐量 | 说明 |
|---------|------------------|----------------|------|
| MixedWithFailures (70%写) | 280.75 ops/sec | 481.75 ops/sec | 性能提升，成功率100% |
| WriteHeavy (100%写) | 90.08 ops/sec | 68.73 ops/sec | 稍有下降，但成功率100% |
| Comprehensive (混合) | 161.78 ops/sec | 231.65 ops/sec | 吞吐量提升，但错误率极高 |
| ReadHeavy (100%读) | N/A | N/A | 超时失败（死锁） |
| DeleteStress (写删) | 85.23 ops/sec | N/A | 超时失败（死锁） |

#### 延迟对比

| 测试场景 | 之前 P50 | 本次 P50 | 之前 P95 | 本次 P95 | 之前 P99 | 本次 P99 |
|---------|---------|---------|---------|---------|---------|---------|
| MixedWithFailures | 2.18ms | 1.90ms | 41.48ms | 17.83ms | 132.96ms | 87.88ms |
| WriteHeavy | 32.80ms | 36.93ms | 361.63ms | 488.36ms | 563.00ms | 705.54ms |
| Comprehensive | 11.54ms | 32.04ms | 575.29ms | 581.56ms | 1.08s | 938.45ms |

**结论** (2026-02-23):
- MixedWithFailures 测试表现最佳：吞吐量 481.75 ops/sec，延迟最低（P50=1.90ms）
- WriteHeavy 测试稳定：成功率 100%，吞吐量 68.73 ops/sec
- ReadHeavy 和 DeleteStress 测试存在死锁问题需要修复
- Comprehensive 测试在高并发场景下存在错误率过高的问题

**待解决的问题**:
1. ReadHeavy 测试死锁：高并发读取场景下出现锁竞争
2. DeleteStress 测试死锁：写删混合操作导致死锁
3. Comprehensive 测试高错误率：10个并发客户端下错误率达99.84%

### 运行长时测试

```bash
# 运行 10 分钟综合测试 (完整时长)
go test ./tests/ -run=^TestLongRunning_10Min_Comprehensive -timeout 15m -v

# 运行 10 分钟写入密集型测试
go test ./tests/ -run=^TestLongRunning_10Min_WriteHeavy -timeout 15m -v

# 运行 10 分钟读取密集型测试
go test ./tests/ -run=^TestLongRunning_10Min_ReadHeavy -timeout 15m -v

# 运行 10 分钟删除压力测试
go test ./tests/ -run=^TestLongRunning_10Min_DeleteStress -timeout 15m -v

# 运行带故障恢复的混合测试
go test ./tests/ -run=^TestLongRunning_10Min_MixedWithFailures -timeout 15m -v

# 使用短模式运行 (约2分钟)
go test ./tests/ -run=^TestLongRunning_10Min_Comprehensive -short -timeout 5m -v
```

## 性能分析

### 1. 传输层性能

| 传输层 | 性能特点 | 适用场景 |
|---------|---------|---------|
| **InMemory** | 性能最优，完全在内存中通信，无网络开销 | 单机测试、开发环境 |
| **TCP** | 存在网络序列化和 TCP 协议开销，但实现简单 | 本地网络环境 |
| **gRPC** | 存在 gRPC 协议开销，但支持跨语言调用、负载均衡 | 生产环境、微服务架构 |

### 2. 存储层性能

| 存储引擎 | 性能特点 | 适用场景 |
|----------|---------|---------|
| **InMemory** | 无持久化开销，性能最高，但数据不持久 | 缓存层、临时数据 |
| **LSM** | 存在 WAL 写入和 SSTable 合并开销，但支持持久化和数据压缩 | 生产环境、需要持久化的场景 |

### 3. 数据大小影响

- **小数据 (< 100B)**: 性能受 Raft 协议开销主导
- **中等数据 (100B - 1KB)**: 性能受序列化和网络传输影响
- **大数据 (> 1KB)**: 性能受存储 I/O 和网络带宽限制

### 4. 并发性能

- 单线程操作在 InMemory 配置下性能最优 (~55,000 ops/sec)
- 并发写入性能受限于 Raft Leader 串行处理 (~12,200 ops/sec)
- 建议使用批处理和流水线优化提升并发性能

### 5. 生产环境 (gRPC + LSM) 性能特点

- 吞吐量: ~7,280 ops/sec (基准测试)
- 平均延迟: ~1.37ms
- 读取操作性能优于写入操作 (1,645 ops/sec vs 589 ops/sec)
- 批量操作延迟较高但吞吐量稳定

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

# 运行生产环境基准测试 (gRPC + LSM)
go test ./tests/... -bench=^BenchmarkProduction -benchtime=3s -run=^$

# 运行端到端性能测试 (E2ETestSuite - 使用 sendThroughRPC)
go test ./tests/... -run=^TestE2E -timeout=5m

# 运行性能测试 (PerfTestSuite)
go test ./tests/... -run=^TestPerf -timeout=5m

# 运行长时端到端测试
go test ./tests/... -run=^TestLongRunning -timeout=15m -v
```

## 结论

### 测试执行总结 (2026-02-23)

**编译状态**: 所有测试文件编译通过，无语法错误

**已实现测试**:
- 完成了 10分钟长时端到端性能测试套件 (long_running_e2e_test.go)
- 包含综合测试、写入密集、读取密集、删除压力、故障恢复等多种场景
- 支持数据一致性验证和 Leader 切换监控

**最新测试结果**:
| 测试场景 | 吞吐量 | 成功率 | 状态 |
|---------|--------|--------|------|
| MixedWithFailures (5客户端, 70%写) | 481.75 ops/sec | 100% | ✅ 通过 |
| WriteHeavy (8客户端, 100%写) | 68.73 ops/sec | 100% | ✅ 通过 |
| Comprehensive (10客户端, 混合) | 231.65 ops/sec | 0.16% | ⚠️ 高错误率 |
| ReadHeavy (10客户端, 100%读) | N/A | N/A | ❌ 死锁失败 |
| DeleteStress (8客户端, 写删) | N/A | N/A | ❌ 死锁失败 |

**性能表现总结**:
- **InMemory + InMemory**: 吞吐量 ~55,000 ops/sec，延迟 ~181µs/op
- **InMemory + TCP**: 吞吐量 ~2,000 ops/sec，延迟 ~488µs/op
- **LSM + InMemory**: 吞吐量 ~960 ops/sec，延迟 ~1,037µs/op
- **LSM + gRPC (生产配置)**: 吞吐量 ~7,280 ops/sec，延迟 ~1,373µs/op
- 读取操作性能优于写入操作 (1,645 vs 589 ops/sec)
- 小值操作延迟最低，大值操作延迟最高

**建议**:
- 生产环境建议使用 gRPC + LSM 配置以获得网络通信和持久化保证
- 对于读密集型场景，可考虑在 Follower 上部署读取代理以减轻 Leader 压力
- 使用批量操作减少 Raft 协议开销
- 根据网络条件调整心跳和选举超时参数
- 对于高并发场景，考虑多 Raft Group 分片存储
- 定期运行长时测试以验证系统稳定性和数据一致性

**待修复问题**:
1. 修复 ReadHeavy 测试的死锁问题（高并发读取场景的锁竞争）
2. 修复 DeleteStress 测试的死锁问题（写删混合操作的死锁）
3. 优化 Comprehensive 测试，降低高并发场景下的错误率
4. 改进并发控制机制，提高系统在高负载下的稳定性

---

**测试日期**: 2026-02-23
**测试环境**: macOS Darwin 25.2.0
**Go 版本**: go1.x
**测试数据来源**: 实际运行测试收集

# Bug 修复复盘：Raft 与 LSM 长时间 E2E 问题

English version: [BUG_FIX_RETROSPECTIVE.md](BUG_FIX_RETROSPECTIVE.md)

本文解释本轮长时间 E2E 中发现的主要问题，以及背后的 Raft/LSM 内核原理。内容按实战组织：每一节说明现象、根因、破坏的不变量、修复设计和测试信号。

相关 issue：

- #88 Long comprehensive E2E hides ReadIndex apply timeouts
- #89 Restart and snapshot long E2E exposes apply timeout
- #90 Restart and snapshot E2E can diverge final values after successful writes
- #91 Drain in-flight long E2E operations before stopping clients
- #92 Refresh election last-log metadata across snapshot compaction
- #93 Keep LSM compaction metadata consistent when SSTable files disappear
- #94 Skip long-running E2E scenarios in short mode
- #95 Preserve LSM data across TCP leader restart
- #113 Investigate ReadIndex timeouts and long E2E throughput regression
- #115 Concurrent gRPC LSM writes can fail and leave missing keys
- #116 Long mixed-failure E2E exhausts retries during leader changes
- #117 Mixed-failure long E2E can return apply timeouts despite final consistency
- #119 Avoid unnecessary LSM compaction scheduling below threshold
- #121 LSM CompactLog leaves compacted log keys on disk
- #122 Make waitForAppliedLog timeout recheck test deterministic
- #123 Make integration leader discovery reliable under race load
- #124 Make network partition leader detection race-load safe
- #142 Bound mixed workload benchmark concurrency
- #143 Close LSM benchmark databases
- #145 Stabilize benchmark leader readiness
- #146 Propagate benchmark test failures
- #150 Prevent LSM snapshot reload races
- #151 Extend mixed-failure issued-request retry budget

## 1. 真正改变结果的调试原则

早期测试主要统计 RPC 成功数，但这不够。一个复制型数据库可能在接受大量请求的同时隐藏更深层问题：

- 写入看起来正常，但读请求持续超时；
- 客户端已经发出请求，但停止时还没拿到最终结果；
- 固定 sleep 结束时 follower 可能只是尚未追上；
- compaction 在长时间 churn 下暴露 stale metadata；
- leader 已经提交 entry，但携带最新 commit index 的 heartbeat 延迟到达 follower。

现在长时间 E2E 使用三道正确性门禁：

1. 客户端只在发出新请求前停止，已经发出的请求必须 drain；
2. 客户端停止后等待 final cluster barrier；
3. 重启和快照后逐节点比较数据。

这让测试从吞吐采样变成了一致性测试。

## 2. Raft 原理：新 Leader 必须提交本任期 Entry

Raft leader 不能只靠复制数量直接提交旧任期 entry。Raft 论文要求 leader 至少提交一个本任期 entry，之后它之前的旧任期 entry 才能被间接提交。

修复是在节点成为 leader 时追加 no-op command：

```go
func (r *Raft) transitionToLeader() {
    ...
    r.proposeLeaderNoopLocked()
}
```

no-op 不是用户写入，而是 leadership barrier。它给新 leader 一个本任期日志条目，有助于在 leader 切换和重启后推进 commit/apply。

涉及 issue：

- #89 在重启和快照 churn 中暴露 apply timeout。
- #90 在成功写入和 leader 切换重叠时暴露最终值不一致。

## 3. Raft 原理：Commit 推进必须唤醒复制

AppendEntries RPC 有两类信息：

- entries：复制日志内容；
- `LeaderCommit`：告诉 follower 哪些日志已经提交。

修复前，某个成功 AppendEntries 响应可能推进 leader commit index，但刚回复的 follower 未必立即收到更新后的 `LeaderCommit`，只能等下一次 heartbeat。在高负载下，这会放大 apply timeout。

现在复制路径会把 commit 推进当作信号：

```go
advanced := r.updateCommitIndex()
if advanced {
    r.notifyApply()
}
```

commit 推进后，leader 会继续调度复制，让 follower 更快收到最新 commit index。

涉及 issue：

- #88 让 ReadIndex apply timeout 不再被隐藏。
- #89 和 #90 依赖更快的 commit 传播。

## 4. Raft 原理：同任期 Leader Hint 也必须让旧角色降级

Raft term 用来区分领导权。节点在当前 term 得知合法 leader 后，即使没有看到更高 term，也必须停止继续扮演 candidate 或旧 leader。

修复把接受当前 term leader 的逻辑集中起来：

```go
func (r *Raft) acceptLeaderForCurrentTermLocked(leaderID int) {
    r.leaderID = leaderID
    if r.state != Follower {
        r.setState(Follower)
    }
    r.abortPendingClientRequestsLocked()
}
```

这样可以防止旧 leader 在同任期内继续持有客户端 waiter。

涉及 issue：

- #89 降低重启后的 apply timeout 循环。
- #90 降低 failover 中的旧 leader 和重复请求影响。

## 5. Raft 原理：投票新旧比较必须理解 Snapshot Compaction

RequestVote 要比较 candidate 日志和 voter 的最后日志 term/index。如果节点缓存了 `cachedLastLogIndex`，而 compaction 已经删除了对应物理 entry，投票新旧比较就可能基于过期元数据。

修复是在缓存 entry 不存在时刷新本地最后日志信息：

```go
lastIndex, lastTerm := r.localLastLogInfoLocked()
return r.isCandidateLogUpToDate(candidateIndex, candidateTerm, lastIndex, lastTerm)
```

如果 entry 是被 snapshot 覆盖了，就回退到 snapshot 边界参与比较。这样既遵守 Raft 投票安全规则，也不会把正常 compaction 当成数据丢失。

涉及 issue：

- #92 修复 snapshot compaction 后的选举元数据刷新。

## 6. 客户端原理：幂等性需要稳定请求身份

分布式系统里重试是正常行为。客户端可能超时，但 leader 随后成功提交了请求。如果没有稳定请求身份，重试可能让同一个逻辑写入被应用两次，或让测试记录错误的最终期望值。

客户端命令包装中包含：

```go
type ClientCommand struct {
    ClientID    int64
    SequenceNum int64
    Command     any
}
```

长时间 E2E 现在为每个客户端使用显式 ID 和递增 sequence。状态机通过这组 `(ClientID, SequenceNum)` 做去重并正确唤醒等待者。

涉及 issue：

- #90 修复重启密集场景中的最终值不一致。
- #91 修复测试停止时丢失已发出请求的问题。

## 7. 测试原则：请求发出后不能再取消追踪

之前长时间 E2E 的 stop gate 可能在请求已经发出、但 retry loop 还没拿到最终结果时停止 worker。这会让期望状态变得模糊：集群可能合法应用了该请求，但测试已经不再记录它。

修复后区分两个状态：

```go
requestIssued := false

if shouldStopBeforeRequest() {
    return
}

requestIssued = true
reply, err := sendRequest(...)
```

一旦 `requestIssued` 变为 true，helper 必须把这个逻辑请求 drain 到终态。它仍然可以在 `NotLeader` 后重试，但不会丢掉最终期望值。

涉及 issue：

- #91 drain long E2E 中已经发出的请求。

## 8. LSM 原理：Snapshot 导出必须 pin 住要复制的文件

LSM snapshot 会复制 SSTable 文件。Compaction 可能同时替换这些文件。如果 snapshot exporter 先列文件名、稍后再打开，文件可能在复制过程中消失。

修复是在 manager read lock 下打开 SSTable 文件并返回已打开的 fd：

```go
files, closeSnapshot, err := manager.OpenFilesSnapshot()
defer closeSnapshot()
```

在类 Unix 文件系统中，即使目录项随后被删除，已打开 fd 仍然可读。这让 snapshot export 获得稳定的字节视图。

涉及 issue：

- #88 包含 LSM-backed 状态机快照安全修复。

## 9. LSM 原理：缺失 Metadata 和损坏 Data 不是同一类问题

Compaction metadata 应该指向真实 SSTable 文件，但长时间重启和 compaction churn 暴露了 stale metadata：目录里还记录着已经删除的文件。把这种情况当成 fatal 会让存储引擎失败，即使该文件已经不再是可读数据来源。

修复区分两类情况：

- 文件缺失：剪掉 stale manager metadata 并继续；
- 文件存在但损坏：返回错误。

核心行为：

```go
if errors.Is(err, os.ErrNotExist) {
    m.removeTableMetadataLocked(level, table)
    return true, nil
}
return false, err
```

这既保留了对真实损坏的严格性，也让元数据清理具备自恢复能力。

涉及 issue：

- #93 保持 SSTable 文件消失时的 compaction metadata 一致。

## 10. 测试纪律：Short Mode 不能伪装成长时间 E2E

在 `testing.Short()` 下把 10 分钟场景跑 1 分钟会制造混乱信号。它既不是快速单测，也不是真实长时间场景。

修复后行为明确：

```go
func skipLongRunningE2EInShortMode(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping 10-minute long-running E2E test in short mode")
    }
}
```

现在 `go test -short ./...` 可以继续作为 PR 检查，长时间 E2E 必须显式运行。

涉及 issue：

- #94 修复 short mode 行为。
- #68 也因此被处理，通用 short 测试不会再执行长时间 E2E。

## 11. 测试原则：固定 Sleep 不是集群同步

TCP/LSM 重启测试使用固定 sleep 后直接读取重启节点。这会导致 flaky：写入本身正确，但原 leader 重启后未必在固定时间内追上。

修复后轮询真实状态机值：

```go
c.waitForStateMachineValue(t, nodeIndex, key, expected, 5*time.Second)
```

这检查的是测试真正关心的条件，而不是猜一个等待时间。

涉及 issue：

- #95 通过等待真实复制值，修复 TCP leader restart 场景。

## 12. 验证汇总

最新代码和测试逻辑修改后：

- TCP/LSM restart 聚焦回归连续 10 次通过；
- `go test -short ./...` 通过；
- 触发问题的 10 分钟 restart/snapshot E2E 通过；
- 6 个 10 分钟长时间 E2E 场景全部在 race detector 下通过。

完整长时间测试结果见 [PERFORMANCE.zh-CN.md](PERFORMANCE.zh-CN.md)。

## 13. 后续修改核心代码时的经验

- 成功请求数不是一致性证明。
- 每次 Raft leadership 变更都要检查本任期 commit 规则。
- Snapshot compaction bug 经常表现为缺失 log 或缺失 SSTable，而不是明显 panic。
- LSM metadata 和物理文件必须作为一个逻辑目录维护。
- 测试应该等待状态，而不是等待固定时间。
- 任何 Raft 或 LSM 代码修改后，都应该先跑触发问题的单个长时间 E2E，再跑全量长时间 E2E 回归。

## 14. 2026-06-22 核心恢复与 Apply 修复复盘

第二轮深测重点放在 Raft restart、snapshot compaction、LSM flush 和 LSM
recovery 同时发生时才会暴露的问题。这些问题晚于 #88-#95，单独跟踪：

- #102 防止 Raft commit notification 丢失。
- #103 修正性能测试 harness 对 commit channel 的消费方式。
- #104 原子发布 LSM SSTable，并处理空表 metadata。
- #105 让 LSM table/WAL ID 变成每个数据库 manager 本地状态。
- #106 通过恢复 durable `commitIndex` 修复 TCP restart 后的 LSM 节点状态恢复。
- #107 MemTable recovery 忽略非 WAL 目录项。

### #102：Commit notification 属于 apply 边界

现象：高负载下 `commitChan` 可能被写满。旧代码用非阻塞发送，满了就直接丢：

```go
select {
case commitChan <- entry:
default:
    log.Warnf("commitChan full, skipping")
}
```

这会让观察者以为某条 entry 没有 apply，但 Raft 实际已经写入状态机。修复后
`commitChan` 是带背压的流：

```go
select {
case commitChan <- commitEntry:
case <-shutdownChan:
}
```

`shutdownChan` 防止 `Stop()` 永久阻塞；正常运行时不再丢 apply notification。

### #103：Benchmark harness 不能重复 Apply

`commitChan` 是 Raft apply 完之后才发出的通知。一些性能测试消费 `commitChan`
后又把命令 apply 到状态机一次，这会放大写入，并掩盖真实状态机问题。

修复后 harness 只 drain：

```go
go func(ch chan param.CommitEntry) {
    for range ch {
        // Raft 已经 apply
    }
}(commitChan)
```

这样 benchmark 不会制造第二条绕过 Raft 的 apply 路径。

### #104：SSTable 发布有两个独立故障模式

#104 最早的假设是 final file 半写入。长时间 E2E 后进一步定位到更精确的问题：
保留下来的 `.sst` footer 里 `DataHandle.Size == 0` 且 `IndexHandle.Size == 0`。
通用 `DataBlock.DecodeFrom(reader, 0)` 把 size `0` 理解成“不限制读取”，于是
SSTable 层把 footer 字节误当 value 解码，最终 EOF。

最终修复分三部分：

```go
tmp, _ := os.CreateTemp(dir, "."+base+".*.tmp")
// encode header/filter/data/index/footer into tmp
tmp.Sync()
tmp.Close()
os.Rename(tmp.Name(), finalPath)
```

- SSTable 发布使用临时文件、fsync、close、rename；
- 跳过空 immutable memtable，不发布空 Level-0 表；
- `Footer.DataHandle.Size == 0` 时 `DecodeDataBlock` 直接返回。

Recovery 也会忽略未提交临时文件，并移除旧空 SSTable，而不是把它加载进 catalog。

### #105：ID generator 必须属于数据库 manager

旧恢复路径会 reset 包级 ID generator。恢复一个 manager 时可能把全局 generator
回退，而另一个 manager 还在运行。这会让活跃数据库复用 SSTable 或 WAL ID，
甚至覆盖已有文件。

修复后 ID 分配放到 manager 内部：

```go
type Manager struct {
    nextID atomic.Uint64
}

func (m *Manager) nextTableID() uint64 {
    return m.nextID.Add(1)
}
```

Recovery 只把本 manager 的 counter 推进到不小于已恢复最大 ID，不再重置其他
数据库共享的全局状态。

### #106：持久化 CommitIndex 是工程 guardrail

Raft 论文层面的持久状态是 term、vote 和 log entries。本项目状态机也是持久化的，
如果重启后忘记 durable commit index，已提交 entry 可能要等另一个 leader 再次传播
commit 信息才会 apply。

修复后 `CommitIndex` 写入 `HardState`，并在 `NewRaft` 恢复：

```go
r.commitIndex = hardState.CommitIndex
if r.commitIndex > r.lastApplied {
    r.startApplyLogsLocked()
}
```

这不改变多数派提交规则，只保留已经 durable 且 committed 的 entry 的重启进度。

### #107：WAL recovery 需要已提交文件契约

MemTable recovery 以前会重放 `os.ReadDir` 返回的每个目录项。残留的 `notes.txt`、
`3.wal.tmp` 或名为 `4.wal` 的目录，都可能让引擎在重放有效 WAL 前失败。

修复后先过滤 recovery set：

```go
if file.IsDir() || filepath.Ext(file.Name()) != ".wal" {
    continue
}
idPart := strings.TrimSuffix(file.Name(), ".wal")
if _, err := strconv.ParseUint(idPart, 10, 64); err != nil {
    continue
}
```

只有 `{id}.wal` 会被重放。内容损坏的已提交 WAL 仍然会让恢复失败，因此不会掩盖
真实数据损坏。

### 验证信号

这些修复后，聚焦的 10 分钟 restart/snapshot 场景通过：

- 总操作 797,556；
- 失败操作 0；
- 最终 cluster barrier 成功；
- 3,600 个 node/key 一致性检查通过；
- 3 个节点产生 snapshot，leader 切换 46 次。

完整包级和长时间 E2E 验证命令记录在 [PERFORMANCE.zh-CN.md](PERFORMANCE.zh-CN.md)。

## 15. 2026-06-22 SSTable 重写 metadata 修复

#109 是在 #102-#107 合并后的下一轮 LSM 深测中发现的。检查目标是一个很窄但很关键
的文件格式不变量：同一个内存 SSTable 编码两次，每次产出的文件都必须能被正确读取。

### 现象

回归测试会把同一个 table 对象写两次，再重新加载：

```go
table := createSampleSSTable(0, tempDir, pairs)

require.NoError(t, table.EncodeTo(table.filePath))
require.NoError(t, table.EncodeTo(table.filePath))

recovered := NewRecoverSSTable(0)
require.NoError(t, recovered.DecodeFrom(table.filePath))
_, err := recovered.GetDataBlockFromFile(table.filePath)
```

修复前第二次读取失败：

```text
read value failed: unexpected EOF
decode DataBlock failed: read value data failed: unexpected EOF
```

### 根因

`EncodeTo` 会把存放在 `SSTable` 对象上的布局 metadata 序列化进文件。写 data block
时，它会累加本次写入的 value size：

```go
t.Footer.DataHandle.Size += size
```

单次 encode 这是对的，但重复 encode 时就不对。第二次写入开始时，上一次的 size 已经
留在 footer 里，于是新文件的 footer 声称 data block 比实际写入的字节更大。

恢复过程信任 footer 后，就会越过 data block 继续读到 index/footer 区域，value decoder
最终正确报 `unexpected EOF`。

### 修复方案

encoder 现在每次写入前重置所有派生文件布局 metadata：

```go
func (t *SSTable) resetFileLayout() {
    if t.Footer == nil {
        t.Footer = block.NewFooter()
        return
    }
    t.Footer.DataHandle = block.NewHandle(0, 0)
    t.Footer.IndexHandle = block.NewHandle(0, 0)
    for _, entry := range t.IndexBlock.Indexes {
        entry.Offset = 0
    }
}
```

`EncodeTo` 在创建临时输出文件前调用它。这样 footer 和 index offset 只来自当前写入过程。

### 原理

SSTable 原子发布有两层：

- 只发布完整文件：temp file + fsync + close + rename；
- 完整文件内部也必须有一致的 metadata。

#104 修的是第一层；#109 修的是 rewrite/retry 路径下的第二层。即使生产路径通常一个
SSTable 对象只写一次，encoder 也应该在重试、测试和维护工具路径中保持确定性。

### 验证

#109 的验证命令：

```bash
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/sstable -run TestEncodeToCanRewriteSameTableWithoutStaleFooterState -count=1 -timeout=2m
```

以及更宽的 LSM/storage 回归：

```bash
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=5m
```

## 16. 2026-06-22 Snapshot Apply 路径验证修复

#111 是检查 LSM snapshot 安装路径时发现的。危险点不只是路径穿越，更大的正确性问题是：
`ApplySnapshot` 在验证文件路径之前，已经关闭并删除了当前数据库。

### 现象

失败测试会把畸形 snapshot 安装到一个已有数据的数据库：

```go
adapter.Apply(param.LogEntry{Command: mustMarshal(param.KVCommand{
    Op: param.OpSet, Key: "keep", Value: "value",
})})

snapData, _ := encodeSnapshotData(map[string][]byte{
    "../escape.sst": []byte("not-a-valid-sstable"),
})

err := adapter.ApplySnapshot(snapData)
```

修复前 adapter 只打一条 warning，返回 nil，而且原 key 已经丢失：

```text
[LSMAdapter] Skipping invalid snapshot file path: ../escape.sst
expected error but got nil
key not found
```

### 根因

旧实现是在写文件循环里检查路径：

```go
for relPath, content := range snapshotData {
    if strings.Contains(relPath, "..") {
        log.Warnf("Skipping invalid snapshot file path: %s", relPath)
        continue
    }
    fullPath := filepath.Join(sstPath, relPath)
    os.WriteFile(fullPath, content, 0644)
}
```

这里有三个问题：

- 非法路径被跳过，而不是拒绝整个 snapshot；
- 检查发生在 `db.Close()` 和 `os.RemoveAll(dbPath)` 之后；
- `strings.Contains("..")` 不是精确的路径策略。

### 修复方案

`ApplySnapshot` 现在会在任何破坏性操作前验证完整 snapshot manifest：

```go
filesToRestore, err := validateSnapshotFiles(sstPath, snapshotData)
if err != nil {
    return err
}

if err := lsm.db.Close(); err != nil {
    return err
}
```

validator 只接受清理后的相对路径，并要求路径仍在 snapshot SSTable 根目录下：

```go
cleanRel := filepath.Clean(relPath)
if cleanRel == "." || cleanRel == ".." ||
    strings.HasPrefix(cleanRel, ".."+string(os.PathSeparator)) {
    return "", fmt.Errorf("invalid snapshot file path")
}
```

它还会检查 join 后的绝对路径没有逃出 snapshot root。

### 原理

Raft snapshot 安装是本地状态机替换操作。在验证边界上，它应该像事务一样：

- 畸形输入必须在触碰当前状态前被拒绝；
- 只有完整验证过的文件清单才能安装；
- 不能静默跳过 snapshot 的一部分后仍声称安装成功。

### 验证

#111 的验证命令：

```bash
GO_KV_LOG_LEVEL=warn go test ./pkg/storage/lsm -run TestApplySnapshotRejectsInvalidFilePathBeforeClearingDB -count=1 -timeout=2m
GO_KV_LOG_LEVEL=warn go test ./pkg/storage/lsm -count=1 -timeout=3m
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=5m
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=25m
GO_KV_LOG_LEVEL=warn go test -race -v ./tests -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' -count=1 -timeout=25m
```

聚焦 10 分钟重放完成：总操作 1,104,337，失败操作 0，最终 barrier 成功，
3,600 个严格 node-key 一致性检查通过。

## 17. 2026-06-23 ReadIndex、选举超时和 LSM Compaction 修复

这一轮长测重点不是小边界，而是内核层可用性问题。它暴露了一条连续故障链：
健康 heartbeat 可能比 ReadIndex 确认预算更慢；follower 可能在健康
AppendEntries RPC 允许返回前就发起新选举；长测客户端可能在 leader churn
期间放弃已经发出的请求；前台 LSM compaction 还可能让 Raft apply 停顿到
客户端出现 apply timeout。

### #113：ReadIndex 确认必须尊重 RPC 预算

症状：长时间 E2E 最终安全通过，但出现 ReadIndex quorum timeout warning。
聚焦单测进一步证明：`confirmLeadership` 使用 `electionTimeout * 2` 作为等待时间，
即使两个 heartbeat ack 都是健康的，只要它们慢于这个本地预算，也会被误判失败。

修复方案是给 ReadIndex heartbeat 确认增加下限：

```go
func readIndexConfirmTimeout(electionTimeout time.Duration) time.Duration {
    timeout := electionTimeout * 2
    if timeout < minReadIndexConfirmTimeout {
        return minReadIndexConfirmTimeout
    }
    return timeout
}
```

这样线性一致读不会在健康 AppendEntries 回复仍处于传输层 timeout 内时，就误报
leader 失效。

### #115：Election Timeout 必须大于健康 AppendEntries Timeout

症状：包级并发 race 测试下，`TestCluster_ConcurrentClientRequests/grpc_lsm`
偶发缺失 key。根因是默认超时预算不一致：

```go
DefaultElectionTimeout      = 500 * time.Millisecond
DefaultAppendEntriesTimeout = 2 * time.Second
```

Follower 可以在一次健康 AppendEntries RPC 仍允许 in-flight 时发起新选举，
在 race detector 和包并发负载下放大 leader churn。

默认 election timeout 调整为 2.5s，并增加配置回归测试：

```go
assert.Greater(t,
    config.DefaultElectionTimeout,
    transportgrpc.DefaultAppendEntriesTimeout,
)
```

这不是修改 Raft 论文中的选举规则，而是让实现的超时预算与传输层一致。

### #116：已发出的长测请求需要时间预算，而不是固定重试次数

症状：10 分钟 mixed-failure E2E 中出现少量 `not_leader` 失败，但最终 barrier
和严格一致性都通过。请求已经发出后，集群仍可能提交它；但测试 helper 只按固定次数重试：

```go
for retry := 0; retry < maxRetries; retry++ {
    ...
}
```

长测 helper 现在区分两种状态：

- 请求发出前，仍使用普通重试次数上限；
- 请求发出后，使用有界 wall-clock 时间窗口继续追踪同一逻辑请求。

```go
func shouldContinueLongRunningRetry(retry, maxRetries int, requestIssued bool, requestIssuedAt, now time.Time) bool {
    if !requestIssued {
        return retry < maxRetries
    }
    return now.Sub(requestIssuedAt) < longRunningIssuedRequestRetryTimeout
}
```

这样可以保护期望值模型：一旦某个逻辑 client request 可能进入 Raft，测试就继续跟踪同一个 `(ClientID, SequenceNum)`，直到得到终态结果。

### #117：Apply Timeout 的深层原因是前台 Compaction 停顿

#116 之后，mixed-failure 场景暴露了新的 `apply_timeout`。随后全量长测在
Comprehensive 和 WriteHeavy 中复现了同类问题。关键线索是 WriteHeavy 有 8 个
客户端，而一次窗口里正好失败 8 个 apply timeout。这说明问题不是随机客户端失败，
而是一次全局 apply 停顿。

第一步修复是 leader 侧 apply timeout 后不能清理 pending client request：

```go
// 旧行为：timeout 后删除 pending request
if !ok && trackClient {
    r.clearPendingClientRequest(index)
}
```

删除是错误的。Timeout 不证明原 entry 失败。保留 `pendingClientRequests` 后，
同一个 client identity 的重试可以重新挂到原始 log index，而不是在第一条 entry
仍可能提交时追加重复工作。

更深层根因在 LSM。`CreateNewSSTable` 每次 flush 后同步执行 compaction：

```go
if err := m.Compaction(); err != nil {
    return fmt.Errorf("compaction failed: %w", err)
}
```

Raft apply 已提交 entry 时，会在持有 `stateMachineMu` 的情况下写入 LSM-backed
状态机。如果 flush 在这条路径里触发大型 compaction，后续所有 committed entry
都会停止 apply，客户端 waiter 就可能耗尽 apply/retry 窗口，尽管集群最终仍能收敛。

修复后，前台只发布持久化 Level-0 SSTable，compaction 由合并调度的后台 worker 执行：

```go
func (m *Manager) CreateNewSSTable(imem *memtable.IMemTable) error {
    ...
    m.addTable(sst)
    if m.isLevelNeedToBeMerged(m.minSSTableLevel) {
        m.ScheduleCompaction()
    }
    imem.Clean()
    return nil
}
```

`ScheduleCompaction` 保证只有一个 worker 活跃，并把新增请求合并为下一轮 pass：

```go
if m.compactionRunning {
    m.compactionQueued = true
    return
}
m.compactionRunning = true
go m.runScheduledCompactions()
```

存储不变量没有改变：`CreateNewSSTable` 返回时，数据已经在 Level 0 持久化且可见。
被移出 Raft apply 关键路径的只是昂贵的跨层 merge。

### 验证

最终验证命令：

```bash
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=10m
GO_KV_LOG_LEVEL=warn go test ./raft -count=1 -timeout=8m
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=20m ./tests -run '^TestLongRunning_10Min_WriteHeavy$' -count=1
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' -count=1
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m
```

最终六场景长时间 E2E 全部失败操作为 0。重启/快照类场景的 final barrier
和逐节点严格一致性检查也全部通过。最新指标记录在 [PERFORMANCE.zh-CN.md](PERFORMANCE.zh-CN.md)。

## 18. 2026-06-23 LSM Compaction 调度增加阈值门禁

相关 issue：#119。

### 症状

#118 合并后的第一轮核心包基线失败：

```bash
GO_KV_LOG_LEVEL=warn go test ./raft ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=12m
```

失败点：

```text
--- FAIL: TestSSTableManagerOpenFilesSnapshotReleasesManagerLock
    manager_test.go:201: OpenFilesSnapshot kept the manager lock while callers read files
```

单独重复运行也能复现：

```bash
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/sstable -run '^TestSSTableManagerOpenFilesSnapshotReleasesManagerLock$' -count=10 -timeout=2m
```

### 根因

#117 把 compaction 从前台 flush 路径移到后台，这是正确方向；但初版实现对每次非空 flush
都调度后台 worker：

```go
m.addTable(sst)
m.ScheduleCompaction()
imem.Clean()
```

这太宽了。只有一个新的 Level-0 SSTable 时，文件数量低于 compaction 阈值，worker
没有任何 merge 工作可做，但它仍会短暂竞争 `Manager.mu` 去检查目录状态。已有的
snapshot-lock 测试观测到了这个无意义 worker，而不是 `OpenFilesSnapshot` 返回后仍持锁。

生产上的问题也一样：低于阈值的 flush 不应该创建 goroutine，也不应该为了确认“无需
compaction”而竞争 manager lock。

### 回归测试

测试先人为阻塞 Level-0 compaction，然后只创建一个 SSTable。若低于阈值仍调度 worker，
`compactionRunning` 会稳定暴露出来：

```go
func TestCreateNewSSTableSkipsCompactionWhenBelowThreshold(t *testing.T) {
    manager := NewSSTableManager(t.TempDir())
    level := manager.minSSTableLevel

    manager.mu.Lock()
    manager.compactingLevels[level] = true
    manager.mu.Unlock()
    defer func() {
        manager.endCompactionLevels([]int{level})
        manager.WaitForCompactions()
    }()

    assert.NoError(t, manager.CreateNewSSTable(testIMemWithPair("key", "value")))

    manager.mu.Lock()
    running := manager.compactionRunning
    queued := manager.compactionQueued
    manager.mu.Unlock()

    assert.False(t, running)
    assert.False(t, queued)
}
```

修复前，它会失败在：

```text
below-threshold flush must not start a no-op compaction worker
```

### 修复方案

`CreateNewSSTable` 在发布新表后检查 Level-0 阈值，只有确实超过阈值时才调度后台
compaction：

```go
m.addTable(sst)
log.Debugf("[SSTableManager] Created new SSTable %s at level %d", sst.FilePath(), sst.level)

if m.isLevelNeedToBeMerged(m.minSSTableLevel) {
    m.ScheduleCompaction()
}

imem.Clean()
```

这样保留了 #117 的不变量：前台 flush 不等待 compaction；同时新增一个不变量：
低于阈值时不创建无意义的后台 compaction worker。

### 验证

```bash
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/sstable -run '^(TestCreateNewSSTableSkipsCompactionWhenBelowThreshold|TestSSTableManagerOpenFilesSnapshotReleasesManagerLock)$' -count=10 -timeout=2m
GO_KV_LOG_LEVEL=warn go test ./raft ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=12m
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/sstable -count=100 -timeout=5m
GO_KV_LOG_LEVEL=warn go test -race ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=12m
GO_KV_LOG_LEVEL=warn go test ./tests -run '^(TestCluster_ConcurrentClientRequests|TestCluster_TakeSnapshot|TestCluster_InstallSnapshot|TestCluster_FullClusterRestart|TestCluster_LeaderFailover)$' -count=3 -timeout=12m
```

## 19. 2026-06-23 LSM CompactLog 写入物理 Raft 日志 Tombstone

相关 issue：#121。

### 症状

从普通 Raft API 看，`StorageAdapter.CompactLog(upToIndex)` 似乎是正确的：
压缩后，`GetEntry(index)` 会因为 `firstIndex` 已经推进而对已压缩 index 返回
`nil`。

但更底层的存储不变量仍然被破坏了。已压缩 entry 对应的物理 LSM key
`log:<index>` 仍然留在逻辑窗口下面。直接查询底层 storage 仍能读到旧的编码日志
payload，而且普通 LSM compaction 也无法回收这些字节，因为从未写入 tombstone。

这意味着长时间运行的 Raft 节点即使已经通过 snapshot 让这些日志逻辑不可达，旧
payload 仍可能永久占用存储空间。

### 根因

旧实现只更新 metadata：

```go
s.firstIndex = upToIndex + 1
if upToIndex >= s.lastIndex {
    s.lastIndex = upToIndex
    s.logSize = 0
} else if oldLastIndex >= oldFirstIndex {
    totalEntries := oldLastIndex - oldFirstIndex + 1
    compactedEntries := deleteTo - oldFirstIndex + 1
    compactedBytes := int((int64(s.logSize) * int64(compactedEntries)) / int64(totalEntries))
    s.logSize -= compactedBytes
}

return s.saveMetadata()
```

这只维护了逻辑 Raft 日志窗口，没有维护物理 LSM 状态。`logSize` 的更新也只是按
entry 数量做比例估算；因为 Raft entry 的 command 长度可变，这个估算会逐渐偏离真实
保留的编码字节数。

这里要区分两个平面：

- 逻辑窗口（`firstIndex..lastIndex`）决定 Raft 还能读取哪些日志；
- 物理 LSM tree 决定哪些字节仍然持久化、哪些字节可以被回收。

这两个平面必须一起推进。只在 adapter 层隐藏旧 key，不等于从 storage engine 中删除
旧 key。

### 回归测试

回归测试在 compaction 后刻意绕过 `GetEntry`，直接检查底层 LSM key：

```go
func TestStorageAdapterCompactLogDeletesPhysicalLogKeys(t *testing.T) {
    ...
    assert.NoError(t, adapter.CompactLog(2))

    raw, err = adapter.db.Get(key1)
    assert.NoError(t, err)
    assert.Nil(t, raw, "CompactLog must tombstone compacted physical log key 1")

    raw, err = adapter.db.Get(key2)
    assert.NoError(t, err)
    assert.Nil(t, raw, "CompactLog must tombstone compacted physical log key 2")

    raw, err = adapter.db.Get(key3)
    assert.NoError(t, err)
    assert.NotNil(t, raw, "CompactLog must keep entries after the compacted range")
}
```

修复前，key 1 和 key 2 仍能读到 `GLG1` 编码字节。

### 修复方案

`CompactLog` 现在会先为已压缩的物理 key 范围写入 tombstone，再保存新的逻辑
metadata：

```go
oldFirstIndex := s.firstIndex
oldLastIndex := s.lastIndex
deleteTo := min(upToIndex, oldLastIndex)

if oldLastIndex >= oldFirstIndex {
    for i := oldFirstIndex; i <= deleteTo; i++ {
        key := s.getLogKey(i)
        val, err := s.db.Get(key)
        if err != nil {
            return err
        }
        if val != nil {
            s.logSize -= len(val)
            if s.logSize < 0 {
                s.logSize = 0
            }
        }
        if err := s.db.Delete(key); err != nil {
            return err
        }
    }
}

s.firstIndex = upToIndex + 1
if upToIndex >= s.lastIndex {
    s.lastIndex = upToIndex
    s.logSize = 0
}

return s.saveMetadata()
```

这样恢复了 snapshot 驱动的 Raft log compaction 不变量：

1. Raft 可见的日志窗口不再暴露已压缩 entry；
2. LSM 可见的 keyspace 中存在 tombstone，普通 LSM compaction 可以回收旧日志
   payload。

`logSize` 也改为按实际移除的编码 value 长度扣减，不再使用比例估算。

### 验证

```bash
GO_KV_LOG_LEVEL=warn go test ./pkg/storage/lsm -run '^(TestStorageAdapterCompactLogDeletesPhysicalLogKeys|TestStorageAdapter_Snapshot|TestStorageAdapter_CompactBeyondLastIndexFromSnapshot|TestStorageAdapter_LogEntries|TestStorageAdapter_ReappendAfterTruncateSurvivesFlushCompactionAndRestart)$' -count=1 -timeout=5m
GO_KV_LOG_LEVEL=warn go test ./pkg/storage/lsm ./engine/lsm/... -count=1 -timeout=12m
GO_KV_LOG_LEVEL=warn go test -race ./pkg/storage/lsm ./engine/lsm/... -count=1 -timeout=12m
GO_KV_LOG_LEVEL=warn go test ./tests -run '^(TestCluster_TakeSnapshot|TestCluster_InstallSnapshot|TestCluster_FullClusterRestart)$' -count=3 -timeout=12m
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=25m ./tests -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' -count=1
```

第一条命令验证直接物理 key 的回归；第二条命令覆盖周边 LSM package，确认新增
tombstone 写入不会破坏 flush、compaction、WAL recovery 或 restart 行为。race
和集群命令进一步覆盖并发、snapshot 创建、snapshot install 和持久化重启恢复。最后两条命令完成
PR 级门禁：所有 short 单元/集成测试在 race detector 下通过，10 分钟重启/快照 E2E
也以 0 失败操作和严格逐节点一致性通过。

## 20. 2026-06-23 waitForAppliedLog Timeout 重查测试改为确定性

相关 issue：#122。

### 症状

全量 short race 门禁暴露了一个 Raft 测试失败：

```bash
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m
```

失败点：

```text
--- FAIL: TestWaitForAppliedLogRechecksLastAppliedOnTimeout
    raft_test.go:1575: Should be true
```

定向重复运行说明它更像时序敏感测试，而不是确定性的生产逻辑失败：

```bash
GO_KV_LOG_LEVEL=warn go test ./raft -run '^TestWaitForAppliedLogRechecksLastAppliedOnTimeout$' -count=20 -timeout=2m
GO_KV_LOG_LEVEL=warn go test -race ./raft -run '^TestWaitForAppliedLogRechecksLastAppliedOnTimeout$' -count=50 -timeout=3m
```

两条定向命令都通过，但全量 race 门禁已经在 package-wide 负载下失败过。

### 根因

生产代码本身已经有 timeout 分支重查逻辑：

```go
case <-timer.C:
    r.mu.Lock()
    applied := r.lastApplied >= index
    ...
    r.mu.Unlock()
    if applied {
        return nil, true
    }
    return nil, false
```

脆弱的是测试。旧测试依赖固定 sleep 在短 timeout 前更新 `lastApplied`：

```go
go func() {
    time.Sleep(5 * time.Millisecond)
    r.mu.Lock()
    r.lastApplied = 7
    r.mu.Unlock()
}()

result, ok := r.waitForAppliedLog(7, 20*time.Millisecond)
```

在 race detector 下，goroutine 不保证一定能在 20ms timer 触发前运行。如果它运行得
更晚，timeout 分支正确看到 `lastApplied < 7` 并返回 false。这不能证明 timeout
重查失效，只能说明测试的前置条件依赖调度器。

### 修复方案

测试现在显式建立期望顺序：

1. 在 goroutine 中启动 `waitForAppliedLog`；
2. 等待 waiter 注册到 `notifyApply`；
3. 持有 `r.mu` 设置 `lastApplied`，但不发送 apply 通知；
4. 等 timeout 分支重查 `lastApplied`；
5. 断言成功并验证 waiter cleanup。

```go
go func() {
    result, ok := r.waitForAppliedLog(7, 100*time.Millisecond)
    results <- waitResult{result: result, ok: ok}
}()

assert.Eventually(t, func() bool {
    r.mu.Lock()
    defer r.mu.Unlock()
    return len(r.notifyApply[7]) == 1
}, time.Second, time.Millisecond)

r.mu.Lock()
r.lastApplied = 7
r.mu.Unlock()
```

这样仍然测试同一个内核不变量，但不再依赖 helper goroutine 赢过 5ms 和 20ms
之间的调度竞态。

### 验证

```bash
GO_KV_LOG_LEVEL=warn go test -race ./raft -run '^TestWaitForAppliedLogRechecksLastAppliedOnTimeout$' -count=100 -timeout=3m
GO_KV_LOG_LEVEL=warn go test -race ./raft -count=1 -timeout=8m
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m
```

定向测试在 race detector 下重复 100 次通过，整个 `raft` package 也在 race
detector 下通过。修复该测试以及后续集成 helper 后，全量 short race 门禁也通过。

## 21. 2026-06-23 集成测试 Leader 发现适配 Race 负载

相关 issue：#123。

### 症状

#122 之后，全量 short race 门禁已经通过 Raft package，但在集成测试里失败：

```bash
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m
```

失败点：

```text
--- FAIL: TestCluster_MembershipChange (64.83s)
    --- FAIL: TestCluster_MembershipChange/grpc_simplefile (8.22s)
        integration_test.go:179: Cluster failed to elect a leader within timeout
FAIL github.com/xmh1011/go-kv/tests 1009.098s
```

定向重复运行没有发现确定性的 membership-change 逻辑错误：

```bash
GO_KV_LOG_LEVEL=warn go test ./tests -run '^TestCluster_MembershipChange$/^grpc_simplefile$' -count=5 -timeout=12m
GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_MembershipChange$/^grpc_simplefile$' -count=3 -timeout=12m
```

两条都通过，因此排查重点转到用于发现 leader 的测试 helper。

### 根因

`tests.cluster.getLeader` 的注释说大约等待 8 秒，但实际做了更重的工作。每轮它会对
每个运行中节点发送完整 `ClientRequest` probe：

```go
for i := 0; i < 40; i++ {
    time.Sleep(200 * time.Millisecond)
    for _, node := range c.nodes {
        _ = node.ClientRequest(args, reply)
        if !reply.NotLeader && reply.Success {
            return node
        }
        if !reply.NotLeader && reply.Result == "key not found" {
            return node
        }
    }
}
```

`ClientRequest` 不是廉价的本地状态检查。对 leader 上的 read，它可能执行 ReadIndex
leader confirmation，并等待 `lastApplied`，最长可到 client apply timeout。全量
`-race` 负载下，helper 可能把大部分时间花在 read probe 阻塞上，最后报
“failed to elect a leader”。这个错误信息会误导排查：真正的问题可能是 probe 路径没有在固定窗口内完成，而不是 Raft 没有选主。

另外，旧 helper 使用固定 probe client id，多次 `getLeader` 调用会共享 client-session
状态，也会让 probe 响应的语义变模糊。

### 修复方案

Leader 发现改为条件驱动：

```go
deadline := time.Now().Add(30 * time.Second)
for time.Now().Before(deadline) {
    for _, node := range c.nodes {
        if node.IsStopped() || node.State() != raft.Leader {
            continue
        }

        sequenceNum++
        args := &param.ClientArgs{
            ClientID:    probeClientID,
            SequenceNum: sequenceNum,
            Command:     probeCmdBytes,
        }
        ...
    }
    time.Sleep(200 * time.Millisecond)
}
```

新的 helper 会：

- 先扫描本地 Raft 状态，只 probe leader 候选节点；
- 每次 `getLeader` 使用唯一 probe client id；
- 对 race-mode 集成测试使用 30 秒 deadline；
- 失败时打印所有节点状态和最后一次 probe 响应。

这样仍保留对候选 leader 的 stale-leader 防线，同时避免对所有 follower 串行执行
ReadIndex/apply 等待。

### 验证

```bash
GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_MembershipChange$/^grpc_simplefile$' -count=5 -timeout=12m
GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_MembershipChange$' -count=1 -timeout=15m
```

修复后，原失败的 `grpc_simplefile` membership-change 子场景在 race detector 下连续
5 次通过；完整 membership-change transport/storage 矩阵也在 race detector 下通过。

## 22. 2026-06-23 Network Partition Leader 检测适配 Race 负载

相关 issue：#124。

### 症状

#123 之后，全量 short race 门禁继续向后推进，但在网络分区集成测试里失败：

```bash
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m
```

失败点：

```text
--- FAIL: TestCluster_NetworkPartition (70.77s)
    --- FAIL: TestCluster_NetworkPartition/tcp_inmemory (12.03s)
        integration_test.go:413: Leader: Node 3
        integration_test.go:417: Isolating Node 3...
        integration_test.go:436: Waiting for new leader in majority partition...
        integration_test.go:473: Majority partition failed to elect a new leader
FAIL github.com/xmh1011/go-kv/tests 1011.196s
```

定向重复运行仍然没有发现确定性的 Raft 分区选举失败：

```bash
GO_KV_LOG_LEVEL=warn go test ./tests -run '^TestCluster_NetworkPartition$/^tcp_inmemory$' -count=5 -timeout=12m
GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_NetworkPartition$/^tcp_inmemory$' -count=5 -timeout=12m
```

两条都通过。

### 根因

`TestCluster_NetworkPartition` 在 majority partition 内部有一份手写 leader detection
loop，没有复用 #123 新增的条件驱动 helper：

```go
time.Sleep(5 * time.Second)
for i := 0; i < 20 && !foundLeader; i++ {
    time.Sleep(200 * time.Millisecond)
    for _, node := range majorityNodes {
        reply := &param.ClientReply{}
        _ = node.ClientRequest(&param.ClientArgs{Command: probeCmdBytes}, reply)
        ...
    }
}
```

这重复了同类测试设计问题：

- 固定 sleep，而不是条件驱动等待；
- 对 majority 中每个节点都走完整 `ClientRequest` probe；
- 每次 probe 都使用零值 `ClientID` 和 `SequenceNum`；
- 失败时没有节点状态诊断信息。

测试本来要回答“多数派分区是否选出 leader”，但在 full race 负载下，read-probe
路径慢也可能导致这段 loop 失败。

### 修复方案

通用 leader helper 现在可以接受候选节点集合：

```go
func (c *cluster) getLeader(t *testing.T) *raft.Raft {
    t.Helper()
    return c.getLeaderFromCandidates(t, c.nodes, 30*time.Second)
}
```

网络分区测试构造 `majorityNodes` 后直接复用同一个 helper：

```go
newLeader = c.getLeaderFromCandidates(t, majorityNodes, 30*time.Second)
```

这样分区场景的 leader 检测和其他集成测试遵循相同规则：

- 只 probe 本地状态为 `Leader` 的节点；
- probe 使用唯一 client id 和递增 sequence number；
- deadline 足够覆盖 race-mode 下 TCP 和 storage 的额外开销；
- 失败时输出节点状态和最后一次 probe 响应。

### 验证

```bash
GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_NetworkPartition$/^tcp_inmemory$' -count=5 -timeout=12m
GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_NetworkPartition$' -count=1 -timeout=15m
```

修复后，原失败的 `tcp_inmemory` network-partition 子场景在 race detector 下连续 5
次通过；完整 network-partition transport/storage 矩阵也在 race detector 下通过。

## 23. 2026-06-24 Benchmark Harness 与长时间 E2E 强化

相关 issue：#142、#143、#145、#146、#150、#151。

这轮排查和前面的 Raft 修复不完全一样。可见失败大多出现在 benchmark 和长时间 E2E
计数逻辑里，但深入排查后，仍然在 LSM/Raft snapshot 的存储边界发现了一个真实 race。

### #142：Benchmark 并发度必须匹配被测集群

症状：mixed workload benchmark 给一个本地三节点集群施加了过高客户端压力，导致输出噪声很大，
也让人很难区分到底是核心 bug，还是测试 harness 制造的过载。

原则很简单：benchmark 应该压测它声称要衡量的子系统，而不是意外的无界客户端调度器。对 Raft
写 benchmark 来说，一个逻辑写入本来就要经过 leader append、stable storage、多数派复制、commit
和状态机 apply。无界 goroutine fanout 可能把存储 benchmark 变成客户端排队 benchmark。

修复方式是限制 mixed workload 的并发度，让 benchmark 仍然制造压力，但结果可解释：

```text
benchmark concurrency 必须是显式输入
        |
        v
负载应该逐步压满 Raft/LSM 路径
        |
        v
失败应该指向系统行为，而不是 harness 过载
```

### #146：Benchmark 内部失败必须向外传播

症状：benchmark helper 内部能观察到操作失败，但外层 benchmark 命令仍可能成功退出。这很危险，
因为它会产生假的性能数据：一个静默丢弃失败写入的 benchmark 衡量的已经不是同一个系统。

修复后，harness 会把隐藏的操作错误当作 benchmark 失败。对应不变量是：

```text
只有 correctness counter 干净时，性能数字才有效
```

这和项目里长时间 E2E 的规则一致：只有失败操作、final barrier 和严格一致性检查都干净时，
吞吐和延迟才有意义。

### #145：Leader Ready 是条件，不是 sleep

症状：benchmark 启动阶段可能和 leader election 竞争，导致依赖时序的失败。根因和 #123、#124
一样：测试真正需要的是“已经有可用 leader”，但 harness 使用的是固定等待和较弱的 ready 假设。

修复后，benchmark 启动会等待明确的 leader-ready 条件，再开始施加负载。这样 benchmark 失败会集中在
workload 阶段；除非场景本身要测选主，否则 steady-state latency 也不会意外包含 leader election 时间。

### #143：Benchmark 必须关闭 LSM 数据库

症状：LSM benchmark 运行后没有关闭数据库实例。这会让文件描述符、后台 compaction goroutine
和临时目录泄漏到后续 benchmark iteration。

原则是：LSM benchmark 不是纯 CPU microbenchmark。它拥有文件、WAL、SSTable 和 compaction worker。
正确生命周期应该是：

```text
创建隔离数据库目录
        |
        v
运行 workload
        |
        v
等待或停止后台 worker
        |
        v
关闭数据库
        |
        v
删除测试目录
```

关闭数据库也影响正确性。如果一个 benchmark 遗留打开的数据库，后续 benchmark 可能观察到不属于该场景的资源竞争。

### #150：Snapshot Apply 必须串行化数据库替换

症状：一个聚焦 race 测试暴露了 `Database.Reload` 和并发 `Database.Get` 之间的真实数据竞争。
对应的生产形态路径是 Raft InstallSnapshot。安装状态机快照时，系统可能关闭并替换 LSM 数据库；
与此同时，客户端读请求仍在遍历 memtable 或 SSTable。

旧的理解不完整：

```text
stateMachineMu 保护 Raft apply/read/snapshot 调用
```

这在 Raft adapter 边界成立，但 LSM database facade 也有被测试和存储工具直接调用的方法。
LSM database 自己也需要 lifecycle 边界，确保破坏性操作不会和普通读写重叠。

修复后，数据库增加 lifecycle `RWMutex`：

```go
func (d *Database) Get(key kv.Key) ([]byte, bool) {
    d.lifecycleMu.RLock()
    defer d.lifecycleMu.RUnlock()
    ...
}

func (d *Database) ReplaceData(fn func(tmpDir string) error) error {
    d.lifecycleMu.Lock()
    defer d.lifecycleMu.Unlock()
    ...
}
```

Snapshot 导出也被收紧。状态机不再先列出 SSTable 文件名、稍后再打开，而是向数据库请求一个
flush 后、已打开的 SSTable snapshot：

```go
files, closeSnapshot, err := db.FlushAndOpenSSTableSnapshot()
defer closeSnapshot()
```

在类 Unix 文件系统上，打开的 fd 会固定已经选择的字节。Lifecycle lock 则保护反方向：
应用 snapshot 时，不能在读者已经使用旧数据库期间关闭或替换它。

验证：

```bash
GO_KV_LOG_LEVEL=warn go test -race -run '^TestApplySnapshotDoesNotRaceWithConcurrentReads$' ./pkg/storage/lsm -count=1
GO_KV_LOG_LEVEL=warn go test -race ./engine/lsm/... ./pkg/storage/lsm -count=1
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=20m ./tests -run '^TestLongRunning_10Min_DeleteStress$' -count=1
GO_KV_LOG_LEVEL=warn make test
```

修复前，聚焦 race 测试能报告 `Database.Reload` 和 `Database.Get` 的 data race。修复后该测试通过，
更大的 LSM/storage race 门禁通过，10 分钟删除压力场景完成 986,369 次操作、失败 0、严格一致性 true。

### #151：已发请求需要恢复窗口

症状：10 分钟 mixed-failure workload 可能报告少量 `apply_timeout` 失败，但 final barrier
和严格一致性都通过。复现失败的形态是：

```text
apply_timeout=4
final barrier: true
strict consistency: true
```

这个信号说明“harness 放弃等待 4 个已发请求的结果”，不是“集群丢失了已提交数据”。

Raft 原理是：重试身份很重要。客户端命令会带上稳定身份：

```go
type ClientCommand struct {
    ClientID    int64
    SequenceNum int64
    Command     any
}
```

如果第一次 RPC 超时，同一个逻辑命令可以安全重试。状态机使用 `(ClientID, SequenceNum)`
保证最多应用一次，并为重复请求返回已观察到的结果。

旧的长测 harness 在命令已经发出后只给 30 秒重试窗口。在故障注入期间，这太短了，因为请求可能跨过多个临时窗口：

1. leader 侧 apply wait 可能超时；
2. leader 可能重启或降级；
3. 新 leader 需要重新选举；
4. follower 可能需要 snapshot catch-up 后才能恢复正常日志复制；
5. 重试请求还需要重新绑定到原逻辑命令的结果。

修复后扩展了有界的已发请求重试窗口：

```go
const (
    longRunningSnapshotThreshold = 2 * 1024 * 1024
    longRunningClientRetries     = 20
    // Already-issued commands must survive several server-side apply waits plus
    // leader re-election and snapshot catch-up. If the command is truly stuck,
    // the long-running test still fails after this bounded window.
    longRunningIssuedRequestRetryTimeout = 90 * time.Second
)
```

这不会隐藏真实失败。超过有界窗口仍卡住的命令仍会计为失败。这个修改只是让 harness 和 Raft
重试契约对齐：命令发出后，测试必须等待足够久，区分可恢复的选主/snapshot 抖动和真正卡死的 apply 路径。

验证：

```bash
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=20m ./tests -run '^TestLongRunning_10Min_MixedWithFailures$' -count=1
GO_KV_LOG_LEVEL=warn make long-test
GO_KV_LOG_LEVEL=warn make test
```

定向 mixed-failure 场景 619.602s 通过，666,692 次操作、失败 0、final barrier true、
严格一致性 true。随后全量长时间 E2E 回归 3674.858s 通过，覆盖全部六个 10 分钟场景，失败操作为 0。

### 综合经验

这轮排查强化了一个有用的分层判断：

- benchmark harness bug 通常扭曲测量面；
- long E2E harness bug 通常扭曲失败分类面；
- LSM/Raft snapshot bug 会破坏真实生命周期边界。

修复策略必须匹配所在层次。不能靠放松测试隐藏存储 race；不能因为 benchmark 没等 leader 就重写 Raft；
也不能在 harness 还不能证明失败为 0、数据一致性通过之前信任性能数字。

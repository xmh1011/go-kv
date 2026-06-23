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

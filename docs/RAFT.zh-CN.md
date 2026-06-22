# Raft 设计

English version: [RAFT.md](RAFT.md)

本文解释 `go-kv` 中 Raft 的实现方式，重点是代码模块、请求流转和必须保持的正确性不变量。

相关源码：

- `raft/raft.go`
- `raft/election.go`
- `raft/replication.go`
- `raft/snapshot.go`
- `pkg/param/*.go`
- `pkg/storage/storage.go`
- `pkg/transport/transport.go`

## 1. 为什么项目需要 Raft

单机 key-value store 的 API 很简单，但分布式 key-value store 要解决一个核心难题：多台机器必须对同一串写入顺序达成一致。

Raft 通过 leader 解决这个问题：

1. 客户端把写请求发给 leader；
2. leader 将每个写请求追加到自己的日志；
3. follower 复制 leader 的日志；
4. 当多数派保存某条日志后，该日志可以提交；
5. 每个节点按日志顺序应用已提交条目。

只要健康节点按相同顺序应用相同的已提交日志，它们的键值状态机会收敛到相同数据。

## 2. 模块布局

| 文件 | 主要职责 |
|---|---|
| `raft.go` | Raft 结构体、生命周期、客户端请求、proposal 批处理、ReadIndex 辅助逻辑。 |
| `election.go` | 选举超时、PreVote、RequestVote RPC、切换为 leader。 |
| `replication.go` | AppendEntries RPC、follower 日志修复、leader 进度、提交和应用。 |
| `snapshot.go` | 本地快照、InstallSnapshot RPC、日志压缩。 |

Raft 依赖两个抽象：

- `storage.Storage`：持久化 term/vote/log/snapshot；
- `transport.Transport`：发送和接收 RPC。

这样共识逻辑不依赖具体磁盘和网络实现。

## 3. 节点状态

每个节点都处于以下状态之一：

```go
const (
    Follower State = iota
    Candidate
    Leader
    Dead
)
```

新手理解：

- **Follower** 被动等待 leader；
- **Candidate** 向其他节点请求投票；
- **Leader** 接收客户端写入并复制日志；
- **Dead** 已停止，不再参与集群。

当前状态使用 atomic 存储，便于快速无锁检查。

## 4. 持久化状态和易失状态

必须崩溃后恢复的状态：

- `currentTerm`：节点见过的最大任期；
- `votedFor`：当前任期投票给谁；
- 日志条目；
- 快照。

Raft 论文通常把 `commitIndex` 作为易失状态。本实现额外把它写入
`param.HardState`，这是面向 LSM 持久状态机的恢复增强：节点重启时会恢复
durable commit index，并调度 apply 尚未推进到 `lastApplied` 的 committed log。

运行中可重建的状态：

- `commitIndex`：节点运行期间已知已提交的最高日志索引；
- `lastApplied`：已应用到状态机的最高日志索引；
- `nextIndex[peer]`：leader 下次发送给某个 follower 的日志索引；
- `matchIndex[peer]`：某个 follower 已确认保存的最高日志索引；
- ReadIndex/lease read 使用的 leader ack 时间。

基本规则：

```text
commitIndex >= lastApplied
```

`lastApplied` 绝不能超过状态机真实应用的位置。

## 5. 选举流程

选举逻辑在 `raft/election.go`。

正常流程：

```text
Follower 长时间没收到 leader 消息
        |
        v
随机选举超时到期
        |
        v
发起 PreVote
        |
        v
PreVote 赢了，增加 term 并正式选举
        |
        v
发送 RequestVote RPC
        |
        v
获得多数派
        |
        v
transitionToLeader
```

PreVote 很重要。它可以避免网络隔离后恢复的旧节点无意义地提高 term，从而干扰健康 leader。

## 6. 投票安全

Follower 只在满足下面条件时投票：

1. candidate term 有效；
2. 当前 term 还没投给别人；
3. candidate 日志至少和自己一样新。

日志新旧检查保护已提交条目。缺少已提交条目的 candidate 不应该成为 leader。

## 7. Leader 初始化

节点成为 leader 后会初始化复制进度：

```text
nextIndex[peer]  = lastLogIndex + 1
matchIndex[peer] = 0
```

随后 leader 发送心跳。心跳就是不带 entries 的 AppendEntries。它用于阻止 follower 发起选举，也用于读请求的 leader 确认。

Leader 还会在自己的任期追加一条 no-op entry。这是 Raft 中常见的安全技巧：leader 不能只靠复制数量直接提交旧任期 entry。只有当新任期 no-op 被提交后，它之前的旧任期 entry 才能被间接提交。

```text
transitionToLeader
        |
        v
初始化 nextIndex 和 matchIndex
        |
        v
追加当前任期 no-op
        |
        v
复制 no-op，多数派确认后推进 commitIndex
```

no-op 不修改用户数据，它是 leadership barrier，用来保证 leader 切换后 commit 和 apply 进度能正确恢复。

## 8. 写路径

写请求从 `ClientRequest` 进入 Raft：

```text
ClientRequest
        |
        v
preHandleClientRequest 检查 leader 和重复请求
        |
        v
必要时包装为 ClientCommand
        |
        v
CommitClient
        |
        v
proposalCh
        |
        v
processBatch
        |
        v
store.AppendEntries
        |
        v
广播 AppendEntries
        |
        v
等待 apply 结果
```

`ClientCommand` 保存：

- `ClientID`
- `SequenceNum`
- 真实命令 payload

这让客户端重试具备幂等性。即使同一个逻辑请求在日志中出现多次，状态机也只应用一次。

## 9. Follower 上的 AppendEntries

`AppendEntries` 做三件事：

1. 心跳；
2. 追加新日志；
3. 修复 follower 与 leader 不一致的日志。

Follower 会检查：

- leader term 是否过期；
- previous log index/term 是否匹配；
- 冲突的本地日志是否需要截断；
- 是否追加新的 leader 日志；
- follower `commitIndex` 是否推进到 leader commit。

实现中使用 `appendEntriesMu` 串行化 follower 侧磁盘修改，避免并发 AppendEntries 同时截断和追加日志导致冲突。

同任期收到合法 leader 的 AppendEntries 也很重要。节点不需要等到更高 term 才降级。如果 candidate 或旧 leader 接受了当前任期 leader，就必须转为 follower，刷新 leader hint，并终止旧的客户端等待者。

## 10. Leader 复制进度

Leader 维护两个 map：

```go
nextIndex[peer]  // 下次发送的索引
matchIndex[peer] // 已确认的最高索引
```

成功时：

```text
nextIndex = prevLogIndex + len(entries) + 1
matchIndex = nextIndex - 1
```

失败时，leader 根据冲突信息回退 `nextIndex`。但它不能低于 `matchIndex + 1`，因为已确认进度必须单调。

如果 `nextIndex` 已经落在本地第一条日志之前，说明 follower 需要快照，而不是普通日志补齐。

## 11. 提交规则

Leader 可以在某个日志索引被多数派保存，并且该日志属于当前任期时推进 `commitIndex`。

当前任期限制是 Raft 安全规则，用于避免 leader 仅靠复制数量错误提交旧任期日志。

`commitIndex` 推进后，Raft 会先持久化新的 HardState，再唤醒 apply loop：

```go
oldCommitIndex := r.commitIndex
r.commitIndex = newCommitIndex
if err := r.persistHardStateLocked(); err != nil {
    r.commitIndex = oldCommitIndex
    return false
}
r.startApplyLogsLocked()
```

持久化 commit index 是实现层 guardrail，不改变多数派提交规则；它只保证重启后
不会忘记 durable committed entry 已经提交，从而继续 apply。

## 12. Apply Loop

Apply loop 将已提交日志应用到状态机：

```text
for index in lastApplied+1 .. commitIndex
        |
        v
读取 LogEntry(index)
        |
        v
解包 ClientCommand
        |
        v
如果是已应用的重复客户端命令则跳过
        |
        v
stateMachine.Apply(entry)
        |
        v
lastApplied = index
        |
        v
通知读等待者和客户端等待者
```

关键不变量：

```text
lastApplied 必须在 Apply 返回之后推进，而不是之前。
```

这对线性一致读很重要。读请求可能在 `lastApplied >= readIndex` 时继续执行，所以 `lastApplied` 必须代表真实状态机进度。

## 13. ReadIndex 和 Lease Read

读请求不需要追加日志，但仍需要 leader 安全性。

读路径：

```text
记录 readIndex = commitIndex
        |
        v
确认 leader 身份
        |
        v
等待 lastApplied >= readIndex
        |
        v
stateMachine.Get(key)
```

支持两种模式：

- `heartbeat`：发送心跳 RPC 确认 leader；
- `lease`：在 lease 时间内复用最近多数派确认。

联合共识期间，leader 确认必须同时满足旧配置多数派和新配置多数派。

## 14. 快照和日志压缩

快照用于限制 Raft 日志大小。

本地快照流程：

```text
日志大小超过阈值
        |
        v
捕获 snapshotIndex = lastApplied
        |
        v
读取 snapshotIndex 的 term
        |
        v
stateMachine.GetSnapshot
        |
        v
store.SaveSnapshot
        |
        v
store.CompactLog(snapshotIndex)
```

InstallSnapshot 流程：

```text
leader 给落后 follower 发送快照
        |
        v
follower 持久化快照
        |
        v
follower 将快照应用到状态机
        |
        v
follower 压缩被覆盖日志
        |
        v
follower 推进 commitIndex 和 lastApplied
```

状态机快照导出/应用需要和普通 apply/read 串行化，因为 LSM 快照可能重写状态机目录。

## 15. 成员变更

成员变更使用 Raft 联合共识。

变更分两阶段：

1. **联合配置**：旧 peer set 和新 peer set 同时生效；
2. **最终配置**：只保留新 peer set。

联合阶段中，提交和 ReadIndex leader 确认都必须同时满足旧配置多数派和新配置多数派。

## 16. 具体状态归属

Raft 论文描述的是逻辑状态。代码里还需要锁、缓存和等待者 map，才能让这些状态被多个 goroutine 安全使用。修改实现前，应先看清楚下面这张 ownership 表。

| 状态 | 主要字段 | 保护方式 | 说明 |
|---|---|---|---|
| 任期和投票 | `currentTerm`、`votedFor` | `r.mu` 加稳定存储写入 | 节点依赖新 term 或 vote 前，必须先持久化。 |
| 节点角色 | `state` | atomic 值，通常在持有 `r.mu` 时变更 | 快速检查可以无锁读取，但状态转换仍要遵守 Raft 锁规则。 |
| 日志边界 | `commitIndex`、`lastApplied`、`cachedLastLogIndex` | `r.mu`；apply 由 `applyMu` 串行化 | `lastApplied` 只能在状态机 apply 后，或被 snapshot 覆盖时推进。 |
| Follower 日志修改 | follower 上的 `store.TruncateLog`、`store.AppendEntries` | `appendEntriesMu` | 防止并发 AppendEntries 交错执行 truncate 和 append。 |
| Leader 复制进度 | `nextIndex`、`matchIndex`、`lastAck` | `r.mu` | `matchIndex` 和成功路径上的 `nextIndex` 必须单调前进。 |
| 状态机 | `stateMachine.Apply`、`Get`、`GetSnapshot`、`ApplySnapshot` | `stateMachineMu` | snapshot install 可能重写 LSM 目录，不能与 apply 或 read 重叠。 |
| 客户端去重 | `clientSessions`、`pendingClientRequests`、`pendingLogClients` | `r.mu` | 保证客户端重试不会被重复执行。 |
| 读/写等待者 | `lastAppliedCond`、`notifyApply` | `r.mu` | 在 apply 推进后唤醒 ReadIndex 和写请求等待者。 |

有些字段不是 Raft 论文里的新参数，而是实现层面的簿记：`cachedLastLogIndex`、`applyMu`、`appendEntriesMu`、`stateMachineMu`、`notifyApply` 和 pending client maps。它们不是新的共识规则，而是为了在 Go 并发实现里保持论文规则。

## 17. AppendEntries 分阶段流程

Follower 侧 `AppendEntries` 被拆成多个阶段，目的是让磁盘 I/O 不长期占用 Raft 主锁：

| 阶段 | 锁状态 | 工作 |
|---|---|---|
| Phase 0 | 持有 `appendEntriesMu` | 串行化本 RPC 和其他 follower 日志修改。 |
| Phase 1 | 短时间持有 `r.mu` | 检查 term，更新 follower 状态，重置选举计时器，处理心跳快路径。 |
| Phase 2 | 不持有 `r.mu`，仍持有 `appendEntriesMu` | 读取本地日志，检测冲突，截断冲突日志，追加新日志。 |
| Phase 3 | 短时间持有 `r.mu` | 确认 term 没变，更新 `cachedLastLogIndex`，推进 follower commit index。 |

这种结构的关键原因是 truncate/append 窗口。如果没有 `appendEntriesMu`，一个 goroutine 可能正在 truncate，另一个 goroutine 或 apply loop 同时读取同一段日志。因此 apply 路径在收集 committed entry 时，也要先拿 `appendEntriesMu`。

冲突处理遵循 Raft 论文 Section 5.3：

```text
for each incoming entry:
    if local entry is missing:
        append incoming entries from here
    if local term differs:
        truncate from this index
        append incoming entries from here
    otherwise:
        keep the existing matching entry
```

这很重要。成功通过一致性检查后，如果无脑追加，可能重复写入已经正确的条目；如果过早 truncate，又可能短暂删除 apply loop 马上要读取的 committed entry。

## 18. Leader 复制内部细节

Leader 会针对每个 follower 在普通日志复制和快照安装之间做选择：

```text
nextIndex[peer] < first local log index
        |
        v
send snapshot

otherwise
        |
        v
send AppendEntries with at most MaxEntriesPerAppendEntries entries
```

`prepareAppendEntriesArgs` 会区分三类情况：

1. 需要的 previous log index 已经被当前 snapshot 覆盖，此时 follower 需要 InstallSnapshot RPC。
2. 本地 store 出现稀疏缺口或尾部暂不可用，此时 leader 刷新 `cachedLastLogIndex`、收紧 `nextIndex`，稍后重试，而不是误判成 follower 必须安装 snapshot。
3. follower 只是落后，但 leader 仍然保留所需日志，此时发送有上限的日志批次。

AppendEntries reply 也必须检查 term。如果响应属于旧 leader term，或者本节点已经不是 leader，就忽略。如果响应携带更高 term，本节点必须 step down。成功响应只能让进度前进：

```text
newNextIndex  = prevLogIndex + len(entries) + 1
newMatchIndex = newNextIndex - 1
```

失败响应会利用冲突信息回退 `nextIndex`，但不能低于 `matchIndex + 1`。

成功响应如果推进了 `commitIndex`，leader 会把它当作新的复制信号。Follower 需要尽快收到新的 `LeaderCommit`；如果只等下一次 heartbeat，在重启或快照 churn 下会放大 apply 和 ReadIndex 延迟。

## 19. Apply、客户端重试和 ReadIndex 内部细节

Apply 路径分两段。

第一段，`fetchEntriesToApply` 收集 committed log entries：

```text
hold appendEntriesMu
hold r.mu
for i = lastApplied+1 .. commitIndex:
    read store.GetEntry(i)
    if entry is missing and a snapshot covers i:
        advance lastApplied to snapshot.LastIncludedIndex
        continue
    if entry is missing and no snapshot covers i:
        fatal, because committed data is unavailable
release locks
```

第二段，`dispatchEntries` 真正应用条目：

```text
unwrap ClientCommand
if client request already applied:
    complete waiter without applying again
else if config change:
    update Raft membership state
else:
    hold stateMachineMu
    stateMachine.Apply(entry)
    release stateMachineMu
completeAppliedEntry(index)
```

`completeAppliedEntry` 是普通日志推进 `lastApplied` 的唯一位置。它还会更新客户端去重状态，并通知 `waitForAppliedLog` 注册的等待者。

ReadIndex 依赖这条不变量：

```text
lastApplied >= readIndex  代表本地状态机已经包含该读请求进入前提交的所有写入。
```

因此，`waitForAppliedLog` 和读等待者在注册等待或超时后，都必须在 `r.mu` 下重新检查 `lastApplied`。通知可能与等待者注册并发发生，重新检查可以关闭这个竞态窗口。

## 20. Snapshot 和 Compaction 的锁顺序

Snapshot 连接了 Raft 日志层和 LSM 状态机层。代码通过固定锁顺序避免导出某一时刻的数据，却同时 apply 或安装另一时刻的数据。

本地快照创建：

```text
hold stateMachineMu
hold r.mu
check threshold and isSnapshotting
capture snapshotIndex = lastApplied
read term at snapshotIndex
mark isSnapshotting
release r.mu
export stateMachine.GetSnapshot while stateMachineMu is still held
release stateMachineMu
async: SaveSnapshot
async: hold appendEntriesMu then r.mu
async: publish snapshot reference and CompactLog(snapshotIndex)
```

Follower 安装快照：

```text
hold r.mu briefly for term and stale-snapshot checks
release r.mu
create snapshot object
hold stateMachineMu
recheck term/index under r.mu
SaveSnapshot
stateMachine.ApplySnapshot
hold appendEntriesMu
CompactLog(snapshot.LastIncludedIndex)
hold r.mu
advance snapshot, commitIndex, lastApplied, cachedLastLogIndex
broadcast lastAppliedCond
```

最重要的规则是：只有在覆盖相应索引的 snapshot 已经持久化或安装后，才允许压缩日志。apply loop 只有在存储中的 snapshot 覆盖缺失 entry 时，才可以跳过该 entry。

## 21. 并发规则

Raft 是并发系统。主要规则：

- `r.mu` 保护 term、vote、index、peer set、leader 进度等 Raft 状态；
- `appendEntriesMu` 串行化 follower 日志修改；
- `stateMachineMu` 串行化状态机 apply、快照导出和快照安装；
- `lastAppliedCond` 唤醒等待 apply 进度的读请求；
- `snapshotWG`、`applyWG` 和 apply waiters 在 shutdown 时被收束；
- `Stop()` 还会等待 `stateMachineMu` 和 `appendEntriesMu` 保护的 in-flight
  临界区，因为 RPC handler 可能在较慢的存储或状态机 I/O 期间释放 `r.mu`。

修改代码时，应尽量保持这些锁顺序和生命周期规则，而不是临时加锁掩盖问题。

## 22. 需要理解的故障场景

实现显式处理这些情况：

- 过期 RequestVote 或 AppendEntries term；
- 更高 term 响应导致 leader step down；
- follower 日志与 leader 冲突；
- follower 落后到已压缩日志之前，需要快照；
- compaction 后本地日志缺口；
- 客户端重复重试；
- 已被快照覆盖、但本地日志不存在的 apply 位置；
- shutdown 时仍有客户端等待 apply 结果。

## 23. 修改 Raft 前的检查清单

修改 Raft 行为前先问：

1. 会不会让 `lastApplied` 早于真实状态机 apply？
2. 会不会在没有确认 leader 身份时服务读请求？
3. 会不会破坏 `matchIndex` 或 `nextIndex` 的单调进度？
4. 会不会压缩未被快照覆盖的日志？
5. 会不会让重复客户端命令应用两次？
6. 联合共识期间是否仍然正确？
7. 是否通过 race 测试和真实多节点 E2E？

这些问题能提前发现大部分正确性回归。

近期具体故障和修复过程见 [BUG_FIX_RETROSPECTIVE.zh-CN.md](BUG_FIX_RETROSPECTIVE.zh-CN.md)。

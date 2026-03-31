# Raft 共识算法实现详解

本文档详细介绍 `go-kv` 项目中 Raft 共识算法的实现逻辑和关键设计决策。

> 相关源码：`raft/raft.go`、`raft/election.go`、`raft/replication.go`、`raft/snapshot.go`

---

## 为什么选择 Raft

分布式系统需要共识算法来解决一个基本问题：**多个节点如何就一系列操作的顺序达成一致**。经典的 Paxos 算法虽然理论上优雅，但以"难以理解和正确实现"著称——Chubby 的作者 Mike Burrows 曾说："世界上只有一种共识协议，就是 Paxos，其他所有协议要么是 Paxos 的变体，要么是错误的。"

Raft 由 Diego Ongaro 和 John Ousterhout 在 2014 年的论文 *"In Search of an Understandable Consensus Algorithm"* 中提出，其设计目标就是**可理解性**。它通过将共识问题分解为三个相对独立的子问题——领导者选举、日志复制、安全性——使得实现者可以逐个攻破。

`go-kv` 选择 Raft 的原因：
- **可实现性**：Raft 论文提供了足够的实现细节，适合从零构建
- **工业验证**：etcd、CockroachDB、TiKV 等生产系统都基于 Raft
- **与 LSM 的天然契合**：Raft 的日志复制（追加写入）与 LSM 树的写优化特性完美匹配

---

## 目录

- [1. 整体架构](#1-整体架构)
- [2. 核心数据结构](#2-核心数据结构)
- [3. 状态机与生命周期](#3-状态机与生命周期)
- [4. 领导者选举](#4-领导者选举)
- [5. 日志复制](#5-日志复制)
- [6. 日志提交与状态机应用](#6-日志提交与状态机应用)
- [7. 线性一致性读](#7-线性一致性读)
- [8. 动态成员变更](#8-动态成员变更)
- [9. 快照与日志压缩](#9-快照与日志压缩)
- [10. 并发模型与锁优化](#10-并发模型与锁优化)
- [11. 性能优化](#11-性能优化)

---

## 1. 整体架构

Raft 模块是 `go-kv` 的核心，负责在多个节点之间维护数据的一致性。整体架构如下：

```
                        ┌─────────────────────────────────────────────┐
                        │              Raft Node                       │
                        │                                              │
   Client Request ──────►  ClientRequest()                             │
                        │       │                                      │
                        │       ├── Read? ──► handleLinearizableRead() │
                        │       │                    │                  │
                        │       │              confirmLeadership()      │
                        │       │                    │                  │
                        │       │              performReadAfterApply()  │
                        │       │                    │                  │
                        │       │              stateMachine.Get()       │
                        │       │                                      │
                        │       └── Write? ─► Submit() ──► proposalCh  │
                        │                                    │         │
                        │                           proposalBatcher()  │
                        │                                    │         │
                        │                           processBatch()     │
                        │                              │      │        │
                        │                     store.Append  broadcast   │
                        │                              │      │        │
                        │                     waitForAppliedLog()       │
                        │                              │               │
                        │                        applyLogs()           │
                        │                              │               │
                        │                   stateMachine.Apply()        │
                        └─────────────────────────────────────────────┘
```

### 模块划分

| 文件 | 职责 |
|------|------|
| `raft.go` | 核心结构体、主循环、客户端请求处理、Proposal 批处理 |
| `election.go` | Pre-Vote + 正式选举、RequestVote RPC 处理 |
| `replication.go` | 日志复制、AppendEntries RPC（三阶段锁）、提交推进、日志应用 |
| `snapshot.go` | 快照生成、InstallSnapshot RPC、日志压缩 |

### 外部依赖接口

Raft 模块通过三个接口与外部系统交互：

```go
// 持久化存储 —— 保存 HardState、日志条目、快照
type Storage interface {
    SetState(state HardState) error       // 持久化 currentTerm + votedFor
    GetState() (HardState, error)         // 恢复 HardState
    AppendEntries(entries []LogEntry) error
    GetEntry(index uint64) (*LogEntry, error)
    TruncateLog(fromIndex uint64) error   // 删除 fromIndex 到末尾的条目
    CompactLog(upToIndex uint64) error    // 删除 upToIndex 之前的条目
    SaveSnapshot(snapshot *Snapshot) error
    ReadSnapshot() (*Snapshot, error)
    FirstLogIndex() (uint64, error)
    LastLogIndex() (uint64, error)
    LogSize() (int, error)
    Close() error
}

// 状态机 —— 应用已提交的命令
type StateMachine interface {
    Apply(entry LogEntry) any             // 应用命令，返回结果
    Get(key string) (string, error)       // 只读查询（线程安全）
    GetSnapshot() ([]byte, error)         // 序列化状态机
    ApplySnapshot(snapshot []byte) error  // 从快照恢复
}

// 网络传输 —— 节点间 RPC 通信
type Transport interface {
    SendRequestVote(target string, req, resp) error
    SendAppendEntries(target string, req, resp) error
    SendInstallSnapshot(target string, req, resp) error
    SendClientRequest(target string, req, resp) error
}
```

---

## 2. 核心数据结构

### 2.1 Raft 结构体

```go
type Raft struct {
    // ===== 并发控制 =====
    mu              sync.Mutex      // 保护所有可变 Raft 状态
    appendEntriesMu sync.Mutex      // 串行化 Follower 侧 AppendEntries 处理
    state           atomic.Int32    // 无锁原子读取节点状态

    // ===== 身份与配置 =====
    id               int
    peerIDs          []int          // 当前配置 (C_old)
    newPeerIDs       []int          // 新配置 (C_new)，联合共识期间使用
    inJointConsensus bool
    knownLeaderID    int

    // ===== 持久化状态（通过 store 持久化） =====
    currentTerm uint64
    votedFor    int                 // -1 表示未投票

    // ===== 日志与提交 =====
    commitIndex        uint64       // 已知被多数派确认的最高日志索引
    lastApplied        uint64       // 已应用到状态机的最高日志索引
    cachedLastLogIndex uint64       // 缓存的最后日志索引，避免重复磁盘查询
    commitChan         chan CommitEntry
    lastAppliedCond    *sync.Cond   // 用于等待 lastApplied 赶上目标索引

    // ===== Leader 易失性状态 =====
    nextIndex  map[int]uint64       // 每个 Follower 的下一个要发送的日志索引
    matchIndex map[int]uint64       // 每个 Follower 已确认复制的最高日志索引
    lastAck    map[int]time.Time    // 每个 Follower 的最后 ACK 时间

    // ===== 选举 =====
    electionResetEvent     time.Time
    electionTimeout        time.Duration  // 基础超时（默认 500ms）
    heartbeatTimeout       time.Duration  // 心跳间隔（默认 100ms）
    currentElectionTimeout time.Duration  // 随机化后的超时

    // ===== 快照 =====
    snapshot          *Snapshot
    isSnapshotting    bool
    snapshotThreshold int

    // ===== Proposal 批处理 =====
    proposalCh chan proposalRequest  // 缓冲 256

    // ===== ReadIndex / Lease Read =====
    lastLeadershipConfirm time.Time
    leadershipCacheTime   time.Duration
    leaseUntil            time.Time
    leaseDuration         time.Duration
    readIndexMode         ReadIndexMode   // "heartbeat" 或 "lease"
}
```

### 2.2 RPC 参数结构

```go
// 日志条目
type LogEntry struct {
    Command any      // KVCommand、ConfigChangeCommand 或 []byte
    Term    uint64
    Index   uint64
}

// 投票请求
type RequestVoteArgs struct {
    Term         uint64
    CandidateID  int
    LastLogIndex uint64
    LastLogTerm  uint64
    PreVote      bool    // Pre-Vote 扩展标志
}

// AppendEntries 请求（心跳 + 日志复制共用）
type AppendEntriesArgs struct {
    Term         uint64
    LeaderID     int
    PrevLogIndex uint64   // 紧接在新条目之前的日志索引
    PrevLogTerm  uint64   // PrevLogIndex 对应的任期
    Entries      []LogEntry
    LeaderCommit uint64
}

// AppendEntries 响应（含快速回退优化）
type AppendEntriesReply struct {
    Term          uint64
    Success       bool
    ConflictIndex uint64  // 快速回退：第一个不匹配的索引
    ConflictTerm  uint64  // 快速回退：冲突点的任期
}
```

---

## 3. 状态机与生命周期

### 3.1 状态定义

```go
type State int
const (
    Follower  State = iota  // 0 —— 被动接收请求
    Candidate               // 1 —— 正在参与选举
    Leader                  // 2 —— 处理客户端请求，复制日志
    Dead                    // 3 —— 节点已关闭
)
```

状态存储在 `atomic.Int32` 中，允许无锁读取（例如 `Submit()` 的快速路径检查）。

### 3.2 主循环 `Run()`

```go
func (r *Raft) Run() {
    ticker := time.NewTicker(r.heartbeatTimeout)
    for {
        select {
        case <-r.shutdownChan:
            return
        case <-ticker.C:
            r.mu.Lock()
            // 只有 Follower/Candidate 检查选举超时
            if r.getState() != Follower && r.getState() != Candidate {
                r.mu.Unlock()
                continue
            }
            if time.Since(r.electionResetEvent) > r.currentElectionTimeout {
                r.mu.Unlock()
                go r.startElection()  // 在 goroutine 中启动，不阻塞主循环
            } else {
                r.mu.Unlock()
            }
        }
    }
}
```

关键设计：
- Tick 频率等于 `heartbeatTimeout`（默认 100ms），足够检测选举超时
- 选举在独立 goroutine 中进行，确保 `Run()` 永远不被阻塞
- `Stop()` 通过关闭 `shutdownChan` 来终止循环

### 3.3 节点初始化

```go
func NewRaft(...) *Raft {
    r := &Raft{...}
    r.setState(Follower)

    // 从持久化存储恢复状态
    hardState, _ := store.GetState()
    r.currentTerm = hardState.CurrentTerm
    r.votedFor = int(hardState.VotedFor)

    // 初始化缓存
    lastIdx, _ := store.LastLogIndex()
    r.cachedLastLogIndex = lastIdx

    r.lastAppliedCond = sync.NewCond(&r.mu)
    return r
}
```

---

## 4. 领导者选举

### 4.1 Pre-Vote 机制

#### 为什么需要 Pre-Vote

考虑以下场景：节点 C 因网络分区与集群隔离。在隔离期间，C 不断发起选举，每次都递增 `currentTerm`，但因为无法联系到多数派而持续失败。当网络恢复时，C 的 `currentTerm` 可能已经远大于集群当前的 Term。此时 C 发出的 RequestVote 会迫使 Leader 和其他 Follower 退回 Follower 状态（因为收到了更高的 Term），导致一次**不必要的 Leader 切换**。更糟糕的是，C 的日志可能是旧的，它无法赢得选举，但已经破坏了集群的稳定性。

Pre-Vote 扩展（来自 Diego Ongaro 的博士论文 §9.6）解决了这个问题：在正式递增 Term 之前，先进行一轮"预投票"，**不修改任何持久化状态**。只有当多数派预同意时，才真正发起选举。这样，一个隔离恢复的节点会在 Pre-Vote 阶段就被拒绝，不会干扰集群。

go-kv 实现了 Raft 的 Pre-Vote 扩展，防止网络分区恢复后的选举干扰。

选举分为两个阶段：

```
Phase 1: Pre-Vote（预投票）
  ┌──────────────────────────────────────────────┐
  │ 不修改 currentTerm，不持久化                    │
  │ 询问：如果我发起选举(term+1)，你会投票给我吗？   │
  │ 如果多数派同意 → 进入 Phase 2                   │
  │ 如果失败 → 静默放弃，不干扰集群                  │
  └──────────────────────────────────────────────┘

Phase 2: 正式选举
  ┌──────────────────────────────────────────────┐
  │ 递增 currentTerm，持久化 HardState             │
  │ 投票给自己，向所有节点发送 RequestVote           │
  │ 如果多数派投票 → 成为 Leader                    │
  │ 如果超时 → 回退为 Follower                     │
  └──────────────────────────────────────────────┘
```

### 4.2 选举流程详解

**Step 1: Pre-Vote 发起** (`startElection`)

```go
func (r *Raft) startElection() {
    r.mu.Lock()
    // 只有 Follower/Candidate 可以发起选举
    preVoteTerm := r.currentTerm + 1  // 不实际修改 currentTerm
    lastLogIndex := r.cachedLastLogIndex
    lastLogTerm, _ := r.getLogTerm(lastLogIndex)
    r.mu.Unlock()

    voteChan := r.broadcastVoteRequests(preVoteTerm, lastLogIndex, lastLogTerm, true)
    go r.handleElectionResult(voteChan, preVoteTerm, true)
}
```

**Step 2: 投票计数** (`handleElectionResult`)

```go
type electionContext struct {
    inJoint     bool
    oldPeers    []int
    newPeers    []int
    majorityOld int     // len(oldPeers)/2 + 1
    majorityNew int     // len(newPeers)/2 + 1
    votesOldConfig int  // 初始 1（自己的票）
    votesNewConfig int  // 如果自己在新配置中，初始 1
}
```

赢得选举的条件：
- **普通模式**：`votesOldConfig >= majorityOld`
- **联合共识模式**：`votesOldConfig >= majorityOld AND votesNewConfig >= majorityNew`（必须同时获得新旧配置的多数派）

**Step 3: 正式选举** (`startRealElection`)

Pre-Vote 成功后触发：

```go
func (r *Raft) startRealElection() {
    r.mu.Lock()
    r.initializeCandidateState()  // term++, votedFor=self, 持久化
    r.mu.Unlock()
    voteChan := r.broadcastVoteRequests(savedTerm, ..., false)
    go r.handleElectionResult(voteChan, savedTerm, false)
}
```

**Step 4: 成为 Leader** (`transitionToLeader`)

```go
func (r *Raft) transitionToLeader(electionTerm uint64) {
    r.mu.Lock()
    r.setState(Leader)
    r.initLeaderState()      // nextIndex = lastLogIndex + 1, matchIndex = 0
    r.startHeartbeat()       // 启动心跳 goroutine
    go r.proposalBatcher()   // 启动 Proposal 批处理 goroutine
    r.mu.Unlock()
}
```

### 4.3 RequestVote 处理

Follower 收到 RequestVote 时的决策逻辑：

```
收到 RequestVote(term, candidateID, lastLogIndex, lastLogTerm, preVote)
  │
  ├── preVote == true?
  │     │
  │     ├── term < currentTerm → 拒绝
  │     ├── Leader 仍然活跃（electionResetEvent 在选举超时内）→ 拒绝（Sticky Leader）
  │     └── 检查日志新鲜度 → 通过则授予 PreVote（不持久化，不重置计时器）
  │
  └── preVote == false?
        │
        ├── term < currentTerm → 拒绝
        ├── term > currentTerm → becomeFollower(term)
        └── 三个条件全部满足才投票：
              1. canVote: votedFor == -1 或 votedFor == candidateID
              2. inConfig: 候选人在当前配置中
              3. logUpToDate: 候选人日志至少与自己一样新
```

### 4.4 日志新鲜度比较

按照 Raft 论文 Section 5.4.1：

```go
func (r *Raft) isLogUpToDate(candidateLastLogIndex, candidateLastLogTerm uint64) bool {
    myLastLogTerm, _ := r.getLogTerm(r.cachedLastLogIndex)
    // 1. 比较最后一条日志的任期
    if candidateLastLogTerm > myLastLogTerm { return true }
    if candidateLastLogTerm < myLastLogTerm { return false }
    // 2. 任期相同，比较日志长度
    return candidateLastLogIndex >= r.cachedLastLogIndex
}
```

---

## 5. 日志复制

### 5.1 Leader 侧：发送日志

**心跳与日志复制共用 `sendAppendEntries`**。这是 Raft 的一个精妙设计——心跳就是空的 AppendEntries RPC，两者共用相同的处理逻辑，既简化了实现，又确保了心跳也能传递最新的 `leaderCommit` 来推进 Follower 的提交进度。

```
sendAppendEntries(peerID)
  │
  ├── determineReplicationAction(peerID)
  │     ├── 不是 Leader → actionDoNothing
  │     ├── nextIndex[peer] < firstLogIndex → actionSendSnapshot（Follower 落后太多）
  │     └── 其他 → actionSendLogs
  │
  ├── actionSendLogs:
  │     prepareAppendEntriesArgs(peerID)
  │       ├── prevLogIndex = nextIndex[peerID] - 1
  │       ├── prevLogTerm = getLogTerm(prevLogIndex)
  │       ├── entries = store[nextIndex..min(nextIndex+32-1, lastLogIndex)]
  │       └── 最多 32 条目/RPC (MaxEntriesPerAppendEntries)
  │
  └── actionSendSnapshot:
        sendSnapshot(peerID) → InstallSnapshot RPC
```

**复制成功的处理：**

```go
func (r *Raft) handleSuccessfulAppendEntries(peerID int, args *AppendEntriesArgs) {
    r.nextIndex[peerID] = args.PrevLogIndex + uint64(len(args.Entries)) + 1
    r.matchIndex[peerID] = r.nextIndex[peerID] - 1
    r.updateCommitIndex()  // 尝试推进 commitIndex
}
```

**复制失败的处理（快速回退）：**

```go
func (r *Raft) handleFailedAppendEntries(peerID int, reply *AppendEntriesReply) {
    if reply.ConflictIndex > 0 {
        r.nextIndex[peerID] = reply.ConflictIndex  // 跳过整个冲突任期
    } else {
        r.nextIndex[peerID]--  // 逐个回退
    }
    go r.sendAppendEntries(peerID)  // 立即重试
}
```

### 5.2 Follower 侧：三阶段锁 AppendEntries

#### 为什么需要三阶段锁

传统的 Raft 实现在处理 AppendEntries 时全程持有全局锁（`r.mu`）。这意味着磁盘 I/O（`TruncateLog`、`AppendEntries`）在锁内执行，其他所有 Raft 操作（包括状态机查询、选举超时检查、客户端请求处理）都必须等待磁盘操作完成。

在 `go-kv` 的 LSM 存储引擎下，单次 `AppendEntries` 的磁盘写入可能涉及 WAL flush 和 MemTable promote，延迟在毫秒级。如果心跳间隔为 100ms，而一次磁盘写入耗时 5ms，那么锁内 I/O 就会消耗 5% 的心跳周期，在高并发场景下成为瓶颈。

三阶段锁的核心思想是：**只在修改内存状态时持锁，磁盘 I/O 在锁外执行**。但这引入了新的复杂性——Phase 2（无锁阶段）期间，任期可能变化（例如收到更高 Term 的 RPC），因此 Phase 3 需要重新验证状态的一致性。

这是 go-kv 的核心优化之一。go-kv 将 AppendEntries 处理拆分为三个阶段，最大限度减少锁的持有时间：

```
AppendEntries RPC 处理（三阶段锁）
═══════════════════════════════════════════════════════

Phase 0: 串行化
  appendEntriesMu.Lock()    // 串行化所有 AppendEntries，防止并发 TruncateLog

Phase 1: 短锁 —— 任期检查 + 心跳处理
  r.mu.Lock()
  ├── handleTermAndHeartbeat(): 任期校验、重置选举计时器
  ├── 心跳快速路径（空 entries）：
  │     checkLogConsistency() → updateFollowerCommitIndex() → 返回
  └── 非空 entries：
        捕获 snapshot 引用和 savedTerm
  r.mu.Unlock()

Phase 2: 无锁磁盘 I/O
  ├── checkLogConsistencyLockFree(): 使用 Phase 1 捕获的 snapshot 检查
  ├── findConflictAndPrepare(): 逐条对比，找到第一个冲突点
  │     ├── 如果条目已存在且一致 → 跳过
  │     ├── 如果条目不存在 → 从此处开始都是新条目
  │     └── 如果条目存在但任期不同 → 从此处截断
  ├── store.TruncateLog(conflictIndex)   // 仅在有冲突时
  └── store.AppendEntries(newEntries)    // 仅追加新条目

Phase 3: 短锁 —— 提交推进
  r.mu.Lock()
  ├── 验证 currentTerm == savedTerm（防止 Phase 2 期间任期变化）
  ├── 更新 cachedLastLogIndex
  └── updateFollowerCommitIndex(): commitIndex = min(LeaderCommit, cachedLastLogIndex)
  r.mu.Unlock()

  appendEntriesMu.Unlock()
```

**关键设计决策：**

1. **`appendEntriesMu` 的作用**：防止两个 AppendEntries RPC 的 Phase 2 并发执行 TruncateLog，避免已提交条目被意外删除。

2. **条件截断 vs 无条件截断**：Phase 2 的 `findConflictAndPrepare` 实现了 Raft 论文 Section 5.3 的正确语义——只截断从第一个冲突点开始的日志。这避免了无条件 `TruncateLog(PrevLogIndex+1)` 导致的窗口期问题（已提交条目在截断和重新追加之间短暂不可见）。

3. **磁盘 I/O 在锁外**：`store.TruncateLog` 和 `store.AppendEntries` 在 Phase 2 执行，不持有 `r.mu`，避免磁盘 I/O 阻塞其他 Raft 操作。

---

## 6. 日志提交与状态机应用

### 6.1 提交推进 (`updateCommitIndex`)

Leader 在收到 Follower 的成功响应后，检查是否可以推进 commitIndex。这里有一个关键的安全性约束：**Leader 只能提交当前任期的日志**（Raft 论文 Figure 8 的经典反例）。

为什么？考虑一个场景：Leader A 在任期 2 写入了日志但未提交就崩溃了，Leader B 在任期 3 当选但也未写入该位置就崩溃了，Leader C 在任期 4 当选。如果 C 发现旧任期 2 的日志已被多数派复制就直接提交它，那么在某些场景下这个"已提交"的日志可能被新 Leader 覆盖——违反了 Raft 的安全性保证。解决方案是 Leader 只提交当前任期的日志；一旦当前任期的日志被提交，根据 Log Matching Property，之前所有任期的日志也会被间接提交。

```go
func (r *Raft) updateCommitIndex() {
    newCommitIndex := r.findMajorityCommitIndex()
    if newCommitIndex > r.commitIndex {
        // Raft 安全性规则：只能提交当前任期的日志
        term, _ := r.getLogTerm(newCommitIndex)
        if term == r.currentTerm {
            r.commitIndex = newCommitIndex
            go r.applyLogs()
        }
    }
}
```

**`findMajorityCommitIndex` 优化**：从 `max(matchIndex[*])` 开始向下搜索，而非从 `lastLogIndex`，减少无效扫描。

**`isReplicatedByMajority`**：
- 普通模式：计算 `matchIndex[peer] >= N` 的节点数，加上 Leader 自己
- 联合共识：必须同时满足旧配置多数派和新配置多数派

### 6.2 日志应用 (`applyLogs`)

#### `fetchEntriesToApply` 的设计动机

日志应用的一个微妙之处在于 `fetchEntriesToApply` 的设计。最直觉的实现是：在锁内读取 `commitIndex` 和 `lastApplied`，然后在锁外从存储中读取条目。但这在 go-kv 的并发模型下是不安全的——在释放锁到读取完成的窗口内，另一个 AppendEntries RPC 可能执行 `TruncateLog`，删除正在读取的条目。

go-kv 的解决方案是：**在锁内完成条目读取**（`fetchEntriesToApply` 全程持锁），只将计算密集型的状态机应用（`dispatchEntries`）放到锁外。这虽然在锁内引入了 I/O，但保证了正确性——条目读取期间不会被截断。这个权衡在 PERFORMANCE.md 的"已尝试但回退的优化"中有详细记录。

```
applyLogs()  // 在 goroutine 中运行
  │
  ├── fetchEntriesToApply()  // 持有 r.mu
  │     ├── 读取 lastApplied+1 到 commitIndex 的所有条目
  │     ├── 更新 lastApplied = commitIndex
  │     ├── 广播 lastAppliedCond（唤醒等待的读请求）
  │     └── 返回 entries 列表
  │
  ├── dispatchEntries(entries)  // 不持锁
  │     for each entry:
  │       ├── ConfigChangeCommand → applyConfigChange()
  │       └── 普通命令 → stateMachine.Apply(entry)
  │             │
  │             ├── 发送到 commitChan（非阻塞）
  │             └── 通知 notifyApply[index]（唤醒等待的客户端）
  │
  └── TakeSnapshot()  // 检查是否需要自动快照
```

### 6.3 客户端等待机制

客户端写入请求通过 `waitForAppliedLog` 同步等待日志被应用：

```go
func (r *Raft) waitForAppliedLog(index uint64, timeout time.Duration) (any, bool) {
    r.mu.Lock()
    // 快速路径：已经应用
    if r.lastApplied >= index { return nil, true }

    // 注册通知 channel
    notifyChan := make(chan any, 1)
    r.notifyApply[index] = notifyChan
    r.mu.Unlock()

    select {
    case result := <-notifyChan:
        return result, true
    case <-time.After(timeout):  // 默认 5 秒
        return nil, false
    }
}
```

---

## 7. 线性一致性读

### 7.1 为什么需要特殊处理读操作

一个常见的误解是"Leader 可以直接从本地状态机读取"。但这在以下场景中是不安全的：

假设集群有 5 个节点，Leader A 被网络分区隔离。此时 A 仍然认为自己是 Leader（选举超时尚未到期），如果直接响应读请求，返回的数据可能是过时的——因为集群的多数派已经选出了新 Leader B，并且 B 可能已经提交了 A 不知道的新写入。

**ReadIndex 协议**解决了这个问题：Leader 在响应读请求前，先通过一轮心跳确认自己仍然是多数派承认的 Leader。这保证了读操作看到的数据至少与 Leader 当前的 `commitIndex` 一样新。

### 7.2 两种模式

go-kv 支持两种 ReadIndex 模式，在一致性和性能之间提供了不同的权衡：

| 模式 | 机制 | 性能 | 安全性 |
|------|------|------|--------|
| **Heartbeat** | 每次读请求发送心跳确认 Leadership | 较低（多一轮网络往返） | 严格线性一致 |
| **Lease** | 基于时钟租约，租约内无需心跳 | 较高（大部分请求无网络开销） | 依赖时钟同步 |

### 7.2 读请求完整流程

```
handleLinearizableRead(cmd, reply)
  │
  r.mu.Lock()
  ├── 检查是否为 Leader
  ├── readIndex = commitIndex  // 记录当前 commitIndex
  │
  ├── Lease 模式 && 租约有效?
  │     └── YES → 直接跳到 performReadAfterApply()
  │
  r.mu.Unlock()
  │
  ├── confirmLeadership()  // 心跳确认
  │     ├── 快速路径：leadershipCacheTime 内 → 直接返回 true
  │     ├── 快速路径：检查 lastAck，足够多的最近确认 → 返回 true
  │     └── 慢路径：并行发送心跳，等待多数派确认
  │
  └── performReadAfterApply(cmd, reply, readIndex)
        │
        r.mu.Lock()
        ├── 重新检查 Leader 状态
        ├── 等待 lastApplied >= readIndex（使用 sync.Cond）
        │     超时：electionTimeout * 2
        r.mu.Unlock()  // 在锁外读取状态机
        │
        └── stateMachine.Get(key)  // 线程安全的 LSM 读取
```

### 7.3 Lease 续约

每次心跳成功后自动续约：

```go
func (r *Raft) tryRenewLease() {
    // 统计最近 electionTimeout 内有 ACK 的节点数
    if recentAcks >= majority {
        r.leaseUntil = time.Now().Add(r.leaseDuration)  // leaseDuration = electionTimeout
    }
}
```

---

## 8. 动态成员变更

### 8.1 联合共识（Joint Consensus）

go-kv 采用 Raft 论文中的两阶段成员变更方法，通过联合共识确保安全性。

```
阶段 1: 进入联合共识 (C_old,new)
  ┌──────────────────────────────────────────────┐
  │ Leader 提交 ConfigChangeCommand{newPeerIDs}   │
  │ 当此日志被应用时：                              │
  │   - inJointConsensus = true                   │
  │   - newPeerIDs = 新配置                        │
  │   - 投票需要同时满足 C_old 和 C_new 的多数派     │
  │   - Leader 立即提议 C_new 日志                  │
  └──────────────────────────────────────────────┘

阶段 2: 提交新配置 (C_new)
  ┌──────────────────────────────────────────────┐
  │ 当 C_new 日志被应用时：                         │
  │   - peerIDs = newPeerIDs（切换到新配置）        │
  │   - inJointConsensus = false                  │
  │   - 如果 Leader 不在新配置中 → 自动降级         │
  └──────────────────────────────────────────────┘
```

### 8.2 联合共识期间的多数派计算

```go
func (r *Raft) isReplicatedByMajority(index uint64) bool {
    if !r.inJointConsensus {
        // 普通模式：只需旧配置多数派
        return countReplicated(r.peerIDs, index) >= len(r.peerIDs)/2+1
    }
    // 联合共识：必须同时满足两个配置的多数派
    oldMajority := countReplicated(r.peerIDs, index) >= len(r.peerIDs)/2+1
    newMajority := countReplicated(r.newPeerIDs, index) >= len(r.newPeerIDs)/2+1
    return oldMajority && newMajority
}
```

---

## 9. 快照与日志压缩

### 9.1 为什么需要快照

Raft 日志不能无限增长——它会消耗磁盘空间，更重要的是会拖慢节点重启时的恢复速度（需要回放所有日志）。快照机制将状态机的完整状态序列化为一个紧凑的数据块，然后安全地丢弃快照之前的所有日志条目。

此外，快照解决了**新节点加入**和**严重落后的 Follower** 的问题。当一个 Follower 落后太多，Leader 已经没有它需要的旧日志时（已被压缩），Leader 可以直接发送快照来快速同步状态，而不需要回放海量的日志条目。

### 9.2 自动快照 (`TakeSnapshot`)

#### 异步快照的设计考量

快照涉及两个重量级操作：`stateMachine.GetSnapshot()`（序列化状态机，涉及 LSM 的全量 flush）和 `store.SaveSnapshot()`（磁盘写入）。如果这些操作在 Raft 全局锁内执行，会阻塞所有心跳和日志复制，可能导致选举超时。

go-kv 将快照拆分为同步阶段和异步阶段。同步阶段（持锁）只做轻量操作：记录 `snapshotIndex`、设置 `isSnapshotting` 标志、获取状态机快照数据。异步阶段（goroutine）执行重量级的磁盘 I/O。`isSnapshotting` 标志确保不会同时执行多个快照。

当 `logSize >= snapshotThreshold` 时自动触发：

```
TakeSnapshot()
  │
  同步阶段（持有 r.mu）:
  ├── 检查 isSnapshotting、snapshotThreshold
  ├── snapshotIndex = lastApplied
  ├── snapshotTerm = getLogTerm(snapshotIndex)
  ├── data = stateMachine.GetSnapshot()  // 序列化状态机
  ├── isSnapshotting = true
  └── r.mu.Unlock()
  │
  异步阶段（goroutine，无锁）:
  ├── store.SaveSnapshot(snapshot)   // 重磁盘 I/O
  ├── r.mu.Lock()
  ├── store.CompactLog(index)        // 删除快照覆盖的旧日志
  ├── r.snapshot = snapshot
  └── isSnapshotting = false
```

### 9.3 快照安装 (`InstallSnapshot`)

Leader 向落后太多的 Follower 发送完整快照：

```
Leader 侧:
  sendSnapshot(peerID)
    ├── store.ReadSnapshot()                    // 无锁读取
    ├── trans.SendInstallSnapshot(target, args)  // 无锁网络 I/O
    └── 成功后更新 nextIndex, matchIndex

Follower 侧:
  InstallSnapshot(args, reply)
    ├── 任期检查
    ├── 过期检查：lastIncludedIndex <= lastApplied → 忽略
    ├── store.SaveSnapshot(snapshot)     // 持久化
    ├── stateMachine.ApplySnapshot(data) // 替换状态机
    ├── store.CompactLog(lastIncludedIndex) // 压缩日志
    └── 更新 commitIndex, lastApplied
```

---

## 10. 并发模型与锁优化

### 10.1 锁层次

```
层次 1 (最外层): appendEntriesMu
  - 仅用于 Follower 侧 AppendEntries 串行化
  - 确保 TruncateLog 和 AppendEntries 不会并发执行

层次 2: mu (全局 Raft 锁)
  - 保护所有可变 Raft 状态
  - 尽量短时间持有，磁盘 I/O 和网络 I/O 移到锁外

层次 3 (最内层): state (atomic.Int32)
  - 无锁原子读取，用于快速路径检查
  - Submit() 的 Leader 检查、IsStopped() 等
```

### 10.2 Goroutine 模型

| Goroutine | 生命周期 | 触发条件 |
|-----------|----------|----------|
| `Run()` | 永久 | NewRaft 后手动调用 |
| `startElection()` | 短暂 | 选举超时 |
| `handleElectionResult()` | 短暂 | 每次选举 |
| `sendVoteRequest()` | 短暂 | 每个 Peer 每次选举 |
| `startHeartbeat()` | Leader 期间 | transitionToLeader |
| `proposalBatcher()` | Leader 期间 | transitionToLeader |
| `sendAppendEntries()` | 短暂 | 每个 Peer 每次心跳/复制 |
| `applyLogs()` | 短暂 | commitIndex 推进时 |
| `TakeSnapshot()` 异步阶段 | 短暂 | logSize 超过阈值 |

### 10.3 关键不变量

1. **HardState 先持久化**：`currentTerm` 和 `votedFor` 在发送任何依赖它们的 RPC 之前持久化
2. **cachedLastLogIndex 同步更新**：每次 `store.AppendEntries` 或 `TruncateLog` 后立即更新缓存
3. **只提交当前任期的日志**：`updateCommitIndex` 中 `term == currentTerm` 的安全性检查
4. **条件截断保护已提交条目**：`findConflictAndPrepare` 只截断真正冲突的日志

---

## 11. 性能优化

### 11.1 Proposal 批处理

多个并发的 `Submit()` 请求被合并为单次磁盘写入：

```
Submit() ──► proposalCh (缓冲 256) ──► proposalBatcher()
                                            │
                                    等待第一个请求（阻塞）
                                            │
                                    非阻塞收集最多 64 个请求
                                            │
                                    processBatch(batch):
                                      r.mu.Lock()
                                      构建 []LogEntry
                                      单次 store.AppendEntries()
                                      r.mu.Unlock()
                                      通知所有请求完成
                                      广播 sendAppendEntries
```

优势：
- N 个并发请求只需 1 次磁盘写入和 1 次加锁
- 减少锁竞争：非 Leader 的 Submit 通过原子检查快速返回

### 11.2 缓存优化

- **`cachedLastLogIndex`**：避免每次 `store.LastLogIndex()` 的磁盘查询
- **`atomic.Int32` 状态**：`Submit()`、`IsStopped()` 等高频路径无需加锁
- **`lastLeadershipConfirm` 缓存**：避免短时间内重复的心跳确认
- **`lastAck` 跟踪**：避免向最近确认过的 Peer 发送不必要的心跳

### 11.3 `findMajorityCommitIndex` 搜索优化

从 `max(matchIndex[*])` 向下搜索而非从 `lastLogIndex`，跳过不可能被多数派确认的索引。

### 11.4 `performReadAfterApply` 无锁读取

状态机的 `Get()` 操作在 `r.mu` 之外执行。由于 LSM `Database.Get()` 内部通过 `memtable.Manager.mu` 和 `sstable.Manager.mu` 保证线程安全，无需 Raft 层额外加锁。

---

## 附录：RPC 处理总结

| RPC | 方向 | 处理函数 | 关键逻辑 |
|-----|------|----------|----------|
| RequestVote | Candidate → Peer | `RequestVote()` | Pre-Vote / 正式投票 |
| AppendEntries | Leader → Follower | `AppendEntries()` | 三阶段锁 |
| InstallSnapshot | Leader → Follower | `InstallSnapshot()` | 快照安装 |
| ClientRequest | Client → Leader | `ClientRequest()` | 读写分流 |

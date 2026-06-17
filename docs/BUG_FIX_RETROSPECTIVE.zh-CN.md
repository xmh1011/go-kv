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

package raft

import (
	"strconv"
	"time"

	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
)

// replicationAction 定义了 Leader 对一个 Follower 应采取的同步动作。
type replicationAction int

const (
	actionDoNothing    replicationAction = iota // 动作：什么都不做（例如，不再是 Leader）
	actionSendLogs                              // 动作：发送日志条目
	actionSendSnapshot                          // 动作：发送快照
)

// sendAppendEntries 作为 Leader 节点为每个对等节点启动的专用 goroutine。
// 主要负责：
//   - 心跳（Heartbeat）: 如果没有新的日志条目要发送，它会发送一个空的 AppendEntries RPC，作为心跳来维持 Leader 的地位并阻止 Follower 发起新的选举。
//   - 日志复制（Log Replication）: 当有新的日志条目时，它会将这些条目通过 AppendEntries RPC 发送给 Follower。
//   - 处理响应: 根据 Follower 的响应来更新 nextIndex 和 matchIndex。
//     如果 Follower 的日志与 Leader 不一致，它会根据响应中的冲突信息 (ConflictIndex, ConflictTerm) 回退 nextIndex 并重试，直到日志达成一致。
func (r *Raft) sendAppendEntries(peerID int) {
	// 1. 决定需要对该 Follower 执行哪种同步操作。
	action := r.determineReplicationAction(peerID)

	// 2. 根据决策结果，执行相应的操作。
	switch action {
	case actionSendLogs:
		// 如果决定发送日志，则调用专门负责日志复制的函数。
		r.replicateLogsToPeer(peerID)
	case actionSendSnapshot:
		// 如果决定发送快照，则调用已有的快照发送函数。
		r.sendSnapshot(peerID)
	case actionDoNothing:
		// 如果无需任何操作，则直接返回。
		return
	}
}

// determineReplicationAction 检查并决定 Leader 应对一个 Follower 采取何种同步措施。
// 它封装了所有的前置检查逻辑。
func (r *Raft) determineReplicationAction(peerID int) replicationAction {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 检查一：如果当前节点不再是 Leader，则不执行任何操作。
	if r.state != Leader {
		return actionDoNothing
	}

	// 检查二：判断 Follower 是否落后太多，以至于其需要的日志已被本地快照压缩。
	firstLogIndex, err := r.getFirstLogIndex()
	if err != nil {
		log.Errorf("[Replication] Node %d failed to get first log index: %v", r.id, err)
		return actionSendLogs // Fallback to sending logs on error
	}

	// 如果 Follower 需要的下一条日志索引小于 Leader 的第一条日志索引，
	// 说明该日志已被压缩，必须发送快照。
	if r.nextIndex[peerID] < firstLogIndex {
		log.Infof("[Snapshot] Node %d log for peer %d (nextIndex=%d) is behind compacted log (firstLogIndex=%d). Decided to send snapshot.", r.id, peerID, r.nextIndex[peerID], firstLogIndex)
		return actionSendSnapshot
	}

	// 如果以上情况都不满足，则执行常规的日志复制操作。
	return actionSendLogs
}

// replicateLogsToPeer 封装了向单个 Peer 发送 AppendEntries RPC 的流程。
// 为了实现流水线，这个函数会异步地发起 RPC，而不是阻塞等待。
func (r *Raft) replicateLogsToPeer(peerID int) {
	r.mu.Lock()
	// 准备 RPC 请求参数。
	args, err := r.prepareAppendEntriesArgs(peerID)
	if err != nil {
		log.Errorf("[Replication] Node %d failed to prepare AppendEntries args for peer %d: %v", r.id, peerID, err)
		r.mu.Unlock()
		return
	}
	savedCurrentTerm := r.currentTerm
	r.mu.Unlock() // 在发起网络调用前解锁。

	// 启动一个新的 goroutine 来实际发送 RPC 并处理响应。
	// 这使得主复制循环（例如心跳循环）可以不必等待网络延迟，
	// 而可以继续检查并发送下一批日志，从而实现流水线效果。
	go func() {
		reply := param.NewAppendEntriesReply()
		if err := r.trans.SendAppendEntries(strconv.Itoa(peerID), args, reply); err != nil {
			log.Errorf("[Log Replication] Node %d failed to send AppendEntries to %d: %s", r.id, peerID, err.Error())
			return
		}

		// 在 RPC 完成后，在后台处理响应。
		r.mu.Lock()
		defer r.mu.Unlock()
		r.processAppendEntriesReply(peerID, args, reply, savedCurrentTerm)
	}()
}

// prepareAppendEntriesArgs 负责构建发送给对等节点的 AppendEntries RPC 参数。
func (r *Raft) prepareAppendEntriesArgs(peerID int) (*param.AppendEntriesArgs, error) {
	prevLogIndex := r.nextIndex[peerID] - 1
	prevLogTerm, err := r.getLogTerm(prevLogIndex)
	if err != nil {
		log.Errorf("[Replication] Node %d failed to get log term at index %d: %v", r.id, prevLogIndex, err)
		return nil, err
	}

	var entries []param.LogEntry
	lastLogIndex, err := r.store.LastLogIndex()
	if err != nil {
		return nil, err
	}
	if r.nextIndex[peerID] <= lastLogIndex {
		// 在生产环境中，存储层应提供一个高效的范围查询方法来代替循环。
		for i := r.nextIndex[peerID]; i <= lastLogIndex; i++ {
			entry, err := r.store.GetEntry(i)
			if err != nil || entry == nil {
				log.Errorf("[Replication] Node %d failed to get entry %d from store: %v", r.id, i, err)
				return nil, err
			}
			entries = append(entries, *entry)
		}
	}

	args := param.NewAppendEntriesArgs(r.currentTerm, r.id, prevLogIndex, prevLogTerm, r.commitIndex, entries)
	return args, nil
}

// processAppendEntriesReply 负责处理从对等节点返回的 AppendEntries 响应。
// 此函数必须在持有锁的情况下被调用。
func (r *Raft) processAppendEntriesReply(peerID int, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply, savedCurrentTerm uint64) {
	if r.currentTerm != savedCurrentTerm || r.state != Leader {
		return
	}

	if reply.Term > r.currentTerm {
		log.Infof("[Log Replication] Node %d found higher term %d from peer %d, becomes Follower", r.id, reply.Term, peerID)
		err := r.becomeFollower(reply.Term)
		if err != nil {
			log.Errorf("[Replication] Node %d failed to persist state when stepping down to Follower: %v", r.id, err)
			return
		}
		return
	}

	if r.state == Leader {
		// 无论 Success 是 true 还是 false，
		// 只要任期匹配，就说明 Follower 确认了我们的 Leader 地位。
		// 这足以用于 ReadIndex 的租约。
		r.lastAck[peerID] = time.Now()

		if reply.Success {
			r.handleSuccessfulAppendEntries(peerID, args)
		} else {
			r.handleFailedAppendEntries(peerID, reply)
		}
	}
}

// handleSuccessfulAppendEntries 在收到成功的 AppendEntries 响应后更新 Leader 的状态。
func (r *Raft) handleSuccessfulAppendEntries(peerID int, args *param.AppendEntriesArgs) {
	newNextIndex := args.PrevLogIndex + uint64(len(args.Entries)) + 1
	r.nextIndex[peerID] = newNextIndex
	r.matchIndex[peerID] = newNextIndex - 1

	r.updateCommitIndex()
}

// handleFailedAppendEntries 在收到失败的 AppendEntries 响应后调整 nextIndex。
func (r *Raft) handleFailedAppendEntries(peerID int, reply *param.AppendEntriesReply) {
	log.Infof("[Log Replication] Node %d rejected AppendEntries from leader %d (ConflictIndex=%d, ConflictTerm=%d)", peerID, r.id, reply.ConflictIndex, reply.ConflictTerm)

	// 根据论文中的优化策略，快速回退 nextIndex。
	if reply.ConflictIndex > 0 {
		r.nextIndex[peerID] = reply.ConflictIndex
	} else {
		// 如果 ConflictIndex 为 0（异常情况），则回退一步
		if r.nextIndex[peerID] > 1 {
			r.nextIndex[peerID]--
		}
	}

	go r.sendAppendEntries(peerID)
}

// updateCommitIndex 检查 Leader 是否可以推进其 commitIndex。
// 计算已在集群多数节点上成功复制的最高日志索引，并更新 Leader 自己的 commitIndex。
// Raft 的安全规则规定，只有当前任期的日志才可以通过这种方式被提交。
func (r *Raft) updateCommitIndex() {
	newCommitIndex := r.findMajorityCommitIndex()

	if newCommitIndex > r.commitIndex {
		entry, err := r.store.GetEntry(newCommitIndex)
		if err != nil || entry == nil {
			log.Errorf("[Replication] Node %d failed to get entry for new commit index %d: %v", r.id, newCommitIndex, err)
			return
		}

		if entry.Term == r.currentTerm {
			log.Infof("[Log Replication] Node %d advances commitIndex to %d (term=%d)", r.id, newCommitIndex, r.currentTerm)
			r.commitIndex = newCommitIndex
			go r.applyLogs()
		}
	}
}

// findMajorityCommitIndex 计算可以被安全提交的最高日志索引。
func (r *Raft) findMajorityCommitIndex() uint64 {
	lastLogIndex, err := r.store.LastLogIndex()
	if err != nil {
		return r.commitIndex // Return current commitIndex on error
	}

	// 从后往前检查每一个日志索引，看它是否满足多数派提交的条件。
	for N := lastLogIndex; N > r.commitIndex; N-- {
		// 检查索引 N 是否被多数节点复制。
		if r.isReplicatedByMajority(N) {
			// 如果满足，这就是可以提交的最高索引，直接返回。
			return N
		}
	}
	return r.commitIndex
}

// isReplicatedByMajority 判断一个日志索引 N 是否已经被多数节点复制。
func (r *Raft) isReplicatedByMajority(index uint64) bool {
	// 在普通模式下，只需计算旧配置的多数派。
	// Leader 自身永远是匹配的。
	matchCountOld := 1
	for _, peerID := range r.peerIDs {
		if r.matchIndex[peerID] >= index {
			matchCountOld++
		}
	}
	majorityOld := (len(r.peerIDs) / 2) + 1

	if !r.inJointConsensus {
		return matchCountOld >= majorityOld
	}

	// 在联合共识模式下，需要同时满足新旧配置的多数派。
	matchCountNew := 0
	// 检查 Leader 自身是否在新配置中。
	if _, isNew := findPeer(r.id, r.newPeerIDs); isNew {
		matchCountNew = 1
	}
	for _, peerID := range r.newPeerIDs {
		if r.matchIndex[peerID] >= index {
			matchCountNew++
		}
	}
	majorityNew := (len(r.newPeerIDs) / 2) + 1

	return matchCountOld >= majorityOld && matchCountNew >= majorityNew
}

// AppendEntries 是 Follower 节点上的 RPC 处理函数，用于接收 Leader 的心跳和日志。
// 任期检查: 如果请求的任期号小于自己的当前任期，则拒绝。如果大于，则更新自己的任期并转为 Follower。
// 一致性检查: 检查 PrevLogIndex 和 PrevLogTerm 是否与自己的日志匹配。如果不匹配，则拒绝请求并返回冲突信息，帮助 Leader 快速定位不一致点。
// 日志追加: 如果一致性检查通过，则将新的日志条目追加到自己的日志中。
// 更新 CommitIndex: 根据 Leader 发来的 LeaderCommit 来更新自己的 commitIndex。
func (r *Raft) AppendEntries(args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. 处理任期检查和心跳。如果 Leader 的任期小于自己，直接拒绝。
	// 如果大于，则转为 Follower。无论哪种情况，只要是合法的 Leader，就重置选举计时器。
	if !r.handleTermAndHeartbeat(args, reply) {
		return nil
	}

	// 2. 进行日志一致性检查。
	// 验证本地日志在 prevLogIndex 处是否与 Leader 发来的信息匹配。
	if ok := r.checkLogConsistency(args, reply); !ok {
		return nil
	}

	// 3. 追加并存储新的日志条目。
	// 如果 Leader 发来了新的日志，则截断本地可能存在的冲突日志，并追加新日志。
	if err := r.appendAndStoreEntries(args); err != nil {
		reply.Success = false
		log.Errorf("[Replication] Node %d failed to append entries: %v", r.id, err)
		return err
	}

	// 4. 根据 Leader 的进度更新本地的 commitIndex。
	r.updateFollowerCommitIndex(args)

	// 所有步骤都成功完成。
	reply.Success = true
	return nil
}

// handleTermAndHeartbeat 负责处理任期检查和重置选举计时器。
// 如果 Leader 的任期有效，返回 true；如果因任期过时而应立即拒绝，返回 false。
func (r *Raft) handleTermAndHeartbeat(args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) bool {
	reply.Term = r.currentTerm
	// 如果 Leader 的任期小于当前节点的任期，说明这是一个过时的 Leader，拒绝其请求。
	if args.Term < r.currentTerm {
		reply.Success = false
		return false
	}

	// 如果 Leader 的任期大于当前节点的任期，说明集群中已经有了新的领导者。
	// 当前节点必须立即更新自己的任期并转为 Follower。
	if args.Term > r.currentTerm {
		// becomeFollower 是一个已有的辅助函数，用于处理状态转换和持久化。
		if err := r.becomeFollower(args.Term); err != nil {
			log.Errorf("[Replication] Node %d failed to persist state when stepping down to Follower: %v", r.id, err)
			// 如果持久化失败，这是一个严重错误，我们拒绝请求。
			reply.Success = false
			return false
		}
		reply.Term = r.currentTerm
	}

	// 只要收到了来自当前（或更新后的）合法 Leader 的消息，就重置选举计时器。
	// 这表明 Leader 仍然活跃，Follower 不需要发起新的选举。
	r.electionResetEvent = time.Now()

	// 重置下一次的随机超时
	r.currentElectionTimeout = r.randomizedElectionTimeout()
	return true
}

// checkLogConsistency 负责检查本地日志是否与 Leader 的日志保持一致。
// 如果不一致，它会填充 reply 中的冲突信息，并返回 false。
func (r *Raft) checkLogConsistency(args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) bool {
	// 如果 prevLogIndex 为 0，表示这是第一条日志之前的虚拟节点，无需检查，直接认为是一致的。
	if args.PrevLogIndex == 0 {
		return true
	}

	prevEntry, err := r.store.GetEntry(args.PrevLogIndex)
	if err != nil {
		log.Errorf("[Replication] Node %d failed to get entry %d from store: %v", r.id, args.PrevLogIndex, err)
		reply.Success = false
		// 如果获取日志失败（例如被压缩了），我们应该给 Leader 一个提示。
		// 最好的方式是告诉 Leader 我们当前的最后一条日志索引，让它从那里开始尝试。
		lastLogIndex, _ := r.store.LastLogIndex()
		reply.ConflictIndex = lastLogIndex + 1
		reply.ConflictTerm = 0
		return false
	}
	// 检查获取到的日志条目是否与 Leader 的期望一致。
	if prevEntry == nil || prevEntry.Term != args.PrevLogTerm {
		// 如果 prevEntry 为 nil，说明本地日志在 prevLogIndex 处没有条目，即日志过短。
		if prevEntry == nil {
			lastLogIndex, _ := r.store.LastLogIndex()
			reply.ConflictIndex = lastLogIndex + 1
			reply.ConflictTerm = 0 // 用 0 表示此处没有日志
		} else {
			// 如果 prevEntry 不为 nil 但任期不匹配，则记录冲突的任期和索引。
			reply.ConflictTerm = prevEntry.Term
			reply.ConflictIndex = args.PrevLogIndex
		}
		reply.Success = false
		return false
	}

	return true
}

// appendAndStoreEntries 负责将 Leader 发来的新日志条目追加到本地存储中。
// 它会先截断任何可能存在的冲突日志。
func (r *Raft) appendAndStoreEntries(args *param.AppendEntriesArgs) error {
	// 仅当 Leader 发来了新的日志条目时才执行操作。
	if len(args.Entries) > 0 {
		// 1. 截断从 prevLogIndex + 1 开始的所有本地日志，以解决任何潜在的冲突。
		if err := r.store.TruncateLog(args.PrevLogIndex + 1); err != nil {
			log.Errorf("[Replication] Node %d failed to truncate log: %v", r.id, err)
			return err
		}
		// 2. 将 Leader 发来的新日志原子性地追加到存储中。
		if err := r.store.AppendEntries(args.Entries); err != nil {
			log.Errorf("[Replication] Node %d failed to append entries to store: %v", r.id, err)
			return err
		}
		log.Infof("[Log Replication] Node %d accepted and stored %d new entries from leader %d", r.id, len(args.Entries), args.LeaderID)
	}
	return nil
}

// updateFollowerCommitIndex 根据 Leader 发来的 leaderCommit 更新 Follower 的 commitIndex。
func (r *Raft) updateFollowerCommitIndex(args *param.AppendEntriesArgs) {
	// 如果 Leader 的 commitIndex 大于 Follower 的 commitIndex，说明有新的日志可以被提交。
	if args.LeaderCommit > r.commitIndex {
		// Follower 的 commitIndex 不能超过其本地日志的最大索引。
		newLastLogIndex, _ := r.store.LastLogIndex()
		oldCommitIndex := r.commitIndex
		r.commitIndex = min(args.LeaderCommit, newLastLogIndex)

		// 如果 commitIndex 确实被推进了，则在后台启动一个 goroutine 应用这些新提交的日志。
		if r.commitIndex > oldCommitIndex {
			log.Infof("[Log Replication] Node %d advances commitIndex to %d", r.id, r.commitIndex)
			go r.applyLogs()
		}
	}
}

// applyLogs 将已提交的日志应用到状态机。此函数会在后台 goroutine 中运行。
func (r *Raft) applyLogs() {
	// 1. 从存储中获取所有需要应用的日志条目。
	entriesToApply, lastAppliedBefore := r.fetchEntriesToApply()
	if len(entriesToApply) == 0 {
		return
	}

	endIndex := lastAppliedBefore + uint64(len(entriesToApply))
	log.Infof("[State Machine] Node %d applying %d entries from index %d to %d", r.id, len(entriesToApply), lastAppliedBefore+1, endIndex)

	// 2. 遍历并分发每一条待应用的日志。
	r.dispatchEntries(entriesToApply)

	// 3. 检查是否需要触发快照
	r.TakeSnapshot()
}

// fetchEntriesToApply 负责从存储中获取所有已提交但尚未应用的日志条目。
// 在获取数据后安全地更新 r.lastApplied 索引。
func (r *Raft) fetchEntriesToApply() ([]param.LogEntry, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var entries []param.LogEntry
	// 检查是否有需要应用的日志。
	if r.commitIndex > r.lastApplied {
		// 循环从存储中逐条读取已提交的日志。
		for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
			entry, err := r.store.GetEntry(i)
			if err != nil || entry == nil {
				// 这是一个严重错误：如果一个日志被标记为已提交，它必须存在于存储中。
				// 在生产环境中，这可能需要让节点 panic 或安全地关闭。
				log.Fatalf("[FATAL] Node %d could not retrieve committed log entry %d to apply it. Error: %v", r.id, i, err)
				return nil, 0
			}
			entries = append(entries, *entry)
		}
	}

	// 记录应用前的 lastApplied 索引，用于日志输出。
	lastAppliedBeforeUpdate := r.lastApplied
	// 如果成功获取了日志，则原子性地更新 lastApplied 索引。
	if len(entries) > 0 {
		r.lastApplied = entries[len(entries)-1].Index
		// 唤醒所有在 handleLinearizableRead 中
		// 等待 lastApplied 追赶的 goroutine。
		r.lastAppliedCond.Broadcast()
	}

	return entries, lastAppliedBeforeUpdate
}

// dispatchEntries 遍历日志条目切片，并根据命令类型将其分发给具体的处理函数。
func (r *Raft) dispatchEntries(entries []param.LogEntry) {
	for _, entry := range entries {
		var result any

		switch cmd := entry.Command.(type) {
		case param.ConfigChangeCommand:
			r.applyConfigChange(cmd, entry.Index)
		default:
			result = r.stateMachine.Apply(entry)
			r.applyStateMachineCommand(entry)
		}

		r.mu.Lock()
		notifyChan, ok := r.notifyApply[entry.Index]
		if ok {
			// 在发送通知前，从 map 中删除 channel，防止重复通知或内存泄漏。
			delete(r.notifyApply, entry.Index)
		}
		// 打印 map 内容以调试
		log.Infof("[Client] dispatchEntries: map content before notify: %+v", r.notifyApply)
		r.mu.Unlock()

		if ok {
			log.Infof("[Client] dispatchEntries: Notifying for index %d", entry.Index)
			notifyChan <- result
		} else {
			log.Infof("[Client] dispatchEntries: No notify channel found for index %d", entry.Index)
		}
	}
}

// applyConfigChange 处理配置变更命令，更新节点的成员状态。
func (r *Raft) applyConfigChange(cmd param.ConfigChangeCommand, entryIndex uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.inJointConsensus {
		// --- Phase 1: 进入联合共识 (C_old,new) ---
		r.inJointConsensus = true
		r.newPeerIDs = cmd.NewPeerIDs
		log.Infof("[Config Change] Node %d entering joint consensus at index %d.", r.id, entryIndex)

		// 如果当前节点是 Leader，它有责任立即提交第二阶段的配置日志 (C_new)，以完成变更。
		if r.state == Leader {
			// 初始化新加入节点的 nextIndex 和 matchIndex
			lastLogIndex, _ := r.store.LastLogIndex()
			for _, peerID := range r.newPeerIDs {
				if _, ok := r.nextIndex[peerID]; !ok {
					r.nextIndex[peerID] = lastLogIndex + 1
					r.matchIndex[peerID] = 0
				}
			}
			r.proposeNewConfigEntry()
		}
	} else {
		// --- Phase 2: 提交新配置 (C_new) ---
		// 此时联合共识结束，节点切换到仅使用新配置。
		r.peerIDs = r.newPeerIDs
		r.newPeerIDs = nil
		r.inJointConsensus = false
		log.Infof("[Config Change] Node %d has transitioned to new configuration at index %d.", r.id, entryIndex)

		// 检查自己是否还属于新配置。
		// 如果 Leader 发现自己被移除了，必须立即“退位” (Step Down)。
		_, exists := findPeer(r.id, r.peerIDs)
		if !exists && r.state == Leader {
			log.Infof("[Config Change] Leader %d detected it is NOT in the new configuration. Stepping down.", r.id)

			// 自动降级为 Follower。
			// 使用 r.currentTerm 即可，因为这不是因为发现了更高任期，而是因为配置变更逻辑。
			// becomeFollower 会更新状态并重置状态机，这会使 Leader 的心跳协程(startHeartbeat)在下一次循环检测时自动退出。
			if err := r.becomeFollower(r.currentTerm); err != nil {
				log.Errorf("[Replication] Node %d failed to step down after removal: %v", r.id, err)
			}
			return
		}
	}
}

// applyStateMachineCommand 将普通的日志条目作为 CommitEntry 发送到客户端的状态机通道。
func (r *Raft) applyStateMachineCommand(entry param.LogEntry) {
	// 这个操作在 Raft 锁之外执行，以避免状态机处理缓慢时阻塞 Raft 的核心逻辑。
	if r.commitChan != nil {
		r.commitChan <- param.CommitEntry{
			Command: entry.Command,
			Index:   entry.Index,
			Term:    entry.Term,
		}
	}
}

// proposeNewConfigEntry 是 Leader 用于提交 C_new（最终配置）日志条目的辅助函数。
func (r *Raft) proposeNewConfigEntry() {
	configCmd := param.NewConfigChangeCommand(r.newPeerIDs)
	lastIndex, err := r.store.LastLogIndex()
	if err != nil {
		log.Errorf("[Replication] Leader %d failed to get last log index for C_new entry: %v", r.id, err)
		return
	}

	newIndex := lastIndex + 1
	newLogEntry := param.NewLogEntry(configCmd, r.currentTerm, newIndex)
	if err := r.store.AppendEntries([]param.LogEntry{newLogEntry}); err != nil {
		log.Errorf("[Replication] leader %d failed to append C_new config entry: %s", r.id, err.Error())
		return
	}
	log.Infof("[Replication] leader %d proposed final C_new config entry at index %d", r.id, newIndex)
}

// findPeer 在给定的 peers 列表中查找指定的 id。
func findPeer(id int, peers []int) (int, bool) {
	for i, p := range peers {
		if p == id {
			return i, true
		}
	}
	return -1, false
}

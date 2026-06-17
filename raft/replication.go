package raft

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/xmh1011/go-kv/pkg/config"
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

var errPeerNeedsSnapshot = errors.New("peer needs snapshot")
var errLocalLogUnavailable = errors.New("local log unavailable")

// sendAppendEntries 作为 Leader 节点为每个对等节点启动的专用 goroutine。
// 主要负责：
//   - 心跳（Heartbeat）: 如果没有新的日志条目要发送，它会发送一个空的 AppendEntries RPC，作为心跳来维持 Leader 的地位并阻止 Follower 发起新的选举。
//   - 日志复制（Log Replication）: 当有新的日志条目时，它会将这些条目通过 AppendEntries RPC 发送给 Follower。
//   - 处理响应: 根据 Follower 的响应来更新 nextIndex 和 matchIndex。
//     如果 Follower 的日志与 Leader 不一致，它会根据响应中的冲突信息 (ConflictIndex, ConflictTerm) 回退 nextIndex 并重试，直到日志达成一致。
func (r *Raft) sendAppendEntries(peerID int) {
	defer r.finishAppendEntries(peerID)

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
	if r.getState() != Leader {
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
		log.Debugf("[Snapshot] Node %d log for peer %d (nextIndex=%d) is behind compacted log (firstLogIndex=%d). Decided to send snapshot.", r.id, peerID, r.nextIndex[peerID], firstLogIndex)
		return actionSendSnapshot
	}

	// 如果以上情况都不满足，则执行常规的日志复制操作。
	return actionSendLogs
}

func (r *Raft) triggerAppendEntries(peerID int) {
	r.mu.Lock()
	shouldStart := r.triggerAppendEntriesLocked(peerID)
	r.mu.Unlock()
	if shouldStart {
		go r.sendAppendEntries(peerID)
	}
}

func (r *Raft) triggerAppendEntriesLocked(peerID int) bool {
	if r.getState() != Leader {
		return false
	}
	if r.replicating == nil {
		r.replicating = make(map[int]bool)
	}
	if r.replicationPending == nil {
		r.replicationPending = make(map[int]bool)
	}
	if r.replicating[peerID] {
		r.replicationPending[peerID] = true
		return false
	}
	r.replicating[peerID] = true
	return true
}

func (r *Raft) finishAppendEntries(peerID int) {
	r.mu.Lock()
	if r.getState() == Leader && r.replicationPending[peerID] {
		r.replicationPending[peerID] = false
		r.mu.Unlock()
		go r.sendAppendEntries(peerID)
		return
	}
	delete(r.replicationPending, peerID)
	delete(r.replicating, peerID)
	r.mu.Unlock()
}

// replicateLogsToPeer 封装了向单个 Peer 发送 AppendEntries RPC 的流程。
// 为了实现流水线，这个函数会异步地发起 RPC，而不是阻塞等待。
//
// 优化：最小化锁持有时间，日志读取在持锁状态下执行（保证一致性），网络发送在锁外执行。
func (r *Raft) replicateLogsToPeer(peerID int) {
	r.mu.Lock()
	// 准备 RPC 请求参数（日志读取在锁内执行以保证一致性）
	args, err := r.prepareAppendEntriesArgs(peerID)
	if err != nil {
		if errors.Is(err, errLocalLogUnavailable) {
			r.mu.Unlock()
			return
		}
		if errors.Is(err, errPeerNeedsSnapshot) {
			r.mu.Unlock()
			r.sendSnapshot(peerID)
			return
		}
		log.Errorf("[Replication] Node %d failed to prepare AppendEntries args for peer %d: %v", r.id, peerID, err)
		r.mu.Unlock()
		return
	}
	savedCurrentTerm := r.currentTerm
	r.mu.Unlock() // 在发起网络调用前解锁。

	reply := param.NewAppendEntriesReply()
	if err := r.trans.SendAppendEntries(strconv.Itoa(peerID), args, reply); err != nil {
		log.Debugf("[Log Replication] Node %d failed to send AppendEntries to %d: %s", r.id, peerID, err.Error())
		return
	}

	// 在 RPC 完成后处理响应。每个 peer 由 triggerAppendEntries 合并调度，
	// 避免失败重试或心跳广播创建无限复制 goroutine。
	r.mu.Lock()
	defer r.mu.Unlock()
	r.processAppendEntriesReply(peerID, args, reply, savedCurrentTerm)
}

// MaxEntriesPerAppendEntries 限制单次 AppendEntries 发送的日志数量
// 避免一次性发送过多日志导致超时和性能问题
const MaxEntriesPerAppendEntries = 32

// prepareAppendEntriesArgs 负责构建发送给对等节点的 AppendEntries RPC 参数。
func (r *Raft) prepareAppendEntriesArgs(peerID int) (*param.AppendEntriesArgs, error) {
	if r.snapshot != nil && r.nextIndex[peerID] <= r.snapshot.LastIncludedIndex {
		log.Debugf("[Snapshot] Node %d peer %d nextIndex=%d is at or before snapshot index %d; sending snapshot", r.id, peerID, r.nextIndex[peerID], r.snapshot.LastIncludedIndex)
		return nil, errPeerNeedsSnapshot
	}

	lastLogIndex := r.cachedLastLogIndex
	if maxNextIndex := lastLogIndex + 1; r.nextIndex[peerID] > maxNextIndex {
		log.Debugf("[Replication] Node %d clamps nextIndex[%d] from %d to %d", r.id, peerID, r.nextIndex[peerID], maxNextIndex)
		r.nextIndex[peerID] = maxNextIndex
	}

	prevLogIndex := r.nextIndex[peerID] - 1
	prevLogTerm, err := r.getLogTermLocked(prevLogIndex)
	if err != nil {
		if errors.Is(err, errLogEntryNotFound) {
			if r.logIndexNeedsSnapshotLocked(prevLogIndex) {
				return nil, errPeerNeedsSnapshot
			}
			r.refreshCachedLastLogIndexLocked()
			if prevLogIndex > r.cachedLastLogIndex {
				r.nextIndex[peerID] = r.cachedLastLogIndex + 1
				return nil, errLocalLogUnavailable
			}
			r.markLocalLogGapLocked(peerID, prevLogIndex)
			return nil, errLocalLogUnavailable
		}
		log.Errorf("[Replication] Node %d failed to get log term at index %d: %v", r.id, prevLogIndex, err)
		return nil, err
	}

	var entries []param.LogEntry
	if r.nextIndex[peerID] <= lastLogIndex {
		// 限制单次发送的日志数量，避免一次性发送过多导致超时
		endIndex := r.nextIndex[peerID] + uint64(MaxEntriesPerAppendEntries) - 1
		if endIndex > lastLogIndex {
			endIndex = lastLogIndex
		}

		for i := r.nextIndex[peerID]; i <= endIndex; i++ {
			entry, err := r.store.GetEntry(i)
			if err != nil {
				log.Errorf("[Replication] Node %d failed to get entry %d from store: %v", r.id, i, err)
				return nil, err
			}
			if entry == nil {
				if r.logIndexNeedsSnapshotLocked(i) {
					log.Debugf("[Snapshot] Node %d entry %d for peer %d is before the local log start; sending snapshot", r.id, i, peerID)
					return nil, errPeerNeedsSnapshot
				}
				r.refreshCachedLastLogIndexLocked()
				if i > r.cachedLastLogIndex {
					if r.nextIndex[peerID] > r.cachedLastLogIndex+1 {
						r.nextIndex[peerID] = r.cachedLastLogIndex + 1
					}
					return nil, errLocalLogUnavailable
				}
				r.markLocalLogGapLocked(peerID, i)
				log.Debugf("[Replication] Node %d local log entry %d unavailable while preparing AppendEntries for peer %d; will retry", r.id, i, peerID)
				return nil, errLocalLogUnavailable
			}
			entries = append(entries, *entry)
		}
	}

	args := param.NewAppendEntriesArgs(r.currentTerm, r.id, prevLogIndex, prevLogTerm, r.commitIndex, entries)
	return args, nil
}

func (r *Raft) markLocalLogGapLocked(peerID int, missingIndex uint64) {
	if missingIndex == 0 {
		return
	}
	newLastIndex := missingIndex - 1
	if r.snapshot != nil && newLastIndex < r.snapshot.LastIncludedIndex {
		newLastIndex = r.snapshot.LastIncludedIndex
	}
	if newLastIndex < r.commitIndex {
		newLastIndex = r.commitIndex
	}
	if newLastIndex < r.cachedLastLogIndex {
		log.Debugf("[Replication] Node %d found local log gap at %d; rewinding cached last log index from %d to %d", r.id, missingIndex, r.cachedLastLogIndex, newLastIndex)
		r.cachedLastLogIndex = newLastIndex
	}
	if r.nextIndex != nil && r.nextIndex[peerID] > r.cachedLastLogIndex+1 {
		r.nextIndex[peerID] = r.cachedLastLogIndex + 1
	}
}

func (r *Raft) logIndexNeedsSnapshotLocked(index uint64) bool {
	if index == 0 || r.store == nil {
		return false
	}

	if r.snapshot != nil && index <= r.snapshot.LastIncludedIndex {
		return true
	}

	storedSnapshot, err := r.store.ReadSnapshot()
	if err != nil {
		log.Debugf("[Snapshot] Node %d failed to read snapshot while checking index %d: %v", r.id, index, err)
	} else if storedSnapshot != nil {
		r.snapshot = storedSnapshot
		if index <= storedSnapshot.LastIncludedIndex {
			return true
		}
	}

	firstLogIndex, err := r.store.FirstLogIndex()
	if err != nil {
		log.Debugf("[Snapshot] Node %d failed to read first log index while checking index %d: %v", r.id, index, err)
		return false
	}
	return firstLogIndex > 0 && index < firstLogIndex
}

// processAppendEntriesReply 负责处理从对等节点返回的 AppendEntries 响应。
// 此函数必须在持有锁的情况下被调用。
func (r *Raft) processAppendEntriesReply(peerID int, args *param.AppendEntriesArgs, reply *param.AppendEntriesReply, savedCurrentTerm uint64) {
	if r.currentTerm != savedCurrentTerm || r.getState() != Leader {
		return
	}

	if reply.Term > r.currentTerm {
		log.Debugf("[Log Replication] Node %d found higher term %d from peer %d, becomes Follower", r.id, reply.Term, peerID)
		err := r.becomeFollower(reply.Term)
		if err != nil {
			log.Errorf("[Replication] Node %d failed to persist state when stepping down to Follower: %v", r.id, err)
			return
		}
		return
	}

	if r.getState() == Leader {
		// 无论 Success 是 true 还是 false，
		// 只要任期匹配，就说明 Follower 确认了我们的 Leader 地位。
		// 这足以用于 ReadIndex 的租约。
		r.lastAck[peerID] = time.Now()

		// 在 Lease Read 模式下，检查是否获得了多数派确认
		// 如果有，则更新租约
		if r.readIndexMode == config.ReadIndexModeLease {
			r.tryRenewLease()
		}

		if reply.Success {
			if r.handleSuccessfulAppendEntries(peerID, args) {
				if r.triggerAppendEntriesLocked(peerID) {
					go r.sendAppendEntries(peerID)
				}
			}
		} else {
			r.handleFailedAppendEntries(peerID, reply)
		}
	}
}

// handleSuccessfulAppendEntries 在收到成功的 AppendEntries 响应后更新 Leader 的状态。
func (r *Raft) handleSuccessfulAppendEntries(peerID int, args *param.AppendEntriesArgs) bool {
	newNextIndex := args.PrevLogIndex + uint64(len(args.Entries)) + 1
	newMatchIndex := newNextIndex - 1
	if newNextIndex > r.nextIndex[peerID] {
		r.nextIndex[peerID] = newNextIndex
	}
	if newMatchIndex > r.matchIndex[peerID] {
		r.matchIndex[peerID] = newMatchIndex
	}

	commitAdvanced := r.updateCommitIndex()

	// Continue streaming the next batch immediately while the peer is still
	// behind. Relying only on the 100ms heartbeat loop limits catch-up to
	// MaxEntriesPerAppendEntries per tick, which leaves snapshot-restored
	// followers minutes behind under long-running write workloads.
	//
	// Also send one more AppendEntries when this ACK advanced commitIndex, even
	// if the peer is otherwise caught up. The ACK was for entries sent with the
	// previous LeaderCommit value, so the follower may have the entry but not
	// know it is committed yet.
	return r.getState() == Leader && (r.nextIndex[peerID] <= r.cachedLastLogIndex || commitAdvanced)
}

// handleFailedAppendEntries 在收到失败的 AppendEntries 响应后调整 nextIndex。
func (r *Raft) handleFailedAppendEntries(peerID int, reply *param.AppendEntriesReply) {
	log.Debugf("[Log Replication] Node %d rejected AppendEntries from leader %d (ConflictIndex=%d, ConflictTerm=%d)", peerID, r.id, reply.ConflictIndex, reply.ConflictTerm)

	// 根据论文中的优化策略，快速回退 nextIndex。
	nextIndex := r.nextIndex[peerID]
	if reply.ConflictIndex > 0 {
		nextIndex = reply.ConflictIndex
	} else {
		// 如果 ConflictIndex 为 0（异常情况），则回退一步
		if r.nextIndex[peerID] > 1 {
			nextIndex = r.nextIndex[peerID] - 1
		}
	}
	if nextIndex > r.nextIndex[peerID] {
		nextIndex = r.nextIndex[peerID]
	}
	if minNextIndex := r.matchIndex[peerID] + 1; nextIndex < minNextIndex {
		nextIndex = minNextIndex
	}
	r.nextIndex[peerID] = nextIndex

	if r.triggerAppendEntriesLocked(peerID) {
		go r.sendAppendEntries(peerID)
	}
}

// updateCommitIndex 检查 Leader 是否可以推进其 commitIndex。
// 计算已在集群多数节点上成功复制的最高日志索引，并更新 Leader 自己的 commitIndex。
// Raft 的安全规则规定，只有当前任期的日志才可以通过这种方式被提交。
func (r *Raft) updateCommitIndex() bool {
	for {
		newCommitIndex := r.findMajorityCommitIndex()
		if newCommitIndex <= r.commitIndex {
			return false
		}

		term, err := r.getLogTermLocked(newCommitIndex)
		if err != nil {
			if errors.Is(err, errLogEntryNotFound) && r.rewindCommitSearchPastLocalGapLocked(newCommitIndex) {
				continue
			}
			log.Errorf("[Replication] Node %d failed to get term for new commit index %d: %v", r.id, newCommitIndex, err)
			return false
		}

		if term == r.currentTerm {
			log.Debugf("[Log Replication] Node %d advances commitIndex to %d (term=%d)", r.id, newCommitIndex, r.currentTerm)
			r.commitIndex = newCommitIndex
			r.startApplyLogsLocked()
			return true
		}
		return false
	}
}

func (r *Raft) rewindCommitSearchPastLocalGapLocked(missingIndex uint64) bool {
	r.refreshCachedLastLogIndexLocked()
	if missingIndex > r.cachedLastLogIndex {
		return true
	}
	if missingIndex <= r.commitIndex {
		return false
	}

	newLastIndex := missingIndex - 1
	if r.snapshot != nil && newLastIndex < r.snapshot.LastIncludedIndex {
		newLastIndex = r.snapshot.LastIncludedIndex
	}
	if newLastIndex >= r.cachedLastLogIndex {
		return false
	}

	log.Warnf("[Replication] Node %d found local log gap at commit candidate %d; rewinding cached last log index from %d to %d", r.id, missingIndex, r.cachedLastLogIndex, newLastIndex)
	r.cachedLastLogIndex = newLastIndex
	for peerID, nextIndex := range r.nextIndex {
		if nextIndex > r.cachedLastLogIndex+1 {
			r.nextIndex[peerID] = r.cachedLastLogIndex + 1
		}
	}
	return true
}

// findMajorityCommitIndex 计算可以被安全提交的最高日志索引。
func (r *Raft) findMajorityCommitIndex() uint64 {
	searchStart := r.commitIndex
	if r.isReplicatedByMajority(r.cachedLastLogIndex) {
		searchStart = r.cachedLastLogIndex
	} else {
		for _, peerID := range r.peerIDs {
			if peerID == r.id {
				continue
			}
			if mi := r.matchIndex[peerID]; mi > searchStart {
				searchStart = mi
			}
		}
		if r.inJointConsensus {
			for _, peerID := range r.newPeerIDs {
				if peerID == r.id {
					continue
				}
				if mi := r.matchIndex[peerID]; mi > searchStart {
					searchStart = mi
				}
			}
		}
	}
	if searchStart > r.cachedLastLogIndex {
		searchStart = r.cachedLastLogIndex
	}

	// 从后往前检查每一个日志索引，看它是否满足多数派提交的条件。
	for N := searchStart; N > r.commitIndex; N-- {
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
		if peerID == r.id {
			continue
		}
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
		if peerID == r.id {
			continue
		}
		if r.matchIndex[peerID] >= index {
			matchCountNew++
		}
	}
	majorityNew := (len(r.newPeerIDs) / 2) + 1

	return matchCountOld >= majorityOld && matchCountNew >= majorityNew
}

// AppendEntries 是 Follower 节点上的 RPC 处理函数，用于接收 Leader 的心跳和日志。
//
// 优化：使用三阶段锁模式，将磁盘 I/O 移出 r.mu 锁。
// Phase 0: appendEntriesMu 串行化所有 AppendEntries 处理
// Phase 1: r.mu 短锁 — 任期检查 + 心跳 + 收集快照信息
// Phase 2: 锁外磁盘 I/O — 一致性检查 + TruncateLog + AppendEntries
// Phase 3: r.mu 短锁 — 验证任期未变 + 更新 commitIndex
func (r *Raft) AppendEntries(args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error {
	// Phase 0: 串行化所有 AppendEntries 处理
	r.appendEntriesMu.Lock()
	defer r.appendEntriesMu.Unlock()

	// === Phase 1: 短锁 — 任期检查 + 心跳处理 ===
	r.mu.Lock()

	// 1. 处理任期检查和心跳。
	if !r.handleTermAndHeartbeat(args, reply) {
		r.mu.Unlock()
		return nil
	}

	// 快速路径：心跳（无日志条目）无需磁盘 I/O
	if len(args.Entries) == 0 {
		// 直接进行日志一致性检查（短操作）
		if ok := r.checkLogConsistency(args, reply); !ok {
			r.mu.Unlock()
			return nil
		}
		r.updateFollowerCommitIndex(args)
		reply.Success = true
		r.mu.Unlock()
		return nil
	}

	// 捕获快照引用用于锁外一致性检查
	snapshot := r.snapshot
	savedTerm := r.currentTerm
	r.mu.Unlock()

	// === Phase 2: 锁外磁盘 I/O ===
	// appendEntriesMu 保证此期间不会有另一个 AppendEntries 执行 TruncateLog

	// 2. 日志一致性检查（锁外）
	if ok := r.checkLogConsistencyLockFree(args, reply, snapshot); !ok {
		return nil
	}

	// 3. Raft 正确的日志追加：仅在发现冲突时截断，保护已提交的条目。
	//    对比 incoming entries 与 store 中已有的条目，从第一个不匹配处截断。
	//    这避免了在 truncate 和 append 之间的窗口期内删除已提交的条目，
	//    从而防止并发 applyLogs goroutine 读到 nil entry 导致 fatal。
	newEntries, truncateFrom, err := r.findConflictAndPrepare(args)
	if err != nil {
		reply.Success = false
		return err
	}

	if truncateFrom > 0 {
		if err := r.store.TruncateLog(truncateFrom); err != nil {
			log.Errorf("[Replication] Node %d failed to truncate log from %d: %v", r.id, truncateFrom, err)
			reply.Success = false
			return err
		}
	}

	if len(newEntries) > 0 {
		if err := r.store.AppendEntries(newEntries); err != nil {
			log.Errorf("[Replication] Node %d failed to append entries to store: %v", r.id, err)
			reply.Success = false
			return err
		}
	}
	log.Debugf("[Log Replication] Node %d accepted and stored %d new entries from leader %d", r.id, len(newEntries), args.LeaderID)

	// === Phase 3: 短锁 — 验证任期 + 更新 commitIndex ===
	r.mu.Lock()

	// 更新缓存的 lastLogIndex：使用 args.Entries 中的最后一个 entry 的 Index，
	// 因为无论是否有冲突截断，最终结果都是 store 中包含了所有 args.Entries。
	newLastIndex := args.Entries[len(args.Entries)-1].Index
	if newLastIndex > r.cachedLastLogIndex {
		r.cachedLastLogIndex = newLastIndex
	} else if truncateFrom > 0 {
		// 如果发生了截断，lastLogIndex 可能减小了
		r.cachedLastLogIndex = newLastIndex
	}

	// 验证任期未在 Phase 2 期间变化
	if r.currentTerm != savedTerm {
		// 日志已经按当时合法的 AppendEntries 写入本地，但不能让旧任期
		// Leader 把这个响应当作当前任期的复制确认。
		reply.Term = r.currentTerm
		reply.Success = false
		r.mu.Unlock()
		return nil
	}

	// 4. 根据 Leader 的进度更新本地的 commitIndex。
	r.updateFollowerCommitIndex(args)

	reply.Success = true
	r.mu.Unlock()
	return nil
}

// checkLogConsistencyLockFree 在锁外执行日志一致性检查。
// 使用传入的 snapshot 引用代替 r.snapshot，直接调用 store 方法。
// 安全性：appendEntriesMu 保证此期间无并发日志修改。
func (r *Raft) checkLogConsistencyLockFree(args *param.AppendEntriesArgs, reply *param.AppendEntriesReply, snapshot *param.Snapshot) bool {
	if args.PrevLogIndex == 0 {
		return true
	}

	// 先检查快照
	if snapshot != nil && args.PrevLogIndex == snapshot.LastIncludedIndex {
		if snapshot.LastIncludedTerm == args.PrevLogTerm {
			return true
		}
		reply.Success = false
		reply.ConflictTerm = snapshot.LastIncludedTerm
		reply.ConflictIndex = args.PrevLogIndex
		return false
	}

	prevEntry, err := r.store.GetEntry(args.PrevLogIndex)
	if err != nil {
		log.Errorf("[Replication] Node %d failed to get entry %d from store: %v", r.id, args.PrevLogIndex, err)
		reply.Success = false
		lastLogIndex, _ := r.store.LastLogIndex()
		reply.ConflictIndex = lastLogIndex + 1
		reply.ConflictTerm = 0
		return false
	}
	if prevEntry == nil || prevEntry.Term != args.PrevLogTerm {
		if prevEntry == nil {
			if consistent, handled := r.checkStoredSnapshotTerm(args.PrevLogIndex, args.PrevLogTerm, reply); handled {
				return consistent
			}

			lastLogIndex, _ := r.store.LastLogIndex()
			reply.ConflictIndex = lastLogIndex + 1
			reply.ConflictTerm = 0
		} else {
			reply.ConflictTerm = prevEntry.Term
			reply.ConflictIndex = args.PrevLogIndex
		}
		reply.Success = false
		return false
	}

	return true
}

// findConflictAndPrepare 对比 incoming entries 与 store 中已有的条目，
// 找到第一个冲突点，返回需要追加的新条目和需要截断的起始索引。
// 这是 Raft 论文 Section 5.3 的正确实现：如果同一索引的新旧日志任期冲突，
// 删除本地旧条目以及它之后的所有条目。
//
// 返回值:
//   - newEntries: 需要追加到 store 的条目（跳过已存在且一致的条目）
//   - truncateFrom: 如果 > 0，表示需要从此索引开始截断
//   - err: 错误
func (r *Raft) findConflictAndPrepare(args *param.AppendEntriesArgs) (newEntries []param.LogEntry, truncateFrom uint64, err error) {
	for i, entry := range args.Entries {
		existing, err := r.store.GetEntry(entry.Index)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to get entry %d: %w", entry.Index, err)
		}

		if existing == nil {
			// 此索引不存在条目，从这里开始都是新的
			return args.Entries[i:], 0, nil
		}

		if existing.Term != entry.Term {
			// 冲突：同一索引但不同任期，从这里截断并追加
			return args.Entries[i:], entry.Index, nil
		}
		// 条目一致，跳过
	}

	// 所有条目都已存在且一致，无需任何操作
	return nil, 0, nil
}

// handleTermAndHeartbeat 负责处理任期检查和重置选举计时器。
// 如果 Leader 的任期有效，返回 true；如果因任期过时而应立即拒绝，返回 false。
func (r *Raft) handleTermAndHeartbeat(args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) bool {
	reply.Term = r.currentTerm
	if r.getState() == Dead {
		reply.Success = false
		return false
	}

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

	r.acceptLeaderForCurrentTermLocked(args.LeaderID)

	// 只要收到了来自当前（或更新后的）合法 Leader 的消息，就重置选举计时器。
	// 这表明 Leader 仍然活跃，Follower 不需要发起新的选举。
	r.electionResetEvent = time.Now()

	// 重置下一次的随机超时
	r.currentElectionTimeout = r.randomizedElectionTimeout()
	return true
}

func (r *Raft) acceptLeaderForCurrentTermLocked(leaderID int) {
	r.knownLeaderID = leaderID
	if r.getState() == Follower {
		return
	}

	log.Debugf("[Log Replication] Node %d accepts leader %d for term %d and steps down from state %d", r.id, leaderID, r.currentTerm, r.getState())
	r.setState(Follower)
	r.abortPendingApplyWaitersLocked()
	r.replicating = make(map[int]bool)
	r.replicationPending = make(map[int]bool)
	r.lastAck = make(map[int]time.Time)
	r.lastLeadershipConfirm = time.Time{}
	r.leaseUntil = time.Time{}
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
		reply.ConflictIndex = r.cachedLastLogIndex + 1
		reply.ConflictTerm = 0
		return false
	}
	// 检查获取到的日志条目是否与 Leader 的期望一致。
	if prevEntry == nil || prevEntry.Term != args.PrevLogTerm {
		// 如果 prevEntry 为 nil，说明本地日志在 prevLogIndex 处没有条目，即日志过短。
		if prevEntry == nil {
			if consistent, handled := r.checkStoredSnapshotTerm(args.PrevLogIndex, args.PrevLogTerm, reply); handled {
				return consistent
			}

			reply.ConflictIndex = r.cachedLastLogIndex + 1
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

func (r *Raft) checkStoredSnapshotTerm(prevLogIndex, prevLogTerm uint64, reply *param.AppendEntriesReply) (bool, bool) {
	snapshotTerm, ok, err := r.readStoredSnapshotTerm(prevLogIndex)
	if err != nil {
		log.Errorf("[Replication] Node %d failed to read snapshot for prevLogIndex %d: %v", r.id, prevLogIndex, err)
		reply.Success = false
		lastLogIndex, _ := r.store.LastLogIndex()
		reply.ConflictIndex = lastLogIndex + 1
		reply.ConflictTerm = 0
		return false, true
	}
	if !ok {
		return false, false
	}
	if snapshotTerm == prevLogTerm {
		return true, true
	}
	reply.Success = false
	reply.ConflictTerm = snapshotTerm
	reply.ConflictIndex = prevLogIndex
	return false, true
}

// updateFollowerCommitIndex 根据 Leader 发来的 leaderCommit 更新 Follower 的 commitIndex。
func (r *Raft) updateFollowerCommitIndex(args *param.AppendEntriesArgs) {
	if args.LeaderCommit > r.commitIndex {
		newLastLogIndex := r.cachedLastLogIndex
		oldCommitIndex := r.commitIndex
		if args.LeaderCommit <= newLastLogIndex {
			r.commitIndex = args.LeaderCommit
		} else {
			r.commitIndex = newLastLogIndex
		}

		if r.commitIndex > oldCommitIndex {
			log.Debugf("[Log Replication] Node %d advances commitIndex to %d", r.id, r.commitIndex)
			r.startApplyLogsLocked()
		}
	}
}

// startApplyLogsLocked schedules committed entries to be applied.
// The caller must hold r.mu. Stop sets the state to Dead under the same lock
// before waiting on applyWG, so no new apply goroutine can be added after
// Stop starts waiting.
func (r *Raft) startApplyLogsLocked() {
	if r.getState() == Dead {
		return
	}
	r.applyWG.Add(1)
	go func() {
		defer r.applyWG.Done()
		r.applyLogs()
	}()
}

// applyLogs 将已提交的日志应用到状态机。此函数会在后台 goroutine 中运行。
func (r *Raft) applyLogs() {
	r.applyMu.Lock()

	for {
		// 1. 从存储中获取所有需要应用的日志条目。
		entriesToApply, lastAppliedBefore := r.fetchEntriesToApply()
		if len(entriesToApply) == 0 {
			r.applyMu.Unlock()
			return
		}

		endIndex := lastAppliedBefore + uint64(len(entriesToApply))
		log.Debugf("[State Machine] Node %d applying %d entries from index %d to %d", r.id, len(entriesToApply), lastAppliedBefore+1, endIndex)

		// 2. 遍历并分发每一条待应用的日志。
		r.dispatchEntries(entriesToApply)

		r.mu.Lock()
		hasBacklog := r.commitIndex > r.lastApplied && r.lastApplied > lastAppliedBefore
		r.mu.Unlock()
		if !hasBacklog {
			break
		}
	}
	r.applyMu.Unlock()

	// 3. 检查是否需要触发快照。快照导出可能会触发 LSM flush 和 SSTable
	// 扫描。只有在当前已提交日志全部 apply 后才尝试快照，避免快照导出
	// 排在 backlog 前面阻塞 lastApplied 追赶 ReadIndex。
	r.TakeSnapshot()
}

// fetchEntriesToApply 负责从存储中获取所有已提交但尚未应用的日志条目。
// 调用方必须串行化 apply 流程，并且只在状态机真正应用后推进 r.lastApplied。
//
// 并发安全：先获取 appendEntriesMu 再获取 r.mu，确保读取 store 期间
// 不会有并发的 TruncateLog + AppendEntries（来自三阶段 AppendEntries 的 Phase 2）。
// 这防止了 Leader 切换时合法的日志截断导致已提交条目短暂不可见的问题。
func (r *Raft) fetchEntriesToApply() ([]param.LogEntry, uint64) {
	r.appendEntriesMu.Lock()
	r.mu.Lock()

	var entries []param.LogEntry
	if r.commitIndex > r.lastApplied {
		lastLogIndex := r.cachedLastLogIndex

		if r.commitIndex > lastLogIndex {
			r.refreshCachedLastLogIndexLocked()
			lastLogIndex = r.cachedLastLogIndex
		}

		applyUntil := r.commitIndex
		if applyUntil > lastLogIndex {
			log.Errorf("[Replication] Node %d commitIndex %d exceeds refreshed lastLogIndex %d; preserving commit index and applying through local tail", r.id, r.commitIndex, lastLogIndex)
			applyUntil = lastLogIndex
		}

		for i := r.lastApplied + 1; i <= applyUntil; i++ {
			entry, err := r.store.GetEntry(i)
			if err != nil || entry == nil {
				if err == nil && r.advanceLastAppliedPastCompactedLogLocked(i) {
					i = r.lastApplied
					continue
				}
				log.Fatalf("[FATAL] Node %d could not retrieve committed log entry %d to apply it. Error: %v", r.id, i, err)
				r.mu.Unlock()
				r.appendEntriesMu.Unlock()
				return nil, 0
			}
			entries = append(entries, *entry)
		}
	}

	lastAppliedBeforeUpdate := r.lastApplied

	r.mu.Unlock()
	r.appendEntriesMu.Unlock()

	return entries, lastAppliedBeforeUpdate
}

func (r *Raft) advanceLastAppliedPastCompactedLogLocked(missingIndex uint64) bool {
	snapshot := r.snapshot
	if snapshot == nil || snapshot.LastIncludedIndex < missingIndex {
		storedSnapshot, err := r.store.ReadSnapshot()
		if err != nil {
			log.Errorf("[State Machine] Node %d failed to read snapshot while applying compacted entry %d: %v", r.id, missingIndex, err)
			return false
		}
		if storedSnapshot != nil {
			snapshot = storedSnapshot
			r.snapshot = storedSnapshot
		}
	}

	if snapshot == nil || snapshot.LastIncludedIndex < missingIndex {
		return false
	}

	if r.lastApplied < snapshot.LastIncludedIndex {
		log.Debugf("[State Machine] Node %d skips compacted entries through snapshot index %d", r.id, snapshot.LastIncludedIndex)
		r.lastApplied = snapshot.LastIncludedIndex
		r.lastAppliedCond.Broadcast()
	}
	return true
}

// dispatchEntries 遍历日志条目切片，并根据命令类型将其分发给具体的处理函数。
func (r *Raft) dispatchEntries(entries []param.LogEntry) {
	for _, entry := range entries {
		var result any
		clientID, sequenceNum, hasClient := param.ClientCommandMetadata(entry.Command)
		appliedEntry := entry
		appliedEntry.Command = param.UnwrapClientCommand(entry.Command)
		if hasClient && r.isClientCommandApplied(clientID, sequenceNum) {
			r.completeAppliedEntry(entry.Index, nil, clientID, sequenceNum, hasClient)
			continue
		}

		switch cmd := appliedEntry.Command.(type) {
		case param.NoopCommand:
			r.completeAppliedEntry(entry.Index, nil, clientID, sequenceNum, hasClient)
		case param.ConfigChangeCommand:
			// 配置变更命令，持有锁处理
			r.applyConfigChange(cmd, entry.Index)
			// 配置变更不需要返回结果，客户端只关心 Success
			result = nil
		default:
			// 普通命令：状态机写入和 lastApplied 推进必须对快照原子可见。
			// 否则快照可能捕获已经写入状态机、但 LastIncludedIndex 尚未覆盖的命令。
			r.stateMachineMu.Lock()
			result = r.stateMachine.Apply(appliedEntry)
			r.completeAppliedEntry(entry.Index, result, clientID, sequenceNum, hasClient)
			r.stateMachineMu.Unlock()

			// 发送到 commitChan（不持有锁）
			r.applyStateMachineCommand(appliedEntry)
			continue
		}

		r.completeAppliedEntry(entry.Index, result, clientID, sequenceNum, hasClient)
	}
}

func (r *Raft) isClientCommandApplied(clientID, sequenceNum int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	lastSeq, exists := r.clientSessions[clientID]
	return exists && sequenceNum <= lastSeq
}

func (r *Raft) completeAppliedEntry(index uint64, result any, clientID, sequenceNum int64, hasClient bool) {
	r.mu.Lock()
	notifyChans := r.notifyApply[index]
	if len(notifyChans) > 0 {
		delete(r.notifyApply, index)
	}
	if hasClient {
		if r.clientSessions == nil {
			r.clientSessions = make(map[int64]int64)
		}
		key := clientRequestKey{clientID: clientID, sequenceNum: sequenceNum}
		if lastSeq, exists := r.clientSessions[clientID]; !exists || sequenceNum > lastSeq {
			r.clientSessions[clientID] = sequenceNum
		}
		delete(r.pendingClientRequests, key)
		delete(r.pendingLogClients, index)
	}
	if index > r.lastApplied {
		r.lastApplied = index
		r.lastAppliedCond.Broadcast()
	}
	r.mu.Unlock()

	for _, notifyChan := range notifyChans {
		log.Debugf("[Client] dispatchEntries: Notifying for index %d", index)
		notifyChan <- result
	}
}

// applyConfigChange 处理配置变更命令，更新节点的成员状态。
func (r *Raft) applyConfigChange(cmd param.ConfigChangeCommand, entryIndex uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	enterJoint := func() {
		r.inJointConsensus = true
		r.newPeerIDs = cmd.NewPeerIDs
		r.jointConfigIndex = entryIndex
		log.Debugf("[Config Change] Node %d entering joint consensus at index %d.", r.id, entryIndex)

		if r.nextIndex == nil {
			r.nextIndex = make(map[int]uint64)
		}
		if r.matchIndex == nil {
			r.matchIndex = make(map[int]uint64)
		}
		for _, peerID := range r.newPeerIDs {
			if _, ok := r.nextIndex[peerID]; !ok {
				r.nextIndex[peerID] = r.cachedLastLogIndex + 1
				r.matchIndex[peerID] = 0
			}
		}
	}

	proposeFinalConfig := func() {
		if r.getState() == Leader {
			r.proposeNewConfigEntry()
		}
	}

	if !r.inJointConsensus {
		// --- Phase 1: 进入联合共识 (C_old,new) ---
		enterJoint()
		proposeFinalConfig()
		return
	}

	if r.jointConfigIndex == entryIndex {
		// Leader enters joint consensus before the C_old,new entry is applied
		// so it can replicate that entry under joint quorum rules. Applying
		// that same entry must not be mistaken for the final C_new entry.
		proposeFinalConfig()
		return
	}

	// --- Phase 2: 提交新配置 (C_new) ---
	// 此时联合共识结束，节点切换到仅使用新配置。
	r.peerIDs = r.newPeerIDs
	r.newPeerIDs = nil
	r.inJointConsensus = false
	r.jointConfigIndex = 0
	log.Debugf("[Config Change] Node %d has transitioned to new configuration at index %d.", r.id, entryIndex)

	// 检查自己是否还属于新配置。
	// 如果 Leader 发现自己被移除了，必须立即“退位” (Step Down)。
	_, exists := findPeer(r.id, r.peerIDs)
	if !exists && r.getState() == Leader {
		log.Debugf("[Config Change] Leader %d detected it is NOT in the new configuration. Stepping down.", r.id)

		// 自动降级为 Follower。
		// 使用 r.currentTerm 即可，因为这不是因为发现了更高任期，而是因为配置变更逻辑。
		// becomeFollower 会更新状态并重置状态机，这会使 Leader 的心跳协程(startHeartbeat)在下一次循环检测时自动退出。
		if err := r.becomeFollower(r.currentTerm); err != nil {
			log.Errorf("[Replication] Node %d failed to step down after removal: %v", r.id, err)
		}
		return
	}
}

// applyStateMachineCommand 将普通的日志条目作为 CommitEntry 发送到客户端的状态机通道。
// 注意：此函数不应该持有 r.mu 锁，以避免死锁。
func (r *Raft) applyStateMachineCommand(entry param.LogEntry) {
	// 使用 recover 防止通道关闭时的 panic
	entryIndex := entry.Index
	nodeID := r.id
	defer func() {
		if rv := recover(); rv != nil {
			// 通道已关闭，忽略发送失败
			log.Debugf("[Replication] Node %d commitChan closed during send of entry %d", nodeID, entryIndex)
		}
	}()

	// 快速检查 channel 是否可用，不持有锁
	r.mu.Lock()
	commitChan := r.commitChan
	r.mu.Unlock()

	if commitChan == nil {
		return
	}

	// 使用非阻塞发送防止死锁
	select {
	case commitChan <- param.CommitEntry{
		Command: entry.Command,
		Index:   entry.Index,
		Term:    entry.Term,
	}:
		// 成功发送
	default:
		// 通道已满，跳过发送（防止阻塞）
		log.Warnf("[Replication] Node %d commitChan full, skipping entry %d", r.id, entry.Index)
	}
}

// proposeNewConfigEntry 是 Leader 用于提交 C_new（最终配置）日志条目的辅助函数。
func (r *Raft) proposeNewConfigEntry() {
	configCmd := param.NewConfigChangeCommand(r.newPeerIDs)
	newIndex := r.cachedLastLogIndex + 1
	newLogEntry := param.NewLogEntry(configCmd, r.currentTerm, newIndex)
	if err := r.store.AppendEntries([]param.LogEntry{newLogEntry}); err != nil {
		log.Errorf("[Replication] leader %d failed to append C_new config entry: %s", r.id, err.Error())
		return
	}
	// 更新缓存
	r.cachedLastLogIndex = newIndex
	log.Debugf("[Replication] leader %d proposed final C_new config entry at index %d", r.id, newIndex)
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

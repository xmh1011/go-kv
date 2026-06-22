package raft

import (
	"errors"
	"strconv"
	"time"

	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
)

// InstallSnapshot 是 Follower 上的 RPC 处理函数，用于接收并安装 Leader 发来的快照。
//
// 优化：将磁盘与状态机重建移到 Raft 锁外执行，减少锁持有时间。
// 状态机重建需要与 Apply/Get/TakeSnapshot 串行化，防止 LSM 目录被并发读写。
func (r *Raft) InstallSnapshot(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
	// 1. 快速任期检查（短锁）
	r.mu.Lock()
	if r.getState() == Dead {
		reply.Term = r.currentTerm
		r.mu.Unlock()
		return nil
	}
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		r.mu.Unlock()
		return nil
	}

	if args.Term > r.currentTerm {
		if err := r.becomeFollower(args.Term); err != nil {
			reply.Term = r.currentTerm
			r.mu.Unlock()
			return nil
		}
	}
	reply.Term = r.currentTerm

	// 重置选举计时器
	r.electionResetEvent = time.Now()

	// 检查快照是否过时
	if args.LastIncludedIndex <= r.lastApplied {
		log.Debugf("[Snapshot] Node %d ignoring snapshot with index %d, already applied up to %d", r.id, args.LastIncludedIndex, r.lastApplied)
		r.mu.Unlock()
		return nil
	}

	r.mu.Unlock()

	log.Debugf("[Snapshot] Node %d received snapshot from leader %d (lastIncludedIndex=%d)", r.id, args.LeaderID, args.LastIncludedIndex)

	// 2. 创建快照对象（锁外）
	snapshot := param.NewSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)

	// 3. 串行化状态机重建，再次确认快照仍然需要安装。
	r.stateMachineMu.Lock()
	defer r.stateMachineMu.Unlock()

	r.mu.Lock()

	// 验证快照索引仍然有效
	if args.Term < r.currentTerm || snapshot.LastIncludedIndex <= r.lastApplied {
		log.Debugf("[Snapshot] Node %d snapshot index %d no longer needed, lastApplied is %d", r.id, snapshot.LastIncludedIndex, r.lastApplied)
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	// 4. 将快照持久化到存储（锁外磁盘 I/O）
	if err := r.store.SaveSnapshot(snapshot); err != nil {
		log.Errorf("[Snapshot] Node %d failed to persist snapshot: %v", r.id, err)
		return err
	}

	// 5. 将快照数据应用到上层状态机
	if err := r.stateMachine.ApplySnapshot(snapshot.Data); err != nil {
		log.Errorf("[Snapshot] Node %d failed to apply snapshot to state machine: %v", r.id, err)
		return err
	}

	// 6. 压缩本地日志
	r.appendEntriesMu.Lock()
	if err := r.store.CompactLog(snapshot.LastIncludedIndex); err != nil {
		r.appendEntriesMu.Unlock()
		log.Errorf("[Snapshot] Node %d failed to compact log after installing snapshot: %v", r.id, err)
		return err
	}
	r.appendEntriesMu.Unlock()

	// 7. 更新内存状态
	r.mu.Lock()
	r.snapshot = snapshot
	r.commitIndex = max(r.commitIndex, snapshot.LastIncludedIndex)
	r.lastApplied = max(r.lastApplied, snapshot.LastIncludedIndex)
	r.cachedLastLogIndex = max(r.cachedLastLogIndex, snapshot.LastIncludedIndex)
	r.lastAppliedCond.Broadcast()
	r.mu.Unlock()

	log.Debugf("[Snapshot] Node %d successfully installed snapshot. lastApplied is now %d.", r.id, r.lastApplied)
	return nil
}

// TakeSnapshot 由上层应用（状态机）在合适的时候调用，以触发一次快照。
// 为异步实现，避免阻塞 Raft 主循环。返回 true 表示已调度一次真实快照。
func (r *Raft) TakeSnapshot() bool {
	r.mu.Lock()

	// 1. 防止并发快照
	if r.isSnapshotting || r.snapshotThreshold <= 0 {
		r.mu.Unlock()
		return false
	}

	// 2. 检查日志大小是否满足阈值
	threshold := r.snapshotThreshold
	logSize, err := r.store.LogSize()
	if err != nil || logSize < threshold {
		r.mu.Unlock()
		return false
	}

	// Mark the snapshot before taking stateMachineMu. Repeated apply loops can
	// then skip immediately instead of queueing behind an active snapshot export.
	r.isSnapshotting = true
	r.mu.Unlock()

	log.Debugf("[Snapshot] Node %d log size %d exceeds threshold %d, preparing snapshot.", r.id, logSize, threshold)

	r.stateMachineMu.Lock()

	// 3. 【同步阶段】捕获快照元数据。此时 stateMachineMu 已阻止
	// 新的 Apply 进入，因此 lastApplied 与随后导出的状态机数据一致。
	r.mu.Lock()
	snapshotIndex := r.lastApplied
	snapshotTerm, err := r.getLogTermLocked(snapshotIndex)
	if err != nil {
		log.Errorf("[Snapshot] Node %d failed to get term at index %d: %v", r.id, snapshotIndex, err)
		r.isSnapshotting = false
		r.mu.Unlock()
		r.stateMachineMu.Unlock()
		return false
	}

	// 释放 Raft 锁。状态机锁继续持有到数据导出完成。
	r.mu.Unlock()

	var snapshotData []byte
	if prepared, ok := r.stateMachine.(storage.PreparedSnapshotStateMachine); ok {
		readSnapshot, prepareErr := prepared.PrepareSnapshot()
		r.stateMachineMu.Unlock()
		if prepareErr != nil {
			err = prepareErr
		} else {
			snapshotData, err = readSnapshot()
		}
	} else {
		snapshotData, err = r.stateMachine.GetSnapshot()
		r.stateMachineMu.Unlock()
	}
	if err != nil {
		log.Errorf("[Snapshot] Node %d failed to get snapshot data: %v", r.id, err)
		r.mu.Lock()
		r.isSnapshotting = false
		r.mu.Unlock()
		return false
	}

	// 4. 【异步阶段】执行耗时的 IO 操作
	r.snapshotWG.Add(1)
	go func(index, term uint64, data []byte) {
		// 确保 goroutine 结束时清理标志
		defer func() {
			r.mu.Lock()
			r.isSnapshotting = false
			r.mu.Unlock()
			r.snapshotWG.Done()
		}()

		log.Debugf("[Snapshot] Node %d starting async persistence for index %d", r.id, index)

		snapshot := param.NewSnapshot(index, term, data)

		// A. 持久化快照到磁盘 (耗时 IO)
		// 这不需要持有 Raft 锁，因为我们操作的是独立的快照文件
		if err := r.store.SaveSnapshot(snapshot); err != nil {
			log.Errorf("[Snapshot] Node %d failed to save snapshot async: %v", r.id, err)
			return
		}

		// B. 压缩日志 (Compact Log)。与 AppendEntries/apply 的日志读写
		// 使用相同顺序加锁，避免截断时并发读取旧日志。
		r.appendEntriesMu.Lock()
		r.mu.Lock()

		// 再次检查状态（防止在 IO 期间节点关闭或状态剧烈变化）
		if r.getState() == Dead {
			r.mu.Unlock()
			r.appendEntriesMu.Unlock()
			return
		}
		// 快照已经持久化，先发布内存引用，再释放 Raft 主锁执行慢速
		// 日志压缩。这样心跳、选举计时器和复制进度不会被大量删除阻塞。
		r.snapshot = snapshot
		r.mu.Unlock()

		// 执行日志截断
		if err := r.store.CompactLog(index); err != nil {
			log.Errorf("[Snapshot] Node %d failed to compact log async: %v", r.id, err)
			r.appendEntriesMu.Unlock()
			return
		}

		log.Debugf("[Snapshot] Node %d async snapshot finished. Saved and compacted up to index %d.", r.id, index)
		r.appendEntriesMu.Unlock()

	}(snapshotIndex, snapshotTerm, snapshotData)
	return true
}

// handleSnapshotTerm 负责处理 InstallSnapshot RPC 中的任期检查和心跳逻辑。
// 如果 Leader 的任期有效，返回 true。此函数必须在持有锁的情况下被调用。
func (r *Raft) handleSnapshotTerm(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) bool {
	reply.Term = r.currentTerm
	if args.Term < r.currentTerm {
		return false
	}

	if args.Term > r.currentTerm {
		if err := r.becomeFollower(args.Term); err != nil {
			return false
		}
		reply.Term = r.currentTerm
	}
	r.electionResetEvent = time.Now()
	return true
}

// persistSnapshot 负责将快照保存到稳定存储，并根据快照索引压缩日志。
func (r *Raft) persistSnapshot(snapshot *param.Snapshot) error {
	// 将快照持久化到存储。
	if err := r.store.SaveSnapshot(snapshot); err != nil {
		log.Errorf("[Snapshot] Node %d failed to save received snapshot: %v", r.id, err)
		return err
	}
	// 更新内存中的快照引用，避免频繁从存储读取。
	r.snapshot = snapshot

	// 压缩本地日志，删除所有已被快照覆盖的条目。
	if err := r.store.CompactLog(snapshot.LastIncludedIndex); err != nil {
		log.Errorf("[Snapshot] Node %d failed to compact log after installing snapshot: %v", r.id, err)
		return err
	}
	return nil
}

// updateStateAfterSnapshot 在成功安装快照后，更新节点的内部状态索引。
func (r *Raft) updateStateAfterSnapshot(snapshotIndex uint64) {
	r.commitIndex = max(r.commitIndex, snapshotIndex)
	r.lastApplied = max(r.lastApplied, snapshotIndex)
	r.cachedLastLogIndex = max(r.cachedLastLogIndex, snapshotIndex)
}

// sendSnapshot 是 Leader 用于向落后的 Follower 发送快照
//
// 优化：将快照读取操作移到锁外执行，减少锁持有时间。
func (r *Raft) sendSnapshot(peerID int) {
	// 1. 快速状态检查（短锁）
	r.mu.Lock()
	if r.getState() != Leader {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	// 2. 从存储中读取最新的快照（锁外磁盘 I/O）
	snapshot, err := r.store.ReadSnapshot()
	if err != nil {
		log.Errorf("[Snapshot] Node %d failed to read snapshot to send to peer %d: %v", r.id, peerID, err)
		return
	}
	if snapshot == nil {
		log.Errorf("[Snapshot] Node %d tried to send snapshot to peer %d, but no snapshot is available.", r.id, peerID)
		return
	}

	// 3. 准备 RPC 参数（短锁）
	r.mu.Lock()
	args := param.NewInstallSnapshotArgs(r.currentTerm, uint64(r.id), snapshot.LastIncludedIndex, snapshot.LastIncludedTerm, snapshot.Data)
	savedCurrentTerm := r.currentTerm
	r.mu.Unlock()

	// 4. 发起 RPC 调用（锁外网络 I/O）
	reply := &param.InstallSnapshotReply{}
	if err := r.trans.SendInstallSnapshot(strconv.Itoa(peerID), args, reply); err != nil {
		log.Debugf("[Snapshot] Node %d failed to send snapshot to %d: %v", r.id, peerID, err)
		return
	}

	// 5. 处理 RPC 响应（持锁）
	r.processSnapshotReply(peerID, reply, snapshot.LastIncludedIndex, savedCurrentTerm)
}

// readSnapshotForSending 负责从存储中读取最新的快照。
// 已废弃：使用 sendSnapshot 中的内联实现替代，避免不必要的锁持有。
// 保留此函数以兼容可能的外部调用。
func (r *Raft) readSnapshotForSending(peerID int) (*param.Snapshot, error) {
	// 不再持有锁，直接读取
	snapshot, err := r.store.ReadSnapshot()
	if err != nil {
		log.Errorf("[Snapshot] Node %d failed to read snapshot to send to peer %d: %v", r.id, peerID, err)
		return nil, err
	}
	if snapshot == nil {
		log.Errorf("[Snapshot] Node %d tried to send snapshot to peer %d, but no snapshot is available.", r.id, peerID)
		return nil, errors.New("no snapshot available to send")
	}
	return snapshot, nil
}

// processSnapshotReply 负责处理来自 Follower 的 InstallSnapshot RPC 响应。
func (r *Raft) processSnapshotReply(peerID int, reply *param.InstallSnapshotReply, snapshotLastIndex uint64, savedCurrentTerm uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 检查响应是否已过期（例如，在 RPC 通信期间，Leader 身份或任期已发生变化）。
	if r.currentTerm != savedCurrentTerm || r.getState() != Leader {
		return
	}

	// 如果对方的任期更高，说明自己已不再是 Leader，应立即转为 Follower。
	if reply.Term > r.currentTerm {
		if err := r.becomeFollower(reply.Term); err != nil {
			log.Errorf("[Snapshot] Node %d failed to persist state after processing snapshot reply: %v", r.id, err)
		}
		return
	}

	// 成功的快照发送也是一个有效的 ACK
	r.lastAck[peerID] = time.Now()

	// 如果一切正常，说明快照已成功发送并被对方接收。
	// 更新该 Follower 的 nextIndex 和 matchIndex，使其指向快照之后的第一个位置。
	r.nextIndex[peerID] = snapshotLastIndex + 1
	r.matchIndex[peerID] = snapshotLastIndex
	log.Debugf("[Snapshot] Node %d successfully sent snapshot to peer %d. nextIndex=%d, matchIndex=%d", r.id, peerID, r.nextIndex[peerID], r.matchIndex[peerID])
}

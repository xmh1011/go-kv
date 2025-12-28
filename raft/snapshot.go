package raft

import (
	"errors"
	"strconv"
	"time"

	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
)

// InstallSnapshot 是 Follower 上的 RPC 处理函数，用于接收并安装 Leader 发来的快照。
func (r *Raft) InstallSnapshot(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. 处理任期检查。如果 Leader 的任期有效，则继续；否则拒绝。
	if !r.handleSnapshotTerm(args, reply) {
		return nil
	}

	log.Infof("[Snapshot] Node %d received snapshot from leader %d (lastIncludedIndex=%d)", r.id, args.LeaderID, args.LastIncludedIndex)

	// 2. 将快照持久化到存储并压缩本地日志。
	snapshot := param.NewSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
	if err := r.persistSnapshot(snapshot); err != nil {
		log.Errorf("[Snapshot] Node %d failed to persist snapshot: %v", r.id, err)
		return err
	}

	// 3. 将快照数据应用到上层状态机
	if err := r.stateMachine.ApplySnapshot(snapshot.Data); err != nil {
		log.Errorf("[Snapshot] Node %d failed to apply snapshot to state machine: %v", r.id, err)
		return err
	}

	// 4. 更新本地的 commitIndex 和 lastApplied 索引。
	r.updateStateAfterSnapshot(snapshot.LastIncludedIndex)

	log.Infof("[Snapshot] Node %d successfully installed snapshot. lastApplied is now %d.", r.id, r.lastApplied)
	return nil
}

// TakeSnapshot 由上层应用（状态机）在合适的时候调用，以触发一次快照。
// 为异步实现，避免阻塞 Raft 主循环。
func (r *Raft) TakeSnapshot() {
	r.mu.Lock()

	// 1. 防止并发快照
	if r.isSnapshotting || r.snapshotThreshold <= 0 {
		r.mu.Unlock()
		return
	}

	// 2. 检查日志大小是否满足阈值
	logSize, err := r.store.LogSize()
	if err != nil || logSize < r.snapshotThreshold {
		r.mu.Unlock()
		return
	}

	log.Infof("[Snapshot] Node %d log size %d exceeds threshold %d, preparing snapshot.", r.id, logSize, r.snapshotThreshold)

	// 3. 【同步阶段】捕获快照元数据和状态机数据
	// 我们必须在持有锁的情况下捕获当前的 lastApplied 及其对应的 Term。
	snapshotIndex := r.lastApplied
	snapshotTermEntry, err := r.store.GetEntry(snapshotIndex)
	if err != nil {
		log.Errorf("[Snapshot] Node %d failed to get entry at index %d: %v", r.id, snapshotIndex, err)
		r.mu.Unlock()
		return
	}
	snapshotTerm := snapshotTermEntry.Term

	// 获取状态机数据。
	// 注意：如果 stateMachine.GetSnapshot() 非常耗时（例如全量序列化），这里仍然会阻塞 Raft。
	// 生产级实现通常要求 StateMachine 支持 Copy-On-Write 或快速 Clone，
	// 或者在此处仅获取一个只读视图/迭代器，将序列化工作移到异步 goroutine 中。
	// 鉴于接口限制，我们假设 GetSnapshot 是相对快速的内存操作。
	snapshotData, err := r.stateMachine.GetSnapshot()
	if err != nil {
		log.Errorf("[Snapshot] Node %d failed to get snapshot data: %v", r.id, err)
		r.mu.Unlock()
		return
	}

	// 标记开始快照，并释放锁
	r.isSnapshotting = true
	r.mu.Unlock()

	// 4. 【异步阶段】执行耗时的 IO 操作
	go func(index, term uint64, data []byte) {
		// 确保 goroutine 结束时清理标志
		defer func() {
			r.mu.Lock()
			r.isSnapshotting = false
			r.mu.Unlock()
		}()

		log.Infof("[Snapshot] Node %d starting async persistence for index %d", r.id, index)

		snapshot := param.NewSnapshot(index, term, data)

		// A. 持久化快照到磁盘 (耗时 IO)
		// 这不需要持有 Raft 锁，因为我们操作的是独立的快照文件
		if err := r.store.SaveSnapshot(snapshot); err != nil {
			log.Errorf("[Snapshot] Node %d failed to save snapshot async: %v", r.id, err)
			return
		}

		// B. 压缩日志 (Compact Log)
		// 这是一个关键操作，通常涉及删除旧的日志文件。
		// 虽然删除文件是 IO 操作，但我们需要确保与 Raft 核心逻辑（如 AppendEntries）的安全性。
		// 大多数 Storage 实现要求 CompactLog 时不能并发写入 Log，或者内部有锁。
		// 为了安全起见，我们在更新内存状态和调用 CompactLog 时重新获取锁。
		r.mu.Lock()

		// 再次检查状态（防止在 IO 期间节点关闭或状态剧烈变化）
		if r.state == Dead {
			r.mu.Unlock()
			return
		}

		// 执行日志截断
		if err := r.store.CompactLog(index); err != nil {
			log.Errorf("[Snapshot] Node %d failed to compact log async: %v", r.id, err)
			r.mu.Unlock()
			return
		}

		// 更新内存中的快照引用
		r.snapshot = snapshot

		log.Infof("[Snapshot] Node %d async snapshot finished. Saved and compacted up to index %d.", r.id, index)
		r.mu.Unlock()

	}(snapshotIndex, snapshotTerm, snapshotData)
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
}

// sendSnapshot 是 Leader 用于向落后的 Follower 发送快照
func (r *Raft) sendSnapshot(peerID int) {
	// 1. 从存储中读取最新的快照以准备发送。
	snapshot, err := r.readSnapshotForSending(peerID)
	if err != nil {
		return
	}

	// 2. 准备 RPC 参数。
	r.mu.Lock()
	args := param.NewInstallSnapshotArgs(r.currentTerm, uint64(r.id), snapshot.LastIncludedIndex, snapshot.LastIncludedTerm, snapshot.Data)
	savedCurrentTerm := r.currentTerm
	r.mu.Unlock()

	// 3. 发起 RPC 调用。
	reply := &param.InstallSnapshotReply{}
	if err := r.trans.SendInstallSnapshot(strconv.Itoa(peerID), args, reply); err != nil {
		log.Errorf("[Snapshot] Node %d failed to send snapshot to %d: %v", r.id, peerID, err)
		return
	}

	// 4. 处理 RPC 响应。
	r.processSnapshotReply(peerID, reply, snapshot.LastIncludedIndex, savedCurrentTerm)
}

// readSnapshotForSending 负责加锁并从存储中读取最新的快照。
func (r *Raft) readSnapshotForSending(peerID int) (*param.Snapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

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
	if r.currentTerm != savedCurrentTerm || r.state != Leader {
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
	log.Infof("[Snapshot] Node %d successfully sent snapshot to peer %d. nextIndex=%d, matchIndex=%d", r.id, peerID, r.nextIndex[peerID], r.matchIndex[peerID])
}

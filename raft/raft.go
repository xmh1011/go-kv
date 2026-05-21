package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xmh1011/go-kv/pkg/config"
	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
	"github.com/xmh1011/go-kv/pkg/storage/kvstore"
	"github.com/xmh1011/go-kv/pkg/transport"
)

// State 定义节点的状态（Consensus Module State）
type State int

const (
	Follower State = iota
	Candidate
	Leader
	Dead // 可选：表示节点已终止（用于优雅关闭）
)

// proposalRequest 表示一个待批处理的 Submit 请求
type proposalRequest struct {
	command any
	result  chan proposalResult
}

// proposalResult 表示批处理后的结果
type proposalResult struct {
	index uint64
	term  uint64
	ok    bool
}

// proposal 批处理常量
const (
	proposalChSize = 256 // proposalCh 缓冲大小
	maxBatchSize   = 64  // 单批最大 proposal 数量
)

type Raft struct {
	// mu 保护对 Raft 状态的并发访问
	mu sync.Mutex

	// appendEntriesMu 串行化 AppendEntries RPC 处理
	// 确保 TruncateLog 和 AppendEntries 不会并发执行
	appendEntriesMu sync.Mutex

	// applyMu serializes apply loops so lastApplied only advances after a
	// committed entry has been delivered to the state machine.
	applyMu sync.Mutex

	// id 是当前节点的服务器ID
	id int

	// Configuration state
	peerIDs          []int // 代表当前有效的配置 (Cold)
	newPeerIDs       []int // 在转换期间代表新配置 (Cnew)
	inJointConsensus bool  // 标记集群是否处于联合共识状态
	knownLeaderID    int   // 当前节点已知的 Leader ID

	// store 负责持久化 Raft 状态和日志信息
	store storage.Storage
	// trans 负责网络通信
	trans transport.Transport
	// stateMachine 应用层的状态机接口
	stateMachine storage.StateMachine

	// --- Raft 核心状态 ---
	currentTerm uint64
	votedFor    int
	state       atomic.Int32 // 使用 atomic 实现无锁状态检查

	// --- 日志与状态机相关 ---
	commitIndex        uint64
	lastApplied        uint64
	cachedLastLogIndex uint64 // 缓存 lastLogIndex 避免重复存储调用
	commitChan         chan param.CommitEntry
	lastAppliedCond    *sync.Cond // 用于等待 lastApplied 赶上 commitIndex

	// --- 快照相关 ---
	// snapshot 在内存中持有当前最新的快照，避免频繁从存储中读取
	snapshot          *param.Snapshot
	isSnapshotting    bool // 标记是否正在后台生成快照
	snapshotThreshold int  // 自动触发快照的日志大小阈值，<=0 表示禁用

	// --- 选举相关 ---
	electionResetEvent     time.Time
	electionTimeout        time.Duration // 基础选举超时时间
	heartbeatTimeout       time.Duration // 心跳间隔
	currentElectionTimeout time.Duration // 当前节点的随机选举超时

	// --- Leader 的易失性状态 ---
	nextIndex  map[int]uint64
	matchIndex map[int]uint64
	lastAck    map[int]time.Time // 跟踪 Leader 收到的每个 peer 的最后 ACK 时间

	// --- 客户端交互状态 ---
	clientSessions map[int64]int64
	notifyApply    map[uint64]chan any

	// --- 内部控制 ---
	shutdownChan chan struct{} // 用于关闭 Run 循环

	// --- Proposal 批处理 ---
	proposalCh chan proposalRequest // 用于 Submit 批处理的 channel

	// --- ReadIndex 优化 ---
	lastLeadershipConfirm time.Time     // 上次确认 Leadership 的时间
	leadershipCacheTime   time.Duration // Leadership 确认缓存时间

	// --- Lease Read ---
	leaseUntil    time.Time            // 租约到期时间（Leader 专用）
	leaseDuration time.Duration        // 租约长度，通常设为 electionTimeout
	readIndexMode config.ReadIndexMode // ReadIndex 实现模式
}

// NewRaft 创建一个新的 Raft 节点。
// 注意：store 参数的类型现在是 storage.KVStorage。
func NewRaft(id int, peerIDs []int, store storage.Storage, stateMachine storage.StateMachine, trans transport.Transport, commitChan chan param.CommitEntry) *Raft {
	r := &Raft{
		id:                id,
		peerIDs:           peerIDs,
		store:             store,
		stateMachine:      stateMachine,
		trans:             trans,
		inJointConsensus:  false,
		votedFor:          -1, // -1 表示未投票
		commitChan:        commitChan,
		nextIndex:         make(map[int]uint64),
		matchIndex:        make(map[int]uint64),
		clientSessions:    make(map[int64]int64),
		notifyApply:       make(map[uint64]chan any),
		shutdownChan:      make(chan struct{}),
		proposalCh:        make(chan proposalRequest, proposalChSize),
		lastAck:           make(map[int]time.Time),
		snapshotThreshold: -1, // 默认禁用自动快照
		electionTimeout:   config.Conf.Raft.ElectionTimeout,
		heartbeatTimeout:  config.Conf.Raft.HeartbeatTimeout,
		// 初始化 ReadIndex 缓存，缓存时间设置为心跳间隔的一半
		leadershipCacheTime: config.Conf.Raft.HeartbeatTimeout / 2,
		// 初始化 Lease Read 相关配置
		leaseDuration: config.Conf.Raft.ElectionTimeout,
		readIndexMode: config.Conf.Raft.ReadIndexMode,
	}
	r.setState(Follower)
	// 从稳定存储中恢复状态。
	if store != nil {
		hardState, err := store.GetState()
		if err != nil {
			log.Fatalf("[Raft] Failed to get hard state from storage: %s", err.Error())
			panic(fmt.Errorf("failed to get hard state: %w", err))
		}
		r.currentTerm = hardState.CurrentTerm
		r.votedFor = int(hardState.VotedFor)

		// 初始化缓存的 lastLogIndex
		lastIdx, err := store.LastLogIndex()
		if err != nil {
			log.Errorf("[Raft] Failed to get last log index from storage: %v", err)
		} else {
			r.cachedLastLogIndex = lastIdx
		}
	}

	r.electionResetEvent = time.Now()
	r.currentElectionTimeout = r.randomizedElectionTimeout()
	r.lastAppliedCond = sync.NewCond(&r.mu)

	return r
}

// ID 返回当前节点的 ID。
func (r *Raft) ID() int {
	return r.id
}

func (r *Raft) Peers() []int {
	return r.peerIDs
}

func (r *Raft) Storage() storage.Storage {
	return r.store
}

func (r *Raft) StateMachine() storage.StateMachine {
	return r.stateMachine
}

func (r *Raft) Transport() transport.Transport {
	return r.trans
}

// CommitChan 返回用于接收提交条目的只读通道。
func (r *Raft) CommitChan() <-chan param.CommitEntry {
	return r.commitChan
}

// getState 返回当前节点的状态（无锁原子读取）。
func (r *Raft) getState() State {
	return State(r.state.Load())
}

// setState 设置节点状态（必须在持有 r.mu 的情况下调用）。
func (r *Raft) setState(s State) {
	r.state.Store(int32(s))
}

// State 返回当前节点的状态。
func (r *Raft) State() State {
	return r.getState()
}

func (r *Raft) IsStopped() bool {
	return r.getState() == Dead
}

// SetSnapshotThreshold 设置自动快照的阈值。
func (r *Raft) SetSnapshotThreshold(threshold int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshotThreshold = threshold
}

// Run 启动 Raft 节点的主循环。
// 它会Ticking，检查选举超时，并在 Follower/Candidate 状态下发起选举。
func (r *Raft) Run() {
	log.Infof("[Core] Node %d starting main loop (Initial timeout: %s)", r.id, r.currentElectionTimeout)
	ticker := time.NewTicker(r.heartbeatTimeout) // 使用心跳间隔作为 tick 频率
	defer ticker.Stop()

	for {
		select {
		case <-r.shutdownChan:
			log.Infof("[Core] Node %d shutting down main loop.", r.id)
			return

		case <-ticker.C:
			r.mu.Lock()

			// 只有 Follower 和 Candidate 状态才需要检查选举超时。
			// Leader 和 Dead 状态都应该忽略 ticker。
			// (Dead 状态最终会被 shutdownChan 捕获，但在这里检查
			// 可以防止 Stop() 和 ticker 之间的竞态条件)
			if r.getState() != Follower && r.getState() != Candidate {
				r.mu.Unlock()
				continue
			}

			if time.Since(r.electionResetEvent) > r.currentElectionTimeout {
				log.Infof("[Core] Node %d election timeout reached (timeout: %s), starting election.", r.id, r.currentElectionTimeout)
				r.currentElectionTimeout = r.randomizedElectionTimeout()

				r.mu.Unlock() // startElection 会自己加锁
				// 选举必须在 goroutine 中启动，
				// 否则会阻塞 Run() 循环，导致 Stop() 无法停止此循环。
				go r.startElection()
			} else {
				r.mu.Unlock()
			}
		}
	}
}

// Stop 停止 Raft 节点的主循环。
func (r *Raft) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-r.shutdownChan:
		// 已经关闭
		return
	default:
	}

	log.Infof("[Core] Node %d received stop signal.", r.id)
	r.setState(Dead)

	// 广播 lastAppliedCond，唤醒可能在等待的读请求
	// 这样它们可以检测到节点已停止并退出
	r.lastAppliedCond.Broadcast()

	close(r.shutdownChan)
}

// randomizedElectionTimeout 返回一个在 [electionTimeout, 2 * electionTimeout) 范围内的随机超时时间。
// 这有助于防止选举时出现平票（split votes）。
func (r *Raft) randomizedElectionTimeout() time.Duration {
	randomRange := int64(r.electionTimeout)
	randomAddition := time.Duration(rand.Int63n(randomRange))
	return r.electionTimeout + randomAddition
}

// ClientRequest 是处理来自客户端请求的 RPC 函数。
// 它负责协调三个主要阶段：前置检查、提交并等待、处理最终结果。
func (r *Raft) ClientRequest(args *param.ClientArgs, reply *param.ClientReply) error {
	// 尝试将命令解析为 KVCommand 来检查它是否为只读
	var cmd param.KVCommand
	isRead := false

	if cmdBytes, ok := args.Command.([]byte); ok {
		if err := json.Unmarshal(cmdBytes, &cmd); err == nil {
			if cmd.Op == param.OpGet {
				isRead = true
			}
		}
	}

	if isRead {
		// 6. 处理线性一致性读
		return r.handleLinearizableRead(cmd, reply)
	}

	return r.handleWriteRequest(args, reply)
}

// handleLinearizableRead 处理只读请求，使用 ReadIndex 机制。
// 支持两种模式：
//   - Heartbeat 模式：每次读请求都发送心跳确认
//   - Lease 模式：基于租约，在租约期内无需心跳确认（高性能）
func (r *Raft) handleLinearizableRead(cmd param.KVCommand, reply *param.ClientReply) error {
	r.mu.Lock()

	// 1. 检查是否为 Leader
	if r.getState() != Leader {
		r.mu.Unlock()
		reply.NotLeader = true
		reply.LeaderHint = r.knownLeaderID
		return nil
	}

	// 2. 记录 ReadIndex
	// 按照 Raft 论文 Section 6.4，ReadIndex 应为当前的 commitIndex。
	// 只要状态机应用到这个 index，就能保证线性一致性。
	readIndex := r.commitIndex

	// 3. 根据配置的 ReadIndex 模式选择确认方式
	if r.readIndexMode == config.ReadIndexModeLease {
		// Lease Read 模式：检查租约是否有效
		now := time.Now()
		if now.Before(r.leaseUntil) {
			// 租约有效，直接执行读操作，无需心跳确认
			log.Debugf("[Lease Read] Node %d lease valid until %v, performing direct read. ReadIndex: %d", r.id, r.leaseUntil, readIndex)
			r.mu.Unlock()
			return r.performReadAfterApply(cmd, reply, readIndex)
		}
		log.Debugf("[Lease Read] Node %d lease expired at %v, falling back to heartbeat confirmation", r.id, r.leaseUntil)
	}

	// Heartbeat 模式或租约已过期：需要心跳确认
	// 为了不阻塞 Raft 锁，我们先释放锁去进行耗时的网络确认
	r.mu.Unlock()

	// 4. Heartbeat 确认 (Confirm Leadership)
	// 向集群广播心跳，确保当前时刻自己依然拥有多数派支持。
	// 这替代了原先依赖时钟的租约检查 (Lease Check)。
	if !r.confirmLeadership() {
		reply.Success = false
		reply.NotLeader = true
		// 确认失败意味着可能发生了网络分区或已有新 Leader，
		// 建议客户端重试。
		return nil
	}

	return r.performReadAfterApply(cmd, reply, readIndex)
}

// performReadAfterApply 等待状态机应用到 ReadIndex 后执行读操作
// 带有超时机制，防止无限阻塞
//
// 优化：stateMachine.Get() 在锁外执行，因为 LSM Database.Get() 内部线程安全
func (r *Raft) performReadAfterApply(cmd param.KVCommand, reply *param.ClientReply, readIndex uint64) error {
	r.mu.Lock()

	// 重新加锁后再次检查状态，防止在确认期间被降级或停止
	if r.getState() != Leader {
		leaderHint := r.knownLeaderID
		r.mu.Unlock()
		reply.NotLeader = true
		reply.LeaderHint = leaderHint
		return nil
	}

	log.Debugf("[ReadIndex] Node %d confirmed leadership. ReadIndex: %d. Waiting for lastApplied (%d)...", r.id, readIndex, r.lastApplied)

	// 设置超时：使用选举超时的 2 倍作为读请求超时
	timeout := r.electionTimeout * 2
	timedOut := false

	// 使用 time.AfterFunc 替代 goroutine+time.Sleep，避免 goroutine 泄漏
	timer := time.AfterFunc(timeout, func() {
		r.lastAppliedCond.Broadcast() // 超时时广播，唤醒等待者
	})
	defer timer.Stop() // 确保读完成后取消定时器

	// 等待状态机追赶上 ReadIndex，同时在每次唤醒时检查 Leader 状态和超时
	deadline := time.Now().Add(timeout)
	for r.lastApplied < readIndex && r.getState() == Leader {
		if time.Now().After(deadline) {
			timedOut = true
			break
		}

		// sync.Cond.Wait() 会释放锁并等待，被唤醒后重新获取锁
		r.lastAppliedCond.Wait()

		if time.Now().After(deadline) {
			timedOut = true
			break
		}
	}

	if timedOut {
		log.Warnf("[ReadIndex] Node %d timed out waiting for lastApplied to reach %d (current: %d)", r.id, readIndex, r.lastApplied)
		r.mu.Unlock()
		reply.Success = false
		reply.Result = "read timeout"
		return nil
	}

	// 检查是否因为 Leader 被降级或节点停止而退出循环
	if r.getState() != Leader {
		leaderHint := r.knownLeaderID
		r.mu.Unlock()
		reply.NotLeader = true
		reply.LeaderHint = leaderHint
		return nil
	}

	// 释放锁，在锁外执行本地读取
	// 安全性：lastApplied >= readIndex 已确认，状态机已应用所有需要的条目
	// LSM Database.Get() 内部线程安全（MemTable 使用跳表，有 RWMutex 保护）
	log.Debugf("[ReadIndex] Node %d state machine ready (lastApplied=%d). performing read.", r.id, r.lastApplied)
	r.mu.Unlock()

	value, err := r.stateMachine.Get(cmd.Key)
	if err != nil {
		reply.Result = err.Error()
		if errors.Is(err, kvstore.ErrKeyNotFound) {
			reply.Success = true
		} else {
			reply.Success = false
		}
	} else {
		reply.Success = true
		reply.Result = value
	}

	return nil
}

// confirmLeadership 辅助方法：向所有节点发送轻量级心跳，并等待多数派确认。
// 返回 true 表示确认成功（自己仍是 Leader）。
//
// 优化：将磁盘 I/O 操作（getLogTerm）移到锁外执行，减少锁持有时间。
func (r *Raft) confirmLeadership() bool {
	// 1. 快速路径检查（短锁）
	r.mu.Lock()
	now := time.Now()

	// 检查是否为 Leader
	if r.getState() != Leader {
		r.mu.Unlock()
		return false
	}

	// 优化1：使用缓存机制，避免短时间内频繁确认 Leadership
	if !r.lastLeadershipConfirm.IsZero() && now.Sub(r.lastLeadershipConfirm) < r.leadershipCacheTime {
		r.mu.Unlock()
		return true
	}

	// 优化2：检查 Lease Read 租约是否有效
	if r.readIndexMode == config.ReadIndexModeLease && now.Before(r.leaseUntil) {
		r.mu.Unlock()
		return true
	}

	// 获取需要的信息
	term := r.currentTerm
	leaderID := r.id
	peerIDs := r.getAllPeerIDs()
	electionTimeout := r.electionTimeout
	leaseDuration := r.leaseDuration

	// 检查 lastAck 时间
	recentAcks := 0
	for _, pid := range peerIDs {
		if pid == r.id {
			recentAcks++
			continue
		}
		if lastAck, ok := r.lastAck[pid]; ok && now.Sub(lastAck) < electionTimeout {
			recentAcks++
		}
	}
	majority := len(peerIDs)/2 + 1
	if recentAcks >= majority {
		// 已经有足够的最近确认，不需要发送心跳
		log.Debugf("[ReadIndex] Node %d has enough recent acks (%d/%d), skipping heartbeat.", leaderID, recentAcks, majority)
		r.lastLeadershipConfirm = now
		if r.readIndexMode == config.ReadIndexModeLease {
			r.leaseUntil = now.Add(leaseDuration)
			log.Debugf("[Lease Read] Node %d renewed lease until %v", r.id, r.leaseUntil)
		}
		r.mu.Unlock()
		return true
	}

	// 收集需要发送心跳的 peer 和它们的 nextIndex
	type peerInfo struct {
		peerID    int
		nextIndex uint64
	}
	var peersToSend []peerInfo

	for _, pid := range peerIDs {
		if pid == r.id {
			continue
		}

		// 如果这个节点最近确认过，跳过它
		if lastAck, ok := r.lastAck[pid]; ok && now.Sub(lastAck) < electionTimeout {
			continue
		}

		nextIdx := r.nextIndex[pid]
		if nextIdx == 0 {
			nextIdx = 1
		}
		peersToSend = append(peersToSend, peerInfo{pid, nextIdx})
	}

	commitIdx := r.commitIndex
	r.mu.Unlock()

	// 2. 如果没有需要发送的 peer，直接返回成功
	if len(peersToSend) == 0 {
		r.mu.Lock()
		r.lastLeadershipConfirm = time.Now()
		r.mu.Unlock()
		return true
	}

	// 3. 在锁外获取 prevLogTerm（磁盘 I/O）
	type hbRequest struct {
		peerID int
		args   *param.AppendEntriesArgs
	}
	var requests []hbRequest

	for _, pi := range peersToSend {
		prevLogIndex := pi.nextIndex - 1
		prevLogTerm, _ := r.getLogTerm(prevLogIndex) // 锁外执行磁盘 I/O

		args := param.NewAppendEntriesArgs(term, leaderID, prevLogIndex, prevLogTerm, commitIdx, nil)
		requests = append(requests, hbRequest{pi.peerID, args})
	}

	// 4. 并行发送心跳（网络 I/O，无锁）
	ackChan := make(chan bool, len(requests))

	for _, req := range requests {
		go func(target int, args *param.AppendEntriesArgs) {
			reply := param.NewAppendEntriesReply()
			if err := r.trans.SendAppendEntries(strconv.Itoa(target), args, reply); err == nil {
				if reply.Term == term {
					// 更新 lastAck
					r.mu.Lock()
					r.lastAck[target] = time.Now()
					r.mu.Unlock()
					ackChan <- true
				} else {
					ackChan <- false
				}
			} else {
				ackChan <- false
			}
		}(req.peerID, req.args)
	}

	// 5. 统计票数
	votes := recentAcks
	timeout := time.After(electionTimeout * 2)

	for i := 0; i < len(requests); i++ {
		select {
		case ok := <-ackChan:
			if ok {
				votes++
			}
		case <-timeout:
			log.Warnf("[ReadIndex] Node %d timed out waiting for heartbeat quorum.", leaderID)
			return false
		}

		if votes >= majority {
			break
		}
	}

	// 6. 更新缓存时间和租约
	r.mu.Lock()
	now = time.Now()
	r.lastLeadershipConfirm = now
	if r.readIndexMode == config.ReadIndexModeLease && votes >= majority {
		r.leaseUntil = now.Add(leaseDuration)
		log.Debugf("[Lease Read] Node %d renewed lease until %v after heartbeat quorum", r.id, r.leaseUntil)
	}
	r.mu.Unlock()

	return votes >= majority
}

// tryRenewLease 检查是否有足够的多数派确认，如果有则更新租约。
// 此函数必须在持有锁的情况下被调用。
func (r *Raft) tryRenewLease() {
	if r.getState() != Leader {
		return
	}

	now := time.Now()
	peerIDs := r.getAllPeerIDs()
	recentAcks := 1 // 自己始终算作一个确认

	for _, pid := range peerIDs {
		if lastAck, ok := r.lastAck[pid]; ok && now.Sub(lastAck) < r.electionTimeout {
			recentAcks++
		}
	}

	majority := len(peerIDs)/2 + 1
	if recentAcks >= majority {
		r.leaseUntil = now.Add(r.leaseDuration)
		log.Debugf("[Lease Read] Node %d renewed lease until %v (acks: %d/%d)", r.id, r.leaseUntil, recentAcks, majority)
	}
}

// handleWriteRequest 处理写请求（通过 Raft 日志）。
func (r *Raft) handleWriteRequest(args *param.ClientArgs, reply *param.ClientReply) error {
	// 1. 执行前置检查。如果不是 Leader 或请求重复，则提前返回。
	if proceed := r.preHandleClientRequest(args, reply); !proceed {
		return nil
	}

	// 2. 将命令提交到 Raft 日志，并同步等待其被状态机应用。
	result, ok, leaderID := r.Commit(args.Command)

	// 3. 根据提交和等待的结果，最终填充客户端的响应。
	r.finalizeClientReply(args, reply, result, ok, leaderID)

	return nil
}

// getLogTerm 返回指定索引的日志条目的任期。
func (r *Raft) getLogTerm(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}

	// 检查是否在快照中
	if r.snapshot != nil && index == r.snapshot.LastIncludedIndex {
		return r.snapshot.LastIncludedTerm, nil
	}

	entry, err := r.store.GetEntry(index)
	if err != nil {
		log.Errorf("[Raft] Failed to get log entry at index %d: %v", index, err)
		return 0, err
	}
	if entry == nil {
		log.Errorf("[Raft] Log entry at index %d not found", index)
		return 0, nil
	}
	return entry.Term, nil
}

// getFirstLogIndex 返回日志中的第一条条目的索引。从存储层查询。
func (r *Raft) getFirstLogIndex() (uint64, error) {
	// 假设快照逻辑还未完全集成到存储层，先处理内存快照
	if r.snapshot != nil {
		return r.snapshot.LastIncludedIndex + 1, nil
	}
	// 从存储中获取第一条日志的索引
	firstIndex, err := r.store.FirstLogIndex()
	if err != nil {
		log.Errorf("[Raft] Failed to get first log index: %v", err)
		return 0, err
	}
	return firstIndex, nil
}

// proposeToLog 在【持有锁】的情况下，将命令写入本地日志。
func (r *Raft) proposeToLog(command any) (param.LogEntry, error) {
	// 1. 使用缓存的 lastLogIndex 确定新日志的索引。
	newIndex := r.cachedLastLogIndex + 1

	// 2. 将新条目原子性地追加并持久化到 Leader 的本地存储中。
	newLogEntry := param.NewLogEntry(command, r.currentTerm, newIndex)
	if err := r.store.AppendEntries([]param.LogEntry{newLogEntry}); err != nil {
		log.Errorf("[Raft] Leader %d failed to append new log entry: %s", r.id, err.Error())
		return param.LogEntry{}, err
	}
	log.Infof("[Raft] Leader %d proposed new log entry at index %d", r.id, newIndex)

	// 3. 更新缓存的 lastLogIndex
	r.cachedLastLogIndex = newLogEntry.Index

	return newLogEntry, nil
}

// preHandleClientRequest 封装了所有在提交日志前需要进行的前置检查。
// 返回值 bool 表示是否应继续处理该请求。
func (r *Raft) preHandleClientRequest(args *param.ClientArgs, reply *param.ClientReply) bool {
	if !r.isLeader() {
		reply.NotLeader = true
		reply.LeaderHint = r.knownLeaderID
		return false
	}
	if r.isDuplicateRequest(args.ClientID, args.SequenceNum) {
		reply.Success = true // 对于重复请求，直接返回成功。
		return false
	}
	return true
}

// Commit 封装了将命令提交到 Raft 日志并等待其被应用的全过程。
// 它返回从状态机获得的结果，一个表示成功的布尔值，以及当前的 Leader ID。
func (r *Raft) Commit(command any) (any, bool, int) {
	index, _, isLeader := r.Submit(command)
	if !isLeader {
		// 在提交过程中，可能失去了 Leader 地位。
		return nil, false, r.knownLeaderID
	}

	// 等待命令被状态机成功应用，或等待超时。
	// 使用 5 秒超时以应对高负载场景下的日志复制延迟
	log.Infof("[Client] Waiting for log index %d to be applied...", index)
	result, ok := r.waitForAppliedLog(index, 5*time.Second)
	return result, ok, r.id
}

// finalizeClientReply 负责根据执行结果，最终构建给客户端的响应。
func (r *Raft) finalizeClientReply(args *param.ClientArgs, reply *param.ClientReply, result any, ok bool, leaderID int) {
	if ok {
		// 命令成功应用。
		r.mu.Lock()
		r.clientSessions[args.ClientID] = args.SequenceNum
		r.mu.Unlock()
		reply.Success = true
		reply.Result = result
	} else {
		// 如果失败，可能是因为超时，也可能是因为中途失去了 Leader 身份。
		reply.Success = false
		if !r.isLeader() {
			reply.NotLeader = true
			reply.LeaderHint = leaderID
		}
	}
}

// Submit 将一个普通的客户端命令追加到 Raft 日志中。
// 优化：通过 proposalCh 发送到批处理 goroutine，多个并发 Submit 可合并为单次磁盘写入。
func (r *Raft) Submit(command any) (uint64, uint64, bool) {
	// 快速检查：无锁原子读取，避免非 Leader 时的 channel 操作
	if r.getState() != Leader {
		return 0, 0, false
	}

	req := proposalRequest{
		command: command,
		result:  make(chan proposalResult, 1),
	}

	// 发送到 proposalCh，如果 channel 满了说明系统过载
	select {
	case r.proposalCh <- req:
	case <-r.shutdownChan:
		return 0, 0, false
	}

	// 等待批处理结果
	select {
	case res := <-req.result:
		return res.index, res.term, res.ok
	case <-r.shutdownChan:
		return 0, 0, false
	}
}

// proposalBatcher 是专用的 proposal 批处理 goroutine。
// 从 proposalCh 批量取出请求，单次加锁 + 单次 store.AppendEntries 完成。
// 在 transitionToLeader 时启动，非 Leader 时自动退出。
func (r *Raft) proposalBatcher() {
	for {
		// 等待第一个请求（阻塞）
		var firstReq proposalRequest
		select {
		case firstReq = <-r.proposalCh:
		case <-r.shutdownChan:
			return
		}

		// 收集更多请求（非阻塞），最多 maxBatchSize 个
		batch := []proposalRequest{firstReq}
	drain:
		for len(batch) < maxBatchSize {
			select {
			case req := <-r.proposalCh:
				batch = append(batch, req)
			default:
				break drain
			}
		}

		// 批量处理
		r.processBatch(batch)

		// 检查是否仍然是 Leader
		if r.getState() != Leader {
			// 排空 proposalCh 中剩余的请求，通知它们失败
			r.drainProposalCh()
			return
		}
	}
}

// processBatch 批量处理一组 proposal 请求。
func (r *Raft) processBatch(batch []proposalRequest) {
	r.mu.Lock()

	// 检查是否仍然是 Leader
	if r.getState() != Leader {
		r.mu.Unlock()
		// 通知所有请求失败
		for _, req := range batch {
			req.result <- proposalResult{ok: false}
		}
		return
	}

	// 构建批量日志条目
	entries := make([]param.LogEntry, 0, len(batch))
	startIndex := r.cachedLastLogIndex + 1
	currentTerm := r.currentTerm

	for i, req := range batch {
		idx := startIndex + uint64(i)
		entries = append(entries, param.NewLogEntry(req.command, currentTerm, idx))
	}

	// 单次磁盘写入
	if err := r.store.AppendEntries(entries); err != nil {
		log.Errorf("[Raft] Leader %d failed to append batch of %d entries: %v", r.id, len(entries), err)
		r.mu.Unlock()
		for _, req := range batch {
			req.result <- proposalResult{ok: false}
		}
		return
	}

	// 更新缓存
	r.cachedLastLogIndex = entries[len(entries)-1].Index
	log.Infof("[Raft] Leader %d proposed batch of %d entries (index %d-%d)", r.id, len(entries), startIndex, r.cachedLastLogIndex)

	// A single-node cluster has no follower replies to trigger commit
	// advancement, so try to commit after the local append as well.
	r.updateCommitIndex()

	// 获取需要通知的 peer 列表
	peersToNotify := r.getAllPeerIDs()
	r.mu.Unlock()

	// 通知所有请求完成
	for i, req := range batch {
		req.result <- proposalResult{
			index: entries[i].Index,
			term:  entries[i].Term,
			ok:    true,
		}
	}

	// 在没有持有锁的情况下广播
	for _, peerID := range peersToNotify {
		if peerID == r.id {
			continue
		}
		go r.sendAppendEntries(peerID)
	}
}

// drainProposalCh 排空 proposalCh 中的请求，通知它们失败。
func (r *Raft) drainProposalCh() {
	for {
		select {
		case req := <-r.proposalCh:
			req.result <- proposalResult{ok: false}
		default:
			return
		}
	}
}

// ChangeConfig 发起一次集群成员变更。
// 它处理成员变更特有的前置检查和状态更新，并将核心的日志提议工作委托给通用函数。
// 实现动态成员变更，支持两阶段提交以确保安全性。
func (r *Raft) ChangeConfig(newPeerIDs []int) (uint64, uint64, bool) {
	r.mu.Lock()

	// 1. 前置检查：确保当前是 Leader 并且没有正在进行的成员变更。
	if r.inJointConsensus || r.getState() != Leader {
		r.mu.Unlock()
		return 0, 0, false // 变更已在进行中
	}

	// 2. 创建配置变更命令并写入本地日志。
	newLogEntry, err := r.proposeToLog(param.NewConfigChangeCommand(newPeerIDs))
	if err != nil {
		r.mu.Unlock()
		return 0, 0, false
	}

	// 3. 提议成功后，Leader 自身立即进入“联合共识”状态。
	r.inJointConsensus = true
	r.newPeerIDs = newPeerIDs

	// Initialize tracking state for new peers
	for _, peerID := range newPeerIDs {
		if _, ok := r.nextIndex[peerID]; !ok {
			r.nextIndex[peerID] = newLogEntry.Index + 1
			r.matchIndex[peerID] = 0
			log.Infof("[Config Change] Initialized nextIndex[%d] = %d", peerID, r.nextIndex[peerID])
		} else {
			log.Infof("[Config Change] nextIndex[%d] already exists: %d", peerID, r.nextIndex[peerID])
		}
	}

	peersToNotify := r.getAllPeerIDs()
	r.mu.Unlock()

	// 4. 在没有持有锁的情况下，安全地广播。
	for _, peerID := range peersToNotify {
		if peerID == r.id {
			continue
		}
		go r.sendAppendEntries(peerID)
	}

	return newLogEntry.Index, newLogEntry.Term, true
}

// getAllPeerIDs is a helper to get all unique peers from both configurations.
func (r *Raft) getAllPeerIDs() []int {
	peerSet := make(map[int]struct{})
	for _, p := range r.peerIDs {
		peerSet[p] = struct{}{}
	}
	if r.inJointConsensus {
		for _, p := range r.newPeerIDs {
			peerSet[p] = struct{}{}
		}
	}

	allPeers := make([]int, 0, len(peerSet))
	for p := range peerSet {
		allPeers = append(allPeers, p)
	}
	return allPeers
}

// becomeFollower 将节点的状态更新为指定新任期的 Follower。
// 它会持久化新状态，并且必须在持有锁的情况下被调用。
func (r *Raft) becomeFollower(newTerm uint64) error {
	log.Infof("[State Change] Node %d received higher term %d. Updating term and becoming follower.", r.id, newTerm)
	r.currentTerm = newTerm
	r.setState(Follower)
	r.votedFor = -1 // 进入新任期时，重置投票记录。

	// 每当我们成为 Follower（无论何种原因），
	// 都应该重置选举计时器，并为下一次超时设置一个新的随机值。
	r.electionResetEvent = time.Now()
	r.currentElectionTimeout = r.randomizedElectionTimeout()

	// 广播 lastAppliedCond，唤醒可能在等待的读请求
	// 这样它们可以检测到 Leader 状态变化并返回 NotLeader 错误
	r.lastAppliedCond.Broadcast()

	if err := r.store.SetState(param.HardState{CurrentTerm: r.currentTerm, VotedFor: uint64(r.votedFor)}); err != nil {
		log.Errorf("[Raft] Node %d failed to persist state after becoming follower: %v", r.id, err)
		return err
	}
	return nil
}

// waitForAppliedLog 等待一个特定索引的日志被状态机应用。
// 它通过一个注册在 notifyApply 映射中的 channel 来实现同步等待。
func (r *Raft) waitForAppliedLog(index uint64, timeout time.Duration) (any, bool) {
	r.mu.Lock()

	// 1. 第一次检查：如果日志已经应用，直接返回。
	if r.lastApplied >= index {
		r.mu.Unlock()
		return nil, true
	}

	// 2. 注册通知 channel。
	notifyChan := make(chan any, 1)
	r.notifyApply[index] = notifyChan

	// 3. 再次检查：防止在注册期间日志被应用。
	if r.lastApplied >= index {
		// 如果此时发现已经应用了，清理刚刚注册的 channel 并返回。
		delete(r.notifyApply, index)
		r.mu.Unlock()
		return nil, true
	}

	r.mu.Unlock()

	// 4. 无论等待成功还是超时，最后都负责清理 channel。
	defer func() {
		r.mu.Lock()
		delete(r.notifyApply, index)
		r.mu.Unlock()
	}()

	// 5. 等待 applyLogs 发出通知，或等待超时。
	select {
	case result := <-notifyChan:
		log.Infof("[Client] Notified that log index %d has been applied.", index)
		return result, true
	case <-time.After(timeout):
		log.Warnf("[Client] Timed out waiting for log index %d to be applied.", index)
		return nil, false
	}
}

// initLeaderState initializes leader state after election
func (r *Raft) initLeaderState() {
	// This method is called with the lock held.
	lastLogIndex := r.cachedLastLogIndex

	r.nextIndex = make(map[int]uint64)
	r.matchIndex = make(map[int]uint64)
	for _, peerID := range r.getAllPeerIDs() {
		if peerID == r.id {
			continue
		}
		r.nextIndex[peerID] = lastLogIndex + 1
		r.matchIndex[peerID] = 0
	}
}

// startHeartbeat starts periodic heartbeat loops
func (r *Raft) startHeartbeat() {
	// This method is called with the lock held.
	go func() {
		ticker := time.NewTicker(r.heartbeatTimeout)
		defer ticker.Stop()

		// Send an initial heartbeat immediately without waiting for the first tick.
		r.mu.Lock()
		r.broadcastHeartbeat()
		r.mu.Unlock()

		for {
			select {
			case <-ticker.C:
				r.mu.Lock()
				if r.getState() != Leader {
					r.mu.Unlock()
					return
				}
				r.broadcastHeartbeat()
				r.mu.Unlock()

			case <-r.shutdownChan:
				// 节点已关闭，退出心跳循环
				return
			}
		}
	}()
}

// broadcastHeartbeat is a helper to send AppendEntries to all peers.
func (r *Raft) broadcastHeartbeat() {
	// This method must be called with the lock held.
	for _, peerID := range r.getAllPeerIDs() {
		if peerID == r.id {
			continue
		}
		go r.sendAppendEntries(peerID)
	}
}

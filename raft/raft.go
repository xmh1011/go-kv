package raft

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/xmh1011/go-kv/pkg/config"
	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
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

type Raft struct {
	// mu 保护对 Raft 状态的并发访问
	mu sync.Mutex

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
	state       State

	// --- 日志与状态机相关 ---
	commitIndex     uint64
	lastApplied     uint64
	commitChan      chan param.CommitEntry
	lastAppliedCond *sync.Cond // 用于等待 lastApplied 赶上 commitIndex

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
		state:             Follower,
		votedFor:          -1, // -1 表示未投票
		commitChan:        commitChan,
		nextIndex:         make(map[int]uint64),
		matchIndex:        make(map[int]uint64),
		clientSessions:    make(map[int64]int64),
		notifyApply:       make(map[uint64]chan any),
		shutdownChan:      make(chan struct{}),
		lastAck:           make(map[int]time.Time),
		snapshotThreshold: -1, // 默认禁用自动快照
		electionTimeout:   config.Conf.Raft.ElectionTimeout,
		heartbeatTimeout:  config.Conf.Raft.HeartbeatTimeout,
	}
	// 从稳定存储中恢复状态。
	if store != nil {
		hardState, err := store.GetState()
		if err != nil {
			log.Fatalf("[Raft] Failed to get hard state from storage: %s", err.Error())
			panic(fmt.Errorf("failed to get hard state: %w", err))
		}
		r.currentTerm = hardState.CurrentTerm
		r.votedFor = int(hardState.VotedFor)
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

// State 返回当前节点的状态。
func (r *Raft) State() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

func (r *Raft) IsStopped() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == Dead
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
			if r.state != Follower && r.state != Candidate {
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
	r.state = Dead
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
func (r *Raft) handleLinearizableRead(cmd param.KVCommand, reply *param.ClientReply) error {
	r.mu.Lock()

	// 1. 检查是否为 Leader
	if r.state != Leader {
		r.mu.Unlock()
		reply.NotLeader = true
		reply.LeaderHint = r.knownLeaderID
		return nil
	}

	// 2. 记录 ReadIndex
	// 按照 Raft 论文 Section 6.4，ReadIndex 应为当前的 commitIndex。
	// 只要状态机应用到这个 index，就能保证线性一致性。
	readIndex := r.commitIndex

	// 为了不阻塞 Raft 锁，我们先释放锁去进行耗时的网络确认
	r.mu.Unlock()

	// 3. Heartbeat 确认 (Confirm Leadership)
	// 向集群广播心跳，确保当前时刻自己依然拥有多数派支持。
	// 这替代了原先依赖时钟的租约检查 (Lease Check)。
	if !r.confirmLeadership() {
		reply.Success = false
		reply.NotLeader = true
		// 确认失败意味着可能发生了网络分区或已有新 Leader，
		// 建议客户端重试。
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// 重新加锁后再次检查状态，防止在确认期间被降级
	if r.state != Leader {
		reply.NotLeader = true
		reply.LeaderHint = r.knownLeaderID
		return nil
	}

	log.Infof("[ReadIndex] Node %d confirmed leadership. ReadIndex: %d. Waiting for lastApplied (%d)...", r.id, readIndex, r.lastApplied)

	// 4. 等待状态机追赶上 ReadIndex
	for r.lastApplied < readIndex {
		r.lastAppliedCond.Wait()
	}

	// 5. 执行本地读取
	log.Infof("[ReadIndex] Node %d state machine ready (lastApplied=%d). performing read.", r.id, r.lastApplied)

	value, err := r.stateMachine.Get(cmd.Key)
	if err != nil {
		reply.Success = false
		reply.Result = err.Error()
	} else {
		reply.Success = true
		reply.Result = value
	}

	return nil
}

// confirmLeadership 辅助方法：向所有节点发送轻量级心跳，并等待多数派确认。
// 返回 true 表示确认成功（自己仍是 Leader）。
func (r *Raft) confirmLeadership() bool {
	r.mu.Lock()
	term := r.currentTerm
	leaderID := r.id
	peerIDs := r.getAllPeerIDs()

	// 构造心跳请求列表
	// 我们需要为每个 Peer 构造 args，为了避免在循环网络发送时持有锁，
	// 我们先在锁内准备好所有数据。
	type hbRequest struct {
		peerID int
		args   *param.AppendEntriesArgs
	}
	var requests []hbRequest

	for _, pid := range peerIDs {
		if pid == r.id {
			continue
		}

		// 尽量构造合法的 PrevLogIndex，虽然对于 Leadership 确认来说，
		// 只要对方认可 Term 即可（即使 Log 不一致返回 false，只要 Term 没变也算确认）。
		nextIDx, ok := r.nextIndex[pid]
		if !ok || nextIDx == 0 {
			// 防御性编程：如果 nextIndex 未初始化或为 0，避免下溢。
			// 正常情况下 nextIndex 至少为 1 (FirstLogIndex)。
			// 在测试中，如果手动构造 Raft 对象且未初始化 nextIndex，可能会触发此情况。
			log.Warnf("[WARNING] Node %d found invalid nextIndex for peer %d: %d. Defaulting to 1.", r.id, pid, nextIDx)
			nextIDx = 1
		}
		prevLogIndex := nextIDx - 1
		prevLogTerm, _ := r.getLogTerm(prevLogIndex) // 如果获取失败，默认为 0 也可以

		// 构造空日志的 AppendEntries 作为心跳
		args := param.NewAppendEntriesArgs(term, leaderID, prevLogIndex, prevLogTerm, r.commitIndex, nil)
		requests = append(requests, hbRequest{pid, args})
	}
	r.mu.Unlock()

	// 用于收集确认结果的通道
	ackChan := make(chan bool, len(requests))

	// 并行发送心跳
	for _, req := range requests {
		go func(target int, args *param.AppendEntriesArgs) {
			reply := param.NewAppendEntriesReply()
			// 发送 RPC
			if err := r.trans.SendAppendEntries(strconv.Itoa(target), args, reply); err == nil {
				// 关键判断：如果对方返回的 Term 与我们一致，说明对方认可我们的 Leadership。
				// 即使 reply.Success 为 false (日志冲突)，也不影响 Leadership 的确认。
				if reply.Term == term {
					ackChan <- true
				} else {
					ackChan <- false
				}
			} else {
				ackChan <- false
			}
		}(req.peerID, req.args)
	}

	// 统计票数
	votes := 1 // 自己算一票
	majority := len(peerIDs)/2 + 1

	// 设置一个较短的超时时间，避免读请求无限阻塞
	timeout := time.After(r.electionTimeout)

	for i := 0; i < len(requests); i++ {
		select {
		case ok := <-ackChan:
			if ok {
				votes++
			}
		case <-timeout:
			// 超时未集齐多数派
			log.Warnf("[ReadIndex] Node %d timed out waiting for heartbeat quorum.", leaderID)
			return false
		}

		if votes >= majority {
			return true
		}
	}

	return false
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
	// 1. 从存储中获取最后一条日志的索引，以确定新日志的索引。
	lastIndex, err := r.store.LastLogIndex()
	if err != nil {
		log.Errorf("[Raft] Leader %d failed to get last log index to propose new entry: %v", r.id, err)
		return param.LogEntry{}, err
	}
	newIndex := lastIndex + 1

	// 2. 将新条目原子性地追加并持久化到 Leader 的本地存储中。
	newLogEntry := param.NewLogEntry(command, r.currentTerm, newIndex)
	if err := r.store.AppendEntries([]param.LogEntry{newLogEntry}); err != nil {
		log.Errorf("[Raft] Leader %d failed to append new log entry: %s", r.id, err.Error())
		return param.LogEntry{}, err
	}
	log.Infof("[Raft] Leader %d proposed new log entry at index %d", r.id, newIndex)

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
	log.Infof("[Client] Waiting for log index %d to be applied...", index)
	result, ok := r.waitForAppliedLog(index, 2*time.Second)
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
func (r *Raft) Submit(command any) (uint64, uint64, bool) {
	r.mu.Lock()

	// 1. 检查当前节点是否为 Leader。
	if r.state != Leader {
		r.mu.Unlock()
		return 0, 0, false
	}

	// 2. 将命令写入本地日志（此过程在持有锁的情况下完成）。
	newLogEntry, err := r.proposeToLog(command)
	if err != nil {
		r.mu.Unlock()
		return 0, 0, false
	}

	// 3. 在启动 goroutine 之前，我们先获取需要通知的 peer 列表，然后立即解锁。
	peersToNotify := r.getAllPeerIDs()
	r.mu.Unlock()

	// 4. 在没有持有锁的情况下，安全地启动广播 goroutine。
	for _, peerID := range peersToNotify {
		if peerID == r.id {
			continue
		}
		go r.sendAppendEntries(peerID)
	}

	return newLogEntry.Index, newLogEntry.Term, true
}

// ChangeConfig 发起一次集群成员变更。
// 它处理成员变更特有的前置检查和状态更新，并将核心的日志提议工作委托给通用函数。
// 实现动态成员变更，支持两阶段提交以确保安全性。
func (r *Raft) ChangeConfig(newPeerIDs []int) (uint64, uint64, bool) {
	r.mu.Lock()

	// 1. 前置检查：确保当前是 Leader 并且没有正在进行的成员变更。
	if r.inJointConsensus || r.state != Leader {
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
	r.state = Follower
	r.votedFor = -1 // 进入新任期时，重置投票记录。

	// 每当我们成为 Follower（无论何种原因），
	// 都应该重置选举计时器，并为下一次超时设置一个新的随机值。
	r.electionResetEvent = time.Now()
	r.currentElectionTimeout = r.randomizedElectionTimeout()

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
	lastLogIndex, err := r.store.LastLogIndex()
	if err != nil {
		log.Errorf("[Raft] Node %d (new leader) failed to get last log index to initialize state: %v", r.id, err)
		// This is a critical error, might need to step down.
		r.state = Follower
		return
	}

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
			<-ticker.C

			r.mu.Lock()
			if r.state != Leader {
				r.mu.Unlock()
				return
			}
			r.broadcastHeartbeat()
			r.mu.Unlock()
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

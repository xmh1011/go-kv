package raft

import (
	"errors"
	"strconv"
	"time"

	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
)

// electionContext 是一个辅助结构体，用于封装单次选举过程中的所有状态。
type electionContext struct {
	// --- 配置信息 ---
	inJoint     bool  // 当前是否处于联合共识状态
	oldPeers    []int // 旧配置的完整节点列表（包含自身）
	newPeers    []int // 新配置的完整节点列表（包含自身）
	majorityOld int   // 旧配置需要的多数票
	majorityNew int   // 新配置需要的多数票

	// --- 计票器 ---
	votesOldConfig int // 在旧配置中已获得的票数
	votesNewConfig int // 在新配置中已获得的票数
}

// newElectionContext 是 electionContext 的构造函数。
func newElectionContext(r *Raft) *electionContext {
	r.mu.Lock()
	defer r.mu.Unlock()

	oldPeers := append([]int(nil), r.peerIDs...)
	newPeers := append([]int(nil), r.newPeerIDs...)

	ctx := &electionContext{
		inJoint:        r.inJointConsensus,
		oldPeers:       oldPeers,
		newPeers:       newPeers,
		majorityOld:    len(oldPeers)/2 + 1,
		votesOldConfig: 1, // 默认给自己投一票
		votesNewConfig: 0,
	}
	if ctx.inJoint {
		ctx.majorityNew = len(ctx.newPeers)/2 + 1
		if _, isNew := findPeer(r.id, ctx.newPeers); isNew {
			ctx.votesNewConfig = 1
		}
	}

	return ctx
}

// startElection 主函数：现在作为选举的入口，首先发起 Pre-Vote。
// 只有 Pre-Vote 成功，才会触发正式选举 (startRealElection)。
func (r *Raft) startElection() {
	r.mu.Lock()
	// 检查状态，只有 Follower 或 Candidate 可以发起选举
	if r.state != Follower && r.state != Candidate {
		r.mu.Unlock()
		return
	}

	// Pre-Vote 使用的任期是当前任期 + 1，但此时不修改 r.currentTerm
	preVoteTerm := r.currentTerm + 1

	// 获取日志信息用于比较
	lastLogIndex, lastLogTerm, err := r.getLastLogInfoForElection()
	if err != nil {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	log.Infof("[PreVote] Node %d starting Pre-Vote for term %d", r.id, preVoteTerm)

	// 发起 Pre-Vote 广播，注意最后一个参数 isPreVote = true
	voteChan := r.broadcastVoteRequests(preVoteTerm, lastLogIndex, lastLogTerm, true)

	// 异步处理 Pre-Vote 结果
	go r.handleElectionResult(voteChan, preVoteTerm, true)
}

// startRealElection 发起真正的选举。
// 只有在 Pre-Vote 阶段获得多数支持后，才会调用此方法。
func (r *Raft) startRealElection() {
	r.mu.Lock()

	// 1. 初始化候选人状态：真正增加 currentTerm，投票给自己并持久化。
	if err := r.initializeCandidateState(); err != nil {
		r.mu.Unlock()
		return
	}

	// 2. 获取日志信息
	lastLogIndex, lastLogTerm, err := r.getLastLogInfoForElection()
	if err != nil {
		r.mu.Unlock()
		return
	}

	savedCurrentTerm := r.currentTerm
	r.mu.Unlock()

	log.Infof("[Election] Node %d starting Real Election for term %d", r.id, savedCurrentTerm)

	// 3. 广播正式投票请求，isPreVote = false
	voteChan := r.broadcastVoteRequests(savedCurrentTerm, lastLogIndex, lastLogTerm, false)

	// 4. 异步处理正式选举结果
	go r.handleElectionResult(voteChan, savedCurrentTerm, false)
}

// RequestVote 是处理投票请求的 RPC 入口。
func (r *Raft) RequestVote(args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// --- Pre-Vote 特殊处理 ---
	if args.PreVote {
		return r.handlePreVote(args, reply)
	}

	// --- 正式 RequestVote 逻辑 ---

	// 1. 处理任期相关的检查和状态更新。
	if proceed, err := r.handleVoteTermLogic(args, reply); !proceed {
		return err
	}

	// 2. 做出投票决定
	return r.decideVote(args, reply)
}

// handlePreVote 处理预投票请求。
// 预投票不改变接收者的 Term 或 VotedFor，主要检查 Leader 租约和日志新鲜度。
func (r *Raft) handlePreVote(args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
	reply.Term = r.currentTerm

	// 1. 任期检查：PreVote 的 Term 必须大于当前 Term (或者至少能跟上)
	// 如果 args.Term < r.currentTerm，显然是过时的
	if args.Term < r.currentTerm {
		reply.VoteGranted = false
		return nil
	}

	// 2. 【关键】Leader 租约检查 (Sticky Leader)
	// 如果在 electionTimeout 内收到过 Leader 的消息，说明集群健康，拒绝干扰。
	if r.leaderHasLease() {
		log.Infof("[PreVote] Node %d rejected PreVote from %d (Leader lease active)", r.id, args.CandidateID)
		reply.VoteGranted = false
		return nil
	}

	// 3. 日志新鲜度检查
	isLogUpToDate, err := r.isLogUpToDate(args.LastLogIndex, args.LastLogTerm)
	if err != nil {
		return err
	}

	// 4. 授予预投票 (注意：这里不持久化 VotedFor，也不重置选举定时器)
	if isLogUpToDate {
		reply.VoteGranted = true
		log.Infof("[PreVote] Node %d granted PreVote to %d for term %d", r.id, args.CandidateID, args.Term)
	} else {
		reply.VoteGranted = false
	}
	return nil
}

// leaderHasLease 检查当前节点是否持有有效的 Leader 租约。
// 如果距离上次收到 Leader 心跳的时间小于最小选举超时时间，则认为 Leader 存活。
func (r *Raft) leaderHasLease() bool {
	return time.Since(r.electionResetEvent) < r.electionTimeout
}

// initializeCandidateState 负责将节点状态转换为 Candidate，更新任期，投票给自己，并持久化这些变更。
// 这是成为候选人的第一步，且必须是原子性的。
func (r *Raft) initializeCandidateState() error {
	// 如果我们不是 Follower 或 Candidate（例如，Stop() 被调用且状态为 Dead），
	// 则中止选举，防止 Goroutine 泄露。
	if r.state != Follower && r.state != Candidate {
		log.Infof("[Election] Node %d aborting election start; state is %d", r.id, r.state)
		// 返回一个错误，以便 startElection 协程能安全退出
		return errors.New("cannot start election in non-follower/candidate state")
	}

	// 将状态更新为 Candidate，增加当前任期号，并给自己投票。
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	// 重置选举计时器，为本轮选举设定新的超时时间。
	r.electionResetEvent = time.Now()

	// 持久化更新后的任期和投票记录。
	// 这是至关重要的一步：必须在发送投票请求（RPC）之前将新状态写入稳定存储。
	// 这样可以确保即使节点在发送请求后立即崩溃，重启后也不会忘记自己已经进入了新的任期并投了票，
	// 从而避免在同一个任期内投票给其他候选人，防止脑裂。
	if err := r.store.SetState(param.HardState{CurrentTerm: r.currentTerm, VotedFor: uint64(r.votedFor)}); err != nil {
		log.Errorf("[Election] Node %d failed to persist state before election: %v", r.id, err)
		return err
	}

	log.Infof("[Election] Node %d starts election for term %d", r.id, r.currentTerm)
	return nil
}

// getLastLogInfoForElection 从存储中获取最后一条日志的索引和任期。
// 这些信息将用于填充 RequestVote RPC 参数，以供其他节点进行日志新旧检查。
func (r *Raft) getLastLogInfoForElection() (lastLogIndex uint64, lastLogTerm uint64, err error) {
	// 从存储中获取自己最后一条日志的索引。
	lastLogIndex, err = r.store.LastLogIndex()
	if err != nil {
		log.Errorf("[Election] Node %d failed to get last log index for election: %v", r.id, err)
		return 0, 0, err
	}

	// 如果日志不为空，则获取最后一条日志的任期。
	if lastLogIndex > 0 {
		lastLogTerm, err = r.getLogTerm(lastLogIndex)
		if err != nil {
			log.Errorf("[Election] Node %d failed to get last log term for election: %v", r.id, err)
			return 0, 0, err
		}
	}
	return lastLogIndex, lastLogTerm, nil
}

// broadcastVoteRequests 负责向集群中所有其他节点并行地发送投票请求。
// 它会返回一个 channel，用于接收投票结果。
func (r *Raft) broadcastVoteRequests(term uint64, lastLogIndex uint64, lastLogTerm uint64, isPreVote bool) <-chan *param.VoteResult {
	voteChan := make(chan *param.VoteResult, len(r.getAllPeerIDs()))

	// 并发地向集群中的所有其他节点发送投票请求。
	// 使用 goroutine 可以确保所有请求并行发出，加快选举过程。
	for _, peerID := range r.getAllPeerIDs() {
		// 跳过自己
		if peerID == r.id {
			continue
		}
		go r.sendVoteRequest(peerID, term, lastLogIndex, lastLogTerm, voteChan, isPreVote)
	}
	return voteChan
}

// handleElectionResult 处理投票结果和超时，决定是否成为Leader或回退
// 如果收到了超过半数节点的投票，则该 Candidate 节点成为 Leader。
// 成为 Leader 后，会立即初始化 Leader 的状态（nextIndex 和 matchIndex）并开始发送心跳。
// 如果在选举超时时间内未能获得多数票，选举失败，节点退回为 Follower 状态。
func (r *Raft) handleElectionResult(voteChan <-chan *param.VoteResult, electionTerm uint64, isPreVote bool) {
	// 1. 初始化选举上下文。
	ctx := newElectionContext(r)

	// 检查是否已经满足获胜条件（例如单节点集群，或者已经有足够的票数）
	if ctx.checkWinCondition() {
		if isPreVote {
			log.Infof("[PreVote] Node %d Pre-Vote passed immediately (single node or majority self) for term %d. Starting Real Election.", r.id, electionTerm)
			r.startRealElection()
		} else {
			r.transitionToLeader(electionTerm)
		}
		return
	}

	// 2. 启动选举计时器。
	electionTimer := time.NewTimer(r.electionTimeout)
	defer electionTimer.Stop()

	// 3. 循环等待，直到选举获胜或超时。
	for {
		select {
		case <-r.shutdownChan:
			return

		case result := <-voteChan:
			if r.processVote(ctx, result, electionTerm) {
				// --- 选举获胜 ---
				if isPreVote {
					log.Infof("[PreVote] Node %d Pre-Vote passed for term %d. Starting Real Election.", r.id, electionTerm)
					r.startRealElection() // Pre-Vote 成功，进入正式选举
				} else {
					r.transitionToLeader(electionTerm) // 正式选举成功，成为 Leader
				}
				return
			}

		case <-electionTimer.C:
			// Pre-Vote 超时通常直接忽略，等待下一轮 ticker 触发即可
			if !isPreVote {
				r.handleElectionTimeout(electionTerm)
			} else {
				log.Infof("[PreVote] Node %d Pre-Vote failed/timed out for term %d.", r.id, electionTerm)
			}
			return
		}
	}
}

// processVote 处理单张选票，更新计票器，并检查是否赢得选举。
// 如果赢得选举，则返回 true；否则返回 false。
func (r *Raft) processVote(ctx *electionContext, result *param.VoteResult, electionTerm uint64) (won bool) {
	// 只处理投赞成票的结果。
	if result.VoteGranted {
		log.Infof("[Election] Node %d received a vote from node %d for term %d", r.id, result.VoterID, electionTerm)
		// 检查投票者属于哪个配置，并为相应的计票器加一。
		if _, isOld := findPeer(result.VoterID, ctx.oldPeers); isOld {
			ctx.votesOldConfig++
		}
		if ctx.inJoint {
			if _, isNew := findPeer(result.VoterID, ctx.newPeers); isNew {
				ctx.votesNewConfig++
			}
		}

		// 检查是否满足获胜条件。
		if ctx.checkWinCondition() {
			// 注意：这里不再调用 transitionToLeader，而是只返回 true。
			// 状态转换由 handleElectionResult 负责。
			return true // 选举已获胜
		}
	}
	return false // 尚未获胜
}

// checkWinCondition 根据当前的计票结果和配置状态，判断选举是否获胜。
func (ctx *electionContext) checkWinCondition() bool {
	if ctx.inJoint {
		// 在联合共识状态下，必须同时获得新旧配置的多数票。
		return ctx.votesOldConfig >= ctx.majorityOld && ctx.votesNewConfig >= ctx.majorityNew
	}
	// 在普通状态下，只需获得当前配置的多数票。
	return ctx.votesOldConfig >= ctx.majorityOld
}

// transitionToLeader 封装了当选为 Leader 后的状态转换逻辑。
func (r *Raft) transitionToLeader(electionTerm uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 再次确认自己仍然是本轮选举的候选人，防止因状态变更导致的问题。
	if r.state == Candidate && r.currentTerm == electionTerm {
		log.Infof("[Election] Node %d elected as Leader for term %d", r.id, r.currentTerm)
		r.state = Leader
		r.initLeaderState()
		r.startHeartbeat()
	}
}

// handleElectionTimeout 封装了选举超时后的状态转换逻辑。
func (r *Raft) handleElectionTimeout(electionTerm uint64) {
	log.Infof("[Election] Node %d election timed out for term %d", r.id, electionTerm)
	r.mu.Lock()
	defer r.mu.Unlock()

	// 确认自己仍然是本轮选举的候选人，然后退回为 Follower 状态。
	if r.state == Candidate && r.currentTerm == electionTerm {
		log.Infof("[Election] Node %d election failed, reverting to Follower", r.id)
		// 调用 becomeFollower 会更新 term, state, votedFor 并持久化
		// 因为任期没有变，所以传入 r.currentTerm 即可
		if err := r.becomeFollower(r.currentTerm); err != nil {
			log.Errorf("[Election] Node %d failed to revert to Follower: %v", r.id, err)
		}
	}
}

// sendVoteRequest 向单个Peer发送投票请求。
// 新增 isPreVote 参数，并将其设置到 RPC Args 中。
func (r *Raft) sendVoteRequest(peerID int, term uint64, lastLogIndex uint64, lastLogTerm uint64, voteChan chan<- *param.VoteResult, isPreVote bool) {
	args := param.NewRequestVoteArgs(term, r.id, lastLogIndex, lastLogTerm, isPreVote)
	reply := param.NewRequestVoteReply()

	if err := r.trans.SendRequestVote(strconv.Itoa(peerID), args, reply); err != nil {
		log.Errorf("[Election] Node %d failed to request vote from %d: %v", r.id, peerID, err)
		voteChan <- &param.VoteResult{VoterID: peerID, VoteGranted: false}
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// 收到更高 Term 时的处理：
	// 1. 如果是正式选举，必须立即转为 Follower。
	// 2. 如果是 Pre-Vote，如果对方 Term 更高，意味着集群有更活跃的 Leader，也应该转为 Follower 更新 Term。
	// 修正：如果 VoteGranted 为 true，说明对方认可我们的任期（即使是 Pre-Vote 的未来任期），不应视为发现更高任期。
	// 只有在 VoteGranted 为 false 且 reply.Term > r.currentTerm 时，才需要退回。
	// 对于 Pre-Vote，args.Term = r.currentTerm + 1。如果 reply.Term == args.Term 且 VoteGranted=true，这是好事。
	if reply.Term > r.currentTerm && !reply.VoteGranted {
		log.Infof("[Election] Node %d found higher term %d from peer %d, becomes Follower", r.id, reply.Term, peerID)
		// 注意：这里更新到 reply.Term
		if err := r.becomeFollower(reply.Term); err != nil {
			log.Errorf("[Election] Node %d failed to persist state: %v", r.id, err)
		}
		// 即使 Pre-Vote 失败也返回，防止误判
		voteChan <- &param.VoteResult{VoterID: peerID, VoteGranted: false}
		return
	}

	voteChan <- &param.VoteResult{VoterID: peerID, VoteGranted: reply.VoteGranted}
}

// handleVoteTermLogic 封装了所有与任期相关的逻辑。
// 返回值 bool 表示是否应继续后续的投票判断。
// 此函数必须在持有锁的情况下被调用。
func (r *Raft) handleVoteTermLogic(args *param.RequestVoteArgs, reply *param.RequestVoteReply) (bool, error) {
	// 如果对方的任期低于自己，这是一个过时的请求，直接拒绝。
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.VoteGranted = false
		return false, nil
	}

	// 如果对方的任期高于自己，则更新自己的状态为 Follower。
	if args.Term > r.currentTerm {
		if err := r.becomeFollower(args.Term); err != nil {
			reply.VoteGranted = false
			return false, err // 持久化失败是严重错误
		}
	}
	// 更新 reply 中的任期号以匹配当前（可能已更新的）任期。
	reply.Term = r.currentTerm
	return true, nil
}

// decideVote 封装了最终的投票决策逻辑。
// 它检查投票资格和日志新鲜度，并据此授予或拒绝投票。
// 此函数必须在持有锁的情况下被调用。
func (r *Raft) decideVote(args *param.RequestVoteArgs, reply *param.RequestVoteReply) error {
	// 检查自己是否有资格投票（在本任期内还未投票，或已投给当前候选人）。
	canVote := r.votedFor == -1 || r.votedFor == args.CandidateID

	// 检查候选人是否在自己的配置中
	_, inConfig := findPeer(args.CandidateID, r.peerIDs)
	if r.inJointConsensus {
		_, inNewConfig := findPeer(args.CandidateID, r.newPeerIDs)
		inConfig = inConfig || inNewConfig
	}

	// 检查候选人的日志是否至少和自己一样新。
	logIsUpToDate, err := r.isLogUpToDate(args.LastLogIndex, args.LastLogTerm)
	if err != nil {
		reply.VoteGranted = false
		return err
	}

	// 只有同时满足三个条件时，才授予投票。
	if canVote && inConfig && logIsUpToDate {
		if err := r.grantVote(args.CandidateID); err != nil {
			reply.VoteGranted = false
			return err
		}
		reply.VoteGranted = true
	} else {
		// 否则，拒绝投票，并记录详细原因。
		log.Infof("[RequestVote] Node %d denying vote for term %d to candidate %d. (canVote=%t, inConfig=%t, logIsUpToDate=%t)", r.id, r.currentTerm, args.CandidateID, canVote, inConfig, logIsUpToDate)
		reply.VoteGranted = false
	}
	return nil
}

// isLeader 是一个简单的辅助函数，用于检查节点是否为 Leader。
// 此函数必须在持有锁的情况下被调用。
func (r *Raft) isLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == Leader
}

// isDuplicateRequest 检查一个客户端请求是否是重复的。
// 它通过比较请求的序列号和服务器记录的该客户端的最后一个序列号来判断。
func (r *Raft) isDuplicateRequest(clientID int64, sequenceNum int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	lastSeqNum, exists := r.clientSessions[clientID]
	if exists && sequenceNum <= lastSeqNum {
		log.Infof("[Client] Duplicate request detected from client %d (seq: %d)", clientID, sequenceNum)
		return true
	}
	return false
}

// isLogUpToDate 检查候选人的日志是否至少和本节点一样新。
// 这是 Raft 选举安全规则的核心实现。此函数必须在持有锁的情况下被调用。
func (r *Raft) isLogUpToDate(candidateLastLogIndex, candidateLastLogTerm uint64) (bool, error) {
	// 从存储中获取本节点的最后一条日志信息。
	localLastLogIndex, err := r.store.LastLogIndex()
	if err != nil {
		log.Errorf("[Election] Node %d failed to get last log index from store: %v", r.id, err)
		return false, err
	}

	localLastLogTerm, err := r.getLogTerm(localLastLogIndex)
	if err != nil { // 检查 err 是否为 nil
		log.Errorf("[Election] Node %d failed to get last log entry from store: %v", r.id, err)
		return false, err
	}

	// 1. 如果任期号不同，任期号大的日志更新。
	// 2. 如果任期号相同，日志更长的（索引更大）的更新。
	if candidateLastLogTerm > localLastLogTerm || (candidateLastLogTerm == localLastLogTerm && candidateLastLogIndex >= localLastLogIndex) {
		return true, nil
	}

	return false, nil
}

// grantVote 记录为指定候选人投票的动作，并将其持久化。
// 此函数必须在持有锁的情况下被调用。
func (r *Raft) grantVote(candidateID int) error {
	log.Infof("[RequestVote] Node %d granting vote for term %d to candidate %d.", r.id, r.currentTerm, candidateID)
	r.votedFor = candidateID
	r.electionResetEvent = time.Now()

	if err := r.store.SetState(param.HardState{CurrentTerm: r.currentTerm, VotedFor: uint64(r.votedFor)}); err != nil {
		log.Errorf("[Election] Node %d failed to persist vote: %v", r.id, err)
		return err
	}
	return nil
}

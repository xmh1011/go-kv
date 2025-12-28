package param

// RequestVoteArgs See figure 2 in the paper.
type RequestVoteArgs struct {
	Term         uint64 // 候选人的任期号
	CandidateID  int    // 候选人的ID
	LastLogIndex uint64 // 候选人最后一条日志的索引
	LastLogTerm  uint64 // 候选人最后一条日志的任期号
	PreVote      bool   // 是否是预投票
}

func NewRequestVoteArgs(term uint64, candidateID int, lastLogIndex, lastLogTerm uint64, isPreVote bool) *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         term,
		CandidateID:  candidateID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		PreVote:      isPreVote,
	}
}

// RequestVoteReply 定义RequestVote RPC响应 See figure 2 in the paper.
type RequestVoteReply struct {
	Term        uint64 // 当前节点的任期号（用于候选者更新自身）
	VoteGranted bool   // 是否投票给候选者
	CandidateID int    // 投票者的ID (注意：这里应该是 VoterID，但为了兼容性先保留 CandidateID 命名，或者修改为 VoterID)
}

func NewRequestVoteReply() *RequestVoteReply {
	return &RequestVoteReply{
		Term:        0,
		VoteGranted: false,
	}
}

// VoteResult is used to carry the result of a RequestVote RPC,
// including the voter's ID for joint consensus counting.
type VoteResult struct {
	VoterID     int  // The ID of the node that cast the vote.
	VoteGranted bool // Whether the vote was granted.
}

// AppendEntriesArgs is the RPC argument for appendEntries requests (log replication + heartbeats).
type AppendEntriesArgs struct {
	Term         uint64     // Leader's current term
	LeaderID     int        // Leader's ID (for follower redirection)
	PrevLogIndex uint64     // Index of log entry immediately preceding new ones
	PrevLogTerm  uint64     // Term of PrevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit uint64     // Leader's commitIndex
}

// NewAppendEntriesArgs creates a new AppendEntriesArgs struct.
func NewAppendEntriesArgs(term uint64, leaderID int, prevLogIndex, prevLogTerm, leaderCommit uint64, entries []LogEntry) *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
}

// AppendEntriesReply is the RPC response for appendEntries requests.
type AppendEntriesReply struct {
	Term          uint64 // Current term (for leader to update itself)
	Success       bool   // True if follower contained entry matching PrevLogIndex/Term
	ConflictIndex uint64 // If conflict, the first index where they differ
	ConflictTerm  uint64 // If conflict, the term of the conflicting entry
}

// NewAppendEntriesReply creates a new AppendEntriesReply struct.
func NewAppendEntriesReply() *AppendEntriesReply {
	return &AppendEntriesReply{
		Term:          0,
		Success:       false,
		ConflictIndex: 0,
		ConflictTerm:  0,
	}
}

// InstallSnapshotArgs 定义InstallSnapshot RPC请求
type InstallSnapshotArgs struct {
	Term              uint64 // 领导者的任期号
	LeaderID          uint64 // 领导者ID
	LastIncludedIndex uint64 // 快照中包含的最后一条日志的索引
	LastIncludedTerm  uint64 // 快照中包含的最后一条日志的任期号
	Offset            uint64 // 数据偏移量（分块传输时使用）
	Data              []byte // 原始快照数据
	Done              bool   // 是否为最后一块
}

// NewInstallSnapshotArgs 创建一个新的 InstallSnapshotArgs 实例
func NewInstallSnapshotArgs(term, leaderID, lastIncludedIndex, lastIncludedTerm uint64, data []byte) *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          leaderID,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Offset:            0,
		Data:              data,
		Done:              true,
	}
}

// InstallSnapshotReply 定义InstallSnapshot RPC响应
type InstallSnapshotReply struct {
	Term uint64 // 当前节点的任期号
}

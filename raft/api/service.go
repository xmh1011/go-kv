package api

import (
	"github.com/xmh1011/go-kv/pkg/param"
)

// RaftService 定义了 Raft 节点需要暴露给 Transport 的 RPC 处理方法。
// Transport 层通过此接口回调 Raft 核心逻辑。
type RaftService interface {
	RequestVote(args *param.RequestVoteArgs, reply *param.RequestVoteReply) error

	AppendEntries(args *param.AppendEntriesArgs, reply *param.AppendEntriesReply) error

	InstallSnapshot(args *param.InstallSnapshotArgs, reply *param.InstallSnapshotReply) error

	ClientRequest(args *param.ClientArgs, reply *param.ClientReply) error
}

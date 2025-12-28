package transport

import (
	"errors"
	"fmt"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/transport/grpc"
	"github.com/xmh1011/go-kv/pkg/transport/inmemory"
	"github.com/xmh1011/go-kv/pkg/transport/tcp"
	"github.com/xmh1011/go-kv/raft/api"
)

const (
	TcpTransport      = "tcp"
	GrpcTransport     = "grpc"
	InMemoryTransport = "inmemory"
)

// Transport 定义了 Raft 节点之间以及客户端与节点之间通信所需的方法。
type Transport interface {
	// Addr 返回当前 Transport 监听的实际地址。
	Addr() string

	// SetPeers 设置节点 ID 到地址的映射。
	SetPeers(peers map[int]string)

	// RegisterRaft 注册 Raft 实例，用于处理接收到的 RPC 请求。
	RegisterRaft(raftInstance api.RaftService)

	// Start 注册 RPC 服务并开始接受连接。
	Start() error

	// Close 关闭监听器，停止接受新的连接。
	Close() error

	// SendRequestVote 发送 RequestVote RPC 请求。
	SendRequestVote(target string, req *param.RequestVoteArgs, resp *param.RequestVoteReply) error

	// SendAppendEntries 发送 AppendEntries RPC 请求。
	SendAppendEntries(target string, req *param.AppendEntriesArgs, resp *param.AppendEntriesReply) error

	// SendInstallSnapshot 发送 InstallSnapshot RPC 请求。
	SendInstallSnapshot(target string, req *param.InstallSnapshotArgs, resp *param.InstallSnapshotReply) error

	// SendClientRequest 发送客户端请求到指定的 Raft 节点。
	SendClientRequest(target string, req *param.ClientArgs, resp *param.ClientReply) error
}

func NewTransport(transportType, addr string) (Transport, error) {
	switch transportType {
	case TcpTransport:
		return tcp.NewTransport(addr)
	case GrpcTransport:
		return grpc.NewTransport(addr)
	case InMemoryTransport:
		return inmemory.NewTransport(addr), nil
	default:
		return nil, fmt.Errorf("unknown transport type: %s", transportType)
	}
}

func NewClientTransport(clientAddr, transportType string) (Transport, error) {
	switch transportType {
	case TcpTransport:
		return tcp.NewTransport(clientAddr)
	case GrpcTransport:
		return grpc.NewTransport(clientAddr)
	case InMemoryTransport:
		return nil, errors.New("'inmemory' transport does not support cross-process communication. You cannot use the standalone client with an in-memory server")
	default:
		return nil, fmt.Errorf("unknown transport type: %s", transportType)
	}
}

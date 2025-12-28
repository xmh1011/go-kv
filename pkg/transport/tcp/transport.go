package tcp

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/raft/api"
)

const (
	defaultRPCTimeout         = 2 * time.Second
	installSnapshotRPCTimeout = 5 * time.Second
)

// Transport 实现了 Transport 接口，通过 TCP 和 net/rpc 进行通信。
type Transport struct {
	listener  net.Listener
	localAddr string // 实际监听的地址 (IP:Port)

	raft   api.RaftService
	server *rpc.Server

	mu        sync.RWMutex
	peers     map[string]*rpc.Client // 缓存 RPC 客户端连接
	resolvers map[int]string         // NodeID -> Address 映射
}

// NewTransport 创建一个新的 Transport 实例并立即开始监听端口。
func NewTransport(listenAddr string) (*Transport, error) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	return &Transport{
		listener:  listener,
		localAddr: listener.Addr().String(),
		peers:     make(map[string]*rpc.Client),
		resolvers: make(map[int]string),
		server:    rpc.NewServer(),
	}, nil
}

// Addr 返回当前 Transport 监听的实际地址。
func (t *Transport) Addr() string {
	return t.localAddr
}

// SetPeers 设置节点 ID 到地址的映射。
func (t *Transport) SetPeers(peers map[int]string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.resolvers = make(map[int]string)
	for id, addr := range peers {
		t.resolvers[id] = addr
	}

	for _, client := range t.peers {
		client.Close()
	}
	t.peers = make(map[string]*rpc.Client)
}

// RegisterRaft 注册 Raft 实例，用于处理接收到的 RPC 请求。
func (t *Transport) RegisterRaft(raftInstance api.RaftService) {
	t.raft = raftInstance
}

// Start 注册 RPC 服务并开始接受连接。
func (t *Transport) Start() error {
	if t.raft == nil {
		return errors.New("raft instance not registered")
	}

	// 使用一个固定的服务名 "RaftService" 来注册服务，
	// 这样客户端就可以使用 "RaftService.MethodName" 来调用。
	err := t.server.RegisterName("RaftService", t.raft)
	if err != nil {
		return err
	}

	go t.acceptConnections()

	log.Infof("[TCPTransport] Service started on %s", t.localAddr)
	return nil
}

// acceptConnections 循环接受并处理新的 TCP 连接。
func (t *Transport) acceptConnections() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			var opErr *net.OpError
			if errors.As(err, &opErr) && opErr.Err.Error() == "use of closed network connection" {
				return
			}
			continue
		}
		go t.server.ServeConn(conn)
	}
}

// Close 关闭监听器，停止接受新的连接。
func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, client := range t.peers {
		client.Close()
	}
	t.peers = make(map[string]*rpc.Client)

	if t.listener != nil {
		return t.listener.Close()
	}
	return nil
}

// getPeerAddress 根据 NodeID 获取地址。
func (t *Transport) getPeerAddress(nodeIDStr string) (string, error) {
	id, err := strconv.Atoi(nodeIDStr)
	if err != nil {
		return "", fmt.Errorf("invalid node id: %s", nodeIDStr)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	addr, ok := t.resolvers[id]
	if !ok {
		return "", fmt.Errorf("address not found for node %d", id)
	}
	return addr, nil
}

// getPeerClient 获取或创建一个到目标节点的 RPC 客户端。
func (t *Transport) getPeerClient(targetID string) (*rpc.Client, error) {
	addr, err := t.getPeerAddress(targetID)
	if err != nil {
		return nil, err
	}

	t.mu.RLock()
	client, ok := t.peers[targetID]
	t.mu.RUnlock()

	if ok && client != nil {
		return client, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if client, ok := t.peers[targetID]; ok && client != nil {
		return client, nil
	}

	log.Infof("[TCPTransport] Dialing new connection to node %s at %s", targetID, addr)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	client = rpc.NewClient(conn)
	t.peers[targetID] = client
	return client, nil
}

// remoteCall 是一个通用的 RPC 调用函数。
func (t *Transport) remoteCall(targetID, method string, args any, reply any, timeout time.Duration) error {
	client, err := t.getPeerClient(targetID)
	if err != nil {
		return err
	}

	fullMethod := "RaftService." + method
	call := client.Go(fullMethod, args, reply, nil)

	select {
	case <-call.Done:
		err = call.Error
	case <-time.After(timeout):
		err = errors.New("rpc call timed out")
	}

	if err != nil {
		t.mu.Lock()
		if cachedClient, ok := t.peers[targetID]; ok && cachedClient == client {
			delete(t.peers, targetID)
			client.Close()
		}
		t.mu.Unlock()
		return err
	}
	return nil
}

// SendRequestVote 发送 RequestVote RPC 请求。
func (t *Transport) SendRequestVote(target string, req *param.RequestVoteArgs, resp *param.RequestVoteReply) error {
	return t.remoteCall(target, "RequestVote", req, resp, defaultRPCTimeout)
}

// SendAppendEntries 发送 AppendEntries RPC 请求。
func (t *Transport) SendAppendEntries(target string, req *param.AppendEntriesArgs, resp *param.AppendEntriesReply) error {
	return t.remoteCall(target, "AppendEntries", req, resp, defaultRPCTimeout)
}

// SendInstallSnapshot 发送 InstallSnapshot RPC 请求。
func (t *Transport) SendInstallSnapshot(target string, req *param.InstallSnapshotArgs, resp *param.InstallSnapshotReply) error {
	return t.remoteCall(target, "InstallSnapshot", req, resp, installSnapshotRPCTimeout)
}

// SendClientRequest 发送客户端请求到指定的 Raft 节点。
func (t *Transport) SendClientRequest(target string, req *param.ClientArgs, resp *param.ClientReply) error {
	return t.remoteCall(target, "ClientRequest", req, resp, defaultRPCTimeout)
}

package inmemory

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/raft/api"
)

// Transport 是一个基于内存的 Transport 实现，用于在单个进程内模拟 Raft 节点间的通信。
type Transport struct {
	mu           sync.RWMutex
	localAddr    string                     // 本地节点的地址
	peers        map[string]api.RaftService // 存储集群中其他节点的引用 (Addr -> Server)
	resolvers    map[int]string             // NodeID -> Address
	allowedPeers map[string]bool            // 允许通信的节点地址 (用于模拟网络分区)
	raft         api.RaftService
}

// NewTransport 创建一个新的 Transport 实例。
// addr 是当前使用此 transport 的节点的地址。
func NewTransport(addr string) *Transport {
	return &Transport{
		localAddr: addr,
		peers:     make(map[string]api.RaftService),
		resolvers: make(map[int]string),
	}
}

// Addr 返回当前 Transport 监听的实际地址。
func (t *Transport) Addr() string {
	return t.localAddr
}

// SetPeers 设置节点 ID 到地址的映射。
// 在 InMemoryTransport 中，我们使用它来控制网络连通性（模拟分区），同时也用于 ID 解析。
func (t *Transport) SetPeers(peers map[int]string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.resolvers = make(map[int]string)
	t.allowedPeers = make(map[string]bool)

	for id, addr := range peers {
		t.resolvers[id] = addr
		t.allowedPeers[addr] = true
	}
}

// RegisterRaft 注册 Raft 实例。
func (t *Transport) RegisterRaft(raftInstance api.RaftService) {
	t.raft = raftInstance
}

// Start 启动 Transport。
func (t *Transport) Start() error {
	return nil
}

// Close 关闭 Transport。
func (t *Transport) Close() error {
	return nil
}

// Connect 将一个节点（peer）添加到 transport 的注册表中。
// 这样，当前的 transport 就知道如何“发送”消息给这个 peer。
func (t *Transport) Connect(peerAddr string, server api.RaftService) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[peerAddr] = server
}

// Disconnect 从 transport 的注册表中移除一个节点。
func (t *Transport) Disconnect(peerAddr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, peerAddr)
}

// getPeer 根据目标 ID 或地址查找对应的 RPCServer。
// 支持直接传入地址，如果解析 ID 失败，则作为地址处理。
func (t *Transport) getPeer(target string) (api.RaftService, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var addr string

	// 1. 尝试将 target 解析为 NodeID (int)
	id, err := strconv.Atoi(target)
	if err == nil {
		// 如果是数字，通过 resolvers 查找对应的地址
		resolvedAddr, ok := t.resolvers[id]
		if !ok {
			return nil, fmt.Errorf("address not found for node %d", id)
		}
		addr = resolvedAddr
	} else {
		addr = target
	}

	// 2. 检查网络分区 (Partition Check)
	// 如果设置了 allowedPeers，必须检查是否允许通信
	if t.allowedPeers != nil {
		if !t.allowedPeers[addr] {
			return nil, fmt.Errorf("network partition: peer %s not reachable", addr)
		}
	}

	// 3. 获取 Server 实例
	peer, ok := t.peers[addr]
	if !ok {
		return nil, fmt.Errorf("could not connect to peer: %s (addr: %s)", target, addr)
	}
	return peer, nil
}

// SendRequestVote 向目标节点发送 RequestVote RPC。
// 这是一个同步的、内存中的方法调用。
func (t *Transport) SendRequestVote(target string, req *param.RequestVoteArgs, resp *param.RequestVoteReply) error {
	peer, err := t.getPeer(target)
	if err != nil {
		return err
	}
	// 直接调用目标节点的 RequestVote 方法
	return peer.RequestVote(req, resp)
}

// SendAppendEntries 向目标节点发送 AppendEntries RPC。
func (t *Transport) SendAppendEntries(target string, req *param.AppendEntriesArgs, resp *param.AppendEntriesReply) error {
	peer, err := t.getPeer(target)
	if err != nil {
		return err
	}
	// 直接调用目标节点的 AppendEntries 方法
	return peer.AppendEntries(req, resp)
}

// SendInstallSnapshot 向目标节点发送 InstallSnapshot RPC。
func (t *Transport) SendInstallSnapshot(target string, req *param.InstallSnapshotArgs, resp *param.InstallSnapshotReply) error {
	peer, err := t.getPeer(target)
	if err != nil {
		return err
	}
	// 直接调用目标节点的 InstallSnapshot 方法
	return peer.InstallSnapshot(req, resp)
}

// SendClientRequest 将客户端请求发送到目标 Raft 节点。
func (t *Transport) SendClientRequest(target string, req *param.ClientArgs, resp *param.ClientReply) error {
	peer, err := t.getPeer(target)
	if err != nil {
		return err
	}
	// 直接调用目标节点的 ClientRequest 方法
	return peer.ClientRequest(req, resp)
}

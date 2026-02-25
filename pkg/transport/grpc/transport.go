package grpc

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/transport/grpc/pb"
	"github.com/xmh1011/go-kv/raft/api"
)

// ==================== 超时策略 ====================
//
// 生产级超时设计原则：
// - AppendEntries: 基于 ElectionTimeout 动态计算（避免频繁重试）
// - RequestVote: 快速失败（选举阶段系统不可用）
// - InstallSnapshot: 使用流式传输，不依赖超时
// - ClientRequest: 合理的客户端超时

const (
	// DefaultRequestVoteTimeout : 选举阶段快速失败进入下一个 Term
	// 原因：如果一个节点迟迟不响应投票请求（可能是挂了），
	// Candidate 应该快速失败并重试其他节点
	DefaultRequestVoteTimeout = 300 * time.Millisecond

	// DefaultClientRequestTimeout ClientRequest: 客户端请求超时
	DefaultClientRequestTimeout = 5 * time.Second

	// DefaultChunkSendTimeout InstallSnapshot 流式传输：每个块的发送超时
	// 原因：流式传输不会因为大文件而超时，只需要设置单块的超时
	DefaultChunkSendTimeout = 10 * time.Second

	// AppendEntriesTimeoutRatio AppendEntries 基准比例：ElectionTimeout 的百分比
	// 原因：这是最高频调用，包含磁盘 fsync，太短会导致频繁重试
	// 默认为 70%，可根据网络/磁盘条件调整
	AppendEntriesTimeoutRatio = 0.70
)

// Transport implements transport.Transport using gRPC.
type Transport struct {
	pb.UnimplementedRaftServiceServer
	listener  net.Listener
	localAddr string

	raft       api.RaftService
	grpcServer *grpc.Server
	mu         sync.RWMutex
	conns      map[string]*grpc.ClientConn
	clients    map[string]pb.RaftServiceClient
	resolvers  map[int]string

	// 用于动态计算 AppendEntries 超时
	electionTimeout time.Duration
}

// NewTransport creates a new gRPC Transport.
func NewTransport(listenAddr string) (*Transport, error) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	return &Transport{
		listener:  listener,
		localAddr: listener.Addr().String(),
		conns:     make(map[string]*grpc.ClientConn),
		clients:   make(map[string]pb.RaftServiceClient),
		resolvers: make(map[int]string),
		// 设置最大消息大小为 100MB，支持流式快照传输
		grpcServer: grpc.NewServer(
			grpc.MaxRecvMsgSize(100*1024*1024),
			grpc.MaxSendMsgSize(100*1024*1024),
		),
	}, nil
}

// SetElectionTimeout 设置选举超时，用于动态计算 AppendEntries 超时
func (t *Transport) SetElectionTimeout(timeout time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.electionTimeout = timeout
}

// getAppendEntriesTimeout 基于 ElectionTimeout 动态计算超时
// 返回 ElectionTimeout 的 70%，保证在心跳间隔内完成
func (t *Transport) getAppendEntriesTimeout() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.electionTimeout > 0 {
		return time.Duration(float64(t.electionTimeout) * AppendEntriesTimeoutRatio)
	}
	// 默认值：200ms 的 70% = 140ms
	return 140 * time.Millisecond
}

// Addr returns the local address.
func (t *Transport) Addr() string {
	return t.localAddr
}

// SetPeers sets the peer resolvers.
func (t *Transport) SetPeers(peers map[int]string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.resolvers = make(map[int]string)
	for id, addr := range peers {
		t.resolvers[id] = addr
	}

	// Close existing connections to force reconnection with new addresses if needed
	for _, conn := range t.conns {
		conn.Close()
	}
	t.conns = make(map[string]*grpc.ClientConn)
	t.clients = make(map[string]pb.RaftServiceClient)
}

// RegisterRaft registers the Raft RPC server.
func (t *Transport) RegisterRaft(raftInstance api.RaftService) {
	t.raft = raftInstance
}

// Start starts the gRPC server.
func (t *Transport) Start() error {
	if t.raft == nil {
		return errors.New("raft instance not registered")
	}

	pb.RegisterRaftServiceServer(t.grpcServer, t)

	go func() {
		if err := t.grpcServer.Serve(t.listener); err != nil {
			log.Infof("[GRPCTransport] Server stopped: %v", err)
		}
	}()

	log.Infof("[GRPCTransport] Service started on %s", t.localAddr)
	return nil
}

// Close stops the gRPC server and closes all connections.
func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.grpcServer.Stop()

	for _, conn := range t.conns {
		conn.Close()
	}
	t.conns = make(map[string]*grpc.ClientConn)
	t.clients = make(map[string]pb.RaftServiceClient)

	return nil
}

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

func (t *Transport) getPeerClient(targetID string) (pb.RaftServiceClient, error) {
	t.mu.RLock()
	client, ok := t.clients[targetID]
	t.mu.RUnlock()
	if ok {
		return client, nil
	}

	addr, err := t.getPeerAddress(targetID)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if client, ok := t.clients[targetID]; ok {
		return client, nil
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024),
			grpc.MaxCallSendMsgSize(100*1024*1024),
		),
	)
	if err != nil {
		return nil, err
	}

	client = pb.NewRaftServiceClient(conn)
	t.conns[targetID] = conn
	t.clients[targetID] = client

	return client, nil
}

// ==================== Client side implementation ====================

// SendRequestVote 发送 RequestVote RPC 请求。
// 超时策略：快速失败（300ms），选举阶段系统不可用，希望选举越快越好
func (t *Transport) SendRequestVote(target string, req *param.RequestVoteArgs, resp *param.RequestVoteReply) error {
	client, err := t.getPeerClient(target)
	if err != nil {
		return err
	}

	pbReq := &pb.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  int64(req.CandidateID),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
		PreVote:      req.PreVote,
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultRequestVoteTimeout)
	defer cancel()

	pbResp, err := client.RequestVote(ctx, pbReq)
	if err != nil {
		return err
	}

	resp.Term = pbResp.Term
	resp.VoteGranted = pbResp.VoteGranted
	resp.CandidateID = int(pbResp.CandidateId)

	return nil
}

// SendAppendEntries 发送 AppendEntries RPC 请求。
// 超时策略：基于 ElectionTimeout 动态计算（默认 70%）
// 原因：这是最高频调用，包含磁盘 fsync，太短会导致频繁重试
func (t *Transport) SendAppendEntries(target string, req *param.AppendEntriesArgs, resp *param.AppendEntriesReply) error {
	client, err := t.getPeerClient(target)
	if err != nil {
		return err
	}

	pbEntries := make([]*pb.LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		cmdBytes, err := encode(entry.Command)
		if err != nil {
			return err
		}
		pbEntries[i] = &pb.LogEntry{
			Command: cmdBytes,
			Term:    entry.Term,
			Index:   entry.Index,
		}
	}

	pbReq := &pb.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     int64(req.LeaderID),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: req.LeaderCommit,
	}

	// 基于配置的 ElectionTimeout 动态计算超时
	timeout := t.getAppendEntriesTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	pbResp, err := client.AppendEntries(ctx, pbReq)
	if err != nil {
		return err
	}

	resp.Term = pbResp.Term
	resp.Success = pbResp.Success
	resp.ConflictIndex = pbResp.ConflictIndex
	resp.ConflictTerm = pbResp.ConflictTerm

	return nil
}

// SendInstallSnapshot 发送 InstallSnapshot RPC 请求。
// 内部使用流式传输，支持大文件和断点续传。
func (t *Transport) SendInstallSnapshot(target string, req *param.InstallSnapshotArgs, resp *param.InstallSnapshotReply) error {
	// 直接调用流式传输，获得大文件支持和断点续传能力
	// Raft 传入的是完整快照数据（req.Offset=0, req.Done=true）
	if err := t.SendInstallSnapshotStream(target, req.Term, req.LeaderID, req.LastIncludedIndex, req.LastIncludedTerm, req.Data); err != nil {
		return err
	}
	// 流式传输成功后，返回当前 Term（需要从当前状态获取）
	// 由于流式传输已经成功，这里返回请求的 Term
	resp.Term = req.Term
	return nil
}

// SendInstallSnapshotStream 发送 InstallSnapshot RPC 请求（流式传输方式）。
// 支持大文件传输和断点续传，不依赖单一超时。
func (t *Transport) SendInstallSnapshotStream(target string, term, leaderID, lastIncludedIndex, lastIncludedTerm uint64, data []byte) error {
	client, err := t.getPeerClient(target)
	if err != nil {
		return err
	}

	// 建立流式连接
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.InstallSnapshotStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to create snapshot stream: %w", err)
	}

	// 配置流式传输参数
	const chunkSize = 4 * 1024 * 1024 // 4MB per chunk
	totalSize := uint64(len(data))
	offset := uint64(0)

	log.Infof("[GRPCTransport] Starting streaming snapshot transfer: target=%s, size=%d bytes, chunks=%d",
		target, totalSize, (totalSize+chunkSize-1)/chunkSize)

	// 流式发送数据块
	for offset < totalSize {
		// 发送超时：每个块 10 秒，避免单块卡死
		sendCtx, sendCancel := context.WithTimeout(context.Background(), DefaultChunkSendTimeout)

		chunkEnd := offset + chunkSize
		if chunkEnd > totalSize {
			chunkEnd = totalSize
		}

		chunk := &pb.InstallSnapshotChunk{
			Term:              term,
			LeaderId:          leaderID,
			LastIncludedIndex: lastIncludedIndex,
			LastIncludedTerm:  lastIncludedTerm,
			Offset:            offset,
			Data:              data[offset:chunkEnd],
			DataSize:          totalSize,
			Done:              chunkEnd == totalSize,
		}

		// 发送数据块（使用带超时的 context）
		select {
		case <-sendCtx.Done():
			sendCancel()
			return fmt.Errorf("timeout sending chunk at offset %d", offset)
		default:
			if err := stream.Send(chunk); err != nil {
				sendCancel()
				return fmt.Errorf("failed to send chunk at offset %d: %w", offset, err)
			}
		}

		sendCancel()

		// 接收确认
		ack, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive ack for chunk at offset %d: %w", offset, err)
		}

		if !ack.Accepted {
			// 服务端拒绝了该块，可能是需要断点续传
			if ack.NextOffset > offset {
				log.Warnf("[GRPCTransport] Chunk rejected at offset %d, server requests offset %d (resuming...)", offset, ack.NextOffset)
				offset = ack.NextOffset
				continue
			}
			return fmt.Errorf("chunk rejected at offset %d: %s", offset, ack.Error)
		}

		offset = chunkEnd

		// 进度日志（每 100MB 打印一次）
		if offset%(100*1024*1024) == 0 || offset == totalSize {
			log.Infof("[GRPCTransport] Snapshot transfer progress: %d/%d bytes (%.1f%%)", offset, totalSize, float64(offset)*100/float64(totalSize))
		}
	}

	// 发送最后的 done 信号并关闭流
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("failed to close snapshot stream: %w", err)
	}

	// 等待最终的确认
	_, err = stream.Recv()
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to receive final ack: %w", err)
	}

	log.Infof("[GRPCTransport] Snapshot transfer completed: target=%s, size=%d bytes", target, totalSize)

	return nil
}

// SendClientRequest 发送客户端请求到指定的 Raft 节点。
// 超时策略：5 秒客户端请求超时
func (t *Transport) SendClientRequest(target string, req *param.ClientArgs, resp *param.ClientReply) error {
	client, err := t.getPeerClient(target)
	if err != nil {
		return err
	}

	cmdBytes, err := encode(req.Command)
	if err != nil {
		return err
	}

	pbReq := &pb.ClientRequestRequest{
		ClientId:    req.ClientID,
		SequenceNum: req.SequenceNum,
		Command:     cmdBytes,
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultClientRequestTimeout)
	defer cancel()

	pbResp, err := client.ClientRequest(ctx, pbReq)
	if err != nil {
		return err
	}

	result, err := decode(pbResp.Result)
	if err != nil {
		return err
	}

	resp.Success = pbResp.Success
	resp.Result = result
	resp.NotLeader = pbResp.NotLeader
	resp.LeaderHint = int(pbResp.LeaderHint)

	return nil
}

// ==================== Server side implementation ====================

func (t *Transport) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	args := &param.RequestVoteArgs{
		Term:         req.Term,
		CandidateID:  int(req.CandidateId),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
		PreVote:      req.PreVote,
	}
	reply := &param.RequestVoteReply{}

	if err := t.raft.RequestVote(args, reply); err != nil {
		return nil, err
	}

	return &pb.RequestVoteResponse{
		Term:        reply.Term,
		VoteGranted: reply.VoteGranted,
		CandidateId: int64(reply.CandidateID),
	}, nil
}

func (t *Transport) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	entries := make([]param.LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		cmd, err := decode(entry.Command)
		if err != nil {
			return nil, err
		}
		entries[i] = param.LogEntry{
			Command: cmd,
			Term:    entry.Term,
			Index:   entry.Index,
		}
	}

	args := &param.AppendEntriesArgs{
		Term:         req.Term,
		LeaderID:     int(req.LeaderId),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}
	reply := &param.AppendEntriesReply{}

	if err := t.raft.AppendEntries(args, reply); err != nil {
		return nil, err
	}

	return &pb.AppendEntriesResponse{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictIndex: reply.ConflictIndex,
		ConflictTerm:  reply.ConflictTerm,
	}, nil
}

func (t *Transport) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	args := &param.InstallSnapshotArgs{
		Term:              req.Term,
		LeaderID:          req.LeaderId,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Offset:            req.Offset,
		Data:              req.Data,
		Done:              req.Done,
	}
	reply := &param.InstallSnapshotReply{}

	if err := t.raft.InstallSnapshot(args, reply); err != nil {
		return nil, err
	}

	return &pb.InstallSnapshotResponse{
		Term: reply.Term,
	}, nil
}

// InstallSnapshotStream 流式接收快照数据
func (t *Transport) InstallSnapshotStream(stream pb.RaftService_InstallSnapshotStreamServer) error {
	var snapshotBuffer []byte
	var snapshotMetadata *param.InstallSnapshotArgs
	var expectedOffset uint64 = 0
	var totalSize uint64 = 0
	var receivedBytes uint64 = 0

	log.Infof("[GRPCTransport] Starting to receive streaming snapshot")

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving snapshot chunk: %w", err)
		}

		// 第一个 chunk 包含快照元数据
		if snapshotMetadata == nil {
			snapshotMetadata = &param.InstallSnapshotArgs{
				Term:              chunk.Term,
				LeaderID:          chunk.LeaderId,
				LastIncludedIndex: chunk.LastIncludedIndex,
				LastIncludedTerm:  chunk.LastIncludedTerm,
			}
			totalSize = chunk.DataSize
			expectedOffset = 0

			// 预分配缓冲区（避免多次扩容）
			if totalSize > 0 && totalSize < 100*1024*1024 { // 小于 100MB 预分配
				snapshotBuffer = make([]byte, 0, totalSize)
			} else {
				snapshotBuffer = make([]byte, 0, 10*1024*1024) // 至少 10MB 缓冲
			}

			log.Infof("[GRPCTransport] Snapshot metadata: term=%d, leader=%d, lastIndex=%d, size=%d bytes",
				chunk.Term, chunk.LeaderId, chunk.LastIncludedIndex, totalSize)
		}

		// 断点续传检查
		if chunk.Offset != expectedOffset {
			log.Warnf("[GRPCTransport] Expected offset %d, got %d (requesting resume)",
				expectedOffset, chunk.Offset)

			// 发送拒绝并请求正确的偏移量
			if err := stream.Send(&pb.InstallSnapshotAck{
				Accepted:      false,
				NextOffset:    expectedOffset,
				Error:         fmt.Sprintf("offset mismatch: expected %d, got %d", expectedOffset, chunk.Offset),
				ReceivedBytes: receivedBytes,
			}); err != nil {
				return fmt.Errorf("failed to send resume request: %w", err)
			}
			continue
		}

		// 追加数据
		snapshotBuffer = append(snapshotBuffer, chunk.Data...)
		receivedBytes += uint64(len(chunk.Data))
		expectedOffset = receivedBytes

		// 发送确认
		if err := stream.Send(&pb.InstallSnapshotAck{
			Accepted:      true,
			NextOffset:    expectedOffset,
			ReceivedBytes: receivedBytes,
		}); err != nil {
			return fmt.Errorf("failed to send chunk ack: %w", err)
		}

		// 进度日志
		if chunk.Done || receivedBytes%(50*1024*1024) == 0 {
			log.Infof("[GRPCTransport] Snapshot receive progress: %d/%d bytes (%.1f%%)",
				receivedBytes, totalSize, float64(receivedBytes)*100/float64(totalSize))
		}

		// 最后一个 chunk，完成快照安装
		if chunk.Done {
			// 将缓冲区分块发送给 Raft
			chunkSize := 4 * 1024 * 1024 // 4MB chunks
			var currentOffset uint64 = 0

			reply := &param.InstallSnapshotReply{}

			for currentOffset < uint64(len(snapshotBuffer)) {
				chunkEnd := currentOffset + uint64(chunkSize)
				if chunkEnd > uint64(len(snapshotBuffer)) {
					chunkEnd = uint64(len(snapshotBuffer))
				}

				snapshotMetadata.Offset = currentOffset
				snapshotMetadata.Data = snapshotBuffer[currentOffset:chunkEnd]
				snapshotMetadata.Done = (chunkEnd == uint64(len(snapshotBuffer)))

				if err := t.raft.InstallSnapshot(snapshotMetadata, reply); err != nil {
					return fmt.Errorf("failed to install snapshot chunk at offset %d: %w", currentOffset, err)
				}

				currentOffset = chunkEnd
			}

			log.Infof("[GRPCTransport] Snapshot installation completed: size=%d bytes", receivedBytes)
			return nil
		}
	}

	return fmt.Errorf("snapshot stream ended without done marker")
}

func (t *Transport) ClientRequest(ctx context.Context, req *pb.ClientRequestRequest) (*pb.ClientRequestResponse, error) {
	cmd, err := decode(req.Command)
	if err != nil {
		return nil, err
	}

	args := &param.ClientArgs{
		ClientID:    req.ClientId,
		SequenceNum: req.SequenceNum,
		Command:     cmd,
	}
	reply := &param.ClientReply{}

	if err := t.raft.ClientRequest(args, reply); err != nil {
		return nil, err
	}

	resBytes, err := encode(reply.Result)
	if err != nil {
		return nil, err
	}

	return &pb.ClientRequestResponse{
		Success:    reply.Success,
		Result:     resBytes,
		NotLeader:  reply.NotLeader,
		LeaderHint: int64(reply.LeaderHint),
	}, nil
}

// ==================== Helper functions for encoding/decoding ====================

func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode(data []byte) (any, error) {
	var v any
	if len(data) == 0 {
		return nil, nil
	}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

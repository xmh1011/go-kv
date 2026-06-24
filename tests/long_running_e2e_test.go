package tests

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
	"github.com/xmh1011/go-kv/pkg/transport"
	"github.com/xmh1011/go-kv/raft"
)

var errLongRunningTestStopped = errors.New("test stopped")

// LongRunningMetrics 记录长时测试的性能指标
type LongRunningMetrics struct {
	TestName          string
	Duration          time.Duration
	TotalOps          int64
	SuccessOps        int64
	FailedOps         int64
	WriteOps          int64
	ReadOps           int64
	DeleteOps         int64
	BytesRead         int64
	BytesWritten      int64
	LatencyP50        time.Duration
	LatencyP95        time.Duration
	LatencyP99        time.Duration
	ThroughputOps     float64
	WriteThroughput   float64
	ReadThroughput    float64
	DeleteThroughput  float64
	ErrorRate         float64
	LeaderElections   int32
	LeaderDowntime    time.Duration
	DataConsistencyOK bool
	KeysVerified      int64
	SnapshotCount     int32
	SnapshotMaxIndex  uint64
	WALSize           int64
	MemTableFlushes   int32
	FailureReasons    []FailureReasonCount
}

type FailureReasonCount struct {
	Reason string
	Count  int64
}

func TestValidateLongRunningMetricsRejectsHiddenFailures(t *testing.T) {
	metrics := LongRunningMetrics{
		TestName:          "read-heavy",
		TotalOps:          10,
		SuccessOps:        9,
		FailedOps:         1,
		DataConsistencyOK: true,
		KeysVerified:      10,
		FailureReasons: []FailureReasonCount{{
			Reason: "apply_timeout",
			Count:  1,
		}},
	}

	err := validateLongRunningMetrics(metrics, longRunningValidationOptions{
		RequireNoFailedOps:  true,
		RequireConsistency:  true,
		RequireVerifiedKeys: true,
	})
	if err == nil {
		t.Fatal("expected hidden failed operations to be rejected")
	}
	if !strings.Contains(err.Error(), "1 failed operations") || !strings.Contains(err.Error(), "apply_timeout=1") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestValidateLongRunningMetricsRequiresConsistencyEvidence(t *testing.T) {
	metrics := LongRunningMetrics{
		TestName:          "delete-stress",
		TotalOps:          10,
		SuccessOps:        10,
		DataConsistencyOK: false,
		KeysVerified:      0,
	}

	err := validateLongRunningMetrics(metrics, longRunningValidationOptions{
		RequireNoFailedOps:  true,
		RequireConsistency:  true,
		RequireVerifiedKeys: true,
	})
	if err == nil {
		t.Fatal("expected missing consistency evidence to be rejected")
	}
	if !strings.Contains(err.Error(), "consistency check failed") {
		t.Fatalf("expected consistency failure, got: %v", err)
	}
}

func TestValidateLongRunningMetricsRequiresOperationAccounting(t *testing.T) {
	metrics := LongRunningMetrics{
		TestName:          "read-heavy",
		SuccessOps:        10,
		ReadThroughput:    10,
		DataConsistencyOK: true,
		KeysVerified:      10,
	}

	err := validateLongRunningMetrics(metrics, longRunningValidationOptions{
		RequireOperationAccounting: true,
	})
	if err == nil {
		t.Fatal("expected missing operation accounting to be rejected")
	}
	if !strings.Contains(err.Error(), "operation accounting mismatch") {
		t.Fatalf("expected operation accounting failure, got: %v", err)
	}
}

func TestLongRunningStopGateDoesNotCancelIssuedRequest(t *testing.T) {
	stopCh := make(chan struct{})
	close(stopCh)

	if !shouldStopBeforeRequest(stopCh, false) {
		t.Fatal("expected stop signal to cancel before a request is issued")
	}
	if shouldStopBeforeRequest(stopCh, true) {
		t.Fatal("stop signal must not cancel an already issued request")
	}
	if !waitBeforeRetryAfterIssued(stopCh, 0, true) {
		t.Fatal("issued requests should keep retrying despite stop signal")
	}
	if waitBeforeRetryAfterIssued(stopCh, time.Millisecond, false) {
		t.Fatal("not-yet-issued requests should still honor stop signal")
	}
}

func TestLongRunningRetryBudgetExtendsAfterRequestIssued(t *testing.T) {
	now := time.Now()
	issuedAt := now.Add(-longRunningIssuedRequestRetryTimeout / 2)

	if shouldContinueLongRunningRetry(longRunningClientRetries, longRunningClientRetries, false, time.Time{}, now) {
		t.Fatal("not-yet-issued requests should stop at the normal retry limit")
	}
	if !shouldContinueLongRunningRetry(longRunningClientRetries, longRunningClientRetries, true, issuedAt, now) {
		t.Fatal("issued requests should continue beyond the normal retry count")
	}
	if shouldContinueLongRunningRetry(longRunningClientRetries, longRunningClientRetries, true, now.Add(-longRunningIssuedRequestRetryTimeout-time.Millisecond), now) {
		t.Fatal("issued requests should stop after the issued-request retry timeout")
	}
}

func TestLongRunningCounterSnapshotDoesNotReportImpossibleProgress(t *testing.T) {
	var totalOps int64 = 10
	var successOps int64 = 11
	var failedOps int64

	snapshot := snapshotLongRunningCounters(&totalOps, &successOps, &failedOps)
	if snapshot.TotalOps < snapshot.SuccessOps+snapshot.FailedOps {
		t.Fatalf("snapshot reported impossible counters: total=%d success=%d failed=%d",
			snapshot.TotalOps, snapshot.SuccessOps, snapshot.FailedOps)
	}
}

type longRunningCounterSnapshot struct {
	TotalOps   int64
	SuccessOps int64
	FailedOps  int64
}

func snapshotLongRunningCounters(totalOps, successOps, failedOps *int64) longRunningCounterSnapshot {
	success := atomic.LoadInt64(successOps)
	failed := atomic.LoadInt64(failedOps)
	total := atomic.LoadInt64(totalOps)
	accounted := success + failed
	if total < accounted {
		total = accounted
	}

	return longRunningCounterSnapshot{
		TotalOps:   total,
		SuccessOps: success,
		FailedOps:  failed,
	}
}

type longRunningValidationOptions struct {
	RequireNoFailedOps         bool
	RequireConsistency         bool
	RequireVerifiedKeys        bool
	RequireOperationAccounting bool
}

func validateLongRunningMetrics(metrics LongRunningMetrics, opts longRunningValidationOptions) error {
	if opts.RequireNoFailedOps && metrics.FailedOps > 0 {
		return fmt.Errorf("%s completed with %d failed operations (%s)",
			metrics.TestName, metrics.FailedOps, formatFailureReasons(metrics.FailureReasons))
	}
	if opts.RequireConsistency && !metrics.DataConsistencyOK {
		return fmt.Errorf("%s consistency check failed", metrics.TestName)
	}
	if opts.RequireVerifiedKeys && metrics.KeysVerified == 0 {
		return fmt.Errorf("%s consistency check verified zero keys", metrics.TestName)
	}
	if opts.RequireOperationAccounting {
		accountedOps := metrics.WriteOps + metrics.ReadOps + metrics.DeleteOps
		if accountedOps != metrics.SuccessOps {
			return fmt.Errorf("%s operation accounting mismatch: write=%d read=%d delete=%d success=%d",
				metrics.TestName, metrics.WriteOps, metrics.ReadOps, metrics.DeleteOps, metrics.SuccessOps)
		}
	}
	return nil
}

func requireLongRunningMetrics(t *testing.T, metrics LongRunningMetrics, opts longRunningValidationOptions) {
	t.Helper()
	if err := validateLongRunningMetrics(metrics, opts); err != nil {
		t.Fatal(err)
	}
}

func formatFailureReasons(reasons []FailureReasonCount) string {
	if len(reasons) == 0 {
		return "no failure reason recorded"
	}

	parts := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		parts = append(parts, fmt.Sprintf("%s=%d", reason.Reason, reason.Count))
	}
	return strings.Join(parts, ", ")
}

type failureStats struct {
	mu     sync.Mutex
	counts map[string]int64
}

func newFailureStats() *failureStats {
	return &failureStats{counts: make(map[string]int64)}
}

func (fs *failureStats) record(reason string) {
	if reason == "" {
		reason = "unknown"
	}
	fs.mu.Lock()
	fs.counts[reason]++
	fs.mu.Unlock()
}

func (fs *failureStats) snapshot() []FailureReasonCount {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	reasons := make([]string, 0, len(fs.counts))
	for reason := range fs.counts {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)

	result := make([]FailureReasonCount, 0, len(reasons))
	for _, reason := range reasons {
		result = append(result, FailureReasonCount{
			Reason: reason,
			Count:  fs.counts[reason],
		})
	}
	return result
}

const (
	longRunningSnapshotThreshold         = 2 * 1024 * 1024
	longRunningClientRetries             = 20
	longRunningIssuedRequestRetryTimeout = 30 * time.Second
)

// latencySampler 延迟采样器，限制采样数量以控制内存使用
type latencySampler struct {
	mu          sync.Mutex
	latencies   []time.Duration
	maxSamples  int
	sampleCount int64 // 总采样次数（包括被丢弃的）
}

// newLatencySampler 创建一个新的延迟采样器
func newLatencySampler(maxSamples int) *latencySampler {
	return &latencySampler{
		latencies:  make([]time.Duration, 0, maxSamples),
		maxSamples: maxSamples,
	}
}

// add 添加一个延迟样本，使用蓄水池采样策略
func (ls *latencySampler) add(latency time.Duration) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.sampleCount++

	if len(ls.latencies) < ls.maxSamples {
		ls.latencies = append(ls.latencies, latency)
	} else {
		// 蓄水池采样：以 maxSamples/n 的概率替换已有样本
		idx := rand.Int63n(ls.sampleCount)
		if idx < int64(ls.maxSamples) {
			ls.latencies[idx] = latency
		}
	}
}

// getAll 获取所有采样的延迟数据
func (ls *latencySampler) getAll() []time.Duration {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	result := make([]time.Duration, len(ls.latencies))
	copy(result, ls.latencies)
	return result
}

// count 获取采样数量
func (ls *latencySampler) count() int {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return len(ls.latencies)
}

// longRunningCluster 生产环境配置的长时测试集群
type longRunningCluster struct {
	nodes         []*raft.Raft
	transports    []transport.Transport
	storages      []storage.Storage
	stateMachines []storage.StateMachine
	commitChans   []chan param.CommitEntry
	peerMap       map[int]string
	dataDir       string
	// 额外的监控数据
	leaderElections int32
	mu              sync.RWMutex
}

// newLongRunningCluster 创建用于长时测试的生产环境集群
func newLongRunningCluster(t *testing.T, nodeCount int) *longRunningCluster {
	c := &longRunningCluster{
		nodes:         make([]*raft.Raft, nodeCount),
		transports:    make([]transport.Transport, nodeCount),
		storages:      make([]storage.Storage, nodeCount),
		stateMachines: make([]storage.StateMachine, nodeCount),
		commitChans:   make([]chan param.CommitEntry, nodeCount),
		peerMap:       make(map[int]string),
		dataDir:       t.TempDir(),
	}

	// 1. 初始化传输层 - 使用 gRPC 模拟生产环境网络
	for i := 0; i < nodeCount; i++ {
		id := i + 1
		addr := "127.0.0.1:0" // 使用随机端口
		trans, err := transport.NewTransport(transport.GrpcTransport, addr)
		if err != nil {
			t.Fatalf("failed to create gRPC transport for node %d: %v", id, err)
		}
		c.transports[i] = trans
		c.peerMap[id] = trans.Addr()
	}

	// 2. 构造初始配置
	initialPeerIDs := make([]int, 0)
	for i := 0; i < nodeCount; i++ {
		initialPeerIDs = append(initialPeerIDs, i+1)
	}

	// 3. 初始化并启动节点 - 使用 LSM 存储引擎
	for i := 0; i < nodeCount; i++ {
		id := i + 1

		// 创建 LSM 存储层和状态机（生产环境配置）
		store, sm, err := storage.NewStorage(storage.LSMStorage, c.dataDir, id)
		if err != nil {
			t.Fatalf("failed to create LSM storage for node %d: %v", id, err)
		}
		c.storages[i] = store
		c.stateMachines[i] = sm
		c.commitChans[i] = make(chan param.CommitEntry, 10000) // 更大的缓冲区应对生产负载

		// 启动后台协程消费 commitChan
		// 注意：Raft 已经在 dispatchEntries 中应用了日志到状态机，
		// 这里只需要从 channel 中读取以防止阻塞，不需要再次应用
		go func(ch chan param.CommitEntry) {
			for range ch {
				// 仅消费 channel，不重复应用
			}
		}(c.commitChans[i])

		// 配置 Transport
		c.transports[i].SetPeers(c.peerMap)

		// 创建 Raft 实例
		rf := raft.NewRaft(id, initialPeerIDs, store, sm, c.transports[i], c.commitChans[i])
		rf.SetSnapshotThreshold(longRunningSnapshotThreshold)
		c.nodes[i] = rf

		// 注册 Raft 到 Transport
		c.transports[i].RegisterRaft(rf)

		// 启动 Transport 监听
		if err := c.transports[i].Start(); err != nil {
			t.Fatalf("failed to start gRPC transport for node %d: %v", id, err)
		}

		// 启动 Raft 主循环
		go rf.Run()
	}

	return c
}

func (c *longRunningCluster) shutdown() {
	for i := 0; i < len(c.nodes); i++ {
		if c.nodes[i] != nil {
			c.nodes[i].Stop()
		}
		if c.transports[i] != nil {
			_ = c.transports[i].Close()
		}
		if c.storages[i] != nil {
			_ = c.storages[i].Close()
		}
		if c.stateMachines[i] != nil {
			if closer, ok := c.stateMachines[i].(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		}
		if c.commitChans[i] != nil {
			close(c.commitChans[i])
		}
	}
}

func (c *longRunningCluster) nodeCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.nodes)
}

func (c *longRunningCluster) nodeAt(index int) *raft.Raft {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if index < 0 || index >= len(c.nodes) {
		return nil
	}
	return c.nodes[index]
}

func (c *longRunningCluster) nodesSnapshot() []*raft.Raft {
	c.mu.RLock()
	defer c.mu.RUnlock()
	nodes := make([]*raft.Raft, len(c.nodes))
	copy(nodes, c.nodes)
	return nodes
}

func (c *longRunningCluster) snapshotStats() (int32, uint64) {
	var snapshotNodes int32
	var maxSnapshotIndex uint64
	for _, node := range c.nodesSnapshot() {
		if node == nil || node.IsStopped() {
			continue
		}
		index := node.SnapshotIndex()
		if index == 0 {
			continue
		}
		snapshotNodes++
		if index > maxSnapshotIndex {
			maxSnapshotIndex = index
		}
	}
	return snapshotNodes, maxSnapshotIndex
}

func (c *longRunningCluster) stateMachineByID(id int) storage.StateMachine {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if id <= 0 || id > len(c.stateMachines) {
		return nil
	}
	return c.stateMachines[id-1]
}

func (c *longRunningCluster) restartNode(t *testing.T, nodeIndex int) {
	t.Helper()

	c.mu.RLock()
	if nodeIndex < 0 || nodeIndex >= len(c.nodes) {
		c.mu.RUnlock()
		t.Fatalf("node index %d out of range", nodeIndex)
	}
	oldNode := c.nodes[nodeIndex]
	oldTransport := c.transports[nodeIndex]
	oldStorage := c.storages[nodeIndex]
	oldStateMachine := c.stateMachines[nodeIndex]
	commitChan := c.commitChans[nodeIndex]
	id := oldNode.ID()
	addr := c.peerMap[id]
	peerIDs := make([]int, 0, len(c.peerMap))
	peerMap := make(map[int]string, len(c.peerMap))
	for peerID, peerAddr := range c.peerMap {
		peerIDs = append(peerIDs, peerID)
		peerMap[peerID] = peerAddr
	}
	c.mu.RUnlock()

	oldNode.Stop()
	if oldTransport != nil {
		_ = oldTransport.Close()
	}
	if oldStorage != nil {
		_ = oldStorage.Close()
	}
	if oldStateMachine != nil {
		if closer, ok := oldStateMachine.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}

	newTrans, err := transport.NewTransport(transport.GrpcTransport, addr)
	if err != nil {
		t.Fatalf("failed to recreate gRPC transport for node %d: %v", id, err)
	}

	store, sm, err := storage.NewStorage(storage.LSMStorage, c.dataDir, id)
	if err != nil {
		t.Fatalf("failed to reload LSM storage for node %d: %v", id, err)
	}

	newRaft := raft.NewRaft(id, peerIDs, store, sm, newTrans, commitChan)
	newRaft.SetSnapshotThreshold(longRunningSnapshotThreshold)
	newTrans.SetPeers(peerMap)
	newTrans.RegisterRaft(newRaft)
	if err := newTrans.Start(); err != nil {
		t.Fatalf("failed to restart gRPC transport for node %d: %v", id, err)
	}

	c.mu.Lock()
	c.nodes[nodeIndex] = newRaft
	c.transports[nodeIndex] = newTrans
	c.storages[nodeIndex] = store
	c.stateMachines[nodeIndex] = sm
	c.mu.Unlock()

	go newRaft.Run()
	t.Logf("[故障恢复] Node %d restarted", id)
}

// getLeader 获取当前 Leader，超时时间更长以适应长时测试
func (c *longRunningCluster) getLeader(t *testing.T) *raft.Raft {
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, node := range c.nodesSnapshot() {
				if node.IsStopped() {
					continue
				}
				if node.State() == raft.Leader {
					return node
				}
			}
		case <-timeout.C:
			t.Fatalf("Cluster failed to elect a leader within 30 seconds")
		}
	}
}

// waitForAllNodesReady 等待所有节点就绪
func (c *longRunningCluster) waitForAllNodesReady(t *testing.T) {
	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			allReady := true
			for _, node := range c.nodesSnapshot() {
				if node.State() == raft.Dead {
					allReady = false
					break
				}
			}
			if allReady && c.getLeader(t) != nil {
				return
			}
		case <-timeout:
			t.Fatal("Cluster failed to become ready within 60 seconds")
		}
	}
}

// monitorLeaderChanges 监控 Leader 变化
func (c *longRunningCluster) monitorLeaderChanges(ctx chan struct{}) {
	var lastLeader *raft.Raft
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, node := range c.nodesSnapshot() {
				if node.State() == raft.Leader {
					if lastLeader != nil && lastLeader.ID() != node.ID() {
						atomic.AddInt32(&c.leaderElections, 1)
					}
					lastLeader = node
					break
				}
			}
		case <-ctx:
			return
		}
	}
}

// sendRequest 发送请求并返回结果（不处理重定向）
func (c *longRunningCluster) sendRequest(node *raft.Raft, cmd param.KVCommand) (bool, time.Duration, error) {
	cmdBytes, _ := json.Marshal(cmd)
	args := &param.ClientArgs{
		ClientID:    rand.Int63(),
		SequenceNum: rand.Int63(),
		Command:     cmdBytes,
	}
	reply := &param.ClientReply{}

	start := time.Now()
	err := node.ClientRequest(args, reply)
	latency := time.Since(start)

	success := err == nil && reply.Success
	return success, latency, err
}

// sendRequestToAnyNode 向任意节点发送请求，自动处理 NotLeader 重定向
// 这是正确的 Raft 客户端实现方式：
// 1. 可以向任意节点发送请求
// 2. 如果节点是 Leader，正常处理
// 3. 如果节点是 Follower，返回 NotLeader + LeaderHint，客户端重定向
func (c *longRunningCluster) sendRequestToAnyNode(cmd param.KVCommand, maxRetries int, stopCh <-chan struct{}) (bool, time.Duration, error) {
	var totalLatency time.Duration
	requestIssued := false
	requestIssuedAt := time.Time{}

	// 初始随机选择一个节点
	nodeIdx := rand.Intn(c.nodeCount())
	node := c.nodeAt(nodeIdx)
	cmdBytes, _ := json.Marshal(cmd)
	args := &param.ClientArgs{
		ClientID:    rand.Int63(),
		SequenceNum: rand.Int63(),
		Command:     cmdBytes,
	}

	for retry := 0; shouldContinueLongRunningRetry(retry, maxRetries, requestIssued, requestIssuedAt, time.Now()); retry++ {
		// Stop only gates new operations. Once this client identity has been
		// sent, keep retrying it so the test's expected-value model cannot miss
		// an operation that commits after shutdown starts.
		if shouldStopBeforeRequest(stopCh, requestIssued) {
			return false, totalLatency, errLongRunningTestStopped
		}

		reply := &param.ClientReply{}

		start := time.Now()
		if !requestIssued {
			requestIssuedAt = start
		}
		requestIssued = true
		err := node.ClientRequest(args, reply)
		latency := time.Since(start)
		totalLatency += latency

		if err == nil && reply.Success {
			return true, totalLatency, nil
		}

		// 如果收到 NotLeader 响应，使用 LeaderHint 重定向
		if reply.NotLeader {
			updatedLeader := false
			if reply.LeaderHint > 0 && reply.LeaderHint <= c.nodeCount() {
				// 使用 LeaderHint 定位新 Leader
				node = c.nodeAt(reply.LeaderHint - 1)
				updatedLeader = true
			} else {
				// LeaderHint 无效，随机选择一个节点重试
				if leader := c.findLeader(); leader != nil {
					node = leader
					updatedLeader = true
				} else {
					node = c.nodeAt(rand.Intn(c.nodeCount()))
				}
			}
			if !updatedLeader && !waitBeforeRetryAfterIssued(stopCh, retryBackoff(retry), requestIssued) {
				return false, totalLatency, errLongRunningTestStopped
			}
			continue
		}

		// 其他错误或 leader 端暂时未完成 apply，按真实客户端语义继续重试。
		node = c.findLeader()
		if node == nil {
			time.Sleep(100 * time.Millisecond)
			node = c.nodeAt(rand.Intn(c.nodeCount()))
		}
	}

	return false, totalLatency, fmt.Errorf("max retries exceeded")
}

func retryBackoff(retry int) time.Duration {
	delay := time.Duration(retry+1) * 25 * time.Millisecond
	if delay > 150*time.Millisecond {
		return 150 * time.Millisecond
	}
	return delay
}

func waitBeforeRetry(stopCh <-chan struct{}, delay time.Duration) bool {
	if delay <= 0 {
		return true
	}
	if stopCh == nil {
		time.Sleep(delay)
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-stopCh:
		return false
	case <-timer.C:
		return true
	}
}

func shouldStopBeforeRequest(stopCh <-chan struct{}, requestIssued bool) bool {
	if stopCh == nil || requestIssued {
		return false
	}
	select {
	case <-stopCh:
		return true
	default:
		return false
	}
}

func waitBeforeRetryAfterIssued(stopCh <-chan struct{}, delay time.Duration, requestIssued bool) bool {
	if requestIssued {
		time.Sleep(delay)
		return true
	}
	return waitBeforeRetry(stopCh, delay)
}

func shouldContinueLongRunningRetry(retry, maxRetries int, requestIssued bool, requestIssuedAt, now time.Time) bool {
	if !requestIssued {
		return retry < maxRetries
	}
	if requestIssuedAt.IsZero() {
		return true
	}
	return now.Sub(requestIssuedAt) < longRunningIssuedRequestRetryTimeout
}

// findLeader 遍历所有节点找到当前 Leader
func (c *longRunningCluster) findLeader() *raft.Raft {
	for _, node := range c.nodesSnapshot() {
		if node.State() == raft.Leader {
			return node
		}
	}
	return nil
}

// getLeaderByID 根据 LeaderHint ID 获取 Leader 节点
func (c *longRunningCluster) getLeaderByID(leaderID int) *raft.Raft {
	if leaderID <= 0 || leaderID > c.nodeCount() {
		return nil
	}
	return c.nodeAt(leaderID - 1)
}

// sendRequestWithLeaderTracking 向当前 Leader 发送请求，自动跟踪 Leader 变化
// 当收到 NotLeader 响应时，更新 currentLeader 并重试
func (c *longRunningCluster) sendRequestWithLeaderTracking(currentLeader *atomic.Value, cmd param.KVCommand, maxRetries int, stopCh <-chan struct{}) (bool, time.Duration, error) {
	success, latency, _, err := c.sendRequestWithLeaderTrackingDetailed(currentLeader, cmd, maxRetries, stopCh)
	return success, latency, err
}

func (c *longRunningCluster) sendRequestWithLeaderTrackingDetailed(currentLeader *atomic.Value, cmd param.KVCommand, maxRetries int, stopCh <-chan struct{}) (bool, time.Duration, string, error) {
	return c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, rand.Int63(), rand.Int63(), cmd, maxRetries, stopCh)
}

func (c *longRunningCluster) sendRequestWithClientLeaderTrackingDetailed(currentLeader *atomic.Value, clientID, sequenceNum int64, cmd param.KVCommand, maxRetries int, stopCh <-chan struct{}) (bool, time.Duration, string, error) {
	var totalLatency time.Duration
	lastFailureReason := "max_retries_exceeded"
	requestIssued := false
	requestIssuedAt := time.Time{}
	cmdBytes, _ := json.Marshal(cmd)
	args := &param.ClientArgs{
		ClientID:    clientID,
		SequenceNum: sequenceNum,
		Command:     cmdBytes,
	}

	for retry := 0; shouldContinueLongRunningRetry(retry, maxRetries, requestIssued, requestIssuedAt, time.Now()); retry++ {
		// Stop only gates new operations. Once the command has been sent, keep
		// retrying the same client request so a late Raft commit is still
		// reflected in the long-running consistency tracker.
		if shouldStopBeforeRequest(stopCh, requestIssued) {
			return false, totalLatency, "stopped", errLongRunningTestStopped
		}

		// 获取当前 Leader
		leader := currentLeader.Load().(*raft.Raft)
		if leader == nil || leader.IsStopped() {
			// 尝试重新查找 Leader
			newLeader := c.findLeader()
			if newLeader == nil {
				lastFailureReason = "no_leader"
				time.Sleep(100 * time.Millisecond)
				continue
			}
			currentLeader.Store(newLeader)
			leader = newLeader
		}

		reply := &param.ClientReply{}

		start := time.Now()
		if !requestIssued {
			requestIssuedAt = start
		}
		requestIssued = true
		err := leader.ClientRequest(args, reply)
		latency := time.Since(start)
		totalLatency += latency

		if err == nil && reply.Success {
			return true, totalLatency, "", nil
		}

		lastFailureReason = classifyClientFailure(err, reply)

		// 如果收到 NotLeader 响应，使用 LeaderHint 更新 Leader
		if reply.NotLeader {
			updatedLeader := false
			if reply.LeaderHint > 0 && reply.LeaderHint <= c.nodeCount() {
				newLeader := c.nodeAt(reply.LeaderHint - 1)
				if !newLeader.IsStopped() && newLeader.State() == raft.Leader {
					currentLeader.Store(newLeader)
					updatedLeader = true
				} else {
					// LeaderHint 无效或节点不可用，尝试重新查找
					newLeader = c.findLeader()
					if newLeader != nil {
						currentLeader.Store(newLeader)
						updatedLeader = true
					}
				}
			} else {
				// LeaderHint 无效，重新查找 Leader
				newLeader := c.findLeader()
				if newLeader != nil {
					currentLeader.Store(newLeader)
					updatedLeader = true
				}
			}
			if !updatedLeader && !waitBeforeRetryAfterIssued(stopCh, retryBackoff(retry), requestIssued) {
				return false, totalLatency, "stopped", errLongRunningTestStopped
			}
			continue
		}

		// 其他错误或 leader 端暂时未完成 apply，按真实客户端语义继续重试。
		newLeader := c.findLeader()
		if newLeader != nil {
			currentLeader.Store(newLeader)
		}
		time.Sleep(100 * time.Millisecond)
	}

	return false, totalLatency, lastFailureReason, fmt.Errorf("max retries exceeded: %s", lastFailureReason)
}

func classifyClientFailure(err error, reply *param.ClientReply) string {
	if err != nil {
		return "client_request_error"
	}
	if reply == nil {
		return "unknown_reply"
	}

	result, _ := reply.Result.(string)
	switch result {
	case "read quorum timeout":
		return "read_quorum_timeout"
	case "read timeout":
		return "read_apply_timeout"
	case "apply timeout":
		return "apply_timeout"
	}

	if reply.NotLeader {
		return "not_leader"
	}
	if result == "" {
		return "unsuccessful_reply"
	}
	return "reply_" + strings.ReplaceAll(result, " ", "_")
}

// getCurrentLeader 获取当前 Leader
func (c *longRunningCluster) getCurrentLeader() *raft.Raft {
	for _, node := range c.nodesSnapshot() {
		if node.State() == raft.Leader {
			return node
		}
	}
	return nil
}

type observedValue struct {
	value  string
	exists bool
}

type consistencyTracker struct {
	mu     sync.RWMutex
	values map[string]observedValue
	keys   []string
}

func newConsistencyTracker() *consistencyTracker {
	return &consistencyTracker{
		values: make(map[string]observedValue),
		keys:   make([]string, 0),
	}
}

func (t *consistencyTracker) recordSet(key, value string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.values[key]; !ok {
		t.keys = append(t.keys, key)
	}
	t.values[key] = observedValue{value: value, exists: true}
}

func (t *consistencyTracker) recordDelete(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.values[key]; !ok {
		t.keys = append(t.keys, key)
	}
	t.values[key] = observedValue{}
}

func (t *consistencyTracker) snapshot(limit int) map[string]observedValue {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if limit <= 0 || limit > len(t.keys) {
		limit = len(t.keys)
	}
	result := make(map[string]observedValue, limit)
	if limit == 0 {
		return result
	}

	start := len(t.keys) - limit
	for _, key := range t.keys[start:] {
		result[key] = t.values[key]
	}
	return result
}

func readObserved(sm storage.StateMachine, key string) observedValue {
	if sm == nil {
		return observedValue{}
	}
	val, err := sm.Get(key)
	if err != nil {
		return observedValue{}
	}
	return observedValue{value: val, exists: true}
}

func (c *longRunningCluster) verifyExpectedConsistency(t *testing.T, expected map[string]observedValue) (bool, int64) {
	return c.verifyExpectedConsistencyWithLog(t, expected, true)
}

func (c *longRunningCluster) verifyExpectedConsistencyWithLog(t *testing.T, expected map[string]observedValue, logMismatch bool) (bool, int64) {
	if len(expected) == 0 {
		return true, 0
	}

	mismatchCount := int64(0)
	verifiedCount := int64(0)
	for _, node := range c.nodesSnapshot() {
		if node == nil || node.IsStopped() {
			continue
		}
		sm := c.stateMachineByID(node.ID())
		for key, want := range expected {
			got := readObserved(sm, key)
			verifiedCount++
			if got.exists != want.exists || got.value != want.value {
				if logMismatch && mismatchCount < 20 {
					t.Logf("Expected mismatch: Node %d - Key '%s': expected=(exists=%t,value=%q), got=(exists=%t,value=%q)",
						node.ID(), key, want.exists, want.value, got.exists, got.value)
				}
				mismatchCount++
			}
		}
	}
	return mismatchCount == 0, verifiedCount
}

func (c *longRunningCluster) waitForExpectedConsistency(t *testing.T, expected map[string]observedValue, timeout time.Duration) (bool, int64) {
	deadline := time.Now().Add(timeout)
	var verified int64
	for time.Now().Before(deadline) {
		consistent, count := c.verifyExpectedConsistencyWithLog(t, expected, false)
		verified = count
		if consistent {
			return true, verified
		}
		time.Sleep(500 * time.Millisecond)
	}
	consistent, count := c.verifyExpectedConsistency(t, expected)
	return consistent, count
}

func (c *longRunningCluster) waitForClusterBarrier(t *testing.T, currentLeader *atomic.Value, label string, timeout time.Duration) bool {
	t.Helper()

	now := time.Now().UnixNano()
	key := fmt.Sprintf("__go_kv_long_barrier_%s_%d", label, now)
	value := fmt.Sprintf("barrier-%s-%d", label, now)
	cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

	success, _, failureReason, err := c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, now, 1, cmd, longRunningClientRetries*2, nil)
	if err != nil || !success {
		t.Logf("[最终屏障同步] 写入失败: label=%s reason=%s err=%v", label, failureReason, err)
		return false
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allApplied := true
		for _, node := range c.nodesSnapshot() {
			if node == nil || node.IsStopped() {
				continue
			}
			got := readObserved(c.stateMachineByID(node.ID()), key)
			if got.exists != true || got.value != value {
				allApplied = false
				break
			}
		}
		if allApplied {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}

	for _, node := range c.nodesSnapshot() {
		if node == nil || node.IsStopped() {
			continue
		}
		got := readObserved(c.stateMachineByID(node.ID()), key)
		if got.exists != true || got.value != value {
			t.Logf("[最终屏障同步] 节点 %d 未应用 barrier: expected=(exists=true,value=%q), got=(exists=%t,value=%q)",
				node.ID(), value, got.exists, got.value)
		}
	}
	return false
}

// verifyDataConsistency 验证所有存活节点的数据一致性，包括缺失/删除状态。
func (c *longRunningCluster) verifyDataConsistency(t *testing.T, sampleKeys []string) (bool, int64) {
	return c.verifyDataConsistencyWithLog(t, sampleKeys, true)
}

func (c *longRunningCluster) verifyDataConsistencyWithLog(t *testing.T, sampleKeys []string, logMismatch bool) (bool, int64) {
	if len(sampleKeys) == 0 {
		return true, 0
	}

	leader := c.findLeader()
	if leader == nil {
		t.Logf("Warning: No leader found during consistency check")
		return true, 0
	}

	leaderSM := c.stateMachineByID(leader.ID())
	leaderData := make(map[string]observedValue, len(sampleKeys))
	for _, key := range sampleKeys {
		leaderData[key] = readObserved(leaderSM, key)
	}

	mismatchCount := int64(0)
	verifiedCount := int64(0)

	for _, node := range c.nodesSnapshot() {
		if node == nil || node.IsStopped() || node.State() == raft.Leader {
			continue
		}
		sm := c.stateMachineByID(node.ID())

		for key, leaderVal := range leaderData {
			val := readObserved(sm, key)
			verifiedCount++

			if val.exists != leaderVal.exists || val.value != leaderVal.value {
				if logMismatch && mismatchCount < 40 {
					t.Logf("Data mismatch: Node %d - Key '%s': Leader=(exists=%t,value=%q), Node=(exists=%t,value=%q)",
						node.ID(), key, leaderVal.exists, leaderVal.value, val.exists, val.value)
				}
				mismatchCount++
			}
		}
	}

	return mismatchCount == 0, verifiedCount
}

func (c *longRunningCluster) waitForDataConsistency(t *testing.T, sampleKeys []string, timeout time.Duration) (bool, int64) {
	deadline := time.Now().Add(timeout)
	var verified int64
	for time.Now().Before(deadline) {
		consistent, count := c.verifyDataConsistencyWithLog(t, sampleKeys, false)
		verified = count
		if consistent {
			return true, verified
		}
		time.Sleep(500 * time.Millisecond)
	}

	consistent, count := c.verifyDataConsistency(t, sampleKeys)
	return consistent, count
}

// ========== 生产环境 10分钟长时性能测试 ==========

// testRunner 用于管理长时测试的超时和进度报告
type testRunner struct {
	duration       time.Duration
	stopCh         chan struct{}
	wg             *sync.WaitGroup
	startTime      time.Time
	timeoutTimer   *time.Timer
	progressTicker *time.Ticker
}

// newTestRunner 创建一个新的测试运行器
func newTestRunner(duration time.Duration, stopCh chan struct{}, wg *sync.WaitGroup) *testRunner {
	return &testRunner{
		duration:       duration,
		stopCh:         stopCh,
		wg:             wg,
		startTime:      time.Now(),
		timeoutTimer:   time.NewTimer(duration),
		progressTicker: time.NewTicker(30 * time.Second),
	}
}

// stop 停止测试运行器
func (r *testRunner) stop() {
	r.timeoutTimer.Stop()
	r.progressTicker.Stop()
}

// run 运行测试主循环，返回是否正常完成
// onProgress 是进度报告回调，每次 ticker 触发时调用
func (r *testRunner) run(t *testing.T, onProgress func(elapsed time.Duration)) {
	defer r.stop()

	for {
		select {
		case <-r.timeoutTimer.C:
			// 超时，停止测试
			t.Logf("[超时] 测试运行 %v 完成，正在停止客户端...", r.duration)
			close(r.stopCh)
			r.wg.Wait()
			return

		case <-r.progressTicker.C:
			elapsed := time.Since(r.startTime)
			onProgress(elapsed)
		}
	}
}

// runWithFailureInjection 运行带故障注入的测试主循环
func (r *testRunner) runWithFailureInjection(t *testing.T, onProgress func(elapsed time.Duration), onFailure func()) {
	defer r.stop()

	failureTicker := time.NewTicker(2 * time.Minute)
	defer failureTicker.Stop()

	for {
		select {
		case <-r.timeoutTimer.C:
			// 超时，停止测试
			t.Logf("[超时] 测试运行 %v 完成，正在停止客户端...", r.duration)
			close(r.stopCh)
			r.wg.Wait()
			return

		case <-r.progressTicker.C:
			elapsed := time.Since(r.startTime)
			onProgress(elapsed)

		case <-failureTicker.C:
			onFailure()
		}
	}
}

func skipLongRunningE2EInShortMode(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping long-running E2E test in short mode; run explicit TestLongRunning_10Min_* tests without -short")
	}
}

// TestLongRunning_10Min_Comprehensive 10分钟综合性能测试
// 模拟生产环境：使用 gRPC + LSM，三节点集群，混合读写删除操作
// 客户端可以向任意节点发送请求，自动处理 NotLeader 重定向
func TestLongRunning_10Min_Comprehensive(t *testing.T) {
	skipLongRunningE2EInShortMode(t)
	duration := 10 * time.Minute

	c := newLongRunningCluster(t, 3)
	defer c.shutdown()

	t.Logf("=== 10分钟长时端到端性能测试开始 ===")
	t.Logf("集群配置: 3节点, gRPC传输, LSM存储")
	t.Logf("测试持续时间: %v", duration)

	// 等待集群就绪
	c.waitForAllNodesReady(t)
	leader := c.getLeader(t)
	t.Logf("集群就绪，Leader: Node %d, 开始预热...", leader.ID())

	// 使用 atomic.Value 存储 Leader 引用，支持动态更新
	currentLeader := &atomic.Value{}
	currentLeader.Store(leader)

	// 启动 Leader 监控
	monitorCtx := make(chan struct{})
	go c.monitorLeaderChanges(monitorCtx)
	defer close(monitorCtx)

	// 预热数据
	warmupCount := 1000
	t.Logf("预热阶段: 写入 %d 条数据...", warmupCount)
	warmupSuccess := 0
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("warmup-key-%d", i)
		value := fmt.Sprintf("warmup-value-%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		success, _, _, _ := c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, 1000, int64(i+1), cmd, longRunningClientRetries, nil)
		if success {
			warmupSuccess++
		}
	}
	t.Logf("预热完成: %d/%d 成功", warmupSuccess, warmupCount)

	// 等待数据同步
	time.Sleep(3 * time.Second)

	// 性能指标 - 使用 latencySampler 控制内存使用
	const maxLatencySamples = 10000
	var (
		totalOps             int64
		successOps           int64
		failedOps            int64
		writeOps             int64
		readOps              int64
		deleteOps            int64
		bytesRead            int64
		bytesWritten         int64
		latencySampler       = newLatencySampler(maxLatencySamples)
		writeLatencySampler  = newLatencySampler(maxLatencySamples)
		readLatencySampler   = newLatencySampler(maxLatencySamples)
		deleteLatencySampler = newLatencySampler(maxLatencySamples)
		failures             = newFailureStats()
		keysForVerification  []string
		sampleKeysMutex      sync.Mutex
	)

	// 并发客户端模拟
	numClients := 10
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numClients)

	t.Logf("启动 %d 个并发客户端...", numClients)

	for clientID := 0; clientID < numClients; clientID++ {
		go func(cid int) {
			defer wg.Done()

			clientPrefix := fmt.Sprintf("client-%d", cid)
			requestClientID := int64(10000 + cid)
			requestSeq := int64(0)
			localKeys := make([]string, 0)

			for {
				select {
				case <-stopCh:
					return
				default:
					// 模拟生产环境的混合操作类型分布
					r := rand.Float64()
					var success bool
					var latency time.Duration
					var err error
					var failureReason string
					attempted := false

					if r < 0.6 { // 60% 写入操作
						key := fmt.Sprintf("%s-key-%d-%d", clientPrefix, cid, rand.Intn(50000))
						value := fmt.Sprintf("%s-val-%d", clientPrefix, rand.Intn(1000000))
						cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

						attempted = true
						requestSeq++
						success, latency, failureReason, err = c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, requestClientID, requestSeq, cmd, longRunningClientRetries, stopCh)
						if errors.Is(err, errLongRunningTestStopped) {
							return
						}

						atomic.AddInt64(&writeOps, 1)
						writeLatencySampler.add(latency)

						if success {
							localKeys = append(localKeys, key)
							atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
						}

					} else if r < 0.85 { // 25% 读取操作
						var key string
						if len(localKeys) > 0 {
							key = localKeys[rand.Intn(len(localKeys))]
						} else {
							key = fmt.Sprintf("%s-key-%d-%d", clientPrefix, cid, rand.Intn(10000))
						}

						cmd := param.KVCommand{Op: param.OpGet, Key: key}
						attempted = true
						requestSeq++
						success, latency, failureReason, err = c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, requestClientID, requestSeq, cmd, longRunningClientRetries, stopCh)
						if errors.Is(err, errLongRunningTestStopped) {
							return
						}

						atomic.AddInt64(&readOps, 1)
						readLatencySampler.add(latency)

					} else { // 15% 删除操作
						if len(localKeys) == 0 {
							continue
						}
						idx := rand.Intn(len(localKeys))
						key := localKeys[idx]
						cmd := param.KVCommand{Op: param.OpDelete, Key: key}

						attempted = true
						requestSeq++
						success, latency, failureReason, err = c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, requestClientID, requestSeq, cmd, longRunningClientRetries, stopCh)
						if errors.Is(err, errLongRunningTestStopped) {
							return
						}

						atomic.AddInt64(&deleteOps, 1)
						deleteLatencySampler.add(latency)

						if success {
							localKeys = append(localKeys[:idx], localKeys[idx+1:]...)
						}
					}

					if !attempted {
						continue
					}
					atomic.AddInt64(&totalOps, 1)
					if success {
						atomic.AddInt64(&successOps, 1)
						latencySampler.add(latency)
					} else {
						atomic.AddInt64(&failedOps, 1)
						failures.record(failureReason)
					}

					// 周期性收集样本键用于一致性验证
					if rand.Intn(100) == 0 && len(localKeys) > 0 {
						sampleKeysMutex.Lock()
						keysForVerification = append(keysForVerification, localKeys[rand.Intn(len(localKeys))])
						if len(keysForVerification) > 1000 {
							keysForVerification = keysForVerification[1:]
						}
						sampleKeysMutex.Unlock()
					}
				}
			}
		}(clientID)
	}

	// 使用 testRunner 管理超时和进度报告
	runner := newTestRunner(duration, stopCh, &wg)
	runner.run(t, func(elapsed time.Duration) {
		counters := snapshotLongRunningCounters(&totalOps, &successOps, &failedOps)

		t.Logf("[进度报告] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, 吞吐量: %.2f ops/sec, 延迟样本: %d",
			elapsed, counters.TotalOps, counters.SuccessOps, counters.FailedOps, float64(counters.SuccessOps)/elapsed.Seconds(), latencySampler.count())
	})

	sampleKeysMutex.Lock()
	verificationKeys := append([]string(nil), keysForVerification...)
	sampleKeysMutex.Unlock()

	// 最终一致性检查：停止客户端后给 follower 留出追赶时间；超时仍不一致才判定失败。
	finalConsistent, finalVerified := c.waitForDataConsistency(t, verificationKeys, 45*time.Second)
	t.Logf("[最终一致性检查] 已验证: %d 条数据, 结果: %v", finalVerified, finalConsistent)
	snapshotNodes, maxSnapshotIndex := c.snapshotStats()

	// 输出测试结果
	metrics := LongRunningMetrics{
		TestName:          "10分钟综合长时测试 (gRPC+LSM)",
		Duration:          duration,
		TotalOps:          totalOps,
		SuccessOps:        successOps,
		FailedOps:         failedOps,
		WriteOps:          writeOps,
		ReadOps:           readOps,
		DeleteOps:         deleteOps,
		BytesRead:         bytesRead,
		BytesWritten:      bytesWritten,
		LatencyP50:        percentileLong(latencySampler.getAll(), 50),
		LatencyP95:        percentileLong(latencySampler.getAll(), 95),
		LatencyP99:        percentileLong(latencySampler.getAll(), 99),
		ThroughputOps:     float64(successOps) / duration.Seconds(),
		WriteThroughput:   float64(writeOps) / duration.Seconds(),
		ReadThroughput:    float64(readOps) / duration.Seconds(),
		DeleteThroughput:  float64(deleteOps) / duration.Seconds(),
		ErrorRate:         float64(failedOps) / float64(totalOps) * 100,
		LeaderElections:   atomic.LoadInt32(&c.leaderElections),
		DataConsistencyOK: finalConsistent,
		KeysVerified:      finalVerified,
		SnapshotCount:     snapshotNodes,
		SnapshotMaxIndex:  maxSnapshotIndex,
		FailureReasons:    failures.snapshot(),
	}

	printLongRunningMetrics(t, &metrics)
	requireLongRunningMetrics(t, metrics, longRunningValidationOptions{
		RequireNoFailedOps:  true,
		RequireConsistency:  true,
		RequireVerifiedKeys: true,
	})
}

// TestLongRunning_10Min_WriteHeavy 10分钟写入密集型测试
// 客户端可以向任意节点发送请求，自动处理 NotLeader 重定向
func TestLongRunning_10Min_WriteHeavy(t *testing.T) {
	skipLongRunningE2EInShortMode(t)
	duration := 10 * time.Minute

	c := newLongRunningCluster(t, 3)
	defer c.shutdown()

	t.Logf("=== 10分钟写入密集型测试 ===")
	t.Logf("集群配置: 3节点, gRPC传输, LSM存储")

	c.waitForAllNodesReady(t)
	leader := c.getLeader(t)
	t.Logf("集群就绪，Leader: Node %d", leader.ID())

	// 使用 atomic.Value 存储 Leader 引用，支持动态更新
	currentLeader := &atomic.Value{}
	currentLeader.Store(leader)

	monitorCtx := make(chan struct{})
	go c.monitorLeaderChanges(monitorCtx)
	defer close(monitorCtx)

	// 性能指标 - 使用 latencySampler 控制内存使用
	const maxLatencySamples = 10000
	var (
		totalOps            int64
		successOps          int64
		failedOps           int64
		bytesWritten        int64
		latencySampler      = newLatencySampler(maxLatencySamples)
		failures            = newFailureStats()
		keysForVerification []string
		sampleKeysMutex     sync.Mutex
	)

	numClients := 8
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numClients)

	t.Logf("启动 %d 个并发写入客户端...", numClients)

	for clientID := 0; clientID < numClients; clientID++ {
		go func(cid int) {
			defer wg.Done()
			requestClientID := int64(20000 + cid)
			requestSeq := int64(0)
			opCount := int64(0)

			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("write-heavy-key-%d-%d", cid, opCount)
					value := fmt.Sprintf("value-%d-%d", cid, rand.Intn(10000000))
					cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

					requestSeq++
					success, latency, failureReason, err := c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, requestClientID, requestSeq, cmd, longRunningClientRetries, stopCh)
					if errors.Is(err, errLongRunningTestStopped) {
						return
					}

					atomic.AddInt64(&totalOps, 1)
					if success {
						atomic.AddInt64(&successOps, 1)
						atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
						latencySampler.add(latency)

						if opCount%100 == 0 {
							sampleKeysMutex.Lock()
							keysForVerification = append(keysForVerification, key)
							if len(keysForVerification) > 1000 {
								keysForVerification = keysForVerification[1:]
							}
							sampleKeysMutex.Unlock()
						}
					} else {
						atomic.AddInt64(&failedOps, 1)
						failures.record(failureReason)
					}
					opCount++
				}
			}
		}(clientID)
	}

	// 使用 testRunner 管理超时和进度报告
	runner := newTestRunner(duration, stopCh, &wg)
	runner.run(t, func(elapsed time.Duration) {
		counters := snapshotLongRunningCounters(&totalOps, &successOps, &failedOps)
		t.Logf("[进度] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, 写入流量: %.2f MB/s, 延迟样本: %d",
			elapsed,
			counters.TotalOps,
			counters.SuccessOps,
			counters.FailedOps,
			float64(atomic.LoadInt64(&bytesWritten))/1024/1024/elapsed.Seconds(),
			latencySampler.count())
	})

	sampleKeysMutex.Lock()
	verificationKeys := append([]string(nil), keysForVerification...)
	sampleKeysMutex.Unlock()

	finalConsistent, finalVerified := c.waitForDataConsistency(t, verificationKeys, 45*time.Second)
	t.Logf("[最终一致性检查] 已验证: %d 条数据, 结果: %v", finalVerified, finalConsistent)
	snapshotNodes, maxSnapshotIndex := c.snapshotStats()

	metrics := LongRunningMetrics{
		TestName:          "10分钟写入密集型测试 (gRPC+LSM)",
		Duration:          duration,
		TotalOps:          totalOps,
		SuccessOps:        successOps,
		FailedOps:         failedOps,
		WriteOps:          totalOps,
		BytesWritten:      bytesWritten,
		LatencyP50:        percentileLong(latencySampler.getAll(), 50),
		LatencyP95:        percentileLong(latencySampler.getAll(), 95),
		LatencyP99:        percentileLong(latencySampler.getAll(), 99),
		ThroughputOps:     float64(successOps) / duration.Seconds(),
		WriteThroughput:   float64(successOps) / duration.Seconds(),
		ErrorRate:         float64(failedOps) / float64(totalOps) * 100,
		LeaderElections:   atomic.LoadInt32(&c.leaderElections),
		DataConsistencyOK: finalConsistent,
		KeysVerified:      finalVerified,
		SnapshotCount:     snapshotNodes,
		SnapshotMaxIndex:  maxSnapshotIndex,
		FailureReasons:    failures.snapshot(),
	}

	printLongRunningMetrics(t, &metrics)
	if failed := atomic.LoadInt64(&failedOps); failed > 0 {
		t.Fatalf("write-heavy workload completed with %d failed writes", failed)
	}
	if finalVerified == 0 {
		t.Fatalf("write-heavy consistency check verified zero keys")
	}
	if !finalConsistent {
		t.Fatalf("write-heavy consistency check failed after waiting for followers to catch up")
	}
}

// TestLongRunning_10Min_MixedWithFailures 10分钟带故障恢复的混合测试
func TestLongRunning_10Min_MixedWithFailures(t *testing.T) {
	skipLongRunningE2EInShortMode(t)
	duration := 10 * time.Minute

	c := newLongRunningCluster(t, 3)
	defer c.shutdown()

	t.Logf("=== 10分钟带故障恢复的混合测试 ===")
	t.Logf("集群配置: 3节点, gRPC传输, LSM存储")

	c.waitForAllNodesReady(t)
	leader := c.getLeader(t)
	t.Logf("集群就绪，Leader: Node %d", leader.ID())

	// 使用 atomic.Value 存储 Leader 引用，支持动态更新
	currentLeader := &atomic.Value{}
	currentLeader.Store(leader)

	monitorCtx := make(chan struct{})
	go c.monitorLeaderChanges(monitorCtx)
	defer close(monitorCtx)

	// 性能指标 - 使用 latencySampler 控制内存使用
	const maxLatencySamples = 10000
	var (
		totalOps       int64
		successOps     int64
		failedOps      int64
		writeOps       int64
		readOps        int64
		bytesRead      int64
		bytesWritten   int64
		latencySampler = newLatencySampler(maxLatencySamples)
		failures       = newFailureStats()
	)
	tracker := newConsistencyTracker()

	numClients := 5
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numClients)

	t.Logf("启动 %d 个并发客户端...", numClients)

	// 客户端执行混合读写操作
	for clientID := 0; clientID < numClients; clientID++ {
		go func(cid int) {
			defer wg.Done()
			requestClientID := int64(30000 + cid)
			requestSeq := int64(0)
			localKeys := make([]string, 0)

			for {
				select {
				case <-stopCh:
					return
				default:
					r := rand.Float64()

					if r < 0.7 { // 70% 写入
						key := fmt.Sprintf("fail-test-key-%d-%d", cid, len(localKeys))
						value := fmt.Sprintf("val-%d", rand.Intn(100000))
						cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

						requestSeq++
						success, latency, failureReason, err := c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, requestClientID, requestSeq, cmd, longRunningClientRetries, stopCh)
						if errors.Is(err, errLongRunningTestStopped) {
							return
						}
						atomic.AddInt64(&totalOps, 1)
						atomic.AddInt64(&writeOps, 1)

						if success {
							atomic.AddInt64(&successOps, 1)
							atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
							localKeys = append(localKeys, key)
							tracker.recordSet(key, value)
							latencySampler.add(latency)
						} else {
							atomic.AddInt64(&failedOps, 1)
							failures.record(failureReason)
						}

					} else { // 30% 读取
						var key string
						if len(localKeys) > 0 {
							key = localKeys[rand.Intn(len(localKeys))]
						} else {
							key = fmt.Sprintf("fail-test-read-%d", rand.Intn(10000))
						}

						cmd := param.KVCommand{Op: param.OpGet, Key: key}
						requestSeq++
						success, latency, failureReason, err := c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, requestClientID, requestSeq, cmd, longRunningClientRetries, stopCh)
						if errors.Is(err, errLongRunningTestStopped) {
							return
						}
						atomic.AddInt64(&totalOps, 1)
						atomic.AddInt64(&readOps, 1)

						if success {
							atomic.AddInt64(&successOps, 1)
							l := currentLeader.Load().(*raft.Raft)
							val, _ := c.stateMachineByID(l.ID()).Get(key)
							atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
							latencySampler.add(latency)
						} else {
							atomic.AddInt64(&failedOps, 1)
							failures.record(failureReason)
						}
					}
				}
			}
		}(clientID)
	}

	// 使用 testRunner 管理超时、进度报告和故障注入
	runner := newTestRunner(duration, stopCh, &wg)
	failureCount := 0
	runner.runWithFailureInjection(t,
		func(elapsed time.Duration) {
			counters := snapshotLongRunningCounters(&totalOps, &successOps, &failedOps)
			t.Logf("[进度] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, Leader切换: %d, 延迟样本: %d",
				elapsed,
				counters.TotalOps,
				counters.SuccessOps,
				counters.FailedOps,
				atomic.LoadInt32(&c.leaderElections),
				latencySampler.count())
		},
		func() {
			if failureCount < 2 { // 最多触发2次故障
				victimIndex := -1
				for _, node := range c.nodesSnapshot() {
					if node != nil && !node.IsStopped() && node.State() != raft.Leader {
						victimIndex = node.ID() - 1
						break
					}
				}

				if victimIndex >= 0 {
					t.Logf("[故障模拟] 重启节点 %d", victimIndex+1)
					c.restartNode(t, victimIndex)
					failureCount++
				}
			}
		})

	expected := tracker.snapshot(1200)
	barrierOK := c.waitForClusterBarrier(t, currentLeader, "mixed_failures", 45*time.Second)
	t.Logf("[最终屏障同步] 结果: %v", barrierOK)
	finalConsistent, finalVerified := c.waitForExpectedConsistency(t, expected, 45*time.Second)
	finalConsistent = barrierOK && finalConsistent
	t.Logf("[最终严格一致性检查] 已验证: %d 条节点键组合, 结果: %v", finalVerified, finalConsistent)
	snapshotNodes, maxSnapshotIndex := c.snapshotStats()

	metrics := LongRunningMetrics{
		TestName:          "10分钟带故障恢复的混合测试 (gRPC+LSM)",
		Duration:          duration,
		TotalOps:          totalOps,
		SuccessOps:        successOps,
		FailedOps:         failedOps,
		WriteOps:          writeOps,
		ReadOps:           readOps,
		BytesRead:         bytesRead,
		BytesWritten:      bytesWritten,
		LatencyP50:        percentileLong(latencySampler.getAll(), 50),
		LatencyP95:        percentileLong(latencySampler.getAll(), 95),
		LatencyP99:        percentileLong(latencySampler.getAll(), 99),
		ThroughputOps:     float64(successOps) / duration.Seconds(),
		WriteThroughput:   float64(writeOps) / duration.Seconds(),
		ReadThroughput:    float64(readOps) / duration.Seconds(),
		ErrorRate:         float64(failedOps) / float64(totalOps) * 100,
		LeaderElections:   atomic.LoadInt32(&c.leaderElections),
		DataConsistencyOK: finalConsistent,
		KeysVerified:      finalVerified,
		SnapshotCount:     snapshotNodes,
		SnapshotMaxIndex:  maxSnapshotIndex,
		FailureReasons:    failures.snapshot(),
	}

	printLongRunningMetrics(t, &metrics)
	requireLongRunningMetrics(t, metrics, longRunningValidationOptions{
		RequireNoFailedOps:  true,
		RequireConsistency:  true,
		RequireVerifiedKeys: true,
	})
}

// TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots 覆盖生产中的节点重启、快照和严格一致性场景。
func TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots(t *testing.T) {
	skipLongRunningE2EInShortMode(t)
	duration := 10 * time.Minute

	c := newLongRunningCluster(t, 3)
	defer c.shutdown()

	t.Logf("=== 10分钟一致性/重启/快照端到端测试 ===")
	t.Logf("集群配置: 3节点, gRPC传输, LSM存储")

	c.waitForAllNodesReady(t)
	leader := c.getLeader(t)
	t.Logf("集群就绪，Leader: Node %d", leader.ID())

	currentLeader := &atomic.Value{}
	currentLeader.Store(leader)

	monitorCtx := make(chan struct{})
	go c.monitorLeaderChanges(monitorCtx)
	defer close(monitorCtx)

	tracker := newConsistencyTracker()

	warmupCount := 300
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("consistency-warmup-%d", i)
		value := fmt.Sprintf("warmup-value-%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		success, _, _, _ := c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, 4000, int64(i+1), cmd, longRunningClientRetries, nil)
		if success {
			tracker.recordSet(key, value)
		}
	}
	time.Sleep(3 * time.Second)

	const maxLatencySamples = 10000
	var (
		totalOps       int64
		successOps     int64
		failedOps      int64
		writeOps       int64
		readOps        int64
		deleteOps      int64
		bytesRead      int64
		bytesWritten   int64
		snapshotCount  int32
		restartCount   int32
		latencySampler = newLatencySampler(maxLatencySamples)
		failures       = newFailureStats()
	)

	numClients := 6
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numClients)

	for clientID := 0; clientID < numClients; clientID++ {
		go func(cid int) {
			defer wg.Done()
			requestClientID := int64(40000 + cid)
			requestSeq := int64(0)
			keySpace := 800

			for {
				select {
				case <-stopCh:
					return
				default:
				}

				key := fmt.Sprintf("consistency-client-%d-key-%d", cid, rand.Intn(keySpace))
				r := rand.Float64()
				var (
					success       bool
					latency       time.Duration
					failureReason string
					err           error
				)

				switch {
				case r < 0.55:
					value := fmt.Sprintf("value-%d-%d", cid, rand.Int63())
					cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
					requestSeq++
					success, latency, failureReason, err = c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, requestClientID, requestSeq, cmd, longRunningClientRetries, stopCh)
					if errors.Is(err, errLongRunningTestStopped) {
						return
					}
					atomic.AddInt64(&writeOps, 1)
					if success {
						tracker.recordSet(key, value)
						atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
					}

				case r < 0.85:
					cmd := param.KVCommand{Op: param.OpGet, Key: key}
					requestSeq++
					success, latency, failureReason, err = c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, requestClientID, requestSeq, cmd, longRunningClientRetries, stopCh)
					if errors.Is(err, errLongRunningTestStopped) {
						return
					}
					atomic.AddInt64(&readOps, 1)
					if success {
						atomic.AddInt64(&bytesRead, int64(len(key)))
					}

				default:
					cmd := param.KVCommand{Op: param.OpDelete, Key: key}
					requestSeq++
					success, latency, failureReason, err = c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, requestClientID, requestSeq, cmd, longRunningClientRetries, stopCh)
					if errors.Is(err, errLongRunningTestStopped) {
						return
					}
					atomic.AddInt64(&deleteOps, 1)
					if success {
						tracker.recordDelete(key)
					}
				}

				atomic.AddInt64(&totalOps, 1)
				if success {
					atomic.AddInt64(&successOps, 1)
					latencySampler.add(latency)
				} else {
					atomic.AddInt64(&failedOps, 1)
					failures.record(failureReason)
				}
			}
		}(clientID)
	}

	runner := newTestRunner(duration, stopCh, &wg)
	runner.runWithFailureInjection(t,
		func(elapsed time.Duration) {
			counters := snapshotLongRunningCounters(&totalOps, &successOps, &failedOps)
			snapshotNodes, maxSnapshotIndex := c.snapshotStats()
			t.Logf("[进度] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, 重启: %d, 手动快照: %d, 快照节点: %d, 最大快照索引: %d, Leader切换: %d, 延迟样本: %d",
				elapsed,
				counters.TotalOps,
				counters.SuccessOps,
				counters.FailedOps,
				atomic.LoadInt32(&restartCount),
				atomic.LoadInt32(&snapshotCount),
				snapshotNodes,
				maxSnapshotIndex,
				atomic.LoadInt32(&c.leaderElections),
				latencySampler.count())
		},
		func() {
			if atomic.LoadInt32(&restartCount) < 3 {
				victimIndex := -1
				for _, node := range c.nodesSnapshot() {
					if node != nil && !node.IsStopped() && node.State() != raft.Leader {
						victimIndex = node.ID() - 1
						break
					}
				}
				if victimIndex >= 0 {
					t.Logf("[故障模拟] 重启 follower 节点 %d", victimIndex+1)
					c.restartNode(t, victimIndex)
					atomic.AddInt32(&restartCount, 1)
				}
			}

			leader := c.getLeader(t)
			currentLeader.Store(leader)
			if leader.TakeSnapshot() {
				atomic.AddInt32(&snapshotCount, 1)
			}
		})

	expected := tracker.snapshot(1200)
	barrierOK := c.waitForClusterBarrier(t, currentLeader, "consistency_restarts", 45*time.Second)
	t.Logf("[最终屏障同步] 结果: %v", barrierOK)
	finalConsistent, finalVerified := c.waitForExpectedConsistency(t, expected, 45*time.Second)
	finalConsistent = barrierOK && finalConsistent
	t.Logf("[最终严格一致性检查] 已验证: %d 条节点键组合, 结果: %v", finalVerified, finalConsistent)
	snapshotNodes, maxSnapshotIndex := c.snapshotStats()

	metrics := LongRunningMetrics{
		TestName:          "10分钟重启与快照严格一致性测试 (gRPC+LSM)",
		Duration:          duration,
		TotalOps:          totalOps,
		SuccessOps:        successOps,
		FailedOps:         failedOps,
		WriteOps:          writeOps,
		ReadOps:           readOps,
		DeleteOps:         deleteOps,
		BytesRead:         bytesRead,
		BytesWritten:      bytesWritten,
		LatencyP50:        percentileLong(latencySampler.getAll(), 50),
		LatencyP95:        percentileLong(latencySampler.getAll(), 95),
		LatencyP99:        percentileLong(latencySampler.getAll(), 99),
		ThroughputOps:     float64(successOps) / duration.Seconds(),
		WriteThroughput:   float64(writeOps) / duration.Seconds(),
		ReadThroughput:    float64(readOps) / duration.Seconds(),
		DeleteThroughput:  float64(deleteOps) / duration.Seconds(),
		ErrorRate:         float64(failedOps) / float64(totalOps) * 100,
		LeaderElections:   atomic.LoadInt32(&c.leaderElections),
		DataConsistencyOK: finalConsistent,
		KeysVerified:      finalVerified,
		SnapshotCount:     snapshotNodes,
		SnapshotMaxIndex:  maxSnapshotIndex,
		FailureReasons:    failures.snapshot(),
	}

	printLongRunningMetrics(t, &metrics)
	requireLongRunningMetrics(t, metrics, longRunningValidationOptions{
		RequireNoFailedOps:  true,
		RequireConsistency:  true,
		RequireVerifiedKeys: true,
	})
}

// TestLongRunning_10Min_ReadHeavy 10分钟读取密集型测试
func TestLongRunning_10Min_ReadHeavy(t *testing.T) {
	skipLongRunningE2EInShortMode(t)
	duration := 10 * time.Minute

	c := newLongRunningCluster(t, 3)
	defer c.shutdown()

	t.Logf("=== 10分钟读取密集型测试 ===")
	t.Logf("集群配置: 3节点, gRPC传输, LSM存储")

	c.waitForAllNodesReady(t)
	leader := c.getLeader(t)
	t.Logf("集群就绪，Leader: Node %d", leader.ID())

	// 使用 atomic.Value 存储 Leader，支持动态更新
	currentLeader := &atomic.Value{}
	currentLeader.Store(leader)

	// 预热大量数据
	warmupCount := 1000
	warmupKeys := make([]string, 0, warmupCount)
	t.Logf("预热阶段: 写入 %d 条数据...", warmupCount)
	warmupSuccess := 0
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("read-warmup-key-%d", i)
		value := fmt.Sprintf("read-warmup-value-%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		// 预热阶段使用 nil stopCh，因为没有启动客户端
		success, _, _, _ := c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, 5000, int64(i+1), cmd, longRunningClientRetries, nil)
		if success {
			warmupSuccess++
			warmupKeys = append(warmupKeys, key)
		}
	}
	t.Logf("预热完成: %d/%d 成功，等待同步...", warmupSuccess, warmupCount)
	if warmupSuccess != warmupCount {
		t.Fatalf("read-heavy warmup wrote %d/%d keys", warmupSuccess, warmupCount)
	}
	time.Sleep(3 * time.Second)

	// 性能指标 - 使用 latencySampler 控制内存使用
	const maxLatencySamples = 10000
	var (
		totalOps       int64
		successOps     int64
		failedOps      int64
		readOps        int64
		bytesRead      int64
		latencySampler = newLatencySampler(maxLatencySamples)
		failures       = newFailureStats()
	)

	numClients := 10
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numClients)

	t.Logf("启动 %d 个并发读取客户端...", numClients)

	for clientID := 0; clientID < numClients; clientID++ {
		go func(cid int) {
			defer wg.Done()
			requestClientID := int64(50000 + cid)
			requestSeq := int64(0)
			for {
				// 先检查是否应该停止
				select {
				case <-stopCh:
					return
				default:
				}

				key := fmt.Sprintf("read-warmup-key-%d", rand.Intn(warmupCount))
				cmd := param.KVCommand{Op: param.OpGet, Key: key}

				requestSeq++
				success, latency, failureReason, err := c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, requestClientID, requestSeq, cmd, longRunningClientRetries, stopCh)
				if errors.Is(err, errLongRunningTestStopped) {
					return
				}

				// 请求完成后再次检查是否应该停止
				select {
				case <-stopCh:
					return
				default:
				}

				atomic.AddInt64(&totalOps, 1)
				atomic.AddInt64(&readOps, 1)
				if success {
					atomic.AddInt64(&successOps, 1)
					// 使用当前 Leader 获取数据大小
					l := currentLeader.Load().(*raft.Raft)
					val, _ := c.stateMachineByID(l.ID()).Get(key)
					atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
					latencySampler.add(latency)
				} else {
					atomic.AddInt64(&failedOps, 1)
					failures.record(failureReason)
				}
			}
		}(clientID)
	}

	// 使用 testRunner 管理超时和进度报告
	runner := newTestRunner(duration, stopCh, &wg)
	runner.run(t, func(elapsed time.Duration) {
		counters := snapshotLongRunningCounters(&totalOps, &successOps, &failedOps)
		t.Logf("[进度] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, 读取流量: %.2f MB/s, 延迟样本: %d",
			elapsed,
			counters.TotalOps,
			counters.SuccessOps,
			counters.FailedOps,
			float64(atomic.LoadInt64(&bytesRead))/1024/1024/elapsed.Seconds(),
			latencySampler.count())
	})

	finalConsistent, finalVerified := c.waitForDataConsistency(t, warmupKeys, 45*time.Second)
	t.Logf("[最终一致性检查] 已验证: %d 条数据, 结果: %v", finalVerified, finalConsistent)
	snapshotNodes, maxSnapshotIndex := c.snapshotStats()

	metrics := LongRunningMetrics{
		TestName:          "10分钟读取密集型测试 (gRPC+LSM)",
		Duration:          duration,
		TotalOps:          totalOps,
		SuccessOps:        successOps,
		FailedOps:         failedOps,
		ReadOps:           readOps,
		BytesRead:         bytesRead,
		LatencyP50:        percentileLong(latencySampler.getAll(), 50),
		LatencyP95:        percentileLong(latencySampler.getAll(), 95),
		LatencyP99:        percentileLong(latencySampler.getAll(), 99),
		ThroughputOps:     float64(successOps) / duration.Seconds(),
		ReadThroughput:    float64(readOps) / duration.Seconds(),
		ErrorRate:         float64(failedOps) / float64(totalOps) * 100,
		LeaderElections:   atomic.LoadInt32(&c.leaderElections),
		DataConsistencyOK: finalConsistent,
		KeysVerified:      finalVerified,
		SnapshotCount:     snapshotNodes,
		SnapshotMaxIndex:  maxSnapshotIndex,
		FailureReasons:    failures.snapshot(),
	}

	printLongRunningMetrics(t, &metrics)
	requireLongRunningMetrics(t, metrics, longRunningValidationOptions{
		RequireNoFailedOps:         true,
		RequireConsistency:         true,
		RequireVerifiedKeys:        true,
		RequireOperationAccounting: true,
	})
}

// TestLongRunning_10Min_DeleteStress 10分钟删除压力测试
func TestLongRunning_10Min_DeleteStress(t *testing.T) {
	skipLongRunningE2EInShortMode(t)
	duration := 10 * time.Minute

	c := newLongRunningCluster(t, 3)
	defer c.shutdown()

	t.Logf("=== 10分钟删除压力测试 ===")
	t.Logf("集群配置: 3节点, gRPC传输, LSM存储")

	c.waitForAllNodesReady(t)
	leader := c.getLeader(t)
	t.Logf("集群就绪，Leader: Node %d", leader.ID())

	// 使用 atomic.Value 存储 Leader 引用，支持动态更新
	currentLeader := &atomic.Value{}
	currentLeader.Store(leader)

	monitorCtx := make(chan struct{})
	go c.monitorLeaderChanges(monitorCtx)
	defer close(monitorCtx)

	// 性能指标 - 使用 latencySampler 控制内存使用
	const maxLatencySamples = 10000
	var (
		totalOps             int64
		successOps           int64
		failedOps            int64
		writeOps             int64
		deleteOps            int64
		latencySampler       = newLatencySampler(maxLatencySamples)
		deleteLatencySampler = newLatencySampler(maxLatencySamples)
		failures             = newFailureStats()
	)
	tracker := newConsistencyTracker()

	numClients := 8
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numClients)

	// 每个客户端维护自己的键集合
	clientKeys := make([][]string, numClients)
	for i := range clientKeys {
		clientKeys[i] = make([]string, 0)
	}

	t.Logf("启动 %d 个并发客户端进行写删操作...", numClients)

	for clientID := 0; clientID < numClients; clientID++ {
		go func(cid int) {
			defer wg.Done()
			requestClientID := int64(60000 + cid)
			requestSeq := int64(0)
			opCount := int64(0)

			for {
				select {
				case <-stopCh:
					return
				default:
					// 周期性删除操作
					if opCount > 100 && len(clientKeys[cid]) > 10 && rand.Float64() < 0.4 {
						// 删除操作
						idx := rand.Intn(len(clientKeys[cid]))
						key := clientKeys[cid][idx]

						cmd := param.KVCommand{Op: param.OpDelete, Key: key}
						requestSeq++
						success, latency, failureReason, err := c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, requestClientID, requestSeq, cmd, longRunningClientRetries, stopCh)
						if errors.Is(err, errLongRunningTestStopped) {
							return
						}

						atomic.AddInt64(&totalOps, 1)
						if success {
							atomic.AddInt64(&successOps, 1)
							atomic.AddInt64(&deleteOps, 1)
							deleteLatencySampler.add(latency)
							latencySampler.add(latency)

							// 移除已删除的键
							clientKeys[cid] = append(clientKeys[cid][:idx], clientKeys[cid][idx+1:]...)
							tracker.recordDelete(key)
						} else {
							atomic.AddInt64(&failedOps, 1)
							failures.record(failureReason)
							latencySampler.add(latency)
						}
					} else {
						// 写入操作
						key := fmt.Sprintf("delete-test-key-%d-%d", cid, opCount)
						value := fmt.Sprintf("val-%d", rand.Intn(10000))
						cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

						requestSeq++
						success, latency, failureReason, err := c.sendRequestWithClientLeaderTrackingDetailed(currentLeader, requestClientID, requestSeq, cmd, longRunningClientRetries, stopCh)
						if errors.Is(err, errLongRunningTestStopped) {
							return
						}

						atomic.AddInt64(&totalOps, 1)
						if success {
							atomic.AddInt64(&successOps, 1)
							atomic.AddInt64(&writeOps, 1)
							clientKeys[cid] = append(clientKeys[cid], key)
							tracker.recordSet(key, value)
						} else {
							atomic.AddInt64(&failedOps, 1)
							failures.record(failureReason)
						}
						latencySampler.add(latency)
					}
					opCount++
				}
			}
		}(clientID)
	}

	// 使用 testRunner 管理超时和进度报告
	runner := newTestRunner(duration, stopCh, &wg)
	runner.run(t, func(elapsed time.Duration) {
		counters := snapshotLongRunningCounters(&totalOps, &successOps, &failedOps)
		t.Logf("[进度] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, 写入: %d, 删除: %d, 延迟样本: %d",
			elapsed,
			counters.TotalOps,
			counters.SuccessOps,
			counters.FailedOps,
			atomic.LoadInt64(&writeOps),
			atomic.LoadInt64(&deleteOps),
			latencySampler.count())
	})

	expected := tracker.snapshot(1200)
	barrierOK := c.waitForClusterBarrier(t, currentLeader, "delete_stress", 45*time.Second)
	t.Logf("[最终屏障同步] 结果: %v", barrierOK)
	finalConsistent, finalVerified := c.waitForExpectedConsistency(t, expected, 45*time.Second)
	finalConsistent = barrierOK && finalConsistent
	t.Logf("[最终严格一致性检查] 已验证: %d 条节点键组合, 结果: %v", finalVerified, finalConsistent)
	snapshotNodes, maxSnapshotIndex := c.snapshotStats()

	metrics := LongRunningMetrics{
		TestName:          "10分钟删除压力测试 (gRPC+LSM)",
		Duration:          duration,
		TotalOps:          totalOps,
		SuccessOps:        successOps,
		FailedOps:         failedOps,
		WriteOps:          writeOps,
		DeleteOps:         deleteOps,
		LatencyP50:        percentileLong(latencySampler.getAll(), 50),
		LatencyP95:        percentileLong(latencySampler.getAll(), 95),
		LatencyP99:        percentileLong(latencySampler.getAll(), 99),
		ThroughputOps:     float64(successOps) / duration.Seconds(),
		WriteThroughput:   float64(writeOps) / duration.Seconds(),
		DeleteThroughput:  float64(deleteOps) / duration.Seconds(),
		ErrorRate:         float64(failedOps) / float64(totalOps) * 100,
		LeaderElections:   atomic.LoadInt32(&c.leaderElections),
		DataConsistencyOK: finalConsistent,
		KeysVerified:      finalVerified,
		SnapshotCount:     snapshotNodes,
		SnapshotMaxIndex:  maxSnapshotIndex,
		FailureReasons:    failures.snapshot(),
	}

	printLongRunningMetrics(t, &metrics)
	requireLongRunningMetrics(t, metrics, longRunningValidationOptions{
		RequireNoFailedOps:  true,
		RequireConsistency:  true,
		RequireVerifiedKeys: true,
	})
}

// percentileLong 计算长时测试的延迟百分位
func percentileLong(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	// 对延迟进行排序 - 使用标准库排序 O(n log n)
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	idx := int(float64(len(sorted)) * p / 100)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// printLongRunningMetrics 打印长时测试性能指标
func printLongRunningMetrics(t *testing.T, metrics *LongRunningMetrics) {
	t.Logf("\n========================================")
	t.Logf("长时性能测试结果: %s", metrics.TestName)
	t.Logf("========================================")
	t.Logf("测试时长: %v", metrics.Duration)
	t.Logf("----------------------------------------")
	t.Logf("操作统计:")
	t.Logf("  总操作数: %d", metrics.TotalOps)
	t.Logf("  成功操作: %d", metrics.SuccessOps)
	t.Logf("  失败操作: %d", metrics.FailedOps)
	t.Logf("  成功率: %.2f%%", float64(metrics.SuccessOps)/float64(metrics.TotalOps)*100)
	if len(metrics.FailureReasons) > 0 {
		t.Logf("  失败原因:")
		for _, reason := range metrics.FailureReasons {
			t.Logf("    %s: %d", reason.Reason, reason.Count)
		}
	}
	t.Logf("----------------------------------------")
	t.Logf("操作类型分布:")
	t.Logf("  写入操作: %d (%.1f%%)", metrics.WriteOps, float64(metrics.WriteOps)/float64(metrics.SuccessOps)*100)
	t.Logf("  读取操作: %d (%.1f%%)", metrics.ReadOps, float64(metrics.ReadOps)/float64(metrics.SuccessOps)*100)
	t.Logf("  删除操作: %d (%.1f%%)", metrics.DeleteOps, float64(metrics.DeleteOps)/float64(metrics.SuccessOps)*100)
	t.Logf("----------------------------------------")
	t.Logf("性能指标:")
	t.Logf("  总吞吐量: %.2f ops/sec", metrics.ThroughputOps)
	t.Logf("  写入吞吐量: %.2f ops/sec", metrics.WriteThroughput)
	t.Logf("  读取吞吐量: %.2f ops/sec", metrics.ReadThroughput)
	t.Logf("  删除吞吐量: %.2f ops/sec", metrics.DeleteThroughput)
	t.Logf("  错误率: %.4f%%", metrics.ErrorRate)
	t.Logf("----------------------------------------")
	if metrics.BytesRead > 0 {
		t.Logf("  读取流量: %.2f MB/s", float64(metrics.BytesRead)/1024/1024/metrics.Duration.Seconds())
	}
	if metrics.BytesWritten > 0 {
		t.Logf("  写入流量: %.2f MB/s", float64(metrics.BytesWritten)/1024/1024/metrics.Duration.Seconds())
	}
	t.Logf("----------------------------------------")
	t.Logf("延迟统计:")
	t.Logf("  P50: %v", metrics.LatencyP50)
	t.Logf("  P95: %v", metrics.LatencyP95)
	t.Logf("  P99: %v", metrics.LatencyP99)
	t.Logf("----------------------------------------")
	t.Logf("集群状态:")
	t.Logf("  Leader 切换次数: %d", metrics.LeaderElections)
	t.Logf("  快照节点数: %d", metrics.SnapshotCount)
	if metrics.SnapshotMaxIndex > 0 {
		t.Logf("  最大快照索引: %d", metrics.SnapshotMaxIndex)
	}
	t.Logf("  数据一致性: %v", metrics.DataConsistencyOK)
	t.Logf("  已验证数据条数: %d", metrics.KeysVerified)
	t.Logf("========================================\n")
}

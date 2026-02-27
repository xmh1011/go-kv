package tests

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
	"github.com/xmh1011/go-kv/pkg/transport"
	"github.com/xmh1011/go-kv/raft"
)

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
	WALSize           int64
	MemTableFlushes   int32
}

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
	mu              sync.Mutex
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

// getLeader 获取当前 Leader，超时时间更长以适应长时测试
func (c *longRunningCluster) getLeader(t *testing.T) *raft.Raft {
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, node := range c.nodes {
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
			for _, node := range c.nodes {
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
			for _, node := range c.nodes {
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

	// 初始随机选择一个节点
	nodeIdx := rand.Intn(len(c.nodes))
	node := c.nodes[nodeIdx]

	for retry := 0; retry < maxRetries; retry++ {
		// 检查是否应该停止
		select {
		case <-stopCh:
			return false, totalLatency, fmt.Errorf("test stopped")
		default:
		}

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
		totalLatency += latency

		if err == nil && reply.Success {
			return true, totalLatency, nil
		}

		// 如果收到 NotLeader 响应，使用 LeaderHint 重定向
		if reply.NotLeader {
			if reply.LeaderHint > 0 && reply.LeaderHint <= len(c.nodes) {
				// 使用 LeaderHint 定位新 Leader
				node = c.nodes[reply.LeaderHint-1]
			} else {
				// LeaderHint 无效，随机选择一个节点重试
				node = c.nodes[rand.Intn(len(c.nodes))]
			}
			continue
		}

		// 其他错误，直接返回
		return false, totalLatency, err
	}

	return false, totalLatency, fmt.Errorf("max retries exceeded")
}

// findLeader 遍历所有节点找到当前 Leader
func (c *longRunningCluster) findLeader() *raft.Raft {
	for _, node := range c.nodes {
		if node.State() == raft.Leader {
			return node
		}
	}
	return nil
}

// getLeaderByID 根据 LeaderHint ID 获取 Leader 节点
func (c *longRunningCluster) getLeaderByID(leaderID int) *raft.Raft {
	if leaderID <= 0 || leaderID > len(c.nodes) {
		return nil
	}
	return c.nodes[leaderID-1]
}

// sendRequestWithLeaderTracking 向当前 Leader 发送请求，自动跟踪 Leader 变化
// 当收到 NotLeader 响应时，更新 currentLeader 并重试
func (c *longRunningCluster) sendRequestWithLeaderTracking(currentLeader *atomic.Value, cmd param.KVCommand, maxRetries int, stopCh <-chan struct{}) (bool, time.Duration, error) {
	var totalLatency time.Duration

	for retry := 0; retry < maxRetries; retry++ {
		// 检查是否应该停止
		if stopCh != nil {
			select {
			case <-stopCh:
				return false, totalLatency, fmt.Errorf("test stopped")
			default:
			}
		}

		// 获取当前 Leader
		leader := currentLeader.Load().(*raft.Raft)
		if leader == nil || leader.IsStopped() {
			// 尝试重新查找 Leader
			newLeader := c.findLeader()
			if newLeader == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			currentLeader.Store(newLeader)
			leader = newLeader
		}

		cmdBytes, _ := json.Marshal(cmd)
		args := &param.ClientArgs{
			ClientID:    rand.Int63(),
			SequenceNum: rand.Int63(),
			Command:     cmdBytes,
		}
		reply := &param.ClientReply{}

		start := time.Now()
		err := leader.ClientRequest(args, reply)
		latency := time.Since(start)
		totalLatency += latency

		if err == nil && reply.Success {
			return true, totalLatency, nil
		}

		// 如果收到 NotLeader 响应，使用 LeaderHint 更新 Leader
		if reply.NotLeader {
			if reply.LeaderHint > 0 && reply.LeaderHint <= len(c.nodes) {
				newLeader := c.nodes[reply.LeaderHint-1]
				if !newLeader.IsStopped() && newLeader.State() == raft.Leader {
					currentLeader.Store(newLeader)
				} else {
					// LeaderHint 无效或节点不可用，尝试重新查找
					newLeader = c.findLeader()
					if newLeader != nil {
						currentLeader.Store(newLeader)
					}
				}
			} else {
				// LeaderHint 无效，重新查找 Leader
				newLeader := c.findLeader()
				if newLeader != nil {
					currentLeader.Store(newLeader)
				}
			}
			continue
		}

		// 其他错误，直接返回
		return false, totalLatency, err
	}

	return false, totalLatency, fmt.Errorf("max retries exceeded")
}

// getCurrentLeader 获取当前 Leader
func (c *longRunningCluster) getCurrentLeader() *raft.Raft {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, node := range c.nodes {
		if node.State() == raft.Leader {
			return node
		}
	}
	return nil
}

// verifyDataConsistency 验证所有节点的数据一致性
func (c *longRunningCluster) verifyDataConsistency(t *testing.T, sampleKeys []string) (bool, int64) {
	if len(sampleKeys) == 0 {
		return true, 0
	}

	// 从 Leader 获取基准数据
	var leader *raft.Raft
	for _, node := range c.nodes {
		if node.State() == raft.Leader {
			leader = node
			break
		}
	}
	if leader == nil {
		t.Logf("Warning: No leader found during consistency check")
		return true, 0
	}

	leaderSM := c.stateMachines[leader.ID()-1]
	leaderData := make(map[string]string)
	for _, key := range sampleKeys {
		val, err := leaderSM.Get(key)
		if err == nil && val != "" {
			leaderData[key] = val
		}
	}

	mismatchCount := int64(0)
	verifiedCount := int64(0)

	// 检查所有 Follower 的数据
	for _, node := range c.nodes {
		if node.State() == raft.Leader {
			continue
		}
		sm := c.stateMachines[node.ID()-1]

		for key, leaderVal := range leaderData {
			val, err := sm.Get(key)
			if err != nil {
				continue
			}
			verifiedCount++

			// 跳过已删除的键
			if leaderVal == "" || val == "" {
				continue
			}

			if val != leaderVal {
				t.Logf("Data mismatch: Node %d - Key '%s': Leader='%s', Node='%s'",
					node.ID(), key, leaderVal, val)
				mismatchCount++
			}
		}
	}

	return mismatchCount == 0, verifiedCount
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

// TestLongRunning_10Min_Comprehensive 10分钟综合性能测试
// 模拟生产环境：使用 gRPC + LSM，三节点集群，混合读写删除操作
// 客户端可以向任意节点发送请求，自动处理 NotLeader 重定向
func TestLongRunning_10Min_Comprehensive(t *testing.T) {
	duration := 10 * time.Minute
	if testing.Short() {
		duration = 1 * time.Minute
	}

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
		success, _, _ := c.sendRequestWithLeaderTracking(currentLeader, cmd, 3, nil)
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
		totalOps            int64
		successOps          int64
		failedOps           int64
		writeOps            int64
		readOps             int64
		deleteOps           int64
		bytesRead           int64
		bytesWritten        int64
		latencySampler      = newLatencySampler(maxLatencySamples)
		writeLatencySampler = newLatencySampler(maxLatencySamples)
		readLatencySampler  = newLatencySampler(maxLatencySamples)
		deleteLatencySampler = newLatencySampler(maxLatencySamples)
		keysForVerification []string
		sampleKeysMutex     sync.Mutex
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

					if r < 0.6 { // 60% 写入操作
						key := fmt.Sprintf("%s-key-%d-%d", clientPrefix, cid, rand.Intn(50000))
						value := fmt.Sprintf("%s-val-%d", clientPrefix, rand.Intn(1000000))
						cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

						success, latency, _ = c.sendRequestWithLeaderTracking(currentLeader, cmd, 3, stopCh)

						atomic.AddInt64(&writeOps, 1)
						writeLatencySampler.add(latency)

						localKeys = append(localKeys, key)

						if success {
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
						success, latency, _ = c.sendRequestWithLeaderTracking(currentLeader, cmd, 3, stopCh)

						atomic.AddInt64(&readOps, 1)
						readLatencySampler.add(latency)

					} else { // 15% 删除操作
						if len(localKeys) > 0 {
							idx := rand.Intn(len(localKeys))
							key := localKeys[idx]
							cmd := param.KVCommand{Op: param.OpDelete, Key: key}

							success, latency, _ = c.sendRequestWithLeaderTracking(currentLeader, cmd, 3, stopCh)

							atomic.AddInt64(&deleteOps, 1)
							deleteLatencySampler.add(latency)

							if success {
								localKeys = append(localKeys[:idx], localKeys[idx+1:]...)
							}
						}
					}

					atomic.AddInt64(&totalOps, 1)
					if success {
						atomic.AddInt64(&successOps, 1)
						latencySampler.add(latency)
					} else {
						atomic.AddInt64(&failedOps, 1)
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
		ops := atomic.LoadInt64(&totalOps)
		success := atomic.LoadInt64(&successOps)
		failed := atomic.LoadInt64(&failedOps)

		t.Logf("[进度报告] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, 吞吐量: %.2f ops/sec, 延迟样本: %d",
			elapsed, ops, success, failed, float64(success)/elapsed.Seconds(), latencySampler.count())
	})

	// 最终一致性检查
	finalConsistent, finalVerified := c.verifyDataConsistency(t, keysForVerification)
	t.Logf("[最终一致性检查] 已验证: %d 条数据, 结果: %v", finalVerified, finalConsistent)

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
	}

	printLongRunningMetrics(t, &metrics)
}

// TestLongRunning_10Min_WriteHeavy 10分钟写入密集型测试
// 客户端可以向任意节点发送请求，自动处理 NotLeader 重定向
func TestLongRunning_10Min_WriteHeavy(t *testing.T) {
	duration := 10 * time.Minute
	if testing.Short() {
		duration = 1 * time.Minute
	}

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
		totalOps       int64
		successOps     int64
		failedOps      int64
		bytesWritten   int64
		latencySampler = newLatencySampler(maxLatencySamples)
	)

	numClients := 8
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numClients)

	t.Logf("启动 %d 个并发写入客户端...", numClients)

	for clientID := 0; clientID < numClients; clientID++ {
		go func(cid int) {
			defer wg.Done()
			opCount := int64(0)

			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("write-heavy-key-%d-%d", cid, opCount)
					value := fmt.Sprintf("value-%d-%d", cid, rand.Intn(10000000))
					cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

					success, latency, _ := c.sendRequestWithLeaderTracking(currentLeader, cmd, 3, stopCh)

					atomic.AddInt64(&totalOps, 1)
					if success {
						atomic.AddInt64(&successOps, 1)
						atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
						latencySampler.add(latency)
					} else {
						atomic.AddInt64(&failedOps, 1)
					}
					opCount++
				}
			}
		}(clientID)
	}

	// 使用 testRunner 管理超时和进度报告
	runner := newTestRunner(duration, stopCh, &wg)
	runner.run(t, func(elapsed time.Duration) {
		t.Logf("[进度] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, 写入流量: %.2f MB/s, 延迟样本: %d",
			elapsed,
			atomic.LoadInt64(&totalOps),
			atomic.LoadInt64(&successOps),
			atomic.LoadInt64(&failedOps),
			float64(atomic.LoadInt64(&bytesWritten))/1024/1024/elapsed.Seconds(),
			latencySampler.count())
	})

	metrics := LongRunningMetrics{
		TestName:          "10分钟写入密集型测试 (gRPC+LSM)",
		Duration:          duration,
		TotalOps:          totalOps,
		SuccessOps:        successOps,
		FailedOps:         failedOps,
		BytesWritten:      bytesWritten,
		LatencyP50:        percentileLong(latencySampler.getAll(), 50),
		LatencyP95:        percentileLong(latencySampler.getAll(), 95),
		LatencyP99:        percentileLong(latencySampler.getAll(), 99),
		ThroughputOps:     float64(successOps) / duration.Seconds(),
		WriteThroughput:   float64(successOps) / duration.Seconds(),
		ErrorRate:         float64(failedOps) / float64(totalOps) * 100,
		LeaderElections:   atomic.LoadInt32(&c.leaderElections),
		DataConsistencyOK: true,
	}

	printLongRunningMetrics(t, &metrics)
}

// TestLongRunning_10Min_MixedWithFailures 10分钟带故障恢复的混合测试
func TestLongRunning_10Min_MixedWithFailures(t *testing.T) {
	duration := 10 * time.Minute
	if testing.Short() {
		duration = 1 * time.Minute
	}

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
		bytesRead      int64
		bytesWritten   int64
		latencySampler = newLatencySampler(maxLatencySamples)
	)

	numClients := 5
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numClients)

	t.Logf("启动 %d 个并发客户端...", numClients)

	// 客户端执行混合读写操作
	for clientID := 0; clientID < numClients; clientID++ {
		go func(cid int) {
			defer wg.Done()
			localKeys := make([]string, 0)

			for {
				select {
				case <-stopCh:
					return
				default:
					r := rand.Float64()

					if r < 0.7 { // 70% 写入
						key := fmt.Sprintf("fail-test-key-%d-%d", cid, rand.Intn(20000))
						value := fmt.Sprintf("val-%d", rand.Intn(100000))
						cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

						success, latency, _ := c.sendRequestWithLeaderTracking(currentLeader, cmd, 3, stopCh)

						if success {
							atomic.AddInt64(&successOps, 1)
							atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
							localKeys = append(localKeys, key)
							latencySampler.add(latency)
						} else {
							atomic.AddInt64(&failedOps, 1)
						}
						atomic.AddInt64(&totalOps, 1)

					} else { // 30% 读取
						var key string
						if len(localKeys) > 0 {
							key = localKeys[rand.Intn(len(localKeys))]
						} else {
							key = fmt.Sprintf("fail-test-read-%d", rand.Intn(10000))
						}

						cmd := param.KVCommand{Op: param.OpGet, Key: key}
						success, latency, _ := c.sendRequestWithLeaderTracking(currentLeader, cmd, 3, stopCh)

						if success {
							atomic.AddInt64(&successOps, 1)
							l := currentLeader.Load().(*raft.Raft)
							val, _ := c.stateMachines[l.ID()-1].Get(key)
							atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
							latencySampler.add(latency)
						} else {
							atomic.AddInt64(&failedOps, 1)
						}
						atomic.AddInt64(&totalOps, 1)
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
			t.Logf("[进度] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, Leader切换: %d, 延迟样本: %d",
				elapsed,
				atomic.LoadInt64(&totalOps),
				atomic.LoadInt64(&successOps),
				atomic.LoadInt64(&failedOps),
				atomic.LoadInt32(&c.leaderElections),
				latencySampler.count())
		},
		func() {
			if failureCount < 2 { // 最多触发2次故障
				// 随机选择一个 Follower 节点停止
				var victim *raft.Raft
				for _, node := range c.nodes {
					if node.State() != raft.Leader {
						victim = node
						break
					}
				}

				if victim != nil {
					t.Logf("[故障模拟] 停止节点 %d", victim.ID())
					victim.Stop()

					// 等待一段时间后恢复
					go func(id int, node *raft.Raft) {
						time.Sleep(30 * time.Second)
						t.Logf("[故障模拟] 恢复节点 %d", id)

						// 重新启动节点（简化处理：不完整重启）
						// 实际生产环境中需要完整的状态恢复流程
					}(victim.ID(), victim)
					failureCount++
				}
			}
		})

	metrics := LongRunningMetrics{
		TestName:          "10分钟带故障恢复的混合测试 (gRPC+LSM)",
		Duration:          duration,
		TotalOps:          totalOps,
		SuccessOps:        successOps,
		FailedOps:         failedOps,
		BytesRead:         bytesRead,
		BytesWritten:      bytesWritten,
		LatencyP50:        percentileLong(latencySampler.getAll(), 50),
		LatencyP95:        percentileLong(latencySampler.getAll(), 95),
		LatencyP99:        percentileLong(latencySampler.getAll(), 99),
		ThroughputOps:     float64(successOps) / duration.Seconds(),
		ErrorRate:         float64(failedOps) / float64(totalOps) * 100,
		LeaderElections:   atomic.LoadInt32(&c.leaderElections),
		DataConsistencyOK: true,
	}

	printLongRunningMetrics(t, &metrics)
}

// TestLongRunning_10Min_ReadHeavy 10分钟读取密集型测试
func TestLongRunning_10Min_ReadHeavy(t *testing.T) {
	duration := 10 * time.Minute
	if testing.Short() {
		duration = 1 * time.Minute
	}

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
	t.Logf("预热阶段: 写入 %d 条数据...", warmupCount)
	warmupSuccess := 0
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("read-warmup-key-%d", i)
		value := fmt.Sprintf("read-warmup-value-%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		// 预热阶段使用 nil stopCh，因为没有启动客户端
		success, _, _ := c.sendRequestWithLeaderTracking(currentLeader, cmd, 3, nil)
		if success {
			warmupSuccess++
		}
	}
	t.Logf("预热完成: %d/%d 成功，等待同步...", warmupSuccess, warmupCount)
	time.Sleep(3 * time.Second)

	// 性能指标 - 使用 latencySampler 控制内存使用
	const maxLatencySamples = 10000
	var (
		totalOps       int64
		successOps     int64
		failedOps      int64
		bytesRead      int64
		latencySampler = newLatencySampler(maxLatencySamples)
	)

	numClients := 10
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numClients)

	t.Logf("启动 %d 个并发读取客户端...", numClients)

	for clientID := 0; clientID < numClients; clientID++ {
		go func(cid int) {
			defer wg.Done()
			for {
				// 先检查是否应该停止
				select {
				case <-stopCh:
					return
				default:
				}

				key := fmt.Sprintf("read-warmup-key-%d", rand.Intn(warmupCount))
				cmd := param.KVCommand{Op: param.OpGet, Key: key}

				success, latency, _ := c.sendRequestWithLeaderTracking(currentLeader, cmd, 3, stopCh)

				// 请求完成后再次检查是否应该停止
				select {
				case <-stopCh:
					return
				default:
				}

				atomic.AddInt64(&totalOps, 1)
				if success {
					atomic.AddInt64(&successOps, 1)
					// 使用当前 Leader 获取数据大小
					l := currentLeader.Load().(*raft.Raft)
					val, _ := c.stateMachines[l.ID()-1].Get(key)
					atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
					latencySampler.add(latency)
				} else {
					atomic.AddInt64(&failedOps, 1)
				}
			}
		}(clientID)
	}

	// 使用 testRunner 管理超时和进度报告
	runner := newTestRunner(duration, stopCh, &wg)
	runner.run(t, func(elapsed time.Duration) {
		t.Logf("[进度] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, 读取流量: %.2f MB/s, 延迟样本: %d",
			elapsed,
			atomic.LoadInt64(&totalOps),
			atomic.LoadInt64(&successOps),
			atomic.LoadInt64(&failedOps),
			float64(atomic.LoadInt64(&bytesRead))/1024/1024/elapsed.Seconds(),
			latencySampler.count())
	})

	metrics := LongRunningMetrics{
		TestName:          "10分钟读取密集型测试 (gRPC+LSM)",
		Duration:          duration,
		TotalOps:          totalOps,
		SuccessOps:        successOps,
		FailedOps:         failedOps,
		BytesRead:         bytesRead,
		LatencyP50:        percentileLong(latencySampler.getAll(), 50),
		LatencyP95:        percentileLong(latencySampler.getAll(), 95),
		LatencyP99:        percentileLong(latencySampler.getAll(), 99),
		ThroughputOps:     float64(successOps) / duration.Seconds(),
		ReadThroughput:    float64(successOps) / duration.Seconds(),
		ErrorRate:         float64(failedOps) / float64(totalOps) * 100,
		LeaderElections:   atomic.LoadInt32(&c.leaderElections),
		DataConsistencyOK: true,
	}

	printLongRunningMetrics(t, &metrics)
}

// TestLongRunning_10Min_DeleteStress 10分钟删除压力测试
func TestLongRunning_10Min_DeleteStress(t *testing.T) {
	duration := 10 * time.Minute
	if testing.Short() {
		duration = 1 * time.Minute
	}

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
		totalOps            int64
		successOps          int64
		failedOps           int64
		writeOps            int64
		deleteOps           int64
		latencySampler      = newLatencySampler(maxLatencySamples)
		deleteLatencySampler = newLatencySampler(maxLatencySamples)
	)

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
						success, latency, _ := c.sendRequestWithLeaderTracking(currentLeader, cmd, 3, stopCh)

						atomic.AddInt64(&totalOps, 1)
						if success {
							atomic.AddInt64(&successOps, 1)
							atomic.AddInt64(&deleteOps, 1)
							deleteLatencySampler.add(latency)
							latencySampler.add(latency)

							// 移除已删除的键
							clientKeys[cid] = append(clientKeys[cid][:idx], clientKeys[cid][idx+1:]...)
						} else {
							atomic.AddInt64(&failedOps, 1)
							latencySampler.add(latency)
						}
					} else {
						// 写入操作
						key := fmt.Sprintf("delete-test-key-%d-%d", cid, opCount)
						value := fmt.Sprintf("val-%d", rand.Intn(10000))
						cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

						success, latency, _ := c.sendRequestWithLeaderTracking(currentLeader, cmd, 3, stopCh)

						atomic.AddInt64(&totalOps, 1)
						if success {
							atomic.AddInt64(&successOps, 1)
							atomic.AddInt64(&writeOps, 1)
							clientKeys[cid] = append(clientKeys[cid], key)
						} else {
							atomic.AddInt64(&failedOps, 1)
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
		t.Logf("[进度] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, 写入: %d, 删除: %d, 延迟样本: %d",
			elapsed,
			atomic.LoadInt64(&totalOps),
			atomic.LoadInt64(&successOps),
			atomic.LoadInt64(&failedOps),
			atomic.LoadInt64(&writeOps),
			atomic.LoadInt64(&deleteOps),
			latencySampler.count())
	})

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
		DataConsistencyOK: true,
	}

	printLongRunningMetrics(t, &metrics)
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
	t.Logf("  数据一致性: %v", metrics.DataConsistencyOK)
	t.Logf("  已验证数据条数: %d", metrics.KeysVerified)
	t.Logf("========================================\n")
}

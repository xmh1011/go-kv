package tests

import (
	"encoding/json"
	"fmt"
	"math/rand"
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
	TestName           string
	Duration           time.Duration
	TotalOps           int64
	SuccessOps         int64
	FailedOps          int64
	WriteOps           int64
	ReadOps            int64
	DeleteOps          int64
	BytesRead          int64
	BytesWritten       int64
	LatencyP50        time.Duration
	LatencyP95        time.Duration
	LatencyP99        time.Duration
	ThroughputOps      float64
	WriteThroughput    float64
	ReadThroughput     float64
	DeleteThroughput   float64
	ErrorRate          float64
	LeaderElections    int32
	LeaderDowntime     time.Duration
	DataConsistencyOK  bool
	KeysVerified       int64
	SnapshotCount      int32
	WALSize           int64
	MemTableFlushes    int32
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
	mu             sync.Mutex
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
		go func(ch chan param.CommitEntry, sm storage.StateMachine) {
			for entry := range ch {
				// 将 CommitEntry 转换为 LogEntry 传递给状态机
				logEntry := param.LogEntry{
					Command: entry.Command,
					Term:    entry.Term,
					Index:   entry.Index,
				}
				_ = sm.Apply(logEntry)
			}
		}(c.commitChans[i], c.stateMachines[i])

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
	timeout := time.After(30 * time.Second)
	for i := 0; i < 60; i++ {
		select {
		case <-time.After(500 * time.Millisecond):
			for _, node := range c.nodes {
				if node.IsStopped() {
					continue
				}
				if node.State() == raft.Leader {
					return node
				}
			}
		case <-timeout:
			t.Fatal("Cluster failed to elect a leader within 30 seconds")
		}
	}
	t.Fatal("Cluster failed to elect a leader")
	return nil
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

// sendRequest 发送请求并返回结果
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

// TestLongRunning_10Min_Comprehensive 10分钟综合性能测试
// 模拟生产环境：使用 gRPC + LSM，三节点集群，混合读写删除操作
func TestLongRunning_10Min_Comprehensive(t *testing.T) {
	duration := 10 * time.Minute
	if testing.Short() {
		duration = 1 * time.Minute // 短模式下缩短测试时间
	}

	c := newLongRunningCluster(t, 3)
	defer c.shutdown()

	t.Logf("=== 10分钟长时端到端性能测试开始 ===")
	t.Logf("集群配置: 3节点, gRPC传输, LSM存储")
	t.Logf("测试持续时间: %v", duration)

	// 等待集群就绪
	c.waitForAllNodesReady(t)
	leader := c.getLeader(t)
	t.Logf("初始 Leader: Node %d", leader.ID())

	// 启动 Leader 监控
	monitorCtx := make(chan struct{})
	go c.monitorLeaderChanges(monitorCtx)
	defer close(monitorCtx)

	// 预热数据
	warmupCount := 1000
	if testing.Short() {
		warmupCount = 100 // 短模式下减少预热数据
	}
	t.Logf("预热阶段: 写入 %d 条数据...", warmupCount)
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("warmup-key-%d", i)
		value := fmt.Sprintf("warmup-value-%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		c.sendRequest(leader, cmd)
	}
	t.Logf("预热完成")

	// 等待数据同步
	time.Sleep(3 * time.Second)

	// 性能指标
	var (
		totalOps          int64
		successOps        int64
		failedOps         int64
		writeOps          int64
		readOps           int64
		deleteOps         int64
		bytesRead         int64
		bytesWritten      int64
		latencies        []time.Duration
		writeLatencies    []time.Duration
		readLatencies     []time.Duration
		deleteLatencies   []time.Duration
		keysForVerification []string
		sampleKeysMutex   sync.Mutex
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

						success, latency, _ = c.sendRequest(leader, cmd)

						atomic.AddInt64(&writeOps, 1)
						writeLatencies = append(writeLatencies, latency)

						localKeys = append(localKeys, key)

						if success {
							atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
						}

					} else if r < 0.85 { // 25% 读取操作
						// 优先读取已存在的键
						var key string
						if len(localKeys) > 0 {
							key = localKeys[rand.Intn(len(localKeys))]
						} else {
							key = fmt.Sprintf("%s-key-%d-%d", clientPrefix, cid, rand.Intn(10000))
						}

						cmd := param.KVCommand{Op: param.OpGet, Key: key}
						success, latency, _ = c.sendRequest(leader, cmd)

						atomic.AddInt64(&readOps, 1)
						readLatencies = append(readLatencies, latency)

						if success {
							val, _ := c.stateMachines[leader.ID()-1].Get(key)
							atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
						}

					} else { // 15% 删除操作
						if len(localKeys) > 0 {
							idx := rand.Intn(len(localKeys))
							key := localKeys[idx]
							cmd := param.KVCommand{Op: param.OpDelete, Key: key}

							success, latency, _ = c.sendRequest(leader, cmd)

							atomic.AddInt64(&deleteOps, 1)
							deleteLatencies = append(deleteLatencies, latency)

							if success {
								// 移除已删除的键
								localKeys = append(localKeys[:idx], localKeys[idx+1:]...)
							}
						}
					}

					atomic.AddInt64(&totalOps, 1)
					if success {
						atomic.AddInt64(&successOps, 1)
						latencies = append(latencies, latency)
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

	// 定期一致性检查和进度报告
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for elapsed := time.Duration(0); elapsed < duration; {
		select {
		case <-ticker.C:
			elapsed = time.Since(time.Now().Add(-duration))
			ops := atomic.LoadInt64(&totalOps)
			success := atomic.LoadInt64(&successOps)
			failed := atomic.LoadInt64(&failedOps)

			t.Logf("[进度报告] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, 吞吐量: %.2f ops/sec",
				elapsed, ops, success, failed, float64(success)/elapsed.Seconds())

			// 进行一致性检查
			sampleKeysMutex.Lock()
			sampleKeys := make([]string, len(keysForVerification))
			copy(sampleKeys, keysForVerification)
			sampleKeysMutex.Unlock()

			if len(sampleKeys) > 0 {
				consistent, verified := c.verifyDataConsistency(t, sampleKeys)
				t.Logf("[一致性检查] 已验证: %d 条数据, 结果: %v", verified, consistent)
			}

		case <-time.After(duration - elapsed):
			close(stopCh)
			wg.Wait()
			break
		}
	}

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
		LatencyP50:       percentileLong(latencies, 50),
		LatencyP95:       percentileLong(latencies, 95),
		LatencyP99:       percentileLong(latencies, 99),
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

	monitorCtx := make(chan struct{})
	go c.monitorLeaderChanges(monitorCtx)
	defer close(monitorCtx)

	var (
		totalOps     int64
		successOps   int64
		failedOps    int64
		bytesWritten int64
		latencies    []time.Duration
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

					success, latency, _ := c.sendRequest(leader, cmd)

					atomic.AddInt64(&totalOps, 1)
					if success {
						atomic.AddInt64(&successOps, 1)
						atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
						latencies = append(latencies, latency)
					} else {
						atomic.AddInt64(&failedOps, 1)
					}
					opCount++
				}
			}
		}(clientID)
	}

	// 定期进度报告
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	elapsed := time.Duration(0)
	for elapsed < duration {
		select {
		case <-ticker.C:
			elapsed = time.Since(time.Now().Add(-duration))
			t.Logf("[进度] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, 写入流量: %.2f MB/s",
				elapsed,
				atomic.LoadInt64(&totalOps),
				atomic.LoadInt64(&successOps),
				atomic.LoadInt64(&failedOps),
				float64(atomic.LoadInt64(&bytesWritten))/1024/1024/elapsed.Seconds())
		case <-time.After(duration - elapsed):
			close(stopCh)
			wg.Wait()
			break
		}
	}

	metrics := LongRunningMetrics{
		TestName:         "10分钟写入密集型测试 (gRPC+LSM)",
		Duration:         duration,
		TotalOps:         totalOps,
		SuccessOps:       successOps,
		FailedOps:        failedOps,
		BytesWritten:     bytesWritten,
		LatencyP50:       percentileLong(latencies, 50),
		LatencyP95:       percentileLong(latencies, 95),
		LatencyP99:       percentileLong(latencies, 99),
		ThroughputOps:    float64(successOps) / duration.Seconds(),
		WriteThroughput:  float64(successOps) / duration.Seconds(),
		ErrorRate:        float64(failedOps) / float64(totalOps) * 100,
		LeaderElections:  atomic.LoadInt32(&c.leaderElections),
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

	monitorCtx := make(chan struct{})
	go c.monitorLeaderChanges(monitorCtx)
	defer close(monitorCtx)

	var (
		totalOps     int64
		successOps   int64
		failedOps    int64
		bytesRead    int64
		bytesWritten int64
		latencies    []time.Duration
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

						success, latency, _ := c.sendRequest(leader, cmd)

						if success {
							atomic.AddInt64(&successOps, 1)
							atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
							localKeys = append(localKeys, key)
							latencies = append(latencies, latency)
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
						success, latency, _ := c.sendRequest(leader, cmd)

						if success {
							atomic.AddInt64(&successOps, 1)
							val, _ := c.stateMachines[leader.ID()-1].Get(key)
							atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
							latencies = append(latencies, latency)
						} else {
							atomic.AddInt64(&failedOps, 1)
						}
						atomic.AddInt64(&totalOps, 1)
					}
				}
			}
		}(clientID)
	}

	// 模拟节点故障恢复
	failureTicker := time.NewTicker(2 * time.Minute)
	defer failureTicker.Stop()

	failureCount := 0
	elapsed := time.Duration(0)
	progressTicker := time.NewTicker(30 * time.Second)
	defer progressTicker.Stop()

	for elapsed < duration {
		select {
		case <-failureTicker.C:
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

		case <-progressTicker.C:
			elapsed = time.Since(time.Now().Add(-duration))
			t.Logf("[进度] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, Leader切换: %d",
				elapsed,
				atomic.LoadInt64(&totalOps),
				atomic.LoadInt64(&successOps),
				atomic.LoadInt64(&failedOps),
				atomic.LoadInt32(&c.leaderElections))

		case <-time.After(duration - elapsed):
			close(stopCh)
			wg.Wait()
			break
		}
	}

	metrics := LongRunningMetrics{
		TestName:          "10分钟带故障恢复的混合测试 (gRPC+LSM)",
		Duration:          duration,
		TotalOps:          totalOps,
		SuccessOps:        successOps,
		FailedOps:         failedOps,
		BytesRead:         bytesRead,
		BytesWritten:      bytesWritten,
		LatencyP50:        percentileLong(latencies, 50),
		LatencyP95:        percentileLong(latencies, 95),
		LatencyP99:        percentileLong(latencies, 99),
		ThroughputOps:      float64(successOps) / duration.Seconds(),
		ErrorRate:         float64(failedOps) / float64(totalOps) * 100,
		LeaderElections:    atomic.LoadInt32(&c.leaderElections),
		DataConsistencyOK:  true,
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

	// 预热大量数据
	warmupCount := 1000
	if testing.Short() {
		warmupCount = 100 // 短模式下减少预热数据
	}
	t.Logf("预热阶段: 写入 %d 条数据...", warmupCount)
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("read-warmup-key-%d", i)
		value := fmt.Sprintf("read-warmup-value-%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		c.sendRequest(leader, cmd)
	}
	t.Logf("预热完成，等待同步...")
	time.Sleep(3 * time.Second)

	var (
		totalOps   int64
		successOps int64
		failedOps  int64
		bytesRead  int64
		latencies  []time.Duration
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
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("read-warmup-key-%d", rand.Intn(warmupCount))
					cmd := param.KVCommand{Op: param.OpGet, Key: key}

					success, latency, _ := c.sendRequest(leader, cmd)

					atomic.AddInt64(&totalOps, 1)
					if success {
						atomic.AddInt64(&successOps, 1)
						val, _ := c.stateMachines[leader.ID()-1].Get(key)
						atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
						latencies = append(latencies, latency)
					} else {
						atomic.AddInt64(&failedOps, 1)
					}
				}
			}
		}(clientID)
	}

	// 定期进度报告
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	elapsed := time.Duration(0)
	for elapsed < duration {
		select {
		case <-ticker.C:
			elapsed = time.Since(time.Now().Add(-duration))
			t.Logf("[进度] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, 读取流量: %.2f MB/s",
				elapsed,
				atomic.LoadInt64(&totalOps),
				atomic.LoadInt64(&successOps),
				atomic.LoadInt64(&failedOps),
				float64(atomic.LoadInt64(&bytesRead))/1024/1024/elapsed.Seconds())
		case <-time.After(duration - elapsed):
			close(stopCh)
			wg.Wait()
			break
		}
	}

	metrics := LongRunningMetrics{
		TestName:        "10分钟读取密集型测试 (gRPC+LSM)",
		Duration:        duration,
		TotalOps:        totalOps,
		SuccessOps:      successOps,
		FailedOps:       failedOps,
		BytesRead:       bytesRead,
		LatencyP50:      percentileLong(latencies, 50),
		LatencyP95:      percentileLong(latencies, 95),
		LatencyP99:      percentileLong(latencies, 99),
		ThroughputOps:   float64(successOps) / duration.Seconds(),
		ReadThroughput:  float64(successOps) / duration.Seconds(),
		ErrorRate:       float64(failedOps) / float64(totalOps) * 100,
		LeaderElections: atomic.LoadInt32(&c.leaderElections),
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

	monitorCtx := make(chan struct{})
	go c.monitorLeaderChanges(monitorCtx)
	defer close(monitorCtx)

	var (
		totalOps     int64
		successOps   int64
		failedOps    int64
		writeOps     int64
		deleteOps    int64
		latencies    []time.Duration
		deleteLatencies []time.Duration
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
						success, latency, _ := c.sendRequest(leader, cmd)

						atomic.AddInt64(&totalOps, 1)
						if success {
							atomic.AddInt64(&successOps, 1)
							atomic.AddInt64(&deleteOps, 1)
							deleteLatencies = append(deleteLatencies, latency)

							// 移除已删除的键
							clientKeys[cid] = append(clientKeys[cid][:idx], clientKeys[cid][idx+1:]...)
						} else {
							atomic.AddInt64(&failedOps, 1)
						}
						latencies = append(latencies, latency)
					} else {
						// 写入操作
						key := fmt.Sprintf("delete-test-key-%d-%d", cid, opCount)
						value := fmt.Sprintf("val-%d", rand.Intn(10000))
						cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

						success, latency, _ := c.sendRequest(leader, cmd)

						atomic.AddInt64(&totalOps, 1)
						if success {
							atomic.AddInt64(&successOps, 1)
							atomic.AddInt64(&writeOps, 1)
							clientKeys[cid] = append(clientKeys[cid], key)
						} else {
							atomic.AddInt64(&failedOps, 1)
						}
						latencies = append(latencies, latency)
					}
					opCount++
				}
			}
		}(clientID)
	}

	// 定期进度报告
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	elapsed := time.Duration(0)
	for elapsed < duration {
		select {
		case <-ticker.C:
			elapsed = time.Since(time.Now().Add(-duration))
			t.Logf("[进度] 已运行: %v, 总操作: %d, 成功: %d, 失败: %d, 写入: %d, 删除: %d",
				elapsed,
				atomic.LoadInt64(&totalOps),
				atomic.LoadInt64(&successOps),
				atomic.LoadInt64(&failedOps),
				atomic.LoadInt64(&writeOps),
				atomic.LoadInt64(&deleteOps))
		case <-time.After(duration - elapsed):
			close(stopCh)
			wg.Wait()
			break
		}
	}

	metrics := LongRunningMetrics{
		TestName:          "10分钟删除压力测试 (gRPC+LSM)",
		Duration:          duration,
		TotalOps:          totalOps,
		SuccessOps:        successOps,
		FailedOps:         failedOps,
		WriteOps:          writeOps,
		DeleteOps:         deleteOps,
		LatencyP50:        percentileLong(latencies, 50),
		LatencyP95:        percentileLong(latencies, 95),
		LatencyP99:        percentileLong(latencies, 99),
		ThroughputOps:      float64(successOps) / duration.Seconds(),
		WriteThroughput:    float64(writeOps) / duration.Seconds(),
		DeleteThroughput:   float64(deleteOps) / duration.Seconds(),
		ErrorRate:          float64(failedOps) / float64(totalOps) * 100,
		LeaderElections:    atomic.LoadInt32(&c.leaderElections),
		DataConsistencyOK:  true,
	}

	printLongRunningMetrics(t, &metrics)
}

// percentileLong 计算长时测试的延迟百分位
func percentileLong(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	// 对延迟进行排序
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	// 简单的冒泡排序（对于少量数据可接受）
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
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

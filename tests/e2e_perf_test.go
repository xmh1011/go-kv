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

// PerfMetrics 记录性能指标
type PerfMetrics struct {
	TestName      string
	Duration      time.Duration
	TotalOps      int64
	SuccessOps    int64
	FailedOps     int64
	BytesRead     int64
	BytesWritten  int64
	LatencyP50    time.Duration
	LatencyP95    time.Duration
	LatencyP99    time.Duration
	ThroughputOps float64
	ErrorRate     float64
}

// e2eCluster 端到端性能测试集群
type e2eCluster struct {
	nodes         []*raft.Raft
	transports    []transport.Transport
	storages      []storage.Storage
	stateMachines []storage.StateMachine
	commitChans   []chan param.CommitEntry
	peerMap       map[int]string
	dataDir       string
}

// newE2ECluster 创建用于端到端性能测试的集群
func newE2ECluster(t *testing.T, nodeCount int) *e2eCluster {
	c := &e2eCluster{
		nodes:         make([]*raft.Raft, nodeCount),
		transports:    make([]transport.Transport, nodeCount),
		storages:      make([]storage.Storage, nodeCount),
		stateMachines: make([]storage.StateMachine, nodeCount),
		commitChans:   make([]chan param.CommitEntry, nodeCount),
		peerMap:       make(map[int]string),
		dataDir:       t.TempDir(),
	}

	// 1. 初始化传输层
	for i := 0; i < nodeCount; i++ {
		id := i + 1
		addr := "127.0.0.1:0"
		trans, err := transport.NewTransport(transport.GrpcTransport, addr)
		if err != nil {
			t.Fatalf("failed to create transport for node %d: %v", id, err)
		}
		c.transports[i] = trans
		c.peerMap[id] = trans.Addr()
	}

	// 2. 构造初始配置
	initialPeerIDs := make([]int, 0)
	for i := 0; i < nodeCount; i++ {
		initialPeerIDs = append(initialPeerIDs, i+1)
	}

	// 3. 初始化并启动节点
	for i := 0; i < nodeCount; i++ {
		id := i + 1

		// 创建存储层和状态机
		store, sm, err := storage.NewStorage(storage.LSMStorage, c.dataDir, id)
		if err != nil {
			t.Fatalf("failed to create storage for node %d: %v", id, err)
		}
		c.storages[i] = store
		c.stateMachines[i] = sm
		c.commitChans[i] = make(chan param.CommitEntry, 1000)

		// 启动后台协程消费 commitChan
		go func(ch chan param.CommitEntry) {
			for range ch {
				// 丢弃数据，仅为了防止阻塞
				_, _ = store.LastLogIndex()
				_, _ = sm.Get("test-key")
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
			t.Fatalf("failed to start transport for node %d: %v", id, err)
		}

		// 启动 Raft 主循环
		go rf.Run()
	}

	return c
}

func (c *e2eCluster) shutdown() {
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
		close(c.commitChans[i])
	}
}

func (c *e2eCluster) getLeader(t *testing.T) *raft.Raft {
	timeout := time.After(15 * time.Second)
	for i := 0; i < 30; i++ {
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
			t.Fatal("Cluster failed to elect a leader within 15 seconds")
		}
	}
	t.Fatal("Cluster failed to elect a leader")
	return nil
}

func (c *e2eCluster) sendThroughRPC(node *raft.Raft, cmd param.KVCommand, timeout time.Duration) (bool, time.Duration, error) {
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

// sendThroughNetwork 通过真实网络模拟发送请求
// 注意：由于 gRPC 自连接的限制，这里使用 node.ClientRequest 直接调用
// 这样仍然测量了完整的 Raft 处理链路（包括节点间 gRPC 通信）
// 与 sendThroughRPC 的区别在于这个函数可以未来扩展为使用独立客户端连接
func (c *e2eCluster) sendThroughNetwork(node *raft.Raft, cmd param.KVCommand) (bool, time.Duration, error) {
	cmdBytes, _ := json.Marshal(cmd)
	args := &param.ClientArgs{
		ClientID:    rand.Int63(),
		SequenceNum: rand.Int63(),
		Command:     cmdBytes,
	}
	reply := &param.ClientReply{}

	start := time.Now()
	// 直接调用 ClientRequest - 这包含了完整的 Raft 处理流程
	// 包括日志复制（通过节点间 gRPC）、状态机应用等
	err := node.ClientRequest(args, reply)
	latency := time.Since(start)

	success := err == nil && reply.Success
	return success, latency, err
}

// runE2ETest 运行端到端性能测试
func runE2ETest(t *testing.T, testName string, testFunc func(*e2eCluster) PerfMetrics, duration time.Duration) PerfMetrics {
	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)

	// 预热数据
	warmupCount := 1000
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("warmup-key-%d", i)
		value := fmt.Sprintf("warmup-value-%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		cmdBytes, _ := json.Marshal(cmd)
		args := &param.ClientArgs{
			ClientID:    int64(1),
			SequenceNum: int64(i),
			Command:     cmdBytes,
		}
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(args, reply)
	}
	time.Sleep(2 * time.Second)
	metrics := testFunc(c)
	return metrics
}

// TestE2E_WriteHeavy 写入密集型场景
func TestE2E_WriteHeavy(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesWritten int64
	var latenciesMu sync.Mutex
	var latencies []time.Duration

	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Test: WriteHeavy, Leader: Node %d", leader.ID())

	stopCh := make(chan struct{})
	go func() {
		opCount := 0
		for {
			select {
			case <-stopCh:
				break
			default:
				key := fmt.Sprintf("key-%d", rand.Intn(100000))
				value := fmt.Sprintf("val-%d", rand.Intn(1000000))
				cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

				success, latency, _ := c.sendThroughRPC(leader, cmd, 5*time.Second)

				atomic.AddInt64(&totalOps, 1)
				if success {
					atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
					latenciesMu.Lock()
					latencies = append(latencies, latency)
					latenciesMu.Unlock()
				} else {
					atomic.AddInt64(&failedOps, 1)
				}
				opCount++
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)

	latenciesMu.Lock()
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	latenciesMu.Unlock()

	metrics := PerfMetrics{
		TestName:      "WriteHeavy (写密集型)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesWritten:  bytesWritten,
		LatencyP50:    p50,
		LatencyP95:    p95,
		LatencyP99:    p99,
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	printPerfMetrics(t, &metrics)
}

// TestE2E_ReadHeavy 读取密集型场景
func TestE2E_ReadHeavy(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesRead int64
	var latenciesMu sync.Mutex
	var latencies []time.Duration

	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Test: ReadHeavy, Leader: Node %d", leader.ID())

	// 预热数据
	warmupCount := 1000
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("warmup-key-%d", i)
		value := fmt.Sprintf("warmup-value-%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		cmdBytes, _ := json.Marshal(cmd)
		_ = leader.ClientRequest(&param.ClientArgs{
			ClientID:    int64(1),
			SequenceNum: int64(i),
			Command:     cmdBytes,
		}, &param.ClientReply{})
	}
	time.Sleep(2 * time.Second)

	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				break
			default:
				keyNum := rand.Intn(warmupCount)
				key := fmt.Sprintf("warmup-key-%d", keyNum)
				cmd := param.KVCommand{Op: param.OpGet, Key: key, Value: ""}
				success, latency, _ := c.sendThroughRPC(leader, cmd, 5*time.Second)

				atomic.AddInt64(&totalOps, 1)
				if success {
					val, _ := c.stateMachines[leader.ID()-1].Get(key)
					atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
					latenciesMu.Lock()
					latencies = append(latencies, latency)
					latenciesMu.Unlock()
				} else {
					atomic.AddInt64(&failedOps, 1)
				}
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)

	latenciesMu.Lock()
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	latenciesMu.Unlock()

	metrics := PerfMetrics{
		TestName:      "ReadHeavy (读密集型)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesRead:     bytesRead,
		LatencyP50:    p50,
		LatencyP95:    p95,
		LatencyP99:    p99,
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	printPerfMetrics(t, &metrics)
}

// TestE2E_MixedWorkload 混合工作负载
func TestE2E_MixedWorkload(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesRead, bytesWritten int64
	var latenciesMu sync.Mutex
	var latencies []time.Duration

	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Test: MixedWorkload, Leader: Node %d", leader.ID())

	// 预热数据
	warmupCount := 1000
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("warmup-key-%d", i)
		value := fmt.Sprintf("warmup-value-%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		cmdBytes, _ := json.Marshal(cmd)
		_ = leader.ClientRequest(&param.ClientArgs{
			ClientID:    int64(1),
			SequenceNum: int64(i),
			Command:     cmdBytes,
		}, &param.ClientReply{})
	}
	time.Sleep(2 * time.Second)

	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				break
			default:
				isWrite := rand.Float64() < 0.7 // 70% 写入
				keyNum := rand.Intn(warmupCount)
				key := fmt.Sprintf("key-%d", keyNum)

				var success bool
				var latency time.Duration

				if isWrite {
					value := fmt.Sprintf("val-%d", rand.Intn(1000))
					cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
					success, latency, _ = c.sendThroughRPC(leader, cmd, 5*time.Second)
					if success {
						atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
					}
				} else {
					cmd := param.KVCommand{Op: param.OpGet, Key: key}
					success, latency, _ = c.sendThroughRPC(leader, cmd, 5*time.Second)
					if success {
						val, _ := c.stateMachines[leader.ID()-1].Get(key)
						atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
					}
				}

				atomic.AddInt64(&totalOps, 1)
				if !success {
					atomic.AddInt64(&failedOps, 1)
				}
				latenciesMu.Lock()
				latencies = append(latencies, latency)
				latenciesMu.Unlock()
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)

	latenciesMu.Lock()
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	latenciesMu.Unlock()

	metrics := PerfMetrics{
		TestName:      "MixedWorkload (混合负载 - 70%%写/30%%读)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesRead:     bytesRead,
		BytesWritten:  bytesWritten,
		LatencyP50:    p50,
		LatencyP95:    p95,
		LatencyP99:    p99,
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	printPerfMetrics(t, &metrics)
}

// TestE2E_SmallValues 小值操作场景
func TestE2E_SmallValues(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesRead, bytesWritten int64
	var latenciesMu sync.Mutex
	var latencies []time.Duration

	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Test: SmallValues, Leader: %d", leader.ID())

	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				break
			default:
				key := fmt.Sprintf("k%d", rand.Intn(1000))
				isWrite := rand.Float64() < 0.5

				var success bool
				var latency time.Duration

				if isWrite {
					value := fmt.Sprintf("v%d", rand.Intn(1000))
					cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
					success, latency, _ = c.sendThroughRPC(leader, cmd, 5*time.Second)
					if success {
						atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
					}
				} else {
					cmd := param.KVCommand{Op: param.OpGet, Key: key}
					success, latency, _ = c.sendThroughRPC(leader, cmd, 5*time.Second)
					if success {
						val, _ := c.stateMachines[leader.ID()-1].Get(key)
						atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
					}
				}

				atomic.AddInt64(&totalOps, 1)
				if !success {
					atomic.AddInt64(&failedOps, 1)
				}
				latenciesMu.Lock()
				latencies = append(latencies, latency)
				latenciesMu.Unlock()
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)

	latenciesMu.Lock()
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	latenciesMu.Unlock()

	metrics := PerfMetrics{
		TestName:      "SmallValues (小值操作 - 50%%写/50%%读)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		BytesRead:     bytesRead,
		BytesWritten:  bytesWritten,
		LatencyP50:    p50,
		LatencyP95:    p95,
		LatencyP99:    p99,
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	printPerfMetrics(t, &metrics)
}

// TestE2E_BatchOperations 批量操作场景
func TestE2E_BatchOperations(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesRead, bytesWritten int64
	var batchLatenciesMu sync.Mutex
	var batchLatencies []time.Duration

	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Test: BatchOperations, Leader: %d", leader.ID())

	batchSize := 50
	numBatches := 0

	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				break
			default:
				batchStart := time.Now()

				// 写入批量数据
				for i := 0; i < batchSize; i++ {
					key := fmt.Sprintf("batch-%d-key-%d", numBatches, i)
					value := fmt.Sprintf("batch-%d-val-%d", numBatches, i)
					cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
					cmdBytes, _ := json.Marshal(cmd)

					args := &param.ClientArgs{
						ClientID:    int64(1),
						SequenceNum: int64(numBatches*batchSize + i),
						Command:     cmdBytes,
					}
					reply := &param.ClientReply{}
					_ = leader.ClientRequest(args, reply)
				}

				batchLatency := time.Since(batchStart)
				batchLatenciesMu.Lock()
				batchLatencies = append(batchLatencies, batchLatency)
				batchLatenciesMu.Unlock()
				atomic.AddInt64(&totalOps, int64(batchSize))

				// 读取批量数据验证
				time.Sleep(1 * time.Second)

				readSuccess := 0
				for i := 0; i < batchSize; i++ {
					key := fmt.Sprintf("batch-%d-key-%d", numBatches, i)
					cmd := param.KVCommand{Op: param.OpGet, Key: key}
					success, _, _ := c.sendThroughRPC(leader, cmd, 3*time.Second)
					if success {
						readSuccess++
						val, _ := c.stateMachines[leader.ID()-1].Get(key)
						atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
					}
				}

				if readSuccess == batchSize {
					atomic.AddInt64(&bytesWritten, int64(batchSize*20)) // 假设每个值约20字节
				}

				atomic.AddInt64(&failedOps, int64(batchSize-readSuccess))
				numBatches++
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)

	batchLatenciesMu.Lock()
	p50 := percentile(batchLatencies, 50)
	p95 := percentile(batchLatencies, 95)
	p99 := percentile(batchLatencies, 99)
	batchLatenciesMu.Unlock()

	metrics := PerfMetrics{
		TestName:      "BatchOperations (批量操作 - 50条/批)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesRead:     bytesRead,
		BytesWritten:  bytesWritten,
		LatencyP50:    p50,
		LatencyP95:    p95,
		LatencyP99:    p99,
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	printPerfMetrics(t, &metrics)
}

// TestE2E_DeleteOperations 删除操作场景
func TestE2E_DeleteOperations(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps int64
	var latenciesMu sync.Mutex
	var latencies []time.Duration

	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Test: DeleteOperations, Leader: %d", leader.ID())

	// 预热数据
	warmupCount := 500
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("warmup-key-%d", i)
		value := fmt.Sprintf("warmup-value-%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		cmdBytes, _ := json.Marshal(cmd)
		_ = leader.ClientRequest(&param.ClientArgs{
			ClientID:    int64(1),
			SequenceNum: int64(i),
			Command:     cmdBytes,
		}, &param.ClientReply{})
	}
	time.Sleep(1 * time.Second)

	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				break
			default:
				keyNum := rand.Intn(warmupCount)
				key := fmt.Sprintf("warmup-key-%d", keyNum)
				cmd := param.KVCommand{Op: param.OpDelete, Key: key, Value: ""}
				success, latency, _ := c.sendThroughRPC(leader, cmd, 5*time.Second)

				atomic.AddInt64(&totalOps, 1)
				if success {
					latenciesMu.Lock()
					latencies = append(latencies, latency)
					latenciesMu.Unlock()
				} else {
					atomic.AddInt64(&failedOps, 1)
				}
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)

	latenciesMu.Lock()
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	latenciesMu.Unlock()

	metrics := PerfMetrics{
		TestName:      "DeleteOperations (删除操作)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesRead:     0,
		BytesWritten:  0,
		LatencyP50:    p50,
		LatencyP95:    p95,
		LatencyP99:    p99,
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	printPerfMetrics(t, &metrics)
}

func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	idx := int(float64(len(latencies)) * p / 100)
	if idx >= len(latencies) {
		idx = len(latencies) - 1
	}
	return latencies[idx]
}

// printPerfMetrics 打印性能指标（独立函数，供所有测试套件使用）
func printPerfMetrics(t *testing.T, metrics *PerfMetrics) {
	t.Logf("总操作数: %d", metrics.TotalOps)
	t.Logf("成功操作数: %d", metrics.SuccessOps)
	t.Logf("失败操作数: %d", metrics.FailedOps)
	t.Logf("成功率: %.2f%%", float64(metrics.SuccessOps)/float64(metrics.TotalOps)*100)
	t.Logf("吞吐量: %.2f ops/sec", metrics.ThroughputOps)
	if metrics.BytesRead > 0 {
		t.Logf("读取流量: %.2f MB/s", float64(metrics.BytesRead)/1024/1024/metrics.Duration.Seconds())
	}
	if metrics.BytesWritten > 0 {
		t.Logf("写入流量: %.2f MB/s", float64(metrics.BytesWritten)/1024/1024/metrics.Duration.Seconds())
	}

	t.Logf("P50 延迟: %v", metrics.LatencyP50)
	t.Logf("P95 延迟: %v", metrics.LatencyP95)
	t.Logf("P99 延迟: %v", metrics.LatencyP99)
	t.Logf("==================\n")
}

// ==================== 真实网络 E2E 测试套件 ====================
// NetworkE2ETestSuite 使用 SendClientRequest 进行真实网络调用的性能测试
// 这更接近生产环境的实际场景，包含客户端到 Leader 的网络开销

// NetworkE2ETestSuite 真实网络端到端性能测试套件

// TestNetworkE2E_WriteHeavy 真实网络写入密集型测试
func TestNetworkE2E_WriteHeavy(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesWritten int64
	var latenciesMu sync.Mutex
	var latencies []time.Duration

	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("[Network E2E] Test: WriteHeavy, Leader: Node %d, Address: %s", leader.ID(), c.transports[leader.ID()-1].Addr())

	stopCh := make(chan struct{})
	go func() {
		opCount := 0
		for {
			select {
			case <-stopCh:
				return
			default:
				key := fmt.Sprintf("nw-key-%d", rand.Intn(100000))
				value := fmt.Sprintf("nw-val-%d", rand.Intn(1000000))
				cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

				success, latency, _ := c.sendThroughNetwork(leader, cmd)

				atomic.AddInt64(&totalOps, 1)
				if success {
					atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
					latenciesMu.Lock()
					latencies = append(latencies, latency)
					latenciesMu.Unlock()
				} else {
					atomic.AddInt64(&failedOps, 1)
				}
				opCount++
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)

	latenciesMu.Lock()
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	latenciesMu.Unlock()

	metrics := PerfMetrics{
		TestName:      "[Network E2E] WriteHeavy (写密集型)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesWritten:  bytesWritten,
		LatencyP50:    p50,
		LatencyP95:    p95,
		LatencyP99:    p99,
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	printPerfMetrics(t, &metrics)
}

// TestNetworkE2E_ReadHeavy 真实网络读取密集型测试
func TestNetworkE2E_ReadHeavy(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesRead int64
	var latenciesMu sync.Mutex
	var latencies []time.Duration

	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("[Network E2E] Test: ReadHeavy, Leader: Node %d", leader.ID())

	// 预热数据 - 使用真实网络请求
	warmupCount := 1000
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("nw-warmup-key-%d", i)
		value := fmt.Sprintf("nw-warmup-value-%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		c.sendThroughNetwork(leader, cmd)
	}
	time.Sleep(2 * time.Second)

	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				keyNum := rand.Intn(warmupCount)
				key := fmt.Sprintf("nw-warmup-key-%d", keyNum)
				cmd := param.KVCommand{Op: param.OpGet, Key: key, Value: ""}
				success, latency, _ := c.sendThroughNetwork(leader, cmd)

				atomic.AddInt64(&totalOps, 1)
				if success {
					val, _ := c.stateMachines[leader.ID()-1].Get(key)
					atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
					latenciesMu.Lock()
					latencies = append(latencies, latency)
					latenciesMu.Unlock()
				} else {
					atomic.AddInt64(&failedOps, 1)
				}
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)

	latenciesMu.Lock()
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	latenciesMu.Unlock()

	metrics := PerfMetrics{
		TestName:      "[Network E2E] ReadHeavy (读密集型)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesRead:     bytesRead,
		LatencyP50:    p50,
		LatencyP95:    p95,
		LatencyP99:    p99,
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	printPerfMetrics(t, &metrics)
}

// TestNetworkE2E_MixedWorkload 真实网络混合工作负载测试
func TestNetworkE2E_MixedWorkload(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesRead, bytesWritten int64
	var latenciesMu sync.Mutex
	var latencies []time.Duration

	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("[Network E2E] Test: MixedWorkload, Leader: Node %d", leader.ID())

	// 预热数据
	warmupCount := 1000
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("nw-mixed-warmup-%d", i)
		value := fmt.Sprintf("nw-mixed-val-%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		c.sendThroughNetwork(leader, cmd)
	}
	time.Sleep(2 * time.Second)

	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				isWrite := rand.Float64() < 0.7 // 70% 写入
				keyNum := rand.Intn(warmupCount)
				key := fmt.Sprintf("nw-mixed-warmup-%d", keyNum)

				var success bool
				var latency time.Duration

				if isWrite {
					value := fmt.Sprintf("nw-mixed-val-%d", rand.Intn(1000))
					cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
					success, latency, _ = c.sendThroughNetwork(leader, cmd)
					if success {
						atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
					}
				} else {
					cmd := param.KVCommand{Op: param.OpGet, Key: key, Value: ""}
					success, latency, _ = c.sendThroughNetwork(leader, cmd)
					if success {
						val, _ := c.stateMachines[leader.ID()-1].Get(key)
						atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
					}
				}

				atomic.AddInt64(&totalOps, 1)
				if !success {
					atomic.AddInt64(&failedOps, 1)
				}
				latenciesMu.Lock()
				latencies = append(latencies, latency)
				latenciesMu.Unlock()
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)

	latenciesMu.Lock()
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	latenciesMu.Unlock()

	metrics := PerfMetrics{
		TestName:      "[Network E2E] MixedWorkload (混合负载 - 70%%写/30%%读)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesRead:     bytesRead,
		BytesWritten:  bytesWritten,
		LatencyP50:    p50,
		LatencyP95:    p95,
		LatencyP99:    p99,
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	printPerfMetrics(t, &metrics)
}

// TestNetworkE2E_SmallValues 真实网络小值操作测试
func TestNetworkE2E_SmallValues(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesRead, bytesWritten int64
	var latenciesMu sync.Mutex
	var latencies []time.Duration

	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("[Network E2E] Test: SmallValues, Leader: Node %d", leader.ID())

	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				key := fmt.Sprintf("nw-k%d", rand.Intn(1000))
				isWrite := rand.Float64() < 0.5

				var success bool
				var latency time.Duration

				if isWrite {
					value := fmt.Sprintf("nw-v%d", rand.Intn(1000))
					cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
					success, latency, _ = c.sendThroughNetwork(leader, cmd)
					if success {
						atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
					}
				} else {
					cmd := param.KVCommand{Op: param.OpGet, Key: key, Value: ""}
					success, latency, _ = c.sendThroughNetwork(leader, cmd)
					if success {
						val, _ := c.stateMachines[leader.ID()-1].Get(key)
						atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
					}
				}

				atomic.AddInt64(&totalOps, 1)
				if !success {
					atomic.AddInt64(&failedOps, 1)
				}
				latenciesMu.Lock()
				latencies = append(latencies, latency)
				latenciesMu.Unlock()
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)

	latenciesMu.Lock()
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	latenciesMu.Unlock()

	metrics := PerfMetrics{
		TestName:      "[Network E2E] SmallValues (小值操作 - 50%%写/50%%读)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesRead:     bytesRead,
		BytesWritten:  bytesWritten,
		LatencyP50:    p50,
		LatencyP95:    p95,
		LatencyP99:    p99,
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	printPerfMetrics(t, &metrics)
}

// TestNetworkE2E_BatchOperations 真实网络批量操作测试
func TestNetworkE2E_BatchOperations(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesRead, bytesWritten int64
	var batchLatenciesMu sync.Mutex
	var batchLatencies []time.Duration

	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("[Network E2E] Test: BatchOperations, Leader: Node %d", leader.ID())

	batchSize := 50
	numBatches := 0

	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				batchStart := time.Now()

				// 使用网络请求写入批量数据
				for i := 0; i < batchSize; i++ {
					key := fmt.Sprintf("nw-batch-%d-key-%d", numBatches, i)
					value := fmt.Sprintf("nw-batch-%d-val-%d", numBatches, i)
					cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
					c.sendThroughNetwork(leader, cmd)
				}

				batchLatency := time.Since(batchStart)
				batchLatenciesMu.Lock()
				batchLatencies = append(batchLatencies, batchLatency)
				batchLatenciesMu.Unlock()
				atomic.AddInt64(&totalOps, int64(batchSize))

				// 等待数据同步
				time.Sleep(1 * time.Second)

				// 读取批量数据验证
				readSuccess := 0
				for i := 0; i < batchSize; i++ {
					key := fmt.Sprintf("nw-batch-%d-key-%d", numBatches, i)
					cmd := param.KVCommand{Op: param.OpGet, Key: key, Value: ""}
					success, _, _ := c.sendThroughNetwork(leader, cmd)
					if success {
						readSuccess++
						val, _ := c.stateMachines[leader.ID()-1].Get(key)
						atomic.AddInt64(&bytesRead, int64(len(key)+len(val)))
					}
				}

				if readSuccess == batchSize {
					atomic.AddInt64(&bytesWritten, int64(batchSize*20))
				}
				atomic.AddInt64(&failedOps, int64(batchSize-readSuccess))
				numBatches++
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)

	batchLatenciesMu.Lock()
	p50 := percentile(batchLatencies, 50)
	p95 := percentile(batchLatencies, 95)
	p99 := percentile(batchLatencies, 99)
	batchLatenciesMu.Unlock()

	metrics := PerfMetrics{
		TestName:      "[Network E2E] BatchOperations (批量操作 - 50条/批)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesRead:     bytesRead,
		BytesWritten:  bytesWritten,
		LatencyP50:    p50,
		LatencyP95:    p95,
		LatencyP99:    p99,
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	printPerfMetrics(t, &metrics)
}

// TestNetworkE2E_DeleteOperations 真实网络删除操作测试
func TestNetworkE2E_DeleteOperations(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps int64
	var latenciesMu sync.Mutex
	var latencies []time.Duration

	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("[Network E2E] Test: DeleteOperations, Leader: Node %d", leader.ID())

	// 预热数据
	warmupCount := 500
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("nw-del-warmup-%d", i)
		value := fmt.Sprintf("nw-del-val-%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		c.sendThroughNetwork(leader, cmd)
	}
	time.Sleep(1 * time.Second)

	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				keyNum := rand.Intn(warmupCount)
				key := fmt.Sprintf("nw-del-warmup-%d", keyNum)
				cmd := param.KVCommand{Op: param.OpDelete, Key: key, Value: ""}
				success, latency, _ := c.sendThroughNetwork(leader, cmd)

				atomic.AddInt64(&totalOps, 1)
				if success {
					latenciesMu.Lock()
					latencies = append(latencies, latency)
					latenciesMu.Unlock()
				} else {
					atomic.AddInt64(&failedOps, 1)
				}
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)

	latenciesMu.Lock()
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	latenciesMu.Unlock()

	metrics := PerfMetrics{
		TestName:      "[Network E2E] DeleteOperations (删除操作)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesRead:     0,
		BytesWritten:  0,
		LatencyP50:    p50,
		LatencyP95:    p95,
		LatencyP99:    p99,
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	printPerfMetrics(t, &metrics)
}

// TestNetworkE2E_ConcurrentClients 多并发客户端真实网络测试
func TestNetworkE2E_ConcurrentClients(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesWritten int64
	var latenciesMu sync.Mutex
	var latencies []time.Duration

	c := newE2ECluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("[Network E2E] Test: ConcurrentClients, Leader: Node %d", leader.ID())

	// 模拟多个并发客户端
	numClients := 5
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numClients)

	for clientID := 0; clientID < numClients; clientID++ {
		go func(cid int) {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					key := fmt.Sprintf("nw-client-%d-key-%d", cid, rand.Intn(10000))
					value := fmt.Sprintf("nw-client-%d-val-%d", cid, rand.Intn(1000))
					cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

					success, latency, _ := c.sendThroughNetwork(leader, cmd)

					atomic.AddInt64(&totalOps, 1)
					if success {
						atomic.AddInt64(&bytesWritten, int64(len(key)+len(value)))
						latenciesMu.Lock()
						latencies = append(latencies, latency)
						latenciesMu.Unlock()
					} else {
						atomic.AddInt64(&failedOps, 1)
					}
				}
			}
		}(clientID)
	}

	time.Sleep(duration)
	close(stopCh)
	wg.Wait()

	latenciesMu.Lock()
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	latenciesMu.Unlock()

	metrics := PerfMetrics{
		TestName:      fmt.Sprintf("[Network E2E] ConcurrentClients (%d并发客户端)", numClients),
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesWritten:  bytesWritten,
		LatencyP50:    p50,
		LatencyP95:    p95,
		LatencyP99:    p99,
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	printPerfMetrics(t, &metrics)
}

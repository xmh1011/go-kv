package tests

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
	"github.com/xmh1011/go-kv/pkg/transport"
	"github.com/xmh1011/go-kv/raft"
)

// perfCluster 端到端性能测试集群
type perfCluster struct {
	nodes         []*raft.Raft
	transports    []transport.Transport
	storages      []storage.Storage
	stateMachines []storage.StateMachine
	commitChans   []chan param.CommitEntry
	peerMap       map[int]string
	dataDir       string
}

// newPerfCluster 创建用于端到端性能测试的集群
func newPerfCluster(t *testing.T, nodeCount int) *perfCluster {
	c := &perfCluster{
		nodes:         make([]*raft.Raft, nodeCount),
		transports:    make([]transport.Transport, nodeCount),
		storages:      make([]storage.Storage, nodeCount),
		stateMachines: make([]storage.StateMachine, nodeCount),
		commitChans:   make([]chan param.CommitEntry, nodeCount),
		peerMap:       make(map[int]string),
		dataDir:       t.TempDir(),
	}

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

	initialPeerIDs := make([]int, 0)
	for i := 0; i < nodeCount; i++ {
		initialPeerIDs = append(initialPeerIDs, i+1)
	}

	for i := 0; i < nodeCount; i++ {
		id := i + 1
		store, sm, err := storage.NewStorage(storage.LSMStorage, c.dataDir, id)
		if err != nil {
			t.Fatalf("failed to create storage for node %d: %v", id, err)
		}
		c.storages[i] = store
		c.stateMachines[i] = sm
		c.commitChans[i] = make(chan param.CommitEntry, 1000)
		c.peerMap[id] = c.transports[i].Addr()

		go func(ch chan param.CommitEntry) {
			for range ch {
				// Raft applies entries before publishing commit notifications.
				// These performance tests read state machines directly, so the
				// channel only needs to be drained to avoid apply backpressure.
			}
		}(c.commitChans[i])

		rf := raft.NewRaft(id, initialPeerIDs, store, sm, c.transports[i], c.commitChans[i])
		c.nodes[i] = rf

		c.transports[i].RegisterRaft(rf)

		if err := c.transports[i].Start(); err != nil {
			t.Fatalf("failed to start transport for node %d: %v", id, err)
		}

		go rf.Run()
	}

	return c
}

func (c *perfCluster) shutdown() {
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

func (c *perfCluster) getLeader(t *testing.T) *raft.Raft {
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

func (c *perfCluster) sendThroughRPC(node *raft.Raft, cmd param.KVCommand, timeout time.Duration) (bool, time.Duration, error) {
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

// runPerfTest 运行端到端性能测试
func runPerfTest(t *testing.T, testName string, testFunc func(*perfCluster) PerfMetrics, duration time.Duration) PerfMetrics {
	c := newPerfCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Test: %s, Leader: Node %d", testName, leader.ID())

	warmupCount := 500
	t.Logf("Warmup: writing %d entries...", warmupCount)
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

// PerfTestSuite 端到端性能测试套件
type PerfTestSuite struct{}

// TestPerf_WriteHeavy 写入密集型场景
func (s *PerfTestSuite) TestPerf_WriteHeavy(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesWritten int64
	var latencies []time.Duration

	c := newPerfCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Test: WriteHeavy, Leader: Node %d", leader.ID())

	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		opCount := 0
		for {
			select {
			case <-stopCh:
				return
			default:
				key := fmt.Sprintf("key-%d", rand.Intn(10000))
				value := fmt.Sprintf("val-%d", rand.Intn(100000))
				cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}

				success, latency, _ := c.sendThroughRPC(leader, cmd, 5*time.Second)

				totalOps++
				if success {
					bytesWritten += int64(len(key) + len(value))
					latencies = append(latencies, latency)
				} else {
					failedOps++
				}
				opCount++
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)
	wg.Wait()

	metrics := PerfMetrics{
		TestName:      "WriteHeavy (写密集型)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesWritten:  bytesWritten,
		LatencyP50:    percentile(latencies, 50),
		LatencyP95:    percentile(latencies, 95),
		LatencyP99:    percentile(latencies, 99),
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	s.printMetrics(t, &metrics)
}

// TestPerf_ReadHeavy 读取密集型场景
func (s *PerfTestSuite) TestPerf_ReadHeavy(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesRead int64
	var latencies []time.Duration

	c := newPerfCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Test: ReadHeavy, Leader: Node %d", leader.ID())

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
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				keyNum := rand.Intn(warmupCount)
				key := fmt.Sprintf("key-%d", keyNum)
				cmd := param.KVCommand{Op: param.OpGet, Key: key}
				success, latency, _ := c.sendThroughRPC(leader, cmd, 5*time.Second)

				totalOps++
				if success {
					val, _ := c.stateMachines[leader.ID()-1].Get(key)
					bytesRead += int64(len(key) + len(val))
					latencies = append(latencies, latency)
				} else {
					failedOps++
				}
			}
		}
	}()

	time.Sleep(duration)
	close(stopCh)
	wg.Wait()

	metrics := PerfMetrics{
		TestName:      "ReadHeavy (读密集型)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		FailedOps:     failedOps,
		BytesRead:     bytesRead,
		LatencyP50:    percentile(latencies, 50),
		LatencyP95:    percentile(latencies, 95),
		LatencyP99:    percentile(latencies, 99),
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	s.printMetrics(t, &metrics)
}

// TestPerf_MixedWorkload 混合工作负载
func (s *PerfTestSuite) TestPerf_MixedWorkload(t *testing.T) {
	duration := 30 * time.Second
	var totalOps, failedOps, bytesRead, bytesWritten int64
	var latencies []time.Duration

	c := newPerfCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)
	t.Logf("Test: MixedWorkload, Leader: Node %d", leader.ID())

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
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
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
						bytesWritten += int64(len(key) + len(value))
						latencies = append(latencies, latency)
					}
				} else {
					cmd := param.KVCommand{Op: param.OpGet, Key: key}
					success, latency, _ = c.sendThroughRPC(leader, cmd, 5*time.Second)
					if success {
						val, _ := c.stateMachines[leader.ID()-1].Get(key)
						bytesRead += int64(len(key) + len(val))
					}
				}

				totalOps++
				if !success {
					failedOps++
				}
				latencies = append(latencies, latency)
			}
		}

	}()

	time.Sleep(duration)
	close(stopCh)
	wg.Wait()

	metrics := PerfMetrics{
		TestName:      "MixedWorkload (混合负载 - 70%%写/30%%读)",
		Duration:      duration,
		TotalOps:      totalOps,
		SuccessOps:    totalOps - failedOps,
		BytesRead:     bytesRead,
		BytesWritten:  bytesWritten,
		LatencyP50:    percentile(latencies, 50),
		LatencyP95:    percentile(latencies, 95),
		LatencyP99:    percentile(latencies, 99),
		ThroughputOps: float64(totalOps-failedOps) / duration.Seconds(),
		ErrorRate:     float64(failedOps) / float64(totalOps) * 100,
	}

	s.printMetrics(t, &metrics)
}

// printMetrics 打印性能指标
func (s *PerfTestSuite) printMetrics(t *testing.T, metrics *PerfMetrics) {
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

	t.Logf("===================\n")

	assert.Greater(t, metrics.SuccessOps, int64(0), "should have successful operations")
}

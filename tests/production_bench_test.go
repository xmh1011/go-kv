package tests

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
	"github.com/xmh1011/go-kv/pkg/transport"
	"github.com/xmh1011/go-kv/raft"
)

// productionCluster 创建生产环境类似配置的集群（grpc + lsm）
type productionCluster struct {
	nodes         []*raft.Raft
	transports    []transport.Transport
	storages      []storage.Storage
	stateMachines []storage.StateMachine
	commitChans   []chan param.CommitEntry
	peerMap       map[int]string
	dataDir       string
}

// newProductionCluster 创建用于生产性能测试的集群
// 使用 gRPC 传输层和 LSM 存储引擎，更贴近生产环境
func newProductionCluster(t *testing.T, nodeCount int) *productionCluster {
	c := &productionCluster{
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

		// 启动后台协程消费 commitChan。Raft 在发送提交通知前已经应用
		// 状态机；benchmark 只需要 drain，不能再次 Apply。
		go func(ch chan param.CommitEntry) {
			for range ch {
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

func (c *productionCluster) shutdown() {
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

func (c *productionCluster) getLeader(t *testing.T) *raft.Raft {
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

// ========== 生产环境基准测试 ==========

// BenchmarkProduction_GrpcLsm_3Nodes 测试3节点 gRPC+LSM 配置的生产性能
func BenchmarkProduction_GrpcLsm_3Nodes(b *testing.B) {
	t := &testing.T{}
	c := newProductionCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)

	// 预热
	for i := 0; i < 1000; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("warmup-key-%d", i), Value: fmt.Sprintf("warmup-value-%d", i)}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	time.Sleep(2 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%10000)
		value := fmt.Sprintf("value%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
}

// BenchmarkProduction_GrpcLsm_ConcurrentWrites 测试并发写入的生产性能
func BenchmarkProduction_GrpcLsm_ConcurrentWrites(b *testing.B) {
	t := &testing.T{}
	c := newProductionCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)

	// 预热
	for i := 0; i < 1000; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("warmup-key-%d", i), Value: fmt.Sprintf("warmup-value-%d", i)}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	time.Sleep(2 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := rand.Int63()
		for pb.Next() {
			key := fmt.Sprintf("key%d", i%100000)
			value := fmt.Sprintf("value%d", i)
			cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
			cmdBytes, _ := json.Marshal(cmd)
			reply := &param.ClientReply{}
			_ = leader.ClientRequest(&param.ClientArgs{ClientID: rand.Int63(), SequenceNum: i, Command: cmdBytes}, reply)
			i++
		}
	})
}

// BenchmarkProduction_GrpcLsm_SmallKeys 测试小键值对的生产性能
func BenchmarkProduction_GrpcLsm_SmallKeys(b *testing.B) {
	t := &testing.T{}
	c := newProductionCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: "k", Value: "v"}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
}

// BenchmarkProduction_GrpcLsm_MediumKeys 测试中等大小键值对的生产性能
func BenchmarkProduction_GrpcLsm_MediumKeys(b *testing.B) {
	t := &testing.T{}
	c := newProductionCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)

	mediumValue := string(make([]byte, 256)) // 256B value

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: mediumValue}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
}

// BenchmarkProduction_GrpcLsm_LargeKeys 测试大键值对的生产性能
func BenchmarkProduction_GrpcLsm_LargeKeys(b *testing.B) {
	t := &testing.T{}
	c := newProductionCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)

	largeValue := string(make([]byte, 4096)) // 4KB value

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: largeValue}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
}

// BenchmarkProduction_GrpcLsm_MixedWorkload 测试混合工作负载的生产性能
func BenchmarkProduction_GrpcLsm_MixedWorkload(b *testing.B) {
	t := &testing.T{}
	c := newProductionCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)

	// 预热数据
	warmupCount := 10000
	for i := 0; i < warmupCount; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("warmup-key-%d", i), Value: fmt.Sprintf("warmup-value-%d", i)}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	time.Sleep(2 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := rand.Int63()
		for pb.Next() {
			keyNum := i % int64(warmupCount)
			key := fmt.Sprintf("warmup-key-%d", keyNum)

			cmdBytes, _ := json.Marshal(param.KVCommand{
				Op:    param.OpSet,
				Key:   key,
				Value: fmt.Sprintf("value%d", i),
			})
			reply := &param.ClientReply{}
			_ = leader.ClientRequest(&param.ClientArgs{ClientID: rand.Int63(), SequenceNum: i, Command: cmdBytes}, reply)
			i++
		}
	})
}

// BenchmarkProduction_GrpcLsm_ReadAfterWrite 测试写后读的生产性能
func BenchmarkProduction_GrpcLsm_ReadAfterWrite(b *testing.B) {
	t := &testing.T{}
	c := newProductionCluster(t, 3)
	defer c.shutdown()

	leader := c.getLeader(t)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		value := fmt.Sprintf("value%d", i)

		// 写入
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i) * 2, Command: cmdBytes}, reply)

		// 读取
		cmd = param.KVCommand{Op: param.OpGet, Key: key}
		cmdBytes, _ = json.Marshal(cmd)
		reply = &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i)*2 + 1, Command: cmdBytes}, reply)
	}
}

// BenchmarkProduction_GrpcLsm_5Nodes 测试5节点集群的生产性能
func BenchmarkProduction_GrpcLsm_5Nodes(b *testing.B) {
	t := &testing.T{}
	c := newProductionCluster(t, 5)
	defer c.shutdown()

	leader := c.getLeader(t)

	// 预热
	for i := 0; i < 1000; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("warmup-key-%d", i), Value: fmt.Sprintf("warmup-value-%d", i)}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	time.Sleep(2 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%10000)
		value := fmt.Sprintf("value%d", i)
		cmd := param.KVCommand{Op: param.OpSet, Key: key, Value: value}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
}

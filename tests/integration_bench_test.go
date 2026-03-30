package tests

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
	"github.com/xmh1011/go-kv/pkg/transport"
	"github.com/xmh1011/go-kv/pkg/transport/inmemory"
	"github.com/xmh1011/go-kv/raft"
)

// benchCluster holds cluster resources for benchmark tests
type benchCluster struct {
	nodes      []*raft.Raft
	transports []transport.Transport
	stores     []storage.Storage
	leader     *raft.Raft
}

// setupBenchCluster creates a 3-node Raft cluster for benchmarks.
// storageType: storage.InmemoryStorage or storage.LSMStorage
// transportType: transport.InMemoryTransport or transport.TcpTransport
func setupBenchCluster(b *testing.B, storageType string, transportType string) *benchCluster {
	b.Helper()
	dataDir := b.TempDir()
	peerMap := make(map[int]string)
	c := &benchCluster{}

	// Create transports
	for i := 0; i < 3; i++ {
		trans, err := transport.NewTransport(transportType, "127.0.0.1:0")
		if err != nil {
			b.Fatalf("Failed to create transport: %v", err)
		}
		c.transports = append(c.transports, trans)
		peerMap[i+1] = trans.Addr()
	}

	// Create Raft nodes
	for i := 0; i < 3; i++ {
		store, sm, err := storage.NewStorage(storageType, dataDir, i+1)
		if err != nil {
			b.Fatalf("Failed to create storage: %v", err)
		}
		c.stores = append(c.stores, store)
		commitChan := make(chan param.CommitEntry, 1000)

		go func(ch chan param.CommitEntry) {
			for range ch {
			}
		}(commitChan)

		c.transports[i].SetPeers(peerMap)

		rf := raft.NewRaft(i+1, []int{1, 2, 3}, store, sm, c.transports[i], commitChan)
		c.transports[i].RegisterRaft(rf)
		c.transports[i].Start()
		go rf.Run()
		c.nodes = append(c.nodes, rf)
	}

	// Connect InMemory transports
	for i := 0; i < 3; i++ {
		if imTrans, ok := c.transports[i].(*inmemory.Transport); ok {
			for j := 0; j < 3; j++ {
				if i != j {
					imTrans.Connect(c.transports[j].Addr(), c.nodes[j])
				}
			}
		}
	}

	// Wait for leader election with polling
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if n.State() == raft.Leader {
				c.leader = n
				break
			}
		}
		if c.leader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if c.leader == nil {
		b.Fatal("No leader elected within 5s")
	}

	return c
}

func (c *benchCluster) cleanup() {
	for _, n := range c.nodes {
		n.Stop()
	}
	for _, t := range c.transports {
		t.Close()
	}
	for _, s := range c.stores {
		s.Close()
	}
}

// BenchmarkCluster_3NodesInmemory 测试 3 节点 InMemory 传输的性能
func BenchmarkCluster_3NodesInmemory(b *testing.B) {
	c := setupBenchCluster(b, storage.InmemoryStorage, transport.InMemoryTransport)
	defer c.cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("key%d", i%1000), Value: fmt.Sprintf("value%d", i)}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = c.leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	b.StopTimer()
}

// BenchmarkCluster_3NodesTcp 测试 3 节点 TCP 传输的性能
func BenchmarkCluster_3NodesTcp(b *testing.B) {
	c := setupBenchCluster(b, storage.InmemoryStorage, transport.TcpTransport)
	defer c.cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("key%d", i%1000), Value: fmt.Sprintf("value%d", i)}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = c.leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	b.StopTimer()
}

// BenchmarkCluster_ConcurrentWrites 测试并发写入的性能
func BenchmarkCluster_ConcurrentWrites(b *testing.B) {
	c := setupBenchCluster(b, storage.InmemoryStorage, transport.InMemoryTransport)
	defer c.cleanup()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("key%d", i%10000), Value: fmt.Sprintf("value%d", i)}
			cmdBytes, _ := json.Marshal(cmd)
			reply := &param.ClientReply{}
			_ = c.leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
			i++
		}
	})
	b.StopTimer()
}

// BenchmarkCluster_SmallKeys 测试小键值对的性能
func BenchmarkCluster_SmallKeys(b *testing.B) {
	c := setupBenchCluster(b, storage.InmemoryStorage, transport.InMemoryTransport)
	defer c.cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: "k", Value: "v"}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = c.leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	b.StopTimer()
}

// BenchmarkCluster_MediumKeys 测试中等大小键值对的性能
func BenchmarkCluster_MediumKeys(b *testing.B) {
	c := setupBenchCluster(b, storage.InmemoryStorage, transport.InMemoryTransport)
	defer c.cleanup()

	mediumValue := string(make([]byte, 256)) // 256B value

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("key%d", i%1000), Value: mediumValue}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = c.leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	b.StopTimer()
}

// BenchmarkCluster_LargeKeys 测试大键值对的性能
func BenchmarkCluster_LargeKeys(b *testing.B) {
	c := setupBenchCluster(b, storage.InmemoryStorage, transport.InMemoryTransport)
	defer c.cleanup()

	largeValue := string(make([]byte, 4096)) // 4KB value

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("key%d", i%1000), Value: largeValue}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = c.leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	b.StopTimer()
}

// BenchmarkCluster_3NodesLSM 测试 3 节点 LSM 存储的性能
func BenchmarkCluster_3NodesLSM(b *testing.B) {
	c := setupBenchCluster(b, storage.LSMStorage, transport.InMemoryTransport)
	defer c.cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("key%d", i%1000), Value: fmt.Sprintf("value%d", i)}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = c.leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	b.StopTimer()
}

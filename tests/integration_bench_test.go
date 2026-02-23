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

// BenchmarkCluster_3NodesInmemory 测试3 节点 InMemory 传输的性能
func BenchmarkCluster_3NodesInmemory(b *testing.B) {
	dataDir := b.TempDir()
	peerMap := make(map[int]string)
	var nodes []*raft.Raft
	var stateMachines []storage.StateMachine
	var stores []storage.Storage
	var commitChans []chan param.CommitEntry

	// Create transports and get their addresses
	var transports []transport.Transport
	for i := 0; i < 3; i++ {
		trans, _ := transport.NewTransport(transport.InMemoryTransport, "127.0.0.1:0")
		transports = append(transports, trans)
		peerMap[i+1] = trans.Addr()
	}

	// Create Raft nodes
	for i := 0; i < 3; i++ {
		store, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, i+1)
		stores = append(stores, store)
		stateMachines = append(stateMachines, sm)
		commitChan := make(chan param.CommitEntry, 1000)
		commitChans = append(commitChans, commitChan)

		go func(ch chan param.CommitEntry) {
			for range ch {
			}
		}(commitChan)

		transports[i].SetPeers(peerMap)

		rf := raft.NewRaft(i+1, []int{1, 2, 3}, store, sm, transports[i], commitChan)
		transports[i].RegisterRaft(rf)
		transports[i].Start()
		go rf.Run()
		nodes = append(nodes, rf)
	}

	// 在 InMemoryTransport 中，需要让每个节点的 Transport 连接到其他节点
	// Connect 方法是 InMemoryTransport 特有的方法，需要类型断言
	for i := 0; i < 3; i++ {
		if imTrans, ok := transports[i].(*inmemory.Transport); ok {
			for j := 0; j < 3; j++ {
				if i != j {
					imTrans.Connect(transports[j].Addr(), nodes[j])
				}
			}
		}
	}

	// Wait for leader election
	time.Sleep(800 * time.Millisecond)

	// Find the leader
	var leader *raft.Raft
	for _, n := range nodes {
		if n.State() == raft.Leader {
			leader = n
			break
		}
	}

	if leader == nil {
		b.Fatal("No leader elected")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("key%d", i%1000), Value: fmt.Sprintf("value%d", i)}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	b.StopTimer()

	// Cleanup
	for _, n := range nodes {
		n.Stop()
	}
	for _, t := range transports {
		t.Close()
	}
	for _, s := range stores {
		s.Close()
	}
}

// BenchmarkCluster_3NodesTcp 测试 3 节点 TCP 传输的性能
func BenchmarkCluster_3NodesTcp(b *testing.B) {
	dataDir := b.TempDir()
	peerMap := make(map[int]string)
	var nodes []*raft.Raft
	var stateMachines []storage.StateMachine
	var stores []storage.Storage
	var commitChans []chan param.CommitEntry

	// Create transports and get their addresses
	var transports []transport.Transport
	for i := 0; i < 3; i++ {
		trans, _ := transport.NewTransport(transport.TcpTransport, "127.0.0.1:0")
		transports = append(transports, trans)
		peerMap[i+1] = trans.Addr()
	}

	// Create Raft nodes
	for i := 0; i < 3; i++ {
		store, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, i+1)
		stores = append(stores, store)
		stateMachines = append(stateMachines, sm)
		commitChan := make(chan param.CommitEntry, 1000)
		commitChans = append(commitChans, commitChan)

		go func(ch chan param.CommitEntry) {
			for range ch {
			}
		}(commitChan)

		transports[i].SetPeers(peerMap)

		rf := raft.NewRaft(i+1, []int{1, 2, 3}, store, sm, transports[i], commitChan)
		transports[i].RegisterRaft(rf)
		transports[i].Start()
		go rf.Run()
		nodes = append(nodes, rf)
	}

	// 在 InMemoryTransport 中，需要让每个节点的 Transport 连接到其他节点
	// Connect 方法是 InMemoryTransport 特有的方法，需要类型断言
	for i := 0; i < 3; i++ {
		if imTrans, ok := transports[i].(*inmemory.Transport); ok {
			for j := 0; j < 3; j++ {
				if i != j {
					imTrans.Connect(transports[j].Addr(), nodes[j])
				}
			}
		}
	}

	// Wait for leader election
	time.Sleep(800 * time.Millisecond)

	// Find the leader
	var leader *raft.Raft
	for _, n := range nodes {
		if n.State() == raft.Leader {
			leader = n
			break
		}
	}

	if leader == nil {
		b.Fatal("No leader elected")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("key%d", i%1000), Value: fmt.Sprintf("value%d", i)}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	b.StopTimer()

	// Cleanup
	for _, n := range nodes {
		n.Stop()
	}
	for _, t := range transports {
		t.Close()
	}
	for _, s := range stores {
		s.Close()
	}
}

// BenchmarkCluster_ConcurrentWrites 测试并发写入的性能
func BenchmarkCluster_ConcurrentWrites(b *testing.B) {
	dataDir := b.TempDir()
	peerMap := make(map[int]string)
	var nodes []*raft.Raft
	var stores []storage.Storage
	var transports []transport.Transport

	// Create transports and get their addresses
	for i := 0; i < 3; i++ {
		trans, _ := transport.NewTransport(transport.InMemoryTransport, "127.0.0.1:0")
		transports = append(transports, trans)
		peerMap[i+1] = trans.Addr()
	}

	// Create Raft nodes
	for i := 0; i < 3; i++ {
		store, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, i+1)
		stores = append(stores, store)
		commitChan := make(chan param.CommitEntry, 1000)

		go func(ch chan param.CommitEntry) {
			for range ch {
			}
		}(commitChan)

		transports[i].SetPeers(peerMap)

		rf := raft.NewRaft(i+1, []int{1, 2, 3}, store, sm, transports[i], commitChan)
		transports[i].RegisterRaft(rf)
		transports[i].Start()
		go rf.Run()
		nodes = append(nodes, rf)
	}

	// 在 InMemoryTransport 中，需要让每个节点的 Transport 连接到其他节点
	// Connect 方法是 InMemoryTransport 特有的方法，需要类型断言
	for i := 0; i < 3; i++ {
		if imTrans, ok := transports[i].(*inmemory.Transport); ok {
			for j := 0; j < 3; j++ {
				if i != j {
					imTrans.Connect(transports[j].Addr(), nodes[j])
				}
			}
		}
	}

	// Wait for leader election
	time.Sleep(800 * time.Millisecond)

	// Find the leader
	var leader *raft.Raft
	for _, n := range nodes {
		if n.State() == raft.Leader {
			leader = n
			break
		}
	}

	if leader == nil {
		b.Fatal("No leader elected")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("key%d", i%10000), Value: fmt.Sprintf("value%d", i)}
			cmdBytes, _ := json.Marshal(cmd)
			reply := &param.ClientReply{}
			_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
			i++
		}
	})
	b.StopTimer()

	// Cleanup
	for _, n := range nodes {
		n.Stop()
	}
	for _, t := range transports {
		t.Close()
	}
	for _, s := range stores {
		s.Close()
	}
}

// BenchmarkCluster_SmallKeys 测试小键值对的性能
func BenchmarkCluster_SmallKeys(b *testing.B) {
	dataDir := b.TempDir()
	peerMap := make(map[int]string)
	var nodes []*raft.Raft
	var stores []storage.Storage
	var transports []transport.Transport

	// Create transports and get their addresses
	for i := 0; i < 3; i++ {
		trans, _ := transport.NewTransport(transport.InMemoryTransport, "127.0.0.1:0")
		transports = append(transports, trans)
		peerMap[i+1] = trans.Addr()
	}

	// Create Raft nodes
	for i := 0; i < 3; i++ {
		store, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, i+1)
		stores = append(stores, store)
		commitChan := make(chan param.CommitEntry, 1000)

		go func(ch chan param.CommitEntry) {
			for range ch {
			}
		}(commitChan)

		transports[i].SetPeers(peerMap)

		rf := raft.NewRaft(i+1, []int{1, 2, 3}, store, sm, transports[i], commitChan)
		transports[i].RegisterRaft(rf)
		transports[i].Start()
		go rf.Run()
		nodes = append(nodes, rf)
	}

	// 在 InMemoryTransport 中，需要让每个节点的 Transport 连接到其他节点
	// Connect 方法是 InMemoryTransport 特有的方法，需要类型断言
	for i := 0; i < 3; i++ {
		if imTrans, ok := transports[i].(*inmemory.Transport); ok {
			for j := 0; j < 3; j++ {
				if i != j {
					imTrans.Connect(transports[j].Addr(), nodes[j])
				}
			}
		}
	}

	// Wait for leader election
	time.Sleep(800 * time.Millisecond)

	// Find the leader
	var leader *raft.Raft
	for _, n := range nodes {
		if n.State() == raft.Leader {
			leader = n
			break
		}
	}

	if leader == nil {
		b.Fatal("No leader elected")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: "k", Value: "v"}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	b.StopTimer()

	// Cleanup
	for _, n := range nodes {
		n.Stop()
	}
	for _, t := range transports {
		t.Close()
	}
	for _, s := range stores {
		s.Close()
	}
}

// BenchmarkCluster_MediumKeys 测试中等大小键值对的性能
func BenchmarkCluster_MediumKeys(b *testing.B) {
	dataDir := b.TempDir()
	peerMap := make(map[int]string)
	var nodes []*raft.Raft
	var stores []storage.Storage
	var transports []transport.Transport

	mediumValue := string(make([]byte, 256)) // 256B value

	// Create transports and get their addresses
	for i := 0; i < 3; i++ {
		trans, _ := transport.NewTransport(transport.InMemoryTransport, "127.0.0.1:0")
		transports = append(transports, trans)
		peerMap[i+1] = trans.Addr()
	}

	// Create Raft nodes
	for i := 0; i < 3; i++ {
		store, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, i+1)
		stores = append(stores, store)
		commitChan := make(chan param.CommitEntry, 1000)

		go func(ch chan param.CommitEntry) {
			for range ch {
			}
		}(commitChan)

		transports[i].SetPeers(peerMap)

		rf := raft.NewRaft(i+1, []int{1, 2, 3}, store, sm, transports[i], commitChan)
		transports[i].RegisterRaft(rf)
		transports[i].Start()
		go rf.Run()
		nodes = append(nodes, rf)
	}

	// 在 InMemoryTransport 中，需要让每个节点的 Transport 连接到其他节点
	// Connect 方法是 InMemoryTransport 特有的方法，需要类型断言
	for i := 0; i < 3; i++ {
		if imTrans, ok := transports[i].(*inmemory.Transport); ok {
			for j := 0; j < 3; j++ {
				if i != j {
					imTrans.Connect(transports[j].Addr(), nodes[j])
				}
			}
		}
	}

	// Wait for leader election
	time.Sleep(800 * time.Millisecond)

	// Find the leader
	var leader *raft.Raft
	for _, n := range nodes {
		if n.State() == raft.Leader {
			leader = n
			break
		}
	}

	if leader == nil {
		b.Fatal("No leader elected")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("key%d", i%1000), Value: mediumValue}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	b.StopTimer()

	// Cleanup
	for _, n := range nodes {
		n.Stop()
	}
	for _, t := range transports {
		t.Close()
	}
	for _, s := range stores {
		s.Close()
	}
}

// BenchmarkCluster_LargeKeys 测试大键值对的性能
func BenchmarkCluster_LargeKeys(b *testing.B) {
	dataDir := b.TempDir()
	peerMap := make(map[int]string)
	var nodes []*raft.Raft
	var stores []storage.Storage
	var transports []transport.Transport

	largeValue := string(make([]byte, 4096)) // 4KB value

	// Create transports and get their addresses
	for i := 0; i < 3; i++ {
		trans, _ := transport.NewTransport(transport.InMemoryTransport, "127.0.0.1:0")
		transports = append(transports, trans)
		peerMap[i+1] = trans.Addr()
	}

	// Create Raft nodes
	for i := 0; i < 3; i++ {
		store, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, i+1)
		stores = append(stores, store)
		commitChan := make(chan param.CommitEntry, 1000)

		go func(ch chan param.CommitEntry) {
			for range ch {
			}
		}(commitChan)

		transports[i].SetPeers(peerMap)

		rf := raft.NewRaft(i+1, []int{1, 2, 3}, store, sm, transports[i], commitChan)
		transports[i].RegisterRaft(rf)
		transports[i].Start()
		go rf.Run()
		nodes = append(nodes, rf)
	}

	// 在 InMemoryTransport 中，需要让每个节点的 Transport 连接到其他节点
	// Connect 方法是 InMemoryTransport 特有的方法，需要类型断言
	for i := 0; i < 3; i++ {
		if imTrans, ok := transports[i].(*inmemory.Transport); ok {
			for j := 0; j < 3; j++ {
				if i != j {
					imTrans.Connect(transports[j].Addr(), nodes[j])
				}
			}
		}
	}

	// Wait for leader election
	time.Sleep(800 * time.Millisecond)

	// Find the leader
	var leader *raft.Raft
	for _, n := range nodes {
		if n.State() == raft.Leader {
			leader = n
			break
		}
	}

	if leader == nil {
		b.Fatal("No leader elected")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("key%d", i%1000), Value: largeValue}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	b.StopTimer()

	// Cleanup
	for _, n := range nodes {
		n.Stop()
	}
	for _, t := range transports {
		t.Close()
	}
	for _, s := range stores {
		s.Close()
	}
}

// BenchmarkCluster_3NodesLSM 测试 3 节点 LSM 存储的性能
func BenchmarkCluster_3NodesLSM(b *testing.B) {
	dataDir := b.TempDir()
	peerMap := make(map[int]string)
	var nodes []*raft.Raft
	var stores []storage.Storage
	var transports []transport.Transport

	// Create transports and get their addresses
	for i := 0; i < 3; i++ {
		trans, _ := transport.NewTransport(transport.InMemoryTransport, "127.0.0.1:0")
		transports = append(transports, trans)
		peerMap[i+1] = trans.Addr()
	}

	// Create Raft nodes
	for i := 0; i < 3; i++ {
		store, sm, _ := storage.NewStorage(storage.LSMStorage, dataDir, i+1)
		stores = append(stores, store)
		commitChan := make(chan param.CommitEntry, 1000)

		go func(ch chan param.CommitEntry) {
			for range ch {
			}
		}(commitChan)

		transports[i].SetPeers(peerMap)

		rf := raft.NewRaft(i+1, []int{1, 2, 3}, store, sm, transports[i], commitChan)
		transports[i].RegisterRaft(rf)
		transports[i].Start()
		go rf.Run()
		nodes = append(nodes, rf)
	}

	// 在 InMemoryTransport 中，需要让每个节点的 Transport 连接到其他节点
	// Connect 方法是 InMemoryTransport 特有的方法，需要类型断言
	for i := 0; i < 3; i++ {
		if imTrans, ok := transports[i].(*inmemory.Transport); ok {
			for j := 0; j < 3; j++ {
				if i != j {
					imTrans.Connect(transports[j].Addr(), nodes[j])
				}
			}
		}
	}

	// Wait for leader election
	time.Sleep(800 * time.Millisecond)

	// Find the leader
	var leader *raft.Raft
	for _, n := range nodes {
		if n.State() == raft.Leader {
			leader = n
			break
		}
	}

	if leader == nil {
		b.Fatal("No leader elected")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: fmt.Sprintf("key%d", i%1000), Value: fmt.Sprintf("value%d", i)}
		cmdBytes, _ := json.Marshal(cmd)
		reply := &param.ClientReply{}
		_ = leader.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	b.StopTimer()

	// Cleanup
	for _, n := range nodes {
		n.Stop()
	}
	for _, t := range transports {
		t.Close()
	}
	for _, s := range stores {
		s.Close()
	}
}

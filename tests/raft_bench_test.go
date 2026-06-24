package tests

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmh1011/go-kv/pkg/config"
	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
	"github.com/xmh1011/go-kv/pkg/transport/inmemory"
	"github.com/xmh1011/go-kv/raft"
)

// BenchmarkAppendEntries 测试日志复制的性能
func BenchmarkAppendEntries(b *testing.B) {
	dataDir := b.TempDir()
	store, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, 1)
	defer store.Close()

	rf := raft.NewRaft(1, []int{1, 2, 3}, store, sm, nil, make(chan param.CommitEntry, 1000))
	rf.Stop()

	entries := make([]param.LogEntry, 10)
	for i := 0; i < 10; i++ {
		cmdBytes, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "key", Value: "value"})
		entries[i] = param.LogEntry{
			Command: cmdBytes,
			Term:    1,
			Index:   uint64(i + 1),
		}
	}

	args := param.NewAppendEntriesArgs(1, 1, 0, 0, 0, entries)
	reply := param.NewAppendEntriesReply()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rf.AppendEntries(args, reply)
	}
	b.StopTimer()
}

// BenchmarkRequestVote 测试投票请求的性能
func BenchmarkRequestVote(b *testing.B) {
	dataDir := b.TempDir()
	store, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, 1)
	defer store.Close()

	rf := raft.NewRaft(1, []int{1, 2, 3}, store, sm, nil, make(chan param.CommitEntry, 1000))
	rf.Stop()

	args := param.NewRequestVoteArgs(1, 1, 100, 1, false)
	reply := param.NewRequestVoteReply()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rf.RequestVote(args, reply)
	}
	b.StopTimer()
}

// BenchmarkLogEntrySerialization 测试日志条目序列化的性能
func BenchmarkLogEntrySerialization(b *testing.B) {
	cmd := param.KVCommand{Op: param.OpSet, Key: "test_key", Value: "test_value"}
	cmdBytes, _ := json.Marshal(cmd)
	entry := param.LogEntry{
		Command: cmdBytes,
		Term:    123456,
		Index:   999999,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(entry)
	}
	b.StopTimer()
}

// BenchmarkLogEntryDeserialization 测试日志条目反序列化的性能
func BenchmarkLogEntryDeserialization(b *testing.B) {
	cmd := param.KVCommand{Op: param.OpSet, Key: "test_key", Value: "test_value"}
	cmdBytes, _ := json.Marshal(cmd)
	entry := param.LogEntry{
		Command: cmdBytes,
		Term:    123456,
		Index:   999999,
	}
	data, _ := json.Marshal(entry)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var le param.LogEntry
		_ = json.Unmarshal(data, &le)
	}
	b.StopTimer()
}

// BenchmarkKVCommandSerialization 测试KV命令序列化的性能
func BenchmarkKVCommandSerialization(b *testing.B) {
	cmd := param.KVCommand{Op: param.OpSet, Key: "user:12345:name", Value: "John Doe"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(cmd)
	}
	b.StopTimer()
}

// BenchmarkClientRequestProcessing 测试客户端请求处理的全流程性能
// 这不包括网络传输，仅测试 Raft 层处理逻辑
func BenchmarkClientRequestProcessing(b *testing.B) {
	withFastSingleNodeRaftConfig(b)

	dataDir := b.TempDir()
	store, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, 1)
	defer store.Close()

	trans := inmemory.NewTransport("127.0.0.1:0")
	trans.SetPeers(map[int]string{1: trans.Addr()})

	commitCh := make(chan param.CommitEntry, 1024)
	stopDrain := startBenchmarkCommitDrain(commitCh)
	rf := raft.NewRaft(1, []int{1}, store, sm, trans, commitCh)
	go rf.Run()
	defer func() {
		rf.Stop()
		stopDrain()
	}()

	waitForBenchmarkLeader(b, rf)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: "key", Value: "value"}
		benchmarkClientRequest(b, rf, 1, int64(i+1), cmd)
	}
	b.StopTimer()
}

// BenchmarkStateMachineApply 测试状态机 Apply 操作的性能
func BenchmarkStateMachineApply(b *testing.B) {
	dataDir := b.TempDir()
	_, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: "key", Value: "value"}
		cmdBytes, _ := json.Marshal(cmd)
		entry := param.LogEntry{
			Command: cmdBytes,
			Term:    1,
			Index:   uint64(i + 1),
		}
		_ = sm.Apply(entry)
	}
	b.StopTimer()
}

// BenchmarkStateMachineGet 测试状态机 Get 操作的性能
func BenchmarkStateMachineGet(b *testing.B) {
	dataDir := b.TempDir()
	_, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, 1)

	// Pre-populate
	cmd := param.KVCommand{Op: param.OpSet, Key: "test_key", Value: "test_value"}
	cmdBytes, _ := json.Marshal(cmd)
	entry := param.LogEntry{Command: cmdBytes, Term: 1, Index: 1}
	_ = sm.Apply(entry)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sm.Get("test_key")
	}
	b.StopTimer()
}

// BenchmarkStorageAppendEntries 测试存储层 AppendEntries 的性能
func BenchmarkStorageAppendEntries(b *testing.B) {
	dataDir := b.TempDir()
	store, _, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, 1)
	defer store.Close()

	entries := make([]param.LogEntry, 100)
	for i := 0; i < 100; i++ {
		cmdBytes, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "key", Value: "value"})
		entries[i] = param.LogEntry{
			Command: cmdBytes,
			Term:    1,
			Index:   uint64(i + 1),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.AppendEntries(entries)
	}
	b.StopTimer()
}

// BenchmarkStorageGetEntry 测试存储层 GetEntry 的性能
func BenchmarkStorageGetEntry(b *testing.B) {
	dataDir := b.TempDir()
	store, _, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, 1)
	defer store.Close()

	// Pre-populate with 1000 entries
	entries := make([]param.LogEntry, 1000)
	for i := 0; i < 1000; i++ {
		cmdBytes, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "key", Value: "value"})
		entries[i] = param.LogEntry{
			Command: cmdBytes,
			Term:    1,
			Index:   uint64(i + 1),
		}
	}
	_ = store.AppendEntries(entries)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.GetEntry(uint64(i%1000 + 1))
	}
	b.StopTimer()
}

// BenchmarkMixedWorkload 测试混合读写负载的性能
func BenchmarkMixedWorkload(b *testing.B) {
	withFastSingleNodeRaftConfig(b)

	const (
		keyCount    = 128
		workerCount = 16
		readStride  = 5 // four reads followed by one write
	)

	dataDir := b.TempDir()
	store, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, 1)
	defer store.Close()

	trans := inmemory.NewTransport("127.0.0.1:0")
	trans.SetPeers(map[int]string{1: trans.Addr()})

	commitCh := make(chan param.CommitEntry, 1024)
	stopDrain := startBenchmarkCommitDrain(commitCh)
	rf := raft.NewRaft(1, []int{1}, store, sm, trans, commitCh)
	go rf.Run()
	defer func() {
		rf.Stop()
		stopDrain()
	}()

	waitForBenchmarkLeader(b, rf)

	for i := 0; i < keyCount; i++ {
		cmd := param.KVCommand{Op: param.OpSet, Key: benchmarkKey(i), Value: "seed"}
		benchmarkClientRequest(b, rf, 10_000, int64(i+1), cmd)
	}

	var failures atomic.Int64
	var firstFailure atomic.Value
	recordFailure := func(format string, args ...any) {
		if failures.Add(1) == 1 {
			firstFailure.Store(fmt.Sprintf(format, args...))
		}
	}

	ops := make(chan int, workerCount*2)
	var wg sync.WaitGroup

	b.ResetTimer()

	for workerID := 0; workerID < workerCount; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			clientID := int64(workerID + 1)
			var sequenceNum int64
			for idx := range ops {
				sequenceNum++
				cmd := param.KVCommand{Key: benchmarkKey(idx % keyCount)}
				if idx%readStride != 0 {
					cmd.Op = param.OpGet
				} else {
					cmd.Op = param.OpSet
					cmd.Value = fmt.Sprintf("value-%d", idx)
				}

				reply, err := benchmarkClientRequestResult(rf, clientID, sequenceNum, cmd)
				if err != nil {
					recordFailure("worker %d op %d failed: %v", workerID, idx, err)
					continue
				}
				if reply.NotLeader || !reply.Success {
					recordFailure("worker %d op %d returned success=%t notLeader=%t leaderHint=%d result=%v",
						workerID, idx, reply.Success, reply.NotLeader, reply.LeaderHint, reply.Result)
				}
			}
		}(workerID)
	}

	for i := 0; i < b.N; i++ {
		ops <- i
	}
	close(ops)
	wg.Wait()
	b.StopTimer()

	if failures.Load() > 0 {
		b.Fatalf("mixed workload benchmark saw %d failed operations; first failure: %s",
			failures.Load(), firstFailure.Load())
	}
	b.ReportMetric(float64(workerCount), "workers")
}

func withFastSingleNodeRaftConfig(b *testing.B) {
	b.Helper()
	oldElectionTimeout := config.Conf.Raft.ElectionTimeout
	oldHeartbeatTimeout := config.Conf.Raft.HeartbeatTimeout
	config.Conf.Raft.ElectionTimeout = 100 * time.Millisecond
	config.Conf.Raft.HeartbeatTimeout = 10 * time.Millisecond
	b.Cleanup(func() {
		config.Conf.Raft.ElectionTimeout = oldElectionTimeout
		config.Conf.Raft.HeartbeatTimeout = oldHeartbeatTimeout
	})
}

func waitForBenchmarkLeader(b *testing.B, rf *raft.Raft) {
	b.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rf.State() == raft.Leader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.Fatalf("raft node did not become leader before benchmark timeout")
}

func benchmarkKey(index int) string {
	return fmt.Sprintf("key-%03d", index)
}

func benchmarkClientRequest(b *testing.B, rf *raft.Raft, clientID, sequenceNum int64, cmd param.KVCommand) {
	b.Helper()
	reply, err := benchmarkClientRequestResult(rf, clientID, sequenceNum, cmd)
	if err != nil {
		b.Fatalf("client request failed: %v", err)
	}
	if reply.NotLeader || !reply.Success {
		b.Fatalf("client request returned success=%t notLeader=%t leaderHint=%d result=%v",
			reply.Success, reply.NotLeader, reply.LeaderHint, reply.Result)
	}
}

func benchmarkClientRequestResult(rf *raft.Raft, clientID, sequenceNum int64, cmd param.KVCommand) (*param.ClientReply, error) {
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	reply := &param.ClientReply{}
	err = rf.ClientRequest(&param.ClientArgs{ClientID: clientID, SequenceNum: sequenceNum, Command: cmdBytes}, reply)
	return reply, err
}

func startBenchmarkCommitDrain(ch <-chan param.CommitEntry) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)
		for {
			select {
			case <-ch:
			case <-done:
				return
			}
		}
	}()

	return func() {
		close(done)
		<-stopped
	}
}

// BenchmarkSnapshotCreation 测试快照创建的性能
func BenchmarkSnapshotCreation(b *testing.B) {
	dataDir := b.TempDir()
	store, sm, _ := storage.NewStorage(storage.SimpleFileStorage, dataDir, 1)

	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		cmdBytes, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "key", Value: "value"})
		entry := param.LogEntry{
			Command: cmdBytes,
			Term:    1,
			Index:   uint64(i + 1),
		}
		_ = sm.Apply(entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshotData, _ := sm.GetSnapshot()
		_ = snapshotData
	}
	b.StopTimer()
	store.Close()
}

// BenchmarkSnapshotApply 测试快照应用的性能
func BenchmarkSnapshotApply(b *testing.B) {
	dataDir := b.TempDir()
	store1, sm1, _ := storage.NewStorage(storage.SimpleFileStorage, dataDir, 1)

	// Create snapshot data
	for i := 0; i < 1000; i++ {
		cmdBytes, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "key", Value: "value"})
		entry := param.LogEntry{
			Command: cmdBytes,
			Term:    1,
			Index:   uint64(i + 1),
		}
		_ = sm1.Apply(entry)
	}
	snapshotData, _ := sm1.GetSnapshot()
	store1.Close()

	// Test applying to new state machines
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, sm2, _ := storage.NewStorage(storage.SimpleFileStorage, dataDir, i+2)
		_ = sm2.ApplySnapshot(snapshotData)
	}
	b.StopTimer()
}

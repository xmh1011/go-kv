package raft

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
	"github.com/xmh1011/go-kv/pkg/transport/inmemory"
)

// BenchmarkAppendEntries 测试日志复制的性能
func BenchmarkAppendEntries(b *testing.B) {
	dataDir := b.TempDir()
	store, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, 1)
	defer store.Close()

	rf := NewRaft(1, []int{1, 2, 3}, store, sm, nil, make(chan param.CommitEntry, 1000))
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

	rf := NewRaft(1, []int{1, 2, 3}, store, sm, nil, make(chan param.CommitEntry, 1000))
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
	dataDir := b.TempDir()
	store, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, 1)
	defer store.Close()

	trans := inmemory.NewTransport("127.0.0.1:0")
	trans.SetPeers(map[int]string{1: trans.Addr()})

	rf := NewRaft(1, []int{1}, store, sm, trans, make(chan param.CommitEntry, 1000))
	go rf.Run()

	time.Sleep(300 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmdBytes, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: "key", Value: "value"})
		reply := &param.ClientReply{}
		_ = rf.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(i), Command: cmdBytes}, reply)
	}
	b.StopTimer()
	rf.Stop()
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
	dataDir := b.TempDir()
	store, sm, _ := storage.NewStorage(storage.InmemoryStorage, dataDir, 1)
	defer store.Close()

	trans := inmemory.NewTransport("127.0.0.1:0")
	trans.SetPeers(map[int]string{1: trans.Addr()})

	rf := NewRaft(1, []int{1}, store, sm, trans, make(chan param.CommitEntry, 1000))
	go rf.Run()

	time.Sleep(300 * time.Millisecond)

	var wg sync.WaitGroup

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 80% reads, 20% writes
		if i%5 != 0 {
			// Read
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := "key" + string(rune(idx%100))
				cmdBytes, _ := json.Marshal(param.KVCommand{Op: param.OpGet, Key: key, Value: ""})
				reply := &param.ClientReply{}
				_ = rf.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(idx), Command: cmdBytes}, reply)
			}(i)
		} else {
			// Write
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := "key" + string(rune(idx%100))
				cmdBytes, _ := json.Marshal(param.KVCommand{Op: param.OpSet, Key: key, Value: "value"})
				reply := &param.ClientReply{}
				_ = rf.ClientRequest(&param.ClientArgs{ClientID: 1, SequenceNum: int64(idx), Command: cmdBytes}, reply)
			}(i)
		}
	}
	wg.Wait()
	b.StopTimer()
	rf.Stop()
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

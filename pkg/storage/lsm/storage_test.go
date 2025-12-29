package lsm

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
)

func init() {
	// 初始化日志配置，避免测试时 panic
	log.Init(log.Config{
		Level:   "debug",
		Console: true,
	})
}

func TestStateMachineAdapter_ApplyAndGet(t *testing.T) {
	db, dir := setupTestDB(t, "lsm_sm_test")
	defer cleanupTestDB(t, dir)

	adapter := NewStateMachineAdapter(db)
	defer adapter.Close()

	// Test Set
	cmd := param.KVCommand{Op: param.OpSet, Key: "key1", Value: "value1"}
	cmdBytes, _ := json.Marshal(cmd)
	entry := param.LogEntry{Command: cmdBytes}

	result := adapter.Apply(entry)
	assert.Nil(t, result)

	// Test Get
	val, err := adapter.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)

	// Test Delete
	cmd = param.KVCommand{Op: param.OpDelete, Key: "key1"}
	cmdBytes, _ = json.Marshal(cmd)
	entry = param.LogEntry{Command: cmdBytes}

	result = adapter.Apply(entry)
	assert.Nil(t, result)

	val, err = adapter.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "", val)
}

func TestStorageAdapter_HardState(t *testing.T) {
	db, dir := setupTestDB(t, "lsm_storage_test_hs")
	defer cleanupTestDB(t, dir)

	adapter, err := NewStorageAdapter(db)
	assert.NoError(t, err)
	defer adapter.Close()

	state := param.HardState{CurrentTerm: 10, VotedFor: 2}
	err = adapter.SetState(state)
	assert.NoError(t, err)

	gotState, err := adapter.GetState()
	assert.NoError(t, err)
	assert.Equal(t, state, gotState)
}

func TestStorageAdapter_LogEntries(t *testing.T) {
	db, dir := setupTestDB(t, "lsm_storage_test_log")
	defer cleanupTestDB(t, dir)

	adapter, err := NewStorageAdapter(db)
	assert.NoError(t, err)
	defer adapter.Close()

	entries := []param.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
		{Term: 2, Index: 3, Command: []byte("cmd3")},
	}

	// Test AppendEntries
	err = adapter.AppendEntries(entries)
	assert.NoError(t, err)

	// Test GetEntry
	entry, err := adapter.GetEntry(2)
	assert.NoError(t, err)
	assert.Equal(t, entries[1], *entry)

	// Test First/Last Index
	first, err := adapter.FirstLogIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), first)

	last, err := adapter.LastLogIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), last)

	// Test TruncateLog
	err = adapter.TruncateLog(2)
	assert.NoError(t, err)

	last, err = adapter.LastLogIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), last)

	entry, err = adapter.GetEntry(2)
	assert.NoError(t, err)
	assert.Nil(t, entry)
}

func TestStorageAdapter_Snapshot(t *testing.T) {
	db, dir := setupTestDB(t, "lsm_storage_test_snap")
	defer cleanupTestDB(t, dir)

	adapter, err := NewStorageAdapter(db)
	assert.NoError(t, err)
	defer adapter.Close()

	snap := &param.Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		Data:              []byte("snapshot-data"),
	}

	// Test SaveSnapshot
	err = adapter.SaveSnapshot(snap)
	assert.NoError(t, err)

	// Test ReadSnapshot
	gotSnap, err := adapter.ReadSnapshot()
	assert.NoError(t, err)
	assert.Equal(t, snap, gotSnap)

	// Test CompactLog
	// 先写入一些日志
	entries := []param.LogEntry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 1, Index: 3},
		{Term: 2, Index: 4},
		{Term: 2, Index: 5},
		{Term: 2, Index: 6},
	}
	adapter.AppendEntries(entries)

	// 压缩到 index 4 (保留 5, 6)
	err = adapter.CompactLog(4)
	assert.NoError(t, err)

	first, err := adapter.FirstLogIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), first)

	// 验证旧日志被删除
	entry, err := adapter.GetEntry(3)
	assert.NoError(t, err)
	assert.Nil(t, entry)

	// 验证新日志还在
	entry, err = adapter.GetEntry(5)
	assert.NoError(t, err)
	assert.NotNil(t, entry)
}

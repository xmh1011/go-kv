package lsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/engine/lsm/database"
	"github.com/xmh1011/go-kv/engine/lsm/sstable"
	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
)

func init() {
	// 初始化日志配置，避免测试时 panic
	log.Init(log.Config{
		Level:   "warn",
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
	assert.ErrorIs(t, err, ErrKeyNotFound)
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
	initialSize, err := adapter.LogSize()
	assert.NoError(t, err)

	// Test GetEntry
	entry, err := adapter.GetEntry(2)
	assert.NoError(t, err)
	assert.Equal(t, entries[1], *entry)

	// Test replacement does not inflate log size
	replacement := param.LogEntry{Term: 3, Index: 2, Command: []byte("replacement-cmd")}
	oldEncoded, err := encodeLogEntry(&entries[1])
	assert.NoError(t, err)
	replacementEncoded, err := encodeLogEntry(&replacement)
	assert.NoError(t, err)

	err = adapter.AppendEntries([]param.LogEntry{replacement})
	assert.NoError(t, err)
	sizeAfterReplacement, err := adapter.LogSize()
	assert.NoError(t, err)
	assert.Equal(t, initialSize-len(oldEncoded)+len(replacementEncoded), sizeAfterReplacement)

	entry, err = adapter.GetEntry(2)
	assert.NoError(t, err)
	assert.Equal(t, replacement, *entry)

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

func TestStorageAdapter_LogEntryEncodingUsesBinaryCommands(t *testing.T) {
	tests := []struct {
		name  string
		entry param.LogEntry
	}{
		{
			name: "client command",
			entry: param.LogEntry{
				Term:  7,
				Index: 42,
				Command: param.NewClientCommand(
					99,
					12,
					[]byte(`{"op":2,"key":"k","value":"v"}`),
				),
			},
		},
		{
			name: "raft noop command",
			entry: param.LogEntry{
				Term:    8,
				Index:   43,
				Command: param.NoopCommand{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodeLogEntry(&tt.entry)
			assert.NoError(t, err)
			assert.True(t, bytes.HasPrefix(encoded, []byte(logEntryFormatMagic)))

			decoded, err := decodeLogEntry(encoded)
			assert.NoError(t, err)
			assert.Equal(t, tt.entry, *decoded)
		})
	}
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

func TestStorageAdapter_CompactBeyondLastIndexFromSnapshot(t *testing.T) {
	db, dir := setupTestDB(t, "lsm_storage_test_compact_beyond_last")
	defer cleanupTestDB(t, dir)

	adapter, err := NewStorageAdapter(db)
	assert.NoError(t, err)
	defer adapter.Close()

	err = adapter.AppendEntries([]param.LogEntry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 1, Index: 3},
	})
	assert.NoError(t, err)

	err = adapter.CompactLog(5)
	assert.NoError(t, err)

	first, err := adapter.FirstLogIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), first)

	last, err := adapter.LastLogIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), last)

	entry, err := adapter.GetEntry(3)
	assert.NoError(t, err)
	assert.Nil(t, entry)

	err = adapter.AppendEntries([]param.LogEntry{{Term: 2, Index: 6}})
	assert.NoError(t, err)

	entry, err = adapter.GetEntry(6)
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, uint64(6), entry.Index)
}

func TestStorageAdapter_ReappendAfterTruncateSurvivesFlushCompactionAndRestart(t *testing.T) {
	db, dir := setupTestDB(t, "lsm_storage_test_reappend_after_truncate")
	defer cleanupTestDB(t, dir)

	adapter, err := NewStorageAdapter(db)
	assert.NoError(t, err)

	const totalEntries = 12000
	const truncateFrom = 6000
	payload := bytes.Repeat([]byte("x"), 1024)

	initial := make([]param.LogEntry, 0, totalEntries)
	for i := 1; i <= totalEntries; i++ {
		initial = append(initial, param.LogEntry{
			Term:    1,
			Index:   uint64(i),
			Command: append([]byte(nil), payload...),
		})
	}
	assert.NoError(t, adapter.AppendEntries(initial))
	assert.NoError(t, adapter.db.ForceFlush())

	assert.NoError(t, adapter.TruncateLog(truncateFrom))
	assert.NoError(t, adapter.db.ForceFlush())

	reappended := make([]param.LogEntry, 0, totalEntries-truncateFrom+1)
	for i := truncateFrom; i <= totalEntries; i++ {
		reappended = append(reappended, param.LogEntry{
			Term:    2,
			Index:   uint64(i),
			Command: append([]byte(nil), payload...),
		})
	}
	assert.NoError(t, adapter.AppendEntries(reappended))
	assert.NoError(t, adapter.db.ForceFlush())
	adapter.db.SSTables.WaitForCompactions()

	entry, err := adapter.GetEntry(truncateFrom)
	assert.NoError(t, err)
	if entry == nil {
		t.Fatalf("entry %d missing before restart", truncateFrom)
	}
	assert.NoError(t, adapter.Close())

	reopenedDB := database.Open(dir)
	assert.NoError(t, reopenedDB.Recover())
	reopened, err := NewStorageAdapter(reopenedDB)
	assert.NoError(t, err)
	defer reopened.Close()

	for i := truncateFrom; i <= totalEntries; i++ {
		entry, err := reopened.GetEntry(uint64(i))
		assert.NoError(t, err)
		if entry == nil {
			t.Fatalf("entry %d must survive truncate/reappend/restart; %s", i, describeLogKeyOnDisk(t, reopened.db.GetAllSSTables(), reopened.getLogKey(uint64(i))))
		}
		assert.Equal(t, uint64(2), entry.Term)
		assert.Equal(t, uint64(i), entry.Index)
	}
}

func describeLogKeyOnDisk(t *testing.T, files []string, key string) string {
	t.Helper()
	for _, file := range files {
		level := 0
		_, _ = fmt.Sscanf(filepath.Base(filepath.Dir(file)), "%d-level", &level)
		table := sstable.NewRecoverSSTable(level)
		if err := table.DecodeFrom(file); err != nil {
			continue
		}
		pairs, err := table.GetDataBlockFromFile(file)
		if err != nil {
			continue
		}
		for _, pair := range pairs {
			if string(pair.Key) == key {
				deleted := "value"
				if pair.IsDeleted() {
					deleted = "tombstone"
				}
				return fmt.Sprintf("found %s in %s", deleted, file)
			}
		}
	}
	return fmt.Sprintf("key not present in %d files: %s", len(files), strings.Join(files, ","))
}

package simplefile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/param"
)

func newTestStorage(t *testing.T) (*Storage, string) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "raft_storage.gob")
	s, err := NewStorage(filePath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	return s, filePath
}

func newTestEntries(start, end uint64) []param.LogEntry {
	entries := make([]param.LogEntry, 0, end-start+1)
	for i := start; i <= end; i++ {
		entries = append(entries, param.LogEntry{Term: i, Index: i})
	}
	return entries
}

func TestStorage(t *testing.T) {
	t.Run("Initial State", func(t *testing.T) {
		s, _ := newTestStorage(t)

		lastIDx, err := s.LastLogIndex()
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), lastIDx)

		firstIDx, err := s.FirstLogIndex()
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), firstIDx)

		_, err = s.GetEntry(1)
		assert.ErrorIs(t, err, ErrLogNotFound)
	})

	t.Run("Persistence", func(t *testing.T) {
		s, filePath := newTestStorage(t)

		// Modify state
		newState := param.HardState{CurrentTerm: 5, VotedFor: 2}
		assert.NoError(t, s.SetState(newState))

		entries := newTestEntries(1, 3)
		assert.NoError(t, s.AppendEntries(entries))

		// Close and reopen
		s = nil
		s2, err := NewStorage(filePath)
		assert.NoError(t, err)

		// Verify state persisted
		retrievedState, err := s2.GetState()
		assert.NoError(t, err)
		assert.Equal(t, newState, retrievedState)

		lastIDx, err := s2.LastLogIndex()
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), lastIDx)

		entry2, err := s2.GetEntry(2)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), entry2.Index)
	})

	t.Run("Log Operations", func(t *testing.T) {
		s, _ := newTestStorage(t)
		entries := newTestEntries(1, 5)

		assert.NoError(t, s.AppendEntries(entries))

		lastIDx, err := s.LastLogIndex()
		assert.NoError(t, err)
		assert.Equal(t, uint64(5), lastIDx)

		// Truncate
		assert.NoError(t, s.TruncateLog(4))
		lastIDx, err = s.LastLogIndex()
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), lastIDx)

		_, err = s.GetEntry(4)
		assert.ErrorIs(t, err, ErrLogNotFound)
	})

	t.Run("Snapshot and Compaction", func(t *testing.T) {
		s, filePath := newTestStorage(t)
		entries := newTestEntries(1, 10)
		s.AppendEntries(entries)

		snapshot := &param.Snapshot{LastIncludedIndex: 5, LastIncludedTerm: 5, Data: []byte("snap")}
		assert.NoError(t, s.SaveSnapshot(snapshot))

		assert.NoError(t, s.CompactLog(5))

		// Verify in memory
		assert.Equal(t, uint64(5), s.logOffset)
		_, err := s.GetEntry(5)
		assert.ErrorIs(t, err, ErrLogNotFound)
		entry6, err := s.GetEntry(6)
		assert.NoError(t, err)
		assert.Equal(t, uint64(6), entry6.Index)

		// Verify persistence
		s = nil
		s2, err := NewStorage(filePath)
		assert.NoError(t, err)

		assert.Equal(t, uint64(5), s2.logOffset)
		readSnap, err := s2.ReadSnapshot()
		assert.NoError(t, err)
		assert.Equal(t, snapshot, readSnap)
	})

	t.Run("Corrupted File", func(t *testing.T) {
		// Create a file with garbage data
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "corrupted.gob")
		err := os.WriteFile(filePath, []byte("not a gob file"), 0644)
		assert.NoError(t, err)

		_, err = NewStorage(filePath)
		assert.Error(t, err, "NewStorage should fail with corrupted file")
	})
}

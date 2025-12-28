package inmemory

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/param"
)

// helper to create a series of simple log entries for testing
func newTestEntries(start, end uint64) []param.LogEntry {
	entries := make([]param.LogEntry, 0, end-start+1)
	for i := start; i <= end; i++ {
		entries = append(entries, param.LogEntry{Term: i, Index: i})
	}
	return entries
}

func TestStorage(t *testing.T) {
	t.Run("Initial State", func(t *testing.T) {
		s := NewStorage()
		assert.NotNil(t, s, "NewStorage should not return nil")

		lastIDx, err := s.LastLogIndex()
		assert.NoError(t, err, "LastLogIndex() should not fail")
		assert.Equal(t, uint64(0), lastIDx, "initial last index should be 0")

		firstIDx, err := s.FirstLogIndex()
		assert.NoError(t, err, "FirstLogIndex() should not fail")
		assert.Equal(t, uint64(1), firstIDx, "initial first index should be 1")

		_, err = s.GetEntry(1)
		assert.ErrorIs(t, err, ErrLogNotFound, "should return ErrLogNotFound for initial empty log")
	})
	t.Run("HardState", func(t *testing.T) {
		s := NewStorage()
		initialState, err := s.GetState()
		assert.NoError(t, err, "GetState() should not fail")
		assert.Equal(t, uint64(0), initialState.CurrentTerm, "initial CurrentTerm should be 0")
		assert.Equal(t, uint64(0), initialState.VotedFor, "initial VotedFor should be 0")

		newState := param.HardState{CurrentTerm: 5, VotedFor: 2}
		err = s.SetState(newState)
		assert.NoError(t, err, "SetState() should not fail")

		retrievedState, err := s.GetState()
		assert.NoError(t, err, "GetState() after set should not fail")
		assert.Equal(t, newState, retrievedState, "retrieved state should match set state")
	})
	t.Run("Log Operations (Append, Get, Truncate)", func(t *testing.T) {
		s := NewStorage()
		entries := newTestEntries(1, 5) // Creates entries with Index/Term 1, 2, 3, 4, 5

		err := s.AppendEntries(entries)
		assert.NoError(t, err, "AppendEntries() should not fail")

		lastIDx, err := s.LastLogIndex()
		assert.NoError(t, err, "LastLogIndex() should not fail")
		assert.Equal(t, uint64(5), lastIDx, "last index should be 5 after append")

		entry3, err := s.GetEntry(3)
		assert.NoError(t, err, "GetEntry(3) should not fail")
		assert.Equal(t, uint64(3), entry3.Index, "retrieved entry should have correct index")
		assert.Equal(t, uint64(3), entry3.Term, "retrieved entry should have correct term")

		// Test getting an entry out of bounds
		_, err = s.GetEntry(6)
		assert.ErrorIs(t, err, ErrLogNotFound, "should return ErrLogNotFound for index 6")
		_, err = s.GetEntry(0) // Before logOffset
		if !errors.Is(err, ErrLogNotFound) {
			t.Errorf("expected ErrLogNotFound for index 0, but got %v", err)
		}

		// Truncate from index 4 onwards (removes entries 4 and 5)
		err = s.TruncateLog(4)
		assert.NoError(t, err, "TruncateLog(4) should not fail")

		lastIDx, err = s.LastLogIndex()
		assert.NoError(t, err, "LastLogIndex() should not fail after truncation")
		assert.Equal(t, uint64(3), lastIDx, "last index should be 3 after truncation")

		_, err = s.GetEntry(4)
		assert.ErrorIs(t, err, ErrLogNotFound, "should return ErrLogNotFound for truncated index 4")

		// Truncating with an out-of-bounds index should be a no-op
		err = s.TruncateLog(10)
		assert.NoError(t, err, "TruncateLog(10) with out-of-bounds index should not fail")
		lastIDx, err = s.LastLogIndex()
		assert.NoError(t, err, "LastLogIndex() should not fail")
		assert.Equal(t, uint64(3), lastIDx, "last index should not change after out-of-bounds truncation")
	})

	t.Run("Snapshot and Compaction", func(t *testing.T) {
		s := NewStorage()
		entries := newTestEntries(1, 10)
		s.AppendEntries(entries)

		// 1. Save and read a snapshot
		snapData := []byte("snapshot data")
		snapshot := &param.Snapshot{LastIncludedIndex: 5, LastIncludedTerm: 5, Data: snapData}
		assert.NoError(t, s.SaveSnapshot(snapshot))

		readSnap, err := s.ReadSnapshot()
		assert.NoError(t, err)
		assert.Equal(t, snapshot, readSnap)

		// 2. Compact the log up to index 5
		assert.NoError(t, s.CompactLog(5))

		// 3. --- MODIFIED TEST LOGIC ---
		// Verify state after compaction
		assert.Equal(t, uint64(5), s.logOffset, "expected logOffset to be 5")

		// The entry at index 5 should now be GONE (it's in the snapshot)
		_, err = s.GetEntry(5)
		assert.ErrorIs(t, err, ErrLogNotFound, "GetEntry(5) should fail after compaction")

		// The first available entry in the log should be at index 6
		entry6, err := s.GetEntry(6)
		// 使用 assert.NoError 会在失败时停止测试，防止下一行的nil指针panic
		assert.NoError(t, err, "GetEntry(6) should succeed after compaction")
		assert.Equal(t, uint64(6), entry6.Index, "The first entry after compaction should be index 6")

		lastIDx, _ := s.LastLogIndex()
		assert.Equal(t, uint64(10), lastIDx, "expected last index to still be 10")

		firstIDx, _ := s.FirstLogIndex()
		assert.Equal(t, uint64(6), firstIDx, "expected first index to be 6 after compaction")

		// 4. Test compaction with an index that is too low or too high
		err = s.CompactLog(4) // Already compacted
		assert.NoError(t, err, "CompactLog(4) should not fail")
		assert.Equal(t, uint64(5), s.logOffset, "logOffset should not change for already compacted index")

		err = s.CompactLog(11) // Out of bounds
		assert.ErrorIs(t, err, ErrIndexOutOfBounds, "CompactLog(11) should return ErrIndexOutOfBounds")
	})
}

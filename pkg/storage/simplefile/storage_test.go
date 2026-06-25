package simplefile

import (
	"os"
	"path/filepath"
	"sync"
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
		assert.NoError(t, err, "GetEntry(1) on empty log should not return error")
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
		assert.Nil(t, err, "GetEntry(4) should return nil for truncated index")
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
		assert.NoError(t, err, "GetEntry(5) should not return error after compaction")
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

	t.Run("Compact Beyond Last Index From Snapshot", func(t *testing.T) {
		s, filePath := newTestStorage(t)
		assert.NoError(t, s.AppendEntries(newTestEntries(1, 3)))

		assert.NoError(t, s.CompactLog(5))

		firstIdx, err := s.FirstLogIndex()
		assert.NoError(t, err)
		assert.Equal(t, uint64(6), firstIdx)

		lastIdx, err := s.LastLogIndex()
		assert.NoError(t, err)
		assert.Equal(t, uint64(5), lastIdx)

		assert.NoError(t, s.AppendEntries([]param.LogEntry{{Term: 2, Index: 6}}))

		s2, err := NewStorage(filePath)
		assert.NoError(t, err)

		firstIdx, err = s2.FirstLogIndex()
		assert.NoError(t, err)
		assert.Equal(t, uint64(6), firstIdx)

		lastIdx, err = s2.LastLogIndex()
		assert.NoError(t, err)
		assert.Equal(t, uint64(6), lastIdx)

		entry, err := s2.GetEntry(6)
		assert.NoError(t, err)
		assert.Equal(t, uint64(6), entry.Index)
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

func TestStorageDefensiveCopiesLogEntries(t *testing.T) {
	s, _ := newTestStorage(t)
	originalCommand := []byte("original")
	entry := param.LogEntry{Term: 1, Index: 1, Command: originalCommand}

	assert.NoError(t, s.AppendEntries([]param.LogEntry{entry}))

	originalCommand[0] = 'X'
	entry.Command = []byte("mutated-entry")

	stored, err := s.GetEntry(1)
	assert.NoError(t, err)
	assert.NotNil(t, stored)
	assert.Equal(t, uint64(1), stored.Term)
	assert.Equal(t, uint64(1), stored.Index)
	assert.Equal(t, []byte("original"), stored.Command)

	stored.Term = 99
	stored.Index = 99
	stored.Command.([]byte)[0] = 'Y'

	storedAgain, err := s.GetEntry(1)
	assert.NoError(t, err)
	assert.NotNil(t, storedAgain)
	assert.Equal(t, uint64(1), storedAgain.Term)
	assert.Equal(t, uint64(1), storedAgain.Index)
	assert.Equal(t, []byte("original"), storedAgain.Command)
}

func TestStorageDefensiveCopiesSnapshots(t *testing.T) {
	s, _ := newTestStorage(t)
	snapshot := &param.Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  3,
		Data:              []byte("snapshot"),
	}

	assert.NoError(t, s.SaveSnapshot(snapshot))

	snapshot.LastIncludedIndex = 99
	snapshot.LastIncludedTerm = 99
	snapshot.Data[0] = 'X'

	stored, err := s.ReadSnapshot()
	assert.NoError(t, err)
	assert.NotNil(t, stored)
	assert.Equal(t, uint64(5), stored.LastIncludedIndex)
	assert.Equal(t, uint64(3), stored.LastIncludedTerm)
	assert.Equal(t, []byte("snapshot"), stored.Data)

	stored.LastIncludedIndex = 100
	stored.LastIncludedTerm = 100
	stored.Data[0] = 'Y'

	storedAgain, err := s.ReadSnapshot()
	assert.NoError(t, err)
	assert.NotNil(t, storedAgain)
	assert.Equal(t, uint64(5), storedAgain.LastIncludedIndex)
	assert.Equal(t, uint64(3), storedAgain.LastIncludedTerm)
	assert.Equal(t, []byte("snapshot"), storedAgain.Data)
}

func TestStorageConcurrentPersistAcrossHandles(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "raft_storage.gob")

	s1, err := NewStorage(filePath)
	assert.NoError(t, err)
	s2, err := NewStorage(filePath)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	errCh := make(chan error, 200)

	for i := 0; i < 100; i++ {
		i := i
		wg.Add(2)
		go func() {
			defer wg.Done()
			errCh <- s1.SetState(param.HardState{CurrentTerm: uint64(i*2 + 1), VotedFor: 1})
		}()
		go func() {
			defer wg.Done()
			errCh <- s2.SetState(param.HardState{CurrentTerm: uint64(i*2 + 2), VotedFor: 2})
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		assert.NoError(t, err)
	}

	reopened, err := NewStorage(filePath)
	assert.NoError(t, err)
	state, err := reopened.GetState()
	assert.NoError(t, err)
	assert.Greater(t, state.CurrentTerm, uint64(0))
}

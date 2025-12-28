package simplefile

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/param"
)

func newTestStateMachine(t *testing.T) (*StateMachine, string) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "raft_sm.json")
	sm, err := NewStateMachine(filePath)
	if err != nil {
		t.Fatalf("failed to create state machine: %v", err)
	}
	return sm, filePath
}

func createLogEntry(t *testing.T, op, key, value string) param.LogEntry {
	t.Helper()
	cmd := param.KVCommand{
		Op:    op,
		Key:   key,
		Value: value,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("failed to marshal command: %v", err)
	}
	return param.LogEntry{Command: cmdBytes}
}

func TestStateMachine(t *testing.T) {
	t.Run("Basic Operations", func(t *testing.T) {
		sm, _ := newTestStateMachine(t)

		// Set
		setEntry := createLogEntry(t, "set", "key1", "value1")
		assert.Nil(t, sm.Apply(setEntry))

		// Get
		val, err := sm.Get("key1")
		assert.NoError(t, err)
		assert.Equal(t, "value1", val)

		// Delete
		delEntry := createLogEntry(t, "delete", "key1", "")
		assert.Nil(t, sm.Apply(delEntry))

		_, err = sm.Get("key1")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("Persistence", func(t *testing.T) {
		sm, filePath := newTestStateMachine(t)

		sm.Apply(createLogEntry(t, "set", "persistKey", "persistVal"))

		// Reopen
		sm = nil
		sm2, err := NewStateMachine(filePath)
		assert.NoError(t, err)

		val, err := sm2.Get("persistKey")
		assert.NoError(t, err)
		assert.Equal(t, "persistVal", val)
	})

	t.Run("Snapshot", func(t *testing.T) {
		sm, _ := newTestStateMachine(t)
		sm.Apply(createLogEntry(t, "set", "a", "1"))

		snapData, err := sm.GetSnapshot()
		assert.NoError(t, err)

		// Restore to new SM
		sm2, _ := newTestStateMachine(t)
		err = sm2.ApplySnapshot(snapData)
		assert.NoError(t, err)

		val, err := sm2.Get("a")
		assert.NoError(t, err)
		assert.Equal(t, "1", val)
	})

	t.Run("Corrupted File", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "corrupted.json")
		err := os.WriteFile(filePath, []byte("{invalid-json"), 0644)
		assert.NoError(t, err)

		_, err = NewStateMachine(filePath)
		assert.Error(t, err)
	})
}

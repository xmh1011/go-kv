package simplefile

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
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
		Op:    param.StringToOpType(op),
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

	t.Run("Apply with invalid command format returns error", func(t *testing.T) {
		sm, filePath := newTestStateMachine(t)
		assert.Nil(t, sm.Apply(createLogEntry(t, "set", "existing", "value")))

		tests := []struct {
			name    string
			command any
		}{
			{name: "non byte command", command: "not-bytes"},
			{name: "malformed json", command: []byte("this is not valid json")},
			{name: "wrapped non byte command", command: param.NewClientCommand(1, 1, "not-bytes")},
			{name: "wrapped malformed json", command: param.NewClientCommand(1, 2, []byte("this is not valid json"))},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var result any
				assert.NotPanics(t, func() {
					result = sm.Apply(param.LogEntry{Command: tt.command})
				})
				err, ok := result.(error)
				assert.True(t, ok, "invalid command should return an error")
				assert.Error(t, err)

				val, getErr := sm.Get("existing")
				assert.NoError(t, getErr)
				assert.Equal(t, "value", val)

				reopened, reopenErr := NewStateMachine(filePath)
				assert.NoError(t, reopenErr)
				persisted, persistedErr := reopened.Get("existing")
				assert.NoError(t, persistedErr)
				assert.Equal(t, "value", persisted)
			})
		}
	})
}

func TestStateMachineConcurrentPersistAcrossHandles(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "raft_sm.json")

	sm1, err := NewStateMachine(filePath)
	assert.NoError(t, err)
	sm2, err := NewStateMachine(filePath)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	errCh := make(chan error, 200)

	for i := 0; i < 100; i++ {
		i := i
		entryA := createLogEntry(t, "set", "key-a", string(rune('a'+i%26)))
		entryB := createLogEntry(t, "set", "key-b", string(rune('a'+i%26)))
		wg.Add(2)
		go func() {
			defer wg.Done()
			result := sm1.Apply(entryA)
			if err, ok := result.(error); ok {
				errCh <- err
				return
			}
			errCh <- nil
		}()
		go func() {
			defer wg.Done()
			result := sm2.Apply(entryB)
			if err, ok := result.(error); ok {
				errCh <- err
				return
			}
			errCh <- nil
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		assert.NoError(t, err)
	}

	_, err = NewStateMachine(filePath)
	assert.NoError(t, err)
}

package inmemory

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/pkg/param"
)

// helper function to create a log entry with a KVCommand
func createLogEntry(t *testing.T, op, key, value string) param.LogEntry {
	t.Helper() // Mark this as a test helper function
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
	t.Run("NewInMemoryStateMachine initializes correctly", func(t *testing.T) {
		sm := NewInMemoryStateMachine()
		assert.NotNil(t, sm, "NewInMemoryStateMachine should not return nil")
		assert.NotNil(t, sm.kvStore, "kvStore map should be initialized")
	})

	t.Run("Apply and Get operations", func(t *testing.T) {
		sm := NewInMemoryStateMachine()

		// 1. Get a non-existent key
		_, err := sm.Get("key1")
		assert.ErrorIs(t, err, ErrKeyNotFound, "should return ErrKeyNotFound for non-existent key")

		// 2. Apply a 'set' operation
		setEntry := createLogEntry(t, "set", "key1", "value1")
		result := sm.Apply(setEntry)
		assert.Nil(t, result, "'set' operation should return nil result")

		// 3. Get the new key
		val, err := sm.Get("key1")
		assert.NoError(t, err, "should not error when getting existing key")
		assert.Equal(t, "value1", val, "should get correct value for key")

		// 4. Apply another 'set' to update the key
		updateEntry := createLogEntry(t, "set", "key1", "valueUpdated")
		sm.Apply(updateEntry)
		val, _ = sm.Get("key1")
		assert.Equal(t, "valueUpdated", val, "should get updated value for key")

		// 5. Apply a 'delete' operation
		deleteEntry := createLogEntry(t, "delete", "key1", "")
		result = sm.Apply(deleteEntry)
		assert.Nil(t, result, "'delete' operation should return nil result")

		// 6. Get the deleted key
		_, err = sm.Get("key1")
		assert.ErrorIs(t, err, ErrKeyNotFound, "should return ErrKeyNotFound for deleted key")
	})

	t.Run("Apply with invalid operation", func(t *testing.T) {
		sm := NewInMemoryStateMachine()
		invalidOpEntry := createLogEntry(t, "invalid-op", "key1", "value1")
		result := sm.Apply(invalidOpEntry)
		_, ok := result.(error)
		assert.True(t, ok, "should return an error for unknown operation")
	})

	t.Run("Apply with invalid command format (panic test)", func(t *testing.T) {
		// This test ensures the code panics as expected for a malformed log entry.
		defer func() {
			if r := recover(); r == nil {
				t.Error("The code did not panic on malformed command")
			}
		}()

		sm := NewInMemoryStateMachine()
		invalidEntry := param.LogEntry{Command: []byte("this is not valid json")}
		sm.Apply(invalidEntry)
	})

	t.Run("Snapshot and Restore operations", func(t *testing.T) {
		// 1. Create original state machine and populate it
		sm1 := NewInMemoryStateMachine()
		sm1.Apply(createLogEntry(t, "set", "name", "gopher"))
		sm1.Apply(createLogEntry(t, "set", "lang", "go"))

		// 2. Get a snapshot
		snapshot, err := sm1.GetSnapshot()
		assert.NoError(t, err, "GetSnapshot should not fail")
		assert.NotEmpty(t, snapshot, "GetSnapshot should return non-empty snapshot")

		// 3. Create a new state machine and apply the snapshot
		sm2 := NewInMemoryStateMachine()
		err = sm2.ApplySnapshot(snapshot)
		assert.NoError(t, err, "ApplySnapshot should not fail")

		// 4. Verify the state of the new state machine
		val, err := sm2.Get("name")
		assert.NoError(t, err, "should get 'name' without error")
		assert.Equal(t, "gopher", val, "restored state should have correct value for 'name'")

		val, err = sm2.Get("lang")
		assert.NoError(t, err, "should get 'lang' without error")
		assert.Equal(t, "go", val, "restored state should have correct value for 'lang'")

		// 5. Ensure internal maps are distinct
		assert.True(t, reflect.DeepEqual(sm1.kvStore, sm2.kvStore), "restored kvStore should be deeply equal to the original")

		// A check to ensure they are not the same map instance (ApplySnapshot should create a new map)
		sm1.Apply(createLogEntry(t, "set", "newKey", "newValue"))
		_, err = sm2.Get("newKey")
		assert.Error(t, err, "modifying original state machine should not affect the restored one")
	})

	t.Run("ApplySnapshot overwrites existing state", func(t *testing.T) {
		// Create snapshot from sm1
		sm1 := NewInMemoryStateMachine()
		sm1.Apply(createLogEntry(t, "set", "a", "1"))
		sm1.Apply(createLogEntry(t, "set", "b", "2"))
		snapshot, _ := sm1.GetSnapshot()

		// Create sm2 with conflicting and extra data
		sm2 := NewInMemoryStateMachine()
		sm2.Apply(createLogEntry(t, "set", "b", "old_value"))
		sm2.Apply(createLogEntry(t, "set", "c", "3"))

		// Apply snapshot and check state
		err := sm2.ApplySnapshot(snapshot)
		assert.NoError(t, err, "ApplySnapshot should not fail")

		// Check that 'a' exists, 'b' is updated, and 'c' is gone
		val, err := sm2.Get("a")
		assert.NoError(t, err, "should get 'a' without error")
		assert.Equal(t, "1", val, "restored state should have correct value for 'a'")
		val, err = sm2.Get("b")
		assert.NoError(t, err, "should get 'b' without error")
		assert.Equal(t, "2", val, "restored state should have updated value for 'b'")
		_, err = sm2.Get("c")
		assert.ErrorIs(t, err, ErrKeyNotFound, "'c' should not exist after applying snapshot")
	})

	t.Run("ApplySnapshot with invalid data", func(t *testing.T) {
		sm := NewInMemoryStateMachine()
		invalidSnapshot := []byte("{not-a-valid-json}")
		err := sm.ApplySnapshot(invalidSnapshot)
		assert.Error(t, err, "ApplySnapshot should fail with invalid snapshot data")
	})
}

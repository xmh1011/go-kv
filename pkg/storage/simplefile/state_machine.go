package simplefile

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/xmh1011/go-kv/pkg/param"
)

var ErrKeyNotFound = errors.New("key not found")

// StateMachine implements a simple file-based key-value store.
// It persists the entire state (map) to a file on every write operation.
type StateMachine struct {
	mu       sync.RWMutex
	filePath string
	kvStore  map[string]string
}

// NewStateMachine creates a new simple file-based state machine.
func NewStateMachine(filePath string) (*StateMachine, error) {
	sm := &StateMachine{
		filePath: filePath,
		kvStore:  make(map[string]string),
	}

	if err := sm.load(); err != nil {
		if os.IsNotExist(err) {
			// If file doesn't exist, start with empty store and persist it
			if err := sm.persist(); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return sm, nil
}

func (sm *StateMachine) load() error {
	data, err := os.ReadFile(sm.filePath)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, &sm.kvStore)
}

func (sm *StateMachine) persist() error {
	data, err := json.Marshal(sm.kvStore)
	if err != nil {
		return err
	}

	// Write to temp file and rename for atomicity
	tmpPath := sm.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, sm.filePath)
}

// Apply applies a log entry to the state machine.
func (sm *StateMachine) Apply(entry param.LogEntry) any {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var cmd param.KVCommand
	if err := json.Unmarshal(entry.Command.([]byte), &cmd); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %v", err))
	}

	switch cmd.Op {
	case "set":
		sm.kvStore[cmd.Key] = cmd.Value
		if err := sm.persist(); err != nil {
			// In a real system, we might want to handle persistence failure more gracefully,
			// but for this simple implementation, logging or panicking might be the only option
			// if we can't guarantee durability.
			// For now, we'll just return the error as the result.
			return err
		}
		return nil
	case "delete":
		delete(sm.kvStore, cmd.Key)
		if err := sm.persist(); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unknown operation: %s", cmd.Op)
	}
}

// Get queries a key from the state machine.
func (sm *StateMachine) Get(key string) (string, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if val, ok := sm.kvStore[key]; ok {
		return val, nil
	}
	return "", ErrKeyNotFound
}

// GetSnapshot generates a snapshot of the state machine.
func (sm *StateMachine) GetSnapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return json.Marshal(sm.kvStore)
}

// ApplySnapshot restores the state machine from a snapshot.
func (sm *StateMachine) ApplySnapshot(snapshot []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var newStore map[string]string
	if err := json.Unmarshal(snapshot, &newStore); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	sm.kvStore = newStore
	return sm.persist()
}

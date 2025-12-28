package inmemory

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/xmh1011/go-kv/pkg/param"
)

var ErrKeyNotFound = errors.New("key not found")

// StateMachine 是 StateMachine 接口的一个内存实现，模拟一个简单的KV数据库。
type StateMachine struct {
	mu      sync.RWMutex
	kvStore map[string]string
}

// NewInMemoryStateMachine 创建一个新的内存状态机实例。
func NewInMemoryStateMachine() *StateMachine {
	return &StateMachine{
		kvStore: make(map[string]string),
	}
}

// Apply 将日志条目应用到状态机。
func (sm *StateMachine) Apply(entry param.LogEntry) any {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 假设 Command 是 json 序列化后的 KVCommand
	var cmd param.KVCommand
	// 实际应用中，需要更健壮的命令解析
	if err := json.Unmarshal(entry.Command.([]byte), &cmd); err != nil {
		// 在真实场景中，应 panic 或记录严重错误，因为已提交的日志不应是无效的
		panic(fmt.Sprintf("failed to unmarshal command: %v", err))
	}

	switch cmd.Op {
	case "set":
		sm.kvStore[cmd.Key] = cmd.Value
		return nil // 写操作通常不返回结果
	case "delete":
		delete(sm.kvStore, cmd.Key)
		return nil
	default:
		// 对于 Get 操作，通常不通过 Apply，但这里可以返回错误
		return fmt.Errorf("unknown operation: %s", cmd.Op)
	}
}

// Get 从状态机中查询一个键的值。
func (sm *StateMachine) Get(key string) (string, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if val, ok := sm.kvStore[key]; ok {
		return val, nil
	}
	return "", ErrKeyNotFound
}

// GetSnapshot 生成状态机的快照。
func (sm *StateMachine) GetSnapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// 使用 JSON 格式作为快照
	return json.Marshal(sm.kvStore)
}

// ApplySnapshot 从快照中恢复状态机。
func (sm *StateMachine) ApplySnapshot(snapshot []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var newStore map[string]string
	if err := json.Unmarshal(snapshot, &newStore); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	// 用快照数据完全替换当前状态
	sm.kvStore = newStore
	return nil
}

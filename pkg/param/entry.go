package param

import (
	"encoding/base64"
	"encoding/json"
)

// LogEntry represents a single log entry in the Raft log.
type LogEntry struct {
	Command any
	Term    uint64
	Index   uint64 // Log index, 0 means no index (e.g., for heartbeats)
}

// UnmarshalJSON 自定义 LogEntry 的 JSON 反序列化逻辑
func (le *LogEntry) UnmarshalJSON(data []byte) error {
	// 1. 使用一个临时的别名类型来避免无限递归
	type Alias LogEntry
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(le),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// 2. 检查 Command 是否为 Base64 编码的字符串
	if str, ok := le.Command.(string); ok {
		// 尝试 Base64 解码
		if decoded, err := base64.StdEncoding.DecodeString(str); err == nil {
			le.Command = decoded
		}
		// 如果解码失败，保留原字符串（可能是普通的字符串命令）
	}

	// 3. 尝试将 map[string]interface{} 转换为具体的命令类型
	// 这一步对于 ConfigChangeCommand 是必要的，因为它是一个结构体
	if m, ok := le.Command.(map[string]interface{}); ok {
		// 检查是否符合 ConfigChangeCommand 的特征
		if _, ok := m["NewPeerIDs"]; ok {
			var cfgCmd ConfigChangeCommand
			// 重新序列化再反序列化是一种简单（虽然效率不高）的转换方式
			if b, err := json.Marshal(m); err == nil {
				if err := json.Unmarshal(b, &cfgCmd); err == nil {
					le.Command = cfgCmd
				}
			}
		}
	}

	return nil
}

// NewLogEntry creates a new LogEntry.
func NewLogEntry(command any, term, index uint64) LogEntry {
	return LogEntry{
		Command: command,
		Term:    term,
		Index:   index,
	}
}

// CommitEntry is the data reported by Raft to the commit channel.
// Each commit entry notifies the client that consensus was reached on a command,
// and it can be applied to the client's state machine.
type CommitEntry struct {
	// Command is the client command being committed
	Command any

	// Index is the log index at which the client command is committed
	Index uint64

	// Term is the Raft term at which the client command is committed
	Term uint64
}

// Snapshot 表示 Raft 的快照结构
type Snapshot struct {
	LastIncludedIndex uint64 // 快照中包含的最后一条日志的索引
	LastIncludedTerm  uint64 // 快照中包含的最后一条日志的任期
	Data              []byte // 状态机的快照数据
}

// NewSnapshot 创建一个新的 Snapshot 实例
func NewSnapshot(lastIncludedIndex, lastIncludedTerm uint64, data []byte) *Snapshot {
	return &Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}
}

// HardState 定义需要持久化的状态（必须稳定存储）
type HardState struct {
	CurrentTerm uint64 // 当前任期号
	VotedFor    uint64 // 当前任期内投票给的候选者ID（0表示未投票）
	CommitIndex uint64 // 已知已提交的最高日志索引
}

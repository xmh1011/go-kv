package param

import (
	"encoding/gob"
)

func init() {
	gob.Register(KVCommand{})
	gob.Register(ConfigChangeCommand{})
}

// ClientArgs 封装了来自客户端的请求。
type ClientArgs struct {
	ClientID    int64 // 客户端的唯一ID
	SequenceNum int64 // 客户端为每个请求生成的、单调递增的序列号
	Command     any   // 需要在状态机上执行的命令
}

// NewClientArgs 创建一个新的 ClientArgs 实例。
func NewClientArgs(clientID, sequenceNum int64, command any) *ClientArgs {
	return &ClientArgs{
		ClientID:    clientID,
		SequenceNum: sequenceNum,
		Command:     command,
	}
}

// ClientReply 是 Raft 节点对客户端请求的响应。
type ClientReply struct {
	Success    bool // 请求是否成功处理
	Result     any  // 命令执行后的返回值
	NotLeader  bool // 如果当前节点不是 Leader，此项为 true
	LeaderHint int  // 当前已知的 Leader ID，用于客户端重定向
}

// ConfigChangeCommand holds the new list of peer IDs for a configuration change.
// This command is stored in a LogEntry to be replicated.
type ConfigChangeCommand struct {
	NewPeerIDs []int
}

// NewConfigChangeCommand creates a new ConfigChangeCommand.
func NewConfigChangeCommand(newPeerIDs []int) ConfigChangeCommand {
	return ConfigChangeCommand{
		NewPeerIDs: newPeerIDs,
	}
}

// KVCommand 定义了客户端与状态机交互的命令格式。
type KVCommand struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

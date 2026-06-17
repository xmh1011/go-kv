package param

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strings"
)

func init() {
	gob.Register(KVCommand{})
	gob.Register(ClientCommand{})
	gob.Register(ConfigChangeCommand{})
	gob.Register(NoopCommand{})
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

// ClientCommand is the replicated form of a client write request. Keeping the
// client identity in the log lets every node update duplicate-detection state
// when the entry is applied, even if the original RPC times out.
type ClientCommand struct {
	ClientID    int64
	SequenceNum int64
	Command     any
}

func NewClientCommand(clientID, sequenceNum int64, command any) ClientCommand {
	return ClientCommand{
		ClientID:    clientID,
		SequenceNum: sequenceNum,
		Command:     command,
	}
}

func UnwrapClientCommand(command any) any {
	if wrapped, ok := command.(ClientCommand); ok {
		return wrapped.Command
	}
	return command
}

func ClientCommandMetadata(command any) (clientID, sequenceNum int64, ok bool) {
	if wrapped, ok := command.(ClientCommand); ok {
		return wrapped.ClientID, wrapped.SequenceNum, true
	}
	return 0, 0, false
}

// NoopCommand is an internal Raft entry with no state-machine side effect.
// Leaders append one after election so entries from older terms can be safely
// committed by a current-term log entry, as required by Raft.
type NoopCommand struct{}

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

type OpType int

const (
	OpUnknown OpType = iota
	OpGet
	OpSet
	OpDelete
)

func StringToOpType(s string) OpType {
	switch strings.ToLower(s) {
	case "get":
		return OpGet
	case "set":
		return OpSet
	case "delete":
		return OpDelete
	default:
		return OpUnknown
	}
}

// UnmarshalJSON 支持从 int 或 string 反序列化 OpType
func (o *OpType) UnmarshalJSON(data []byte) error {
	// 尝试作为整数解析
	var i int
	if err := json.Unmarshal(data, &i); err == nil {
		*o = OpType(i)
		return nil
	}

	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		op := StringToOpType(s)
		if op == OpUnknown && strings.ToLower(s) != "unknown" {
			return fmt.Errorf("unknown OpType string: %q", s)
		}
		*o = op
		return nil
	}

	return fmt.Errorf("cannot unmarshal %s into OpType", data)
}

// KVCommand 定义了客户端与状态机交互的命令格式。
type KVCommand struct {
	Op    OpType `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

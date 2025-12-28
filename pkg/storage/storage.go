package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/xmh1011/go-kv/engine/lsm/database"
	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage/inmemory"
	"github.com/xmh1011/go-kv/pkg/storage/lsm"
	"github.com/xmh1011/go-kv/pkg/storage/simplefile"
)

const (
	InmemoryStorage   = "inmemory"
	SimpleFileStorage = "simplefile"
	LSMStorage        = "lsm"
)

// Storage is an interface for stable storage providers in a Raft implementation.
// 它负责持久化 Raft 的核心状态（如 currentTerm 和 votedFor）以及日志条目。
// Raft 共识模块的持久化层。它需要保证在崩溃恢复后能够恢复 Raft 的状态。
type Storage interface {
	// --- HardState 操作 ---

	// SetState 原子地设置 HardState (currentTerm, votedFor)。
	SetState(state param.HardState) error
	// GetState 获取最后保存的 HardState。
	GetState() (param.HardState, error)

	// --- 日志条目操作 ---

	// AppendEntries 追加一批日志条目。实现必须保证这个操作的原子性。
	// 对于 LSM 树，这通常通过一个 Write Batch 来实现。
	AppendEntries(entries []param.LogEntry) error

	// GetEntry 获取指定索引的日志条目。
	GetEntry(index uint64) (*param.LogEntry, error)

	// TruncateLog 删除从 a_given_index (包含) 到日志末尾的所有条目。
	// 当 Follower 的日志与 Leader 发生冲突时，这是必须的操作。
	TruncateLog(fromIndex uint64) error

	// --- 日志元数据操作 ---

	// FirstLogIndex 返回日志中的第一条条目的索引。
	FirstLogIndex() (uint64, error)
	// LastLogIndex 返回日志中的最后一条条目的索引。
	LastLogIndex() (uint64, error)

	LogSize() (int, error) // 返回日志的大小（例如，字节数或条目数）

	// --- 快照操作 ---

	// SaveSnapshot 原子地保存快照数据和元数据。
	// 它应该替换掉任何旧的快照。
	SaveSnapshot(snapshot *param.Snapshot) error

	// ReadSnapshot 读取最后保存的快照。
	// 如果没有快照，则返回一个零值的 Snapshot 结构体。
	ReadSnapshot() (*param.Snapshot, error)

	// CompactLog 永久性地删除指定索引（包含）之前的所有日志。
	// 这个操作在快照成功保存后被调用。
	CompactLog(upToIndex uint64) error

	// Close 关闭数据库连接。
	Close() error
}

// StateMachine 定义了应用层状态机需要实现的接口。
// Raft 模块通过这个接口与上层的业务逻辑（例如，一个 KV 存储）进行交互。
type StateMachine interface {
	// Apply 将一条已经由 Raft 达成共识的日志条目应用到状态机中。
	// 这个方法由 Raft 节点的 applyLogs 循环调用。
	// 对于写请求（如 Put, Delete），状态机需要在这里解析命令并更新其内部状态（例如，写入 LSM 树）。
	// 它应该返回命令执行的结果，这个结果最终会传递给等待的客户端。
	Apply(entry param.LogEntry) any

	// Get 对状态机进行一次只读查询。
	// 这个方法用于处理客户端的读请求。为保证线性一致性读（Linearizable Read），
	// 在一个完整的实现中，这个操作需要与 Leader 确认其领导地位（ReadIndex 或租约机制）。
	// 在简化实现中，它可以直接读取当前状态。
	Get(key string) (string, error)

	// GetSnapshot 请求状态机生成一个当前状态的快照。
	// 返回的数据是一个 []byte，代表了状态机所有数据的序列化形式。
	// 这个方法在 Raft 模块需要进行日志压缩时被调用。
	GetSnapshot() ([]byte, error)

	// ApplySnapshot 将一个快照应用到状态机，用快照中的数据完全覆盖当前状态。
	// 这个方法在 Raft 节点从 Leader 接收并安装快照时被调用。
	ApplySnapshot(snapshot []byte) error
}

func NewStorage(storageType, dataDir string, nodeID int) (Storage, StateMachine, error) {
	nodeDir := fmt.Sprintf("%s/node-%d", dataDir, nodeID)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	switch storageType {
	case InmemoryStorage:
		log.Info("Using in-memory storage")
		return inmemory.NewStorage(), inmemory.NewInMemoryStateMachine(), nil
	case SimpleFileStorage:
		storagePath := filepath.Join(nodeDir, "raft_storage.gob")
		smPath := filepath.Join(nodeDir, "raft_sm.json")

		store, err := simplefile.NewStorage(storagePath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create simplefile storage: %w", err)
		}

		stateMachine, err := simplefile.NewStateMachine(smPath)
		if err != nil {
			store.Close()
			return nil, nil, fmt.Errorf("failed to create simplefile state machine: %w", err)
		}
		log.Infof("Using simple file storage at %s", nodeDir)
		return store, stateMachine, nil
	case LSMStorage:
		// State Machine: 使用 LSM 引擎
		lsmDB := database.Open(filepath.Join(nodeDir, "lsm_statemachine"))
		if err := lsmDB.Recover(); err != nil {
			return nil, nil, fmt.Errorf("failed to recover lsm database for state machine: %w", err)
		}
		lsmStateMachine := lsm.NewStateMachineAdapter(lsmDB)

		// Raft Log Storage: 使用另一个独立的 LSM 实例
		raftLogDB := database.Open(filepath.Join(nodeDir, "lsm_raftlog"))
		if err := raftLogDB.Recover(); err != nil {
			// 如果 raft log 恢复失败，需要关闭 state machine
			lsmStateMachine.Close()
			return nil, nil, fmt.Errorf("failed to recover lsm database for raft log: %w", err)
		}
		lsmStorage, err := lsm.NewStorageAdapter(raftLogDB)
		if err != nil {
			// 如果 storage adapter 创建失败，需要关闭 state machine 和 raft log db
			lsmStateMachine.Close()
			raftLogDB.Close()
			return nil, nil, fmt.Errorf("failed to create lsm storage adapter: %w", err)
		}

		log.Infof("Using LSM storage engine at %s", nodeDir)
		return lsmStorage, lsmStateMachine, nil
	default:
		return nil, nil, fmt.Errorf("unknown storage type: %s", storageType)
	}
}

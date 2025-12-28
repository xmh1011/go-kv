package lsm

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/xmh1011/go-kv/engine/lsm/database"
	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
)

const (
	keyHardState  = "meta:hard_state"
	keyFirstIndex = "meta:first_index"
	keyLastIndex  = "meta:last_index"
	keyLogSize    = "meta:log_size"
	keySnapshot   = "meta:snapshot"
	logKeyPrefix  = "log:"
)

// StorageAdapter 实现了 storage.Storage 接口，
// 使用 LSM 树来存储 Raft 的日志条目和元数据。
type StorageAdapter struct {
	db *database.Database
	mu sync.RWMutex

	// 缓存元数据以提高性能
	firstIndex uint64
	lastIndex  uint64
	logSize    int
}

// NewStorageAdapter 创建一个新的 LSM 存储适配器。
func NewStorageAdapter(db *database.Database) (*StorageAdapter, error) {
	s := &StorageAdapter{db: db}
	if err := s.init(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *StorageAdapter) init() error {
	// 恢复 FirstIndex
	val, err := s.db.Get(keyFirstIndex)
	if err != nil {
		return fmt.Errorf("get first index failed: %w", err)
	}
	if val != nil {
		if err := json.Unmarshal(val, &s.firstIndex); err != nil {
			return fmt.Errorf("unmarshal first index failed: %w", err)
		}
	} else {
		s.firstIndex = 1 // 默认为 1
	}

	// 恢复 LastIndex
	val, err = s.db.Get(keyLastIndex)
	if err != nil {
		return fmt.Errorf("get last index failed: %w", err)
	}
	if val != nil {
		if err := json.Unmarshal(val, &s.lastIndex); err != nil {
			return fmt.Errorf("unmarshal last index failed: %w", err)
		}
	} else {
		s.lastIndex = 0
	}

	// 恢复 LogSize
	val, err = s.db.Get(keyLogSize)
	if err != nil {
		return fmt.Errorf("get log size failed: %w", err)
	}
	if val != nil {
		if err := json.Unmarshal(val, &s.logSize); err != nil {
			return fmt.Errorf("unmarshal log size failed: %w", err)
		}
	} else {
		s.logSize = 0
	}

	log.Infof("[LSMStorage] Initialized. FirstIndex: %d, LastIndex: %d, LogSize: %d", s.firstIndex, s.lastIndex, s.logSize)
	return nil
}

func (s *StorageAdapter) getLogKey(index uint64) string {
	return fmt.Sprintf("%s%020d", logKeyPrefix, index)
}

// SetState 原子地设置 HardState (currentTerm, votedFor)。
func (s *StorageAdapter) SetState(state param.HardState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := s.db.Put(keyHardState, data); err != nil {
		log.Errorf("[LSMStorage] SetState failed: %v", err)
		return err
	}
	return nil
}

// GetState 获取最后保存的 HardState。
func (s *StorageAdapter) GetState() (param.HardState, error) {
	var state param.HardState
	val, err := s.db.Get(keyHardState)
	if err != nil {
		return state, err
	}
	if val == nil {
		return state, nil // 返回空状态
	}
	if err := json.Unmarshal(val, &state); err != nil {
		return state, err
	}
	return state, nil
}

// AppendEntries 追加一批日志条目。
func (s *StorageAdapter) AppendEntries(entries []param.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		key := s.getLogKey(entry.Index)
		if err := s.db.Put(key, data); err != nil {
			log.Errorf("[LSMStorage] Append entry %d failed: %v", entry.Index, err)
			return err
		}
		s.logSize += len(data)

		// 更新 LastIndex
		if entry.Index > s.lastIndex {
			s.lastIndex = entry.Index
		}
	}

	// 持久化 LastIndex 和 LogSize
	if err := s.saveMetadata(); err != nil {
		return err
	}

	return nil
}

// GetEntry 获取指定索引的日志条目。
func (s *StorageAdapter) GetEntry(index uint64) (*param.LogEntry, error) {
	val, err := s.db.Get(s.getLogKey(index))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	var entry param.LogEntry
	if err := json.Unmarshal(val, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// TruncateLog 删除从 fromIndex (包含) 到日志末尾的所有条目。
func (s *StorageAdapter) TruncateLog(fromIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if fromIndex > s.lastIndex {
		return nil
	}

	// 逐个删除
	for i := fromIndex; i <= s.lastIndex; i++ {
		key := s.getLogKey(i)
		// 获取旧值以更新 size
		val, _ := s.db.Get(key)
		if val != nil {
			s.logSize -= len(val)
		}
		if err := s.db.Delete(key); err != nil {
			return err
		}
	}

	s.lastIndex = fromIndex - 1
	if s.lastIndex < s.firstIndex-1 {
		s.lastIndex = s.firstIndex - 1
	}

	return s.saveMetadata()
}

// FirstLogIndex 返回日志中的第一条条目的索引。
func (s *StorageAdapter) FirstLogIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.firstIndex, nil
}

// LastLogIndex 返回日志中的最后一条条目的索引。
func (s *StorageAdapter) LastLogIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndex, nil
}

// LogSize 返回日志的大小。
func (s *StorageAdapter) LogSize() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.logSize, nil
}

// SaveSnapshot 原子地保存快照数据和元数据。
func (s *StorageAdapter) SaveSnapshot(snapshot *param.Snapshot) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	if err := s.db.Put(keySnapshot, data); err != nil {
		log.Errorf("[LSMStorage] SaveSnapshot failed: %v", err)
		return err
	}
	return nil
}

// ReadSnapshot 读取最后保存的快照。
func (s *StorageAdapter) ReadSnapshot() (*param.Snapshot, error) {
	val, err := s.db.Get(keySnapshot)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	var snapshot param.Snapshot
	if err := json.Unmarshal(val, &snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

// CompactLog 永久性地删除指定索引（包含）之前的所有日志。
func (s *StorageAdapter) CompactLog(upToIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if upToIndex < s.firstIndex {
		return nil
	}

	// 逐个删除
	for i := s.firstIndex; i <= upToIndex; i++ {
		key := s.getLogKey(i)
		val, _ := s.db.Get(key)
		if val != nil {
			s.logSize -= len(val)
		}
		if err := s.db.Delete(key); err != nil {
			return err
		}
	}

	s.firstIndex = upToIndex + 1
	if s.firstIndex > s.lastIndex+1 {
		s.firstIndex = s.lastIndex + 1
	}

	return s.saveMetadata()
}

// Close 关闭数据库连接。
func (s *StorageAdapter) Close() error {
	return s.db.Close()
}

func (s *StorageAdapter) saveMetadata() error {
	// 保存 FirstIndex
	if err := s.saveFirstIndex(); err != nil {
		return err
	}
	// 保存 LastIndex
	if err := s.saveLastIndex(); err != nil {
		return err
	}
	// 保存 LogSize
	if err := s.saveLogSize(); err != nil {
		return err
	}
	return nil
}

func (s *StorageAdapter) saveFirstIndex() error {
	data, err := json.Marshal(s.firstIndex)
	if err != nil {
		return err
	}
	return s.db.Put(keyFirstIndex, data)
}

func (s *StorageAdapter) saveLastIndex() error {
	data, err := json.Marshal(s.lastIndex)
	if err != nil {
		return err
	}
	return s.db.Put(keyLastIndex, data)
}

func (s *StorageAdapter) saveLogSize() error {
	data, err := json.Marshal(s.logSize)
	if err != nil {
		return err
	}
	return s.db.Put(keyLogSize, data)
}

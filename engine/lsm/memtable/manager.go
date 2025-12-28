package memtable

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/xmh1011/go-kv/engine/lsm/kv"
	"github.com/xmh1011/go-kv/pkg/config"
	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/utils"
)

var idGenerator atomic.Uint64

// ResetIDGenerator 重置 ID 生成器
func ResetIDGenerator() {
	idGenerator.Store(0)
}

type Manager struct {
	mu                sync.RWMutex
	walPath           string
	Mem               *MemTable
	IMems             []*IMemTable
	maxIMemTableCount int
}

func NewMemTableManager(walPath string) *Manager {
	// Ensure WAL directory exists
	if err := os.MkdirAll(walPath, 0755); err != nil {
		log.Errorf("[MemTableManager] Failed to create WAL directory %s: %s", walPath, err.Error())
	}

	return &Manager{
		walPath:           walPath,
		Mem:               NewMemTable(idGenerator.Add(1), walPath),
		IMems:             make([]*IMemTable, 0),
		maxIMemTableCount: config.Conf.LSM.MaxIMemTableCount,
	}
}

func (m *Manager) Insert(pair kv.KeyValuePair) (*IMemTable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Mem.CanInsert(pair) {
		if err := m.Mem.Insert(pair); err != nil {
			log.Errorf("[MemTableManager] Insert memtable error: %s", err.Error())
			return nil, fmt.Errorf("insert memtable error: %w", err)
		}
		return nil, nil
	}

	log.Info("[MemTableManager] MemTable full, promoting to Immutable MemTable")
	evicted := m.promoteLocked()
	if err := m.Mem.Insert(pair); err != nil {
		log.Errorf("[MemTableManager] Insert after promote error: %s", err.Error())
		return nil, fmt.Errorf("insert after promote error: %w", err)
	}

	return evicted, nil
}

func (m *Manager) Search(key kv.Key) kv.Value {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if value, ok := m.Mem.Search(key); ok {
		return value
	}
	for i := len(m.IMems) - 1; i >= 0; i-- {
		if value, ok := m.IMems[i].Search(key); ok {
			return value
		}
	}
	return nil
}

func (m *Manager) Delete(key kv.Key) (*IMemTable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.Mem.Delete(key); err != nil {
		log.Errorf("[MemTableManager] Delete memtable error: %s", err.Error())
		return nil, fmt.Errorf("delete memtable error: %w", err)
	}

	// 如果 memtable 中没有这个 key，但是说明有可能存在于内存中的 imemtable
	// 而 imemtable 是不可变的，所以这时候需要在 memtable 中插入一条 deleted 记录
	pair := kv.KeyValuePair{
		Key:   key,
		Value: kv.DeletedValue,
	}
	if m.Mem.CanInsert(pair) {
		if err := m.Mem.Insert(pair); err != nil {
			log.Errorf("[MemTableManager] Insert memtable error: %s", err.Error())
			return nil, fmt.Errorf("insert memtable error: %w", err)
		}
		return nil, nil
	}

	log.Info("[MemTableManager] MemTable full during delete, promoting to Immutable MemTable")
	evicted := m.promoteLocked()
	if err := m.Mem.Insert(pair); err != nil {
		log.Errorf("[MemTableManager] Insert after promote error: %s", err.Error())
		return nil, fmt.Errorf("insert after promote error: %w", err)
	}

	return evicted, nil
}

func (m *Manager) GetAll() []*IMemTable {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]*IMemTable, len(m.IMems))
	copy(out, m.IMems)
	return out
}

// promoteLocked：仅在已持有写锁的情况下调用！
func (m *Manager) promoteLocked() *IMemTable {
	var evicted *IMemTable
	if len(m.IMems) >= m.maxIMemTableCount {
		evicted = m.IMems[0]
		m.IMems = m.IMems[1:]
		log.Warn("[MemTableManager] Too many Immutable MemTables, evicting oldest")
	}

	imem := NewIMemTable(m.Mem)
	m.IMems = append(m.IMems, imem)
	m.Mem = NewMemTable(idGenerator.Add(1), m.walPath)

	return evicted
}

// ForcePromote 强制将当前 MemTable 转换为 Immutable MemTable，并返回新生成的 IMemTable。
// 这通常用于快照或手动 Flush。
func (m *Manager) ForcePromote() *IMemTable {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 如果当前 MemTable 为空，则不进行 Promote
	if m.Mem.ApproximateSize() == 0 {
		return nil
	}

	log.Info("[MemTableManager] Force promoting MemTable to Immutable MemTable")
	m.promoteLocked()

	if len(m.IMems) > 0 {
		return m.IMems[len(m.IMems)-1]
	}
	return nil
}

func (m *Manager) CanInsert(pair kv.KeyValuePair) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.Mem.CanInsert(pair)
}

// Recover 从 WALManager 恢复所有 memtable 数据，最多构造 10 个 IMemTable 和 1 个 MemTable
func (m *Manager) Recover() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ResetIDGenerator()
	// 收集所有 WAL 恢复数据
	files, err := os.ReadDir(m.walPath) // 返回的是文件名，而不是文件完整路径
	if err != nil {
		// 如果目录不存在，则不需要恢复，直接返回
		if os.IsNotExist(err) {
			log.Infof("[MemTableManager] WAL directory %s does not exist, skipping recovery", m.walPath)
			return nil
		}
		log.Errorf("[MemTableManager] Failed to read WAL directory %s: %s", m.walPath, err.Error())
		return fmt.Errorf("failed to read WAL directory %s: %w", m.walPath, err)
	}
	// 将所有 WAL 按照 ID 排序，最新的加载为 memtable，其余加载为 imemtable
	sort.Slice(files, func(i, j int) bool { return utils.ExtractID(files[i].Name()) < utils.ExtractID(files[j].Name()) })
	// 构建 IMemTable 和 MemTable
	for i, file := range files {
		mem := NewMemTableWithoutWAL()
		if err = mem.RecoverFromWAL(file.Name(), m.walPath); err != nil {
			log.Errorf("[MemTableManager] Recover from WAL %s failed: %s", file.Name(), err.Error())
			return fmt.Errorf("recover from WAL %s failed: %w", file.Name(), err)
		}
		if i == len(files)-1 {
			m.Mem = mem
			// 并且处理自增 id 的逻辑
			idGenerator.Add(utils.ExtractID(file.Name()))
		} else {
			// 其他的都作为 IMemTable
			if err = mem.Close(); err != nil {
				log.Errorf("[MemTableManager] Close WAL file %s for imemtable failed: %s", file.Name(), err.Error())
				return err
			}
			m.IMems = append(m.IMems, NewIMemTable(mem))
		}
	}

	// 保证最多 10 个 IMemTable
	if len(m.IMems) > m.maxIMemTableCount {
		m.IMems = m.IMems[len(m.IMems)-m.maxIMemTableCount:]
	}

	log.Infof("[MemTableManager] Recovered %d Immutable MemTables and 1 Active MemTable", len(m.IMems))
	return nil
}

// Close closes the active MemTable's WAL.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Mem != nil {
		return m.Mem.Close()
	}
	return nil
}

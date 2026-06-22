package sstable

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/xmh1011/go-kv/engine/lsm/kv"
	"github.com/xmh1011/go-kv/engine/lsm/memtable"
	"github.com/xmh1011/go-kv/engine/lsm/sstable/block"
	"github.com/xmh1011/go-kv/pkg/config"
	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/utils"
)

// Manager 管理内存中的 SSTable 元信息（Footer/Filter/Index）+ 磁盘中的文件记录。
type Manager struct {
	mu sync.RWMutex

	sstPath string

	// levels 保存各层级的 SSTable 元信息，按层级分组，每层内按 id 降序排序
	levels [][]*SSTable

	// fileIndex 用于快速查找 SSTable 的索引 (文件路径 -> *SSTable)
	fileIndex map[string]*SSTable

	// totalMap 记录所有层级的文件路径
	totalMap map[int][]string

	// 稀疏索引，按照 key 排序 level 1 及以上的 SSTable，用于查找
	sparseIndexes [][]*SSTable

	// 异步合并控制
	compactionCond   *sync.Cond
	compactingLevels map[int]bool // 记录各层级的压缩状态
	compactionWG     sync.WaitGroup
	nextID           atomic.Uint64

	minSSTableLevel int
	maxSSTableLevel int
	levelSizeBase   int
}

// SnapshotFile is an opened immutable SSTable file captured for snapshot export.
// The open file descriptor pins the file content while compaction is free to
// update the manager catalog after OpenFilesSnapshot returns.
type SnapshotFile struct {
	Path string
	File *os.File
}

func NewSSTableManager(sstPath string) *Manager {
	minLevel := config.Conf.LSM.MinSSTableLevel
	maxLevel := config.Conf.LSM.MaxSSTableLevel

	// 创建 SSTable 目录
	err := os.MkdirAll(sstPath, os.ModePerm)
	if err != nil {
		log.Errorf("[SSTableManager] Failed to create sstable directory: %s", err.Error())
	}
	// 为各层创建对应目录
	for i := minLevel; i <= maxLevel; i++ {
		if err = os.MkdirAll(sstableLevelPath(i, sstPath), os.ModePerm); err != nil {
			log.Errorf("[SSTableManager] Failed to create sstable level %d directory: %s", i, err.Error())
		}
	}

	mgr := &Manager{
		sstPath:          sstPath,
		levels:           make([][]*SSTable, maxLevel+1),
		fileIndex:        make(map[string]*SSTable),
		totalMap:         make(map[int][]string),
		compactingLevels: make(map[int]bool),
		sparseIndexes:    make([][]*SSTable, maxLevel),
		minSSTableLevel:  minLevel,
		maxSSTableLevel:  maxLevel,
		levelSizeBase:    config.Conf.LSM.LevelSizeBase,
	}
	mgr.compactionCond = sync.NewCond(&mgr.mu)
	return mgr
}

// OpenFilesSnapshot opens the current SSTable files while holding the manager
// read lock, then returns with the lock released. SSTable files are immutable,
// so open file descriptors can be read safely even if later compaction removes
// the original paths.
func (m *Manager) OpenFilesSnapshot() ([]SnapshotFile, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	files := make([]SnapshotFile, 0)
	for _, tables := range m.levels {
		for _, table := range tables {
			path := table.FilePath()
			file, err := os.Open(path)
			if err != nil {
				for _, opened := range files {
					_ = opened.File.Close()
				}
				return nil, fmt.Errorf("open snapshot sstable %s: %w", path, err)
			}
			files = append(files, SnapshotFile{
				Path: path,
				File: file,
			})
		}
	}
	return files, nil
}

// CreateNewSSTable 将 imem 数据构建为 SSTable，写入到磁盘，然后将其元数据添加到内存中。
func (m *Manager) CreateNewSSTable(imem *memtable.IMemTable) error {
	sst := m.buildSSTableFromIMemTable(imem)
	if sst.DataBlock.Len() == 0 {
		imem.Clean()
		log.Debugf("[SSTableManager] Skipped empty immutable MemTable %d", imem.ID())
		return nil
	}

	// 写入 Level0 文件
	filePath := sstableFilePath(sst.id, sst.level, m.sstPath)
	if err := sst.EncodeTo(filePath); err != nil {
		log.Errorf("[SSTableManager] Encode sstable to file %s error: %s", sst.FilePath(), err.Error())
		return fmt.Errorf("encode sstable failed: %w", err)
	}

	// 添加到内存中
	m.addTable(sst)
	log.Debugf("[SSTableManager] Created new SSTable %s at level %d", sst.FilePath(), sst.level)

	// 执行合并逻辑
	if err := m.Compaction(); err != nil {
		log.Errorf("[SSTableManager] Compaction error: %s", err.Error())
		return fmt.Errorf("compaction failed: %w", err)
	}

	imem.Clean() // 删除已经成功落盘的 WAL 文件
	return nil
}

// Search 从低层级向高层级查找 key，同层级按 id 降序查找。
// found 为 true 且 value 为 nil 表示找到了删除标记，调用方应停止继续查找。
func (m *Manager) Search(key kv.Key) (kv.Value, bool, error) {
	// 1. 从高层级向低层级查找
	for level := m.minSSTableLevel; level <= m.maxSSTableLevel; level++ {
		// 2. 等待该层级的潜在合并完成（仅对需要等待的层级）
		if err := m.waitForCompactionIfNeeded(level); err != nil {
			log.Errorf("[SSTableManager] Wait for compaction at level %d failed: %s", level, err.Error())
			return nil, false, fmt.Errorf("wait for compaction failed: %w", err)
		}

		// 3. 先从level 0开始查找
		if level == m.minSSTableLevel {
			val, found, err := m.searchFromLevel0(key)
			if err != nil {
				log.Errorf("[SSTableManager] Search from level 0 failed: %s", err.Error())
				return nil, false, fmt.Errorf("search from level 0 failed: %w", err)
			}
			if found {
				return val, true, nil
			}
			continue
		}

		val, found, err := m.searchFromLevelWithSparseIndex(key, level)
		if err != nil {
			log.Errorf("[SSTableManager] Search from level %d failed: %s", level, err.Error())
			return nil, false, fmt.Errorf("search from level %d failed: %w", level, err)
		}
		if found {
			return val, true, nil
		}
	}

	// 5. 所有层级都未找到
	return nil, false, nil
}

// waitForCompactionIfNeeded 等待指定层级完成合并（如果正在合并）
// 如果层级正在合并，则阻塞直到合并完成；否则立即返回
// 返回可能因等待被中断而产生的错误
func (m *Manager) waitForCompactionIfNeeded(level int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 快速检查：如果该层级没有在合并，直接返回
	if !m.isCompacting(level) {
		return nil
	}

	// 等待合并完成
	for m.isCompacting(level) {
		m.compactionCond.Wait()
	}

	return nil
}

// isCompacting 辅助方法：检查指定层级是否正在合并
func (m *Manager) isCompacting(level int) bool {
	return m.compactingLevels[level]
}

func (m *Manager) searchFromLevel0(key kv.Key) (kv.Value, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tables := m.levels[m.minSSTableLevel]

	// 在当前层级中按表ID降序查找
	for _, table := range tables {
		val, found, err := m.searchFromTable(table, key)
		if err != nil {
			log.Errorf("[SSTableManager] Search from table %s failed: %s", table.FilePath(), err.Error())
			return nil, false, fmt.Errorf("search from table %s failed: %w", table.FilePath(), err)
		}
		if found {
			return val, true, nil
		}
	}

	return nil, false, nil
}

// searchFromLevelWithSparseIndex 使用稀疏索引在指定层级查找key
func (m *Manager) searchFromLevelWithSparseIndex(key kv.Key, level int) (kv.Value, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 1. 使用稀疏索引找到可能包含该key的SSTable范围
	// 稀疏索引是按MinKey排序的，我们可以找到最后一个MinKey小于等于key的SSTable
	sparseIndexes := m.sparseIndexes[level-1]
	index := sort.Search(len(sparseIndexes), func(i int) bool {
		return bytes.Compare([]byte(sparseIndexes[i].Header.MinKey), []byte(key)) > 0
	})
	if index > 0 {
		index-- // 调整到最后一个 <= key 的位置
	}

	// 2. 在SSTable中查找key
	if index < len(sparseIndexes) {
		sst := sparseIndexes[index]
		val, found, err := m.searchFromTable(sst, key)
		if err != nil {
			log.Errorf("[SSTableManager] Search from table %s failed: %s", sst.FilePath(), err.Error())
			return nil, false, fmt.Errorf("search from table %s failed: %w", sst.FilePath(), err)
		}
		if found {
			return val, true, nil
		}
	}

	return nil, false, nil
}

func (m *Manager) searchFromTable(sst *SSTable, key kv.Key) (kv.Value, bool, error) {
	if !sst.MayContain(key) {
		return nil, false, nil
	}

	// 使用迭代器查找
	it := NewSSTableIterator(sst)
	defer it.Close()

	it.Seek(key)
	if it.Valid() && it.Key() == key {
		value, err := it.Value()
		if err != nil {
			return nil, false, err
		}
		if (&kv.KeyValuePair{Key: key, Value: value}).IsDeleted() {
			return nil, true, nil
		}
		return value, true, nil
	}
	return nil, false, nil
}

// Recover 加载所有层中 SSTable 的元数据信息到内存中
func (m *Manager) Recover() error {
	var maxID uint64

	for level := m.minSSTableLevel; level <= m.maxSSTableLevel; level++ {
		dir := sstableLevelPath(level, m.sstPath)
		files, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				log.Debugf("[SSTableManager] Directory %s does not exist, skipping", dir)
				continue
			}
			log.Errorf("[SSTableManager] Failed to read directory %s: %s", dir, err.Error())
			return fmt.Errorf("read directory %s failed: %w", dir, err)
		}

		files = filterSSTableFiles(files)
		if len(files) == 0 {
			log.Debugf("[SSTableManager] Directory %s is empty, skipping", dir)
			continue
		}

		// addTable inserts at the front, so recover in ascending ID order to
		// preserve newest-first lookup order for overlapping Level 0 tables.
		sort.Slice(files, func(i, j int) bool {
			return utils.ExtractID(files[i].Name()) < utils.ExtractID(files[j].Name())
		})

		// 记录最大ID
		latestID := utils.ExtractID(files[len(files)-1].Name())
		if latestID > maxID {
			maxID = latestID
		}

		for _, file := range files {
			filePath := filepath.Join(dir, file.Name())
			table := NewRecoverSSTable(level)
			table.id = utils.ExtractID(file.Name())

			if err := table.DecodeFrom(filePath); err != nil {
				log.Errorf("[SSTableManager] Recover: load meta for file %s error: %s", filePath, err.Error())
				return fmt.Errorf("load meta for file %s failed: %w", filePath, err)
			}
			if table.IndexBlock.Len() == 0 || table.Footer.DataHandle.Size == 0 {
				log.Debugf("[SSTableManager] Recover: removing empty SSTable %s", filePath)
				if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("remove empty sstable %s failed: %w", filePath, err)
				}
				continue
			}

			m.addTable(table)
		}
	}

	m.advanceNextID(maxID)
	log.Debugf("[SSTableManager] Recovered SSTables, max ID: %d", maxID)
	return nil
}

func filterSSTableFiles(files []os.DirEntry) []os.DirEntry {
	filtered := files[:0]
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if filepath.Ext(file.Name()) != "."+sstFileSuffix {
			continue
		}
		if _, err := utils.ExtractIDFromFileName(file.Name()); err != nil {
			continue
		}
		filtered = append(filtered, file)
	}
	return filtered
}

func (m *Manager) buildSSTableFromIMemTable(imem *memtable.IMemTable) *SSTable {
	builder := m.newSSTableBuilder(config.Conf.LSM.MinSSTableLevel)
	imem.RangeScan(func(pair *kv.KeyValuePair) {
		builder.Add(pair)
	})
	return builder.Build()
}

func (m *Manager) newSSTableBuilder(level int) *Builder {
	return NewSSTableBuilderWithID(m.nextTableID(), level, m.sstPath)
}

func (m *Manager) nextTableID() uint64 {
	return m.nextID.Add(1)
}

func (m *Manager) advanceNextID(id uint64) {
	for {
		current := m.nextID.Load()
		if current >= id {
			return
		}
		if m.nextID.CompareAndSwap(current, id) {
			return
		}
	}
}

// addTable 将新的 SSTable 添加到内存中，保持层级和排序（新文件在最前面）
func (m *Manager) addTable(table *SSTable) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.addTableLocked(table)
}

func (m *Manager) addTableLocked(table *SSTable) {
	m.advanceNextID(table.id)
	table.DataBlock = block.NewDataBlock()
	level := table.level
	m.removeTableMetadataLocked(table.FilePath(), level)
	tables := m.levels[level]

	// 直接插入到列表开头（保持降序）
	m.levels[level] = append([]*SSTable{table}, tables...)

	// 添加到文件索引
	m.fileIndex[table.FilePath()] = table

	m.totalMap[level] = append(m.totalMap[level], table.FilePath())

	// 更新稀疏索引（仅对 Level 1 及以上层级）
	if level > m.minSSTableLevel {
		// 根据最小 Key 更新稀疏索引，进行插入
		sparseIndexes := m.sparseIndexes[level-1]
		index := sort.Search(len(sparseIndexes), func(i int) bool {
			return bytes.Compare([]byte(sparseIndexes[i].Header.MinKey), []byte(table.Header.MinKey)) > 0
		})
		sparseIndexes = append(sparseIndexes[:index], append([]*SSTable{table}, sparseIndexes[index:]...)...)
		m.sparseIndexes[level-1] = sparseIndexes
	}
}

type sstableRemoval struct {
	path  string
	level int
}

func sstableRemovals(paths []string, level int) []sstableRemoval {
	removals := make([]sstableRemoval, 0, len(paths))
	for _, path := range paths {
		removals = append(removals, sstableRemoval{path: path, level: level})
	}
	return removals
}

// removeOldSSTables 删除旧的 SSTable 文件
func (m *Manager) removeOldSSTables(oldFiles []string, level int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, oldPath := range oldFiles {
		m.removeTableMetadataLocked(oldPath, level)

		// 物理删除文件
		if err := os.Remove(oldPath); err != nil {
			log.Errorf("[SSTableManager] Remove file %s error: %s", oldPath, err.Error())
			return fmt.Errorf("remove file %s failed: %w", oldPath, err)
		}
	}

	return nil
}

func (m *Manager) removeTableMetadataLocked(path string, level int) {
	var targetID uint64
	if sst, exists := m.fileIndex[path]; exists {
		targetID = sst.id
	}

	for lvl := m.minSSTableLevel; lvl <= m.maxSSTableLevel; lvl++ {
		tables := m.levels[lvl]
		if len(tables) == 0 {
			continue
		}
		filtered := tables[:0]
		for _, table := range tables {
			if table.FilePath() == path || (targetID != 0 && table.id == targetID) {
				continue
			}
			filtered = append(filtered, table)
		}
		m.levels[lvl] = filtered

		if lvl > m.minSSTableLevel {
			sparseIndexes := m.sparseIndexes[lvl-1]
			filteredSparse := sparseIndexes[:0]
			for _, table := range sparseIndexes {
				if table.FilePath() == path || (targetID != 0 && table.id == targetID) {
					continue
				}
				filteredSparse = append(filteredSparse, table)
			}
			m.sparseIndexes[lvl-1] = filteredSparse
		}
	}

	// 从文件索引中移除
	delete(m.fileIndex, path)

	// 从 totalMap 中移除
	for lvl, files := range m.totalMap {
		m.totalMap[lvl] = utils.RemoveString(files, path)
	}
}

func (m *Manager) encodeSSTables(newTables []*SSTable) error {
	for _, table := range newTables {
		if err := table.EncodeTo(table.FilePath()); err != nil {
			log.Errorf("[SSTableManager] Encode sstable to file %s error: %s", table.FilePath(), err.Error())
			return fmt.Errorf("encode sstable failed: %w", err)
		}
	}
	return nil
}

func (m *Manager) installCompactedSSTables(removals []sstableRemoval, newTables []*SSTable) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, removal := range removals {
		m.removeTableMetadataLocked(removal.path, removal.level)
	}
	for _, table := range newTables {
		m.addTableLocked(table)
	}
	for _, removal := range removals {
		if err := os.Remove(removal.path); err != nil && !os.IsNotExist(err) {
			log.Warnf("[SSTableManager] Remove compacted file %s error: %s", removal.path, err.Error())
		}
	}
}

// addNewSSTables 添加新的 SSTable 到指定层级
func (m *Manager) addNewSSTables(newTables []*SSTable) error {
	if err := m.encodeSSTables(newTables); err != nil {
		return err
	}
	for _, nt := range newTables {
		m.addTable(nt)
	}
	return nil
}

// getLevelTables 获取指定层级的所有 SSTable（已排序）
func (m *Manager) getLevelTables(level int) []*SSTable {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tables := m.levels[level]
	if tables == nil {
		return nil
	}

	// 返回副本以避免外部修改
	result := make([]*SSTable, len(tables))
	copy(result, tables)
	return result
}

// getFilesByLevel 获取指定层级的所有文件路径
func (m *Manager) getFilesByLevel(level int) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	files := make([]string, 0, len(m.levels[level]))
	for _, table := range m.levels[level] {
		files = append(files, table.FilePath())
	}
	return files
}

// isLevelNeedToBeMerged 检查层级是否需要合并
func (m *Manager) isLevelNeedToBeMerged(level int) bool {
	return len(m.getFilesByLevel(level)) > m.maxFileNumsInLevel(level)
}

func (m *Manager) maxFileNumsInLevel(level int) int {
	return int(math.Pow(float64(m.levelSizeBase), float64(level+1)))
}

func (m *Manager) WaitForCompactions() {
	m.compactionWG.Wait()
}

func (m *Manager) getSSTableByPath(path string) (*SSTable, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sst, ok := m.fileIndex[path]
	return sst, ok
}

// GetAllFiles 返回所有 SSTable 文件的路径
func (m *Manager) GetAllFiles() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var allFiles []string
	for _, tables := range m.levels {
		for _, table := range tables {
			allFiles = append(allFiles, table.FilePath())
		}
	}
	return allFiles
}

// HoldFilesSnapshot returns the current SSTable file list and holds the manager
// read lock until the returned release function is called. This pins immutable
// SSTable files against compaction removal while a Raft snapshot copies them.
func (m *Manager) HoldFilesSnapshot() ([]string, func()) {
	m.mu.RLock()

	var allFiles []string
	for _, tables := range m.levels {
		for _, table := range tables {
			allFiles = append(allFiles, table.FilePath())
		}
	}
	return allFiles, m.mu.RUnlock
}

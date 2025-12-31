package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/engine/lsm/kv"
	"github.com/xmh1011/go-kv/engine/lsm/memtable"
	"github.com/xmh1011/go-kv/engine/lsm/sstable/block"
	"github.com/xmh1011/go-kv/engine/lsm/sstable/bloom"
	"github.com/xmh1011/go-kv/pkg/config"
)

func TestSSTableManagerCreateNewSSTable(t *testing.T) {
	tmp := t.TempDir()

	// 初始化可变 MemTable 并写入数据
	mem := memtable.NewMemTable(1, tmp)
	_ = mem.Insert(kv.KeyValuePair{Key: "key1", Value: []byte("value1")})
	_ = mem.Insert(kv.KeyValuePair{Key: "key2", Value: []byte("value2")})

	// 冻结为 IMemTable
	imem := memtable.NewIMemTable(mem)
	// 构建 Manager
	ResetIDGenerator()
	manager := NewSSTableManager(tmp)

	// 调用 CreateNewSSTable
	err := manager.CreateNewSSTable(imem)
	assert.NoError(t, err)

	// 检查文件是否存在
	files := manager.getFilesByLevel(0)
	assert.Len(t, files, 1)
	assert.FileExists(t, files[0])

	// 检查是否添加到内存索引
	tables := manager.getLevelTables(0)
	assert.Len(t, tables, 1)
	assert.Equal(t, uint64(1), tables[0].id)

	// 检查 WAL 文件是否被 Clean 删除
	_, err = os.Stat(filepath.Join(tmp, fmt.Sprintf("%d.wal", mem.ID())))
	assert.True(t, os.IsNotExist(err), "WAL 文件应被 Clean 删除")
}

func TestSSTableManagerSearch(t *testing.T) {
	// 1. 创建临时目录和测试数据
	dir := t.TempDir()

	// 2. 创建并初始化SSTable
	sst := NewSSTable()
	sst.id = 1
	sst.level = 0
	sst.filePath = sstableFilePath(sst.id, sst.level, dir)

	// 设置测试数据
	testRecords := []*kv.KeyValuePair{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
		{Key: "key3", Value: []byte("value3")},
	}

	// 创建数据块和过滤器
	sst.FilterBlock = bloom.NewBloomFilter(1024, 3)
	for _, record := range testRecords {
		sst.Add(record)
	}
	sst.Header = block.NewHeader(testRecords[0].Key, testRecords[len(testRecords)-1].Key)

	// 3. 编码并写入文件
	err := sst.EncodeTo(sst.filePath)
	assert.NoError(t, err)
	assert.FileExists(t, sst.filePath)

	// 4. 初始化SSTableManager
	manager := NewSSTableManager(dir)
	err = manager.addNewSSTables([]*SSTable{sst})
	assert.NoError(t, err)

	// 5. 测试Search功能
	tests := []struct {
		name     string
		key      kv.Key
		expected []byte
		wantErr  bool
	}{
		{
			name:     "existing key in first block",
			key:      "key1",
			expected: []byte("value1"),
			wantErr:  false,
		},
		{
			name:     "existing key in middle",
			key:      "key2",
			expected: []byte("value2"),
			wantErr:  false,
		},
		{
			name:     "existing key in last position",
			key:      "key3",
			expected: []byte("value3"),
			wantErr:  false,
		},
		{
			name:     "non-existing key",
			key:      "key4",
			expected: nil,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := manager.Search(tt.key)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, val)
		})
	}

	// 6. 测试布隆过滤器优化 - 查询明显不存在的key
	val, err := manager.Search("definitely_not_exist_key")
	assert.NoError(t, err)
	assert.Nil(t, val)
}

func TestSSTableManagerRecover(t *testing.T) {
	tests := []struct {
		name       string
		levelCount int
	}{
		{
			name:       "Recover single level",
			levelCount: 1,
		},
		{
			name:       "Recover multiple levels",
			levelCount: config.Conf.LSM.MaxSSTableLevel + 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmp := t.TempDir()
			for level := 0; level < tt.levelCount; level++ {
				dir := sstableLevelPath(level, tmp)
				err := os.MkdirAll(dir, 0755)
				assert.NoError(t, err, "Failed to create directory for level %d", level)

				sst := NewSSTable()
				sst.id = uint64(level + 1)
				sst.level = level
				sst.filePath = filepath.Join(dir, fmt.Sprintf("%d.sst", sst.id))
				sst.Header = block.NewHeader("a", "z")
				sst.FilterBlock = bloom.NewBloomFilter(1024, 3)
				sst.Add(&kv.KeyValuePair{Key: "a", Value: []byte("x")})
				err = sst.EncodeTo(sst.filePath)
				assert.NoError(t, err)
			}

			manager := NewSSTableManager(tmp)
			err := manager.Recover()
			assert.NoError(t, err)

			for level := 0; level < tt.levelCount; level++ {
				tables := manager.getLevelTables(level)
				assert.Len(t, tables, 1)
				assert.Equal(t, uint64(level+1), tables[0].id)
			}
		})
	}
}

func TestSSTableManagerRemoveOldSSTables(t *testing.T) {
	dir := t.TempDir()

	// 1. 创建旧 SSTable 文件
	sst1 := NewSSTable()
	sst1.id = 1
	sst1.level = 0
	sst1.filePath = filepath.Join(dir, "1.sst")
	err := os.WriteFile(sst1.filePath, []byte("dummy"), 0644)
	assert.NoError(t, err)

	// 2. 初始化 Manager 并删除旧文件
	manager := NewSSTableManager(dir)
	manager.fileIndex[sst1.filePath] = sst1 // 手动注册到索引
	manager.totalMap[0] = []string{sst1.filePath}

	err = manager.removeOldSSTables([]string{sst1.filePath}, 0)
	assert.NoError(t, err)

	// 3. 验证文件已被删除
	_, err = os.Stat(sst1.filePath)
	assert.True(t, os.IsNotExist(err))
}

func TestSSTableManagerAddTableOrderingAndIndex(t *testing.T) {
	tests := []struct {
		name          string
		insertOrder   []*SSTable
		expectedOrder []uint64 // IDs
	}{
		{
			name: "Insert Ascending",
			insertOrder: []*SSTable{
				{id: 1, level: 1, Header: block.NewHeader("a", "b"), filePath: "1.sst"},
				{id: 2, level: 1, Header: block.NewHeader("c", "d"), filePath: "2.sst"},
			},
			expectedOrder: []uint64{2, 1}, // Expect Descending
		},
		{
			name: "Insert Descending",
			insertOrder: []*SSTable{
				{id: 2, level: 1, Header: block.NewHeader("c", "d"), filePath: "2.sst"},
				{id: 1, level: 1, Header: block.NewHeader("a", "b"), filePath: "1.sst"},
			},
			expectedOrder: []uint64{1, 2}, // Note: Current implementation prepends, so order depends on insertion order if we don't sort.
			// Wait, the implementation says: "m.levels[level] = append([]*SSTable{table}, tables...)"
			// So it always prepends.
			// If we insert 1 then 2 -> [2, 1].
			// If we insert 2 then 1 -> [1, 2].
			// The Recover method sorts by ID desc before adding.
			// But addTable just prepends.
			// So if we add tables dynamically (e.g. flush), new tables (higher ID) come later and are prepended.
			// So Insert Ascending (1 then 2) -> [2, 1] is correct for dynamic addition.
			// Insert Descending (2 then 1) -> [1, 2] would be incorrect state for LSM usually (newer tables should be first),
			// but technically that's what the code does.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			manager := NewSSTableManager(tmpDir)

			for _, sst := range tt.insertOrder {
				// Need to set DataBlock to avoid nil pointer if accessed, though addTable creates it if nil?
				// addTable does: table.DataBlock = block.NewDataBlock()
				manager.addTable(sst)
			}

			tables := manager.getLevelTables(1)
			assert.Len(t, tables, len(tt.expectedOrder))
			for i, id := range tt.expectedOrder {
				assert.Equal(t, id, tables[i].id)
			}
		})
	}
}

func TestWaitForCompactionIfNeeded(t *testing.T) {
	tmpDir := t.TempDir()
	manager := NewSSTableManager(tmpDir)
	level := 2

	// 模拟合并正在进行
	manager.compactingLevels[level] = true

	done := make(chan struct{})
	go func() {
		go func() {
			time.Sleep(100 * time.Millisecond)
			manager.mu.Lock()
			manager.compactingLevels[level] = false
			manager.compactionCond.Broadcast()
			manager.mu.Unlock()
		}()
		err := manager.waitForCompactionIfNeeded(level)
		assert.NoError(t, err)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("waitForCompactionIfNeeded 超时")
	}
}

func TestIsLevelNeedToBeMerged(t *testing.T) {
	tmpDir := t.TempDir()
	manager := NewSSTableManager(tmpDir)
	level := 2
	limit := manager.maxFileNumsInLevel(level)

	tests := []struct {
		name      string
		fileCount int
		want      bool
	}{
		{"Under Limit", limit - 1, false},
		{"Exact Limit", limit, false},
		{"Over Limit", limit + 1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup files
			paths := make([]string, tt.fileCount)
			for i := 0; i < tt.fileCount; i++ {
				paths[i] = filepath.Join("mock", fmt.Sprintf("%d.sst", i))
			}
			manager.totalMap[level] = paths

			assert.Equal(t, tt.want, manager.isLevelNeedToBeMerged(level))
		})
	}
}

func TestGetSSTableByPath(t *testing.T) {
	tmpDir := t.TempDir()
	manager := NewSSTableManager(tmpDir)

	sst := NewSSTable()
	sst.id = 42
	sst.level = 0
	sst.filePath = "mock/path/42.sst"
	manager.addTable(sst)

	tests := []struct {
		name      string
		path      string
		wantFound bool
		wantID    uint64
	}{
		{"Found", "mock/path/42.sst", true, 42},
		{"Not Found", "mock/path/unknown.sst", false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := manager.getSSTableByPath(tt.path)
			assert.Equal(t, tt.wantFound, ok)
			if tt.wantFound {
				assert.Equal(t, tt.wantID, got.id)
			}
		})
	}
}

func TestAddNewSSTables(t *testing.T) {
	tests := []struct {
		name      string
		filePath  string
		wantError bool
	}{
		{"Valid Path", "valid.sst", false},
		{"Invalid Path", "/invalid/path/1.sst", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			manager := NewSSTableManager(tmpDir)

			sst := NewSSTable()
			sst.id = 1
			sst.level = 0
			if tt.wantError {
				sst.filePath = tt.filePath
			} else {
				sst.filePath = filepath.Join(tmpDir, tt.filePath)
			}

			err := manager.addNewSSTables([]*SSTable{sst})
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.FileExists(t, sst.filePath)
			}
		})
	}
}

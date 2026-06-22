package memtable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xmh1011/go-kv/engine/lsm/kv"
	"github.com/xmh1011/go-kv/pkg/config"
)

func TestMemTableBuilderInsertAndEviction(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewMemTableManager(tempDir)

	// 手动构造 key-value 对，每次都填满 MemTable 触发 Promote
	var evicted *IMemTable
	var err error
	for i := 0; i <= config.Conf.LSM.MaxIMemTableCount; i++ {
		// 构造一个大的 kv，使得每次都触发 Promote
		key := kv.Key(fmt.Sprintf("key-%03d", i))
		value := kv.Value(make([]byte, config.Conf.LSM.MaxMemTableSize)) // 触发 Flush
		evicted, err = manager.Insert(kv.KeyValuePair{Key: key, Value: value})
		assert.NoError(t, err, "should not return error on insert")
	}

	// 第一次出现淘汰，应是在第 (maxIMemTableCount+1) 次 Promote 时
	assert.NotNil(t, evicted, "should return evicted imem")

	// 再插一次，继续淘汰第二个 imem
	evicted2, _ := manager.Insert(kv.KeyValuePair{
		Key:   "last",
		Value: make([]byte, 512*1024),
	})
	assert.NotNil(t, evicted2, "second eviction should not be nil")

	// Flush 完成前，待 flush 的 IMemTable 仍必须留在可查询集合中。
	all := manager.GetAll()
	assert.Greater(t, len(all), config.Conf.LSM.MaxIMemTableCount, "pending flush imems should remain visible")

	// 验证当前 MemTable 仍可写入
	ok := manager.CanInsert(kv.KeyValuePair{Key: "z", Value: []byte("zzz")})
	assert.True(t, ok, "should still allow insert to new MemTable")
}

func TestRecoverDoesNotResetLiveManagerIDs(t *testing.T) {
	ResetIDGenerator()

	live := NewMemTableManager(t.TempDir())
	assert.NoError(t, live.Mem.Insert(kv.KeyValuePair{Key: "first", Value: []byte("value")}))
	live.ForcePromote()
	previousActiveID := live.Mem.ID()

	assert.NoError(t, live.Mem.Insert(kv.KeyValuePair{Key: "second", Value: []byte("value")}))

	otherDir := t.TempDir()
	otherMem := NewMemTable(1, otherDir)
	assert.NoError(t, otherMem.Insert(kv.KeyValuePair{Key: "other", Value: []byte("value")}))
	assert.NoError(t, otherMem.Close())

	other := &Manager{
		walPath:           otherDir,
		IMems:             make([]*IMemTable, 0),
		maxIMemTableCount: config.Conf.LSM.MaxIMemTableCount,
		flushing:          make(map[uint64]bool),
	}
	assert.NoError(t, other.Recover())
	defer other.Close()

	live.ForcePromote()

	assert.Greater(t, live.Mem.ID(), previousActiveID, "recovering another manager must not make a live manager reuse its active WAL ID")
}

func TestRecoverIgnoresNonWALDirectoryEntries(t *testing.T) {
	dir := t.TempDir()

	valid := NewMemTable(2, dir)
	assert.NoError(t, valid.Insert(kv.KeyValuePair{Key: "valid", Value: []byte("value")}))
	assert.NoError(t, valid.Close())

	assert.NoError(t, os.WriteFile(filepath.Join(dir, "3.wal.tmp"), []byte("partial"), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("not a wal"), 0644))
	assert.NoError(t, os.Mkdir(filepath.Join(dir, "4.wal"), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "5.extra.wal"), []byte("not a committed wal"), 0644))

	manager := &Manager{
		walPath:           dir,
		IMems:             make([]*IMemTable, 0),
		maxIMemTableCount: config.Conf.LSM.MaxIMemTableCount,
		flushing:          make(map[uint64]bool),
	}
	require.NoError(t, manager.Recover())
	defer manager.Close()

	value, found := manager.Search("valid")
	assert.True(t, found)
	assert.Equal(t, kv.Value("value"), value)
	assert.Equal(t, uint64(2), manager.Mem.ID())
}

func TestInsertTriggersPromotion(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewMemTableManager(tempDir)

	var evicted *IMemTable
	var err error
	for i := 0; i <= config.Conf.LSM.MaxIMemTableCount; i++ {
		// 构造一个大的 kv，使得每次都触发 Promote
		key := kv.Key(fmt.Sprintf("key-%03d", i))
		value := kv.Value(make([]byte, config.Conf.LSM.MaxMemTableSize)) // 触发 Flush
		evicted, err = manager.Insert(kv.KeyValuePair{Key: key, Value: value})
		assert.NoError(t, err, "should not return error on insert")
	}
	// 第一次出现淘汰，应是在第 (maxIMemTableCount+1) 次 Promote 时
	assert.NotNil(t, evicted, "should return evicted imem")

	// 再插入应触发 promote
	evicted, err = manager.Insert(kv.KeyValuePair{Key: "newKey", Value: []byte("newValue")})
	assert.NoError(t, err)
	assert.NotNil(t, evicted, "Should evict one IMemTable")
	assert.GreaterOrEqual(t, len(manager.GetAll()), config.Conf.LSM.MaxIMemTableCount, "pending flush imems should remain searchable")

	// 触发 promote 后再次 delete
	evicted, err = manager.Delete("someKey")
	assert.NoError(t, err)
	assert.Nil(t, evicted, "Should evict one IMemTable")

	val, found := manager.Search("someKey")
	assert.True(t, found, "Deleted key tombstone should be found")
	assert.Nil(t, val, "Deleted key should return nil")
}

func TestFlushCandidateRemainsSearchableUntilFlushCompletes(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewMemTableManager(tempDir)

	var candidate *IMemTable
	var err error
	for i := 0; i <= config.Conf.LSM.MaxIMemTableCount+1; i++ {
		key := kv.Key(fmt.Sprintf("key-%03d", i))
		value := kv.Value(make([]byte, config.Conf.LSM.MaxMemTableSize))
		candidate, err = manager.Insert(kv.KeyValuePair{Key: key, Value: value})
		assert.NoError(t, err)
	}

	assert.NotNil(t, candidate)

	val, found := manager.Search("key-000")
	assert.True(t, found, "flush candidate must remain searchable before SSTable flush completes")
	assert.NotNil(t, val)

	manager.CompleteFlush(candidate, true)
	_, found = manager.Search("key-000")
	assert.False(t, found, "flushed memtable can be removed after SSTable indexing completes")
}

func TestDeleteTriggersPromotion(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewMemTableManager(tempDir)

	// 填满 MemTable
	for i := 0; i < 100000; i++ {
		key := kv.Key(fmt.Sprintf("k%d", i))
		_, _ = manager.Insert(kv.KeyValuePair{Key: key, Value: []byte("v")})
	}

}

func TestSearchFromMemTables(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewMemTableManager(tempDir)
	_, err := manager.Insert(kv.KeyValuePair{Key: "key", Value: []byte("value")})
	assert.NoError(t, err, "Insert should not return error")

	_, err = manager.Insert(kv.KeyValuePair{Key: "key", Value: []byte("newValue")}) // 更新同一 key
	assert.NoError(t, err, "Insert should not return error")

	val, found := manager.Search("key")
	assert.True(t, found)
	assert.Equal(t, kv.Value("newValue"), val)
}

// mockCreateWalFile 在指定目录下创建一个空的 WAL 文件，文件名必须符合 ExtractID 的格式 "000001.wal"
func mockCreateWalFile(t *testing.T, dir string, id uint64) string {
	filename := filepath.Join(dir, fmt.Sprintf("%d.wal", id)) // 比如 "1.wal"
	f, err := os.Create(filename)
	assert.NoError(t, err)
	assert.NoError(t, f.Close(), "WAL file should be created")
	return filename
}

func TestRecoverSuccess(t *testing.T) {
	tempDir := t.TempDir()
	// 创建多个 WAL 文件，id 从 1 到 5
	for i := uint64(1); i <= 5; i++ {
		mockCreateWalFile(t, tempDir, i)
	}

	manager := NewMemTableManager(tempDir)

	// Recover 应成功返回，且最后一个 WAL 恢复的 MemTable 是 manager.Mem，其余是 IMemTable
	err := manager.Recover()
	assert.NoError(t, err)
	assert.NotNil(t, manager.Mem)
	assert.GreaterOrEqual(t, len(manager.IMems), 0)

	assert.GreaterOrEqual(t, len(manager.IMems), 4, "recovery should preserve WAL-backed immutable memtables")

	// 检查 manager.Mem 的 id 是最后一个文件的 id
	lastFile := mockCreateWalFile(t, tempDir, 100)
	_ = os.Remove(lastFile) // 先删了，再做下个测试用
}

// 模拟 WAL 恢复失败（合法 WAL 文件名但内容损坏，必须失败）
func TestRecoverFromWALFail(t *testing.T) {
	tempDir := t.TempDir()

	assert.NoError(t, os.WriteFile(filepath.Join(tempDir, "2.wal"), []byte("corrupt"), 0644))

	manager := NewMemTableManager(tempDir)

	err := manager.Recover()
	assert.Error(t, err)
}

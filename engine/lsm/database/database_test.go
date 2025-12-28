package database

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDatabasePutGetDelete 测试 Put、Get 和 Delete 的功能
func TestDatabasePutGetDelete(t *testing.T) {
	tmpDir := t.TempDir()
	db := Open(tmpDir)
	defer db.Close()

	// 测试查询不存在的 key，预期返回 nil
	val, err := db.Get("nonexistent")
	assert.NoError(t, err)
	assert.Nil(t, val)

	// Put 操作，将 key1 写入 value1
	err = db.Put("key1", []byte("value1"))
	assert.NoError(t, err)

	// Get 操作，查询 key1，预期返回 value1
	val, err = db.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)

	err = db.Put("key1", []byte("value2"))
	assert.NoError(t, err)

	// Get 操作，查询 key1，预期返回 value2
	val, err = db.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value2"), val)

	// Delete 操作，删除 key1
	err = db.Delete("key1")
	assert.NoError(t, err)

	// Get 操作应返回 nil（key1 被删除）
	val, err = db.Get("key1")
	assert.NoError(t, err)
	assert.Nil(t, val)
}

func TestDatabaseDeleteNonExistentKey(t *testing.T) {
	tmpDir := t.TempDir()
	db := Open(tmpDir)
	defer db.Close()

	err := db.Delete("ghostKey")
	assert.NoError(t, err)

	val, err := db.Get("ghostKey")
	assert.NoError(t, err)
	assert.Nil(t, val)
}

func TestDatabasePersistenceAcrossRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	db := Open(tmpDir)

	err := db.Put("hello", []byte("world"))
	assert.NoError(t, err)
	db.Close()

	// 重启数据库
	db2 := Open(tmpDir)
	defer db2.Close()
	err = db2.Recover()
	assert.NoError(t, err)

	val, err := db2.Get("hello")
	assert.NoError(t, err)
	assert.Equal(t, []byte("world"), val)
}

func TestDatabaseRecoveryOnEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	db := Open(tmpDir)
	defer db.Close()

	// 不放数据，直接 Recover 应无异常
	err := db.Recover()
	assert.NoError(t, err)
}

func TestDatabaseFlushToSSTable(t *testing.T) {
	tmpDir := t.TempDir()
	db := Open(tmpDir)

	// 构造足够大的数据使 MemTable 满而触发 Flush
	value := make([]byte, 1024*1024) // 1MB，2条即可超限
	for i := 0; i < 3; i++ {
		err := db.Put(fmt.Sprintf("key%d", i), value)
		assert.NoError(t, err)
	}
	db.Close()

	// 模拟“重启”并只恢复 SSTable 内容
	// 注意：这里应该使用相同的目录来模拟重启
	db2 := Open(tmpDir)
	defer db2.Close()
	err := db2.Recover()
	assert.NoError(t, err)

	// 所有 key 应可查到
	for i := 0; i < 3; i++ {
		val, err := db2.Get(fmt.Sprintf("key%d", i))
		assert.NoError(t, err)
		assert.Equal(t, value, val)
	}
}

func cleanup(dir string) {
	os.RemoveAll(dir)
}

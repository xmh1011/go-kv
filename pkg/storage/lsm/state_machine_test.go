package lsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/engine/lsm/database"
	"github.com/xmh1011/go-kv/pkg/config"
	"github.com/xmh1011/go-kv/pkg/param"
)

func TestStateMachineAdapter_Snapshot(t *testing.T) {
	db, dir := setupTestDB(t, "lsm_sm_snap_test")
	defer cleanupTestDB(t, dir)

	adapter := NewStateMachineAdapter(db)

	// 写入一些数据
	adapter.Apply(param.LogEntry{Command: mustMarshal(param.KVCommand{Op: param.OpSet, Key: "k1", Value: "v1"})})
	adapter.Apply(param.LogEntry{Command: mustMarshal(param.KVCommand{Op: param.OpSet, Key: "k2", Value: "v2"})})

	// 获取快照
	snapData, err := adapter.GetSnapshot()
	assert.NoError(t, err)
	assert.NotNil(t, snapData)

	// 验证快照数据不为空，并使用新的二进制归档格式。
	assert.True(t, bytes.HasPrefix(snapData, lsmSnapshotMagic))
	files, err := decodeSnapshotData(snapData)
	assert.NoError(t, err)
	// 至少应该有一个 SSTable (因为 GetSnapshot 会强制 Flush)
	assert.NotEmpty(t, files)

	// 关闭旧 adapter
	adapter.Close()

	// Update config to use newDir for restore
	newDB, newDir := setupTestDB(t, "lsm_sm_snap_restore")
	defer cleanupTestDB(t, newDir)

	newAdapter := NewStateMachineAdapter(newDB)
	defer newAdapter.Close()

	// 应用快照
	err = newAdapter.ApplySnapshot(snapData)
	assert.NoError(t, err)

	// 验证数据恢复
	val, err := newAdapter.Get("k1")
	assert.NoError(t, err)
	assert.Equal(t, "v1", val)

	val, err = newAdapter.Get("k2")
	assert.NoError(t, err)
	assert.Equal(t, "v2", val)
}

func TestStateMachineAdapterSnapshotIncludesAllImmutableMemTables(t *testing.T) {
	oldLSMConfig := config.Conf.LSM
	config.Conf.LSM.MaxMemTableSize = 128
	config.Conf.LSM.MaxIMemTableCount = 100
	t.Cleanup(func() {
		config.Conf.LSM = oldLSMConfig
	})

	db, dir := setupTestDB(t, "lsm_sm_snap_imems")
	defer cleanupTestDB(t, dir)

	adapter := NewStateMachineAdapter(db)
	defer adapter.Close()

	expected := make(map[string]string)
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("imem-key-%02d", i)
		value := fmt.Sprintf("value-%02d-%s", i, strings.Repeat("x", 64))
		expected[key] = value
		adapter.Apply(param.LogEntry{Command: mustMarshal(param.KVCommand{Op: param.OpSet, Key: key, Value: value})})
	}

	snapData, err := adapter.GetSnapshot()
	assert.NoError(t, err)

	newDB, newDir := setupTestDB(t, "lsm_sm_snap_imems_restore")
	defer cleanupTestDB(t, newDir)

	newAdapter := NewStateMachineAdapter(newDB)
	defer newAdapter.Close()
	assert.NoError(t, newAdapter.ApplySnapshot(snapData))

	for key, expectedValue := range expected {
		got, err := newAdapter.Get(key)
		assert.NoError(t, err, "key %s should be present after snapshot restore", key)
		assert.Equal(t, expectedValue, got)
	}
}

func TestApplySnapshotRejectsInvalidFilePathBeforeClearingDB(t *testing.T) {
	db, dir := setupTestDB(t, "lsm_sm_invalid_snapshot_path")
	defer cleanupTestDB(t, dir)

	adapter := NewStateMachineAdapter(db)
	defer adapter.Close()

	adapter.Apply(param.LogEntry{Command: mustMarshal(param.KVCommand{Op: param.OpSet, Key: "keep", Value: "value"})})

	snapData, err := encodeSnapshotData(map[string][]byte{
		"../escape.sst": []byte("not-a-valid-sstable"),
	})
	assert.NoError(t, err)

	err = adapter.ApplySnapshot(snapData)
	assert.Error(t, err)

	val, err := adapter.Get("keep")
	assert.NoError(t, err)
	assert.Equal(t, "value", val)
}

func setupTestDB(t *testing.T, name string) (*database.Database, string) {
	dir, err := os.MkdirTemp("", name)
	assert.NoError(t, err)

	// database.Open 会自动在 dir 下创建 wal 和 sst 目录
	db := database.Open(dir)
	return db, dir
}

func cleanupTestDB(t *testing.T, dir string) {
	os.RemoveAll(dir)
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

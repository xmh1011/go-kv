package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xmh1011/go-kv/engine/lsm/kv"
)

func TestWALAppendAndRecover(t *testing.T) {
	tempDir := t.TempDir()

	// 创建 WAL 实例
	w, err := NewWAL(1, tempDir)
	assert.NoError(t, err)

	// 准备写入的测试数据
	records := []kv.KeyValuePair{
		{Key: "k1", Value: []byte("v1")},
		{Key: "k2", Value: kv.DeletedValue},
		{Key: "k3", Value: []byte("v3")},
	}

	// 写入 WAL
	for _, r := range records {
		assert.NoError(t, w.Append(r))
	}
	assert.NoError(t, w.Sync())
	assert.NoError(t, w.Close())

	// 构建 WAL 路径
	walPath := filepath.Join(tempDir, "1.wal")

	// 读取 WAL 并验证
	var recovered []kv.KeyValuePair
	recoveredWAL, err := Recover(walPath, func(pair kv.KeyValuePair) {
		recovered = append(recovered, pair)
	})
	assert.NoError(t, err)

	// 关闭 WAL 文件
	assert.NoError(t, recoveredWAL.Close())

	// 验证数据是否一致
	assert.Equal(t, records, recovered)

	// 删除 WAL 文件
	err = recoveredWAL.DeleteFile()
	assert.NoError(t, err)
	_, statErr := os.Stat(walPath)
	assert.True(t, os.IsNotExist(statErr), "WAL file should be deleted")
}

func TestRecoverTruncatesTornTailAfterValidRecords(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "1.wal")

	validRecords := []kv.KeyValuePair{
		{Key: "stable-1", Value: []byte("value-1")},
		{Key: "stable-2", Value: kv.DeletedValue},
	}

	var complete bytes.Buffer
	for _, record := range validRecords {
		require.NoError(t, record.EncodeTo(&complete))
	}
	completeLen := complete.Len()

	var torn bytes.Buffer
	require.NoError(t, (&kv.KeyValuePair{Key: "torn", Value: []byte("value")}).EncodeTo(&torn))
	require.NoError(t, os.WriteFile(walPath, append(complete.Bytes(), torn.Bytes()[:5]...), 0644))

	var recovered []kv.KeyValuePair
	recoveredWAL, err := Recover(walPath, func(pair kv.KeyValuePair) {
		recovered = append(recovered, pair)
	})
	require.NoError(t, err)
	defer recoveredWAL.Close()

	assert.Equal(t, validRecords, recovered)

	stat, err := os.Stat(walPath)
	require.NoError(t, err)
	assert.Equal(t, int64(completeLen), stat.Size(), "recovery should truncate the incomplete trailing record")

	nextRecord := kv.KeyValuePair{Key: "after-recovery", Value: []byte("value-3")}
	require.NoError(t, recoveredWAL.Append(nextRecord))
	require.NoError(t, recoveredWAL.Close())

	var recoveredAgain []kv.KeyValuePair
	reopened, err := Recover(walPath, func(pair kv.KeyValuePair) {
		recoveredAgain = append(recoveredAgain, pair)
	})
	require.NoError(t, err)
	require.NoError(t, reopened.Close())

	assert.Equal(t, append(validRecords, nextRecord), recoveredAgain)
}

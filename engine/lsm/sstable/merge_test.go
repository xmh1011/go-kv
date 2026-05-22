package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/engine/lsm/kv"
)

// TestCompactAndMergeBlocks_Basic 测试基本的块合并功能
func TestCompactAndMergeBlocks_Basic(t *testing.T) {
	// 构造测试数据：两个数据块，包含重叠键和删除标记
	block1 := []kv.KeyValuePair{
		{
			Key:   "alpha",
			Value: []byte("A"),
		},
		{
			Key:   "beta",
			Value: []byte("B"),
		},
		{
			Key:   "beta",
			Value: []byte("B2"), // 重复键，前面的块优先
		},
		{
			Key:   "carrot",
			Value: []byte("C"),
		},
		{
			Key:   "delta",
			Value: []byte("D"),
		},
	}

	tmpDir := t.TempDir()
	manager := NewSSTableManager(tmpDir)

	// 执行合并
	sst := manager.CompactAndMergeKVs(block1, 1)
	assert.NotNil(t, sst[0])
	assert.Equal(t, 1, sst[0].level)

	// 验证数据块：应该已经合并排序并去重
	assert.Len(t, sst[0].DataBlock.Entries, 4, "Should have 4 entries (alpha, beta, carrot, delta)")

	// 验证键顺序和去重
	keys := make([]string, len(sst[0].IndexBlock.Indexes))
	for i, entry := range sst[0].IndexBlock.Indexes {
		keys[i] = string(entry.Key)
	}
	assert.Equal(t, []string{"alpha", "beta", "carrot", "delta"}, keys)

	// 验证重复键的处理（后面的块优先）
	assert.Equal(t, kv.Value("B"), sst[0].DataBlock.Entries[1], "Should use value from later block for duplicate key")

	// 验证布隆过滤器
	assert.True(t, sst[0].MayContain("alpha"))
	assert.True(t, sst[0].MayContain("delta"))
	assert.False(t, sst[0].MayContain("nonexistent"))
	assert.False(t, sst[0].MayContain("deletedKey"), "Deleted key should not be in filter")
}

func TestCompactAndMergeKVs_PreservesNewestDuplicateByInputOrder(t *testing.T) {
	tmpDir := t.TempDir()
	manager := NewSSTableManager(tmpDir)

	tables := manager.CompactAndMergeKVs([]kv.KeyValuePair{
		{Key: "log:0001", Value: []byte("new")},
		{Key: "log:0001", Value: []byte("old")},
		{Key: "log:0002", Value: []byte("next")},
	}, 1)

	assert.Len(t, tables, 1)
	assert.Len(t, tables[0].DataBlock.Entries, 2)
	assert.Equal(t, kv.Value("new"), tables[0].DataBlock.Entries[0])
	assert.Equal(t, kv.Value("next"), tables[0].DataBlock.Entries[1])
}

func TestCompactAndMergeKVs_MaxLevelTombstoneSuppressesOlderValue(t *testing.T) {
	tmpDir := t.TempDir()
	manager := NewSSTableManager(tmpDir)

	tables := manager.CompactAndMergeKVs([]kv.KeyValuePair{
		{Key: "log:0001", Value: kv.DeletedValue},
		{Key: "log:0001", Value: []byte("old")},
	}, manager.maxSSTableLevel)

	assert.Empty(t, tables)
}

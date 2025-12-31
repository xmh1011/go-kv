package sstable

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/engine/lsm/kv"
	"github.com/xmh1011/go-kv/engine/lsm/sstable/block"
	"github.com/xmh1011/go-kv/engine/lsm/sstable/bloom"
)

// createTestSSTable 构造一个 SSTable 实例用于测试
func createTestSSTable(t *testing.T) *SSTable {
	sst := NewSSTable()

	// 构造 DataBlock 1：包含键 "a", "b"
	db1 := &block.DataBlock{
		Entries: []kv.Value{
			[]byte("A"),
			[]byte("B"),
			[]byte("C"),
			[]byte("D"),
			[]byte("E"),
		},
	}

	sst.DataBlock = db1

	// 构造索引块（记录每个数据块的起始键和偏移量）
	sst.IndexBlock = &block.IndexBlock{
		Indexes: []*block.IndexEntry{
			{Key: "a", Offset: 0},
			{Key: "b", Offset: 0},
			{Key: "c", Offset: 0},
			{Key: "d", Offset: 0},
			{Key: "e", Offset: 0},
		},
	}

	// 构造布隆过滤器
	sst.FilterBlock = bloom.NewBloomFilter(1024, 5)
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		sst.FilterBlock.AddString(key)
	}

	return sst
}

func TestSSTableIterator(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "1.sst")

	// 创建并写入SSTable
	original := createTestSSTable(t)
	err := original.EncodeTo(filePath)
	assert.NoError(t, err)

	// 重新加载SSTable进行测试
	loaded := NewSSTable()
	err = loaded.DecodeFrom(filePath)
	assert.NoError(t, err)
	defer func(loaded *SSTable) {
		err := loaded.Remove()
		assert.NoError(t, err, "Failed to remove loaded SSTable file")
	}(loaded)

	t.Run("SequentialTraversal", func(t *testing.T) {
		iter := NewSSTableIterator(loaded)
		defer iter.Close()

		var keys, values []string
		for iter.Valid() {
			keys = append(keys, string(iter.Key()))
			val, err := iter.Value()
			assert.NoError(t, err)
			values = append(values, string(val))
			iter.Next()
		}

		// 验证遍历顺序和内容
		assert.Equal(t, []string{"a", "b", "c", "d", "e"}, keys)
		assert.Equal(t, []string{"A", "B", "C", "D", "E"}, values)
	})

	t.Run("SeekOperations", func(t *testing.T) {
		tests := []struct {
			name        string
			seekKey     kv.Key
			expectKey   kv.Key
			expectVal   string
			expectValid bool
		}{
			{
				name:        "SeekExactMatch",
				seekKey:     "b",
				expectKey:   "b",
				expectVal:   "B",
				expectValid: true,
			},
			{
				name:        "SeekNonExistKey",
				seekKey:     "bb",
				expectKey:   "c",
				expectVal:   "C",
				expectValid: false,
			},
			{
				name:        "SeekFirstKey",
				seekKey:     "a",
				expectKey:   "a",
				expectVal:   "A",
				expectValid: true,
			},
			{
				name:        "SeekLastKey",
				seekKey:     "e",
				expectKey:   "e",
				expectVal:   "E",
				expectValid: true,
			},
			{
				name:        "SeekBeyondLast",
				seekKey:     "z",
				expectKey:   "",
				expectVal:   "",
				expectValid: false,
			},
			{
				name:        "SeekBeforeFirst",
				seekKey:     "aa",
				expectKey:   "",
				expectVal:   "",
				expectValid: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				iter := NewSSTableIterator(loaded)
				defer iter.Close()

				iter.Seek(tt.seekKey)
				assert.Equal(t, tt.expectValid, iter.Valid())

				if tt.expectValid {
					assert.Equal(t, tt.expectKey, iter.Key())
					val, err := iter.Value()
					assert.NoError(t, err)
					assert.Equal(t, tt.expectVal, string(val))
				}
			})
		}
	})

	t.Run("PositioningMethods", func(t *testing.T) {
		tests := []struct {
			name        string
			action      func(it *Iterator)
			expectKey   kv.Key
			expectValid bool
		}{
			{
				name: "SeekToFirst",
				action: func(it *Iterator) {
					it.SeekToFirst()
				},
				expectKey:   "a",
				expectValid: true,
			},
			{
				name: "SeekToLast",
				action: func(it *Iterator) {
					it.SeekToLast()
				},
				expectKey:   "e",
				expectValid: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				iter := NewSSTableIterator(loaded)
				defer iter.Close()

				tt.action(iter)
				assert.Equal(t, tt.expectValid, iter.Valid())
				if tt.expectValid {
					assert.Equal(t, tt.expectKey, iter.Key())
				}
			})
		}
	})
}

func TestFilterBlockIntegration(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "1.sst")

	original := createTestSSTable(t)
	err := original.EncodeTo(filePath)
	assert.NoError(t, err)

	loaded := NewSSTable()
	err = loaded.DecodeFrom(filePath)
	assert.NoError(t, err)

	tests := []struct {
		name     string
		key      string
		expected bool
	}{
		{"ExistingKey_a", "a", true},
		{"ExistingKey_e", "e", true},
		{"NonExistingKey_x", "x", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, loaded.FilterBlock.MayContain(kv.Key(tt.key)))
		})
	}
}

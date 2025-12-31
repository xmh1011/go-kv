package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/engine/lsm/kv"
	"github.com/xmh1011/go-kv/pkg/config"
)

func TestNewBuilder(t *testing.T) {
	tests := []struct {
		name  string
		level int
	}{
		{
			name:  "Level 0",
			level: 0,
		},
		{
			name:  "Level 1",
			level: 1,
		},
		{
			name:  "Level 10",
			level: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			builder := NewSSTableBuilder(tt.level, tmpDir)
			assert.NotNil(t, builder)
			assert.NotNil(t, builder.table)
			assert.Equal(t, uint64(0), builder.size)
			assert.Equal(t, tt.level, builder.table.level)
		})
	}
}

func TestBuilder_Add(t *testing.T) {
	tests := []struct {
		name            string
		pairs           []*kv.KeyValuePair
		expectedLen     int
		expectedIndexes []kv.Key
	}{
		{
			name: "Add single pair",
			pairs: []*kv.KeyValuePair{
				{Key: "key1", Value: kv.Value("value1")},
			},
			expectedLen:     1,
			expectedIndexes: []kv.Key{"key1"},
		},
		{
			name: "Add multiple pairs",
			pairs: []*kv.KeyValuePair{
				{Key: "key1", Value: kv.Value("value1")},
				{Key: "key2", Value: kv.Value("value2")},
			},
			expectedLen:     2,
			expectedIndexes: []kv.Key{"key1", "key2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			builder := NewSSTableBuilder(0, tmpDir)

			var expectedSize uint64
			for _, p := range tt.pairs {
				builder.Add(p)
				expectedSize += p.EstimateSize()
			}

			// 验证 DataBlock
			assert.Equal(t, tt.expectedLen, builder.table.DataBlock.Len())

			// 验证 IndexBlock
			assert.Equal(t, len(tt.expectedIndexes), len(builder.table.IndexBlock.Indexes))
			for i, key := range tt.expectedIndexes {
				assert.Equal(t, key, builder.table.IndexBlock.Indexes[i].Key)
			}

			// 验证 size 计算
			assert.Equal(t, expectedSize, builder.size)
		})
	}
}

func TestBuilder_ShouldFlush(t *testing.T) {
	tests := []struct {
		name     string
		size     uint64
		expected bool
	}{
		{
			name:     "not flush",
			size:     uint64(config.Conf.LSM.MaxSSTableSize) - 1,
			expected: false,
		},
		{
			name:     "exact flush",
			size:     uint64(config.Conf.LSM.MaxSSTableSize),
			expected: true,
		},
		{
			name:     "exceed flush",
			size:     uint64(config.Conf.LSM.MaxSSTableSize) + 1,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			builder := NewSSTableBuilder(0, tmpDir)
			builder.size = tt.size
			assert.Equal(t, tt.expected, builder.ShouldFlush())
		})
	}
}

func TestBuilder_Finalize(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(table *Builder)
		expectedMinKey kv.Key
		expectedMaxKey kv.Key
	}{
		{
			name: "Single entry",
			setup: func(b *Builder) {
				b.table.DataBlock.Add(kv.Value("value1"))
				b.table.IndexBlock.Add("key1", 0)
			},
			expectedMinKey: kv.Key("key1"),
			expectedMaxKey: kv.Key("key1"),
		},
		{
			name: "Multiple entries",
			setup: func(b *Builder) {
				b.table.DataBlock.Add(kv.Value("value1"))
				b.table.DataBlock.Add(kv.Value("value2"))
				b.table.IndexBlock.Add("key1", 0)
				b.table.IndexBlock.Add("key2", 100)
			},
			expectedMinKey: kv.Key("key1"),
			expectedMaxKey: kv.Key("key2"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			builder := NewSSTableBuilder(0, tmpDir)
			tt.setup(builder)

			builder.Finalize()

			// 验证 Header 是否正确设置
			assert.NotNil(t, builder.table.Header)
			assert.Equal(t, tt.expectedMinKey, builder.table.Header.MinKey)
			assert.Equal(t, tt.expectedMaxKey, builder.table.Header.MaxKey)
		})
	}
}

func TestBuilder_Build(t *testing.T) {
	tests := []struct {
		name           string
		pair           *kv.KeyValuePair
		expectedMinKey kv.Key
		expectedMaxKey kv.Key
	}{
		{
			name: "Build with data",
			pair: &kv.KeyValuePair{
				Key:   "testKey",
				Value: kv.Value("testValue"),
			},
			expectedMinKey: kv.Key("testKey"),
			expectedMaxKey: kv.Key("testKey"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			builder := NewSSTableBuilder(0, tmpDir)
			builder.Add(tt.pair)

			sstable := builder.Build()

			// 验证返回的 SSTable
			assert.NotNil(t, sstable)
			assert.Equal(t, builder.table, sstable)

			// 验证 Finalize 已经被调用
			assert.NotNil(t, sstable.Header)
			assert.Equal(t, tt.expectedMinKey, sstable.Header.MinKey)
			assert.Equal(t, tt.expectedMaxKey, sstable.Header.MaxKey)
		})
	}
}

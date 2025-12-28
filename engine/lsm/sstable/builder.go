package sstable

import (
	"github.com/xmh1011/go-kv/engine/lsm/kv"
	"github.com/xmh1011/go-kv/engine/lsm/memtable"
	"github.com/xmh1011/go-kv/engine/lsm/sstable/block"
	"github.com/xmh1011/go-kv/pkg/config"
)

type Builder struct {
	table   *SSTable
	size    uint64
	maxSize uint64
}

func NewSSTableBuilder(level int, rootPath string) *Builder {
	return &Builder{
		table:   NewSSTableWithLevel(level, rootPath),
		size:    0,
		maxSize: uint64(config.Conf.LSM.MaxSSTableSize),
	}
}

// BuildSSTableFromIMemTable 构建一个完整的 SSTable（包含 DataBlock、IndexBlock、FilterBlock）
func BuildSSTableFromIMemTable(imem *memtable.IMemTable, rootPath string) *SSTable {
	builder := NewSSTableBuilder(config.Conf.LSM.MinSSTableLevel, rootPath)

	// 遍历所有 key-value 对
	imem.RangeScan(func(pair *kv.KeyValuePair) {
		builder.Add(pair)
	})

	return builder.Build()
}

// Add 向当前 DataBlock（[]kv.Value）添加记录；若当前 Block 满了，则创建新 Block。
func (b *Builder) Add(pair *kv.KeyValuePair) {
	b.table.Add(pair)
	b.size += pair.EstimateSize()
}

// ShouldFlush 判断是否应该写入磁盘
func (b *Builder) ShouldFlush() bool {
	return b.size >= b.maxSize
}

// Finalize 填充 IndexBlock 和 Header
func (b *Builder) Finalize() {
	// 初始化 Header
	if b.table.DataBlock.Len() > 0 {
		b.table.Header = &block.Header{
			MaxKey: b.table.IndexBlock.Indexes[b.table.DataBlock.Len()-1].Key,
			MinKey: b.table.IndexBlock.Indexes[0].Key,
		}
	}
}

// Build 返回最终构建好的 SSTable
func (b *Builder) Build() *SSTable {
	b.Finalize()
	return b.table
}

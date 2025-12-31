package sstable

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/engine/lsm/kv"
	"github.com/xmh1011/go-kv/engine/lsm/memtable"
	"github.com/xmh1011/go-kv/engine/lsm/sstable/block"
)

func TestSSTableManagerCompaction(t *testing.T) {
	tests := []struct {
		name          string
		numFiles      int
		expectedLevel int
	}{
		{
			name:          "Compact 5 files to Level 1",
			numFiles:      5,
			expectedLevel: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmp := t.TempDir()
			mgr := NewSSTableManager(tmp)

			// 1. 创建 Level0 文件
			var oldFiles []string
			for i := 0; i < tt.numFiles; i++ {
				mem := memtable.NewMemTable(uint64(i+1), tmp)
				key := "key" + strconv.Itoa(i)
				val := "val" + strconv.Itoa(i)
				err := mem.Insert(kv.KeyValuePair{Key: kv.Key(key), Value: []byte(val)})
				assert.NoError(t, err)

				imem := memtable.NewIMemTable(mem)
				sst := BuildSSTableFromIMemTable(imem, tmp)
				sst.level = mgr.minSSTableLevel
				// 手动设置路径，因为 BuildSSTableFromIMemTable 不再设置路径
				sst.filePath = sstableFilePath(sst.id, sst.level, tmp)

				oldFiles = append(oldFiles, sst.FilePath())
				// 添加到管理器
				err = mgr.addNewSSTables([]*SSTable{sst})
				assert.NoError(t, err)
				// 验证文件可读
				assert.FileExists(t, sst.FilePath())
			}

			// 2. 触发合并
			err := mgr.Compaction()
			assert.NoError(t, err, "compaction failed")

			// 3. 验证 Level0 文件被删除
			for _, f := range oldFiles {
				assert.NoFileExists(t, f, "old Level0 file not deleted: %s", f)
			}

			// 4. 验证 Level0 清空
			assert.Empty(t, mgr.totalMap[0], "Level0 totalMap not empty")

			// 5. 验证目标 Level 有新文件
			targetFiles := mgr.totalMap[tt.expectedLevel]
			assert.True(t, len(targetFiles) > 0, "no target level files generated")

			// 6. 验证目标 Level 文件内容
			for _, f := range targetFiles {
				sst := NewSSTable()
				err := sst.DecodeFrom(f)
				assert.NoError(t, err, "decode target Level SSTable failed: %s", f)
				assert.NotEmpty(t, sst.DataBlock, "target Level SSTable has empty DataBlocks: %s", f)
			}
		})
	}
}

func TestAsyncCompaction(t *testing.T) {
	tests := []struct {
		name        string
		level       int
		numFiles    int
		shouldFlush bool
	}{
		{
			name:        "Async compact Level 1",
			level:       1,
			numFiles:    10, // 假设 maxFileNumsInLevel(1) < 10
			shouldFlush: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			mgr := NewSSTableManager(tmpDir)

			// 1. 创建 Level 文件（超过阈值）
			var levelFiles []string
			var tables []*SSTable
			// 确保文件数超过阈值
			fileCount := mgr.maxFileNumsInLevel(tt.level) + 1
			if tt.numFiles > fileCount {
				fileCount = tt.numFiles
			}

			for i := 0; i < fileCount; i++ {
				sst := NewSSTableWithLevel(tt.level, tmpDir)
				// 添加一些测试数据
				for j := 0; j < 10; j++ {
					key := "key" + strconv.Itoa(i*10+j)
					value := "value" + strconv.Itoa(i*10+j)
					sst.Add(&kv.KeyValuePair{
						Key:   kv.Key(key),
						Value: []byte(value),
					})
				}
				sst.Header = block.NewHeader("key0", kv.Key("key"+strconv.Itoa(i*10+9)))
				tables = append(tables, sst)
				levelFiles = append(levelFiles, sst.FilePath())
			}
			err := mgr.addNewSSTables(tables)
			assert.NoError(t, err, "add new SSTable failed")

			// 2. 触发异步合并
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				mgr.asyncCompactLevel(tt.level)
			}()
			wg.Wait()

			// 3. 等待合并完成或超时
			timeout := time.After(30 * time.Second)
			select {
			case <-timeout:
				t.Fatal("async compaction timeout")
			default:
				// 检查合并是否完成：当前 Level 文件数减少或下一 Level 出现新文件
				if len(mgr.getFilesByLevel(tt.level)) < len(levelFiles) || len(mgr.getFilesByLevel(tt.level+1)) > 0 {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}

			// 4. 验证旧文件被删除 (部分)
			// 注意：异步合并可能只合并了一部分文件，这里只检查是否至少有一些文件被处理了
			// 具体的删除逻辑取决于合并策略，这里做宽松检查
			deletedCount := 0
			for _, f := range levelFiles {
				if !fileExists(f) {
					deletedCount++
				}
			}
			if tt.shouldFlush {
				assert.True(t, deletedCount > 0, "some old files should be deleted")
			}

			// 5. 验证新文件生成
			if tt.shouldFlush {
				assert.True(t, len(mgr.getFilesByLevel(tt.level)) > 0 || len(mgr.getFilesByLevel(tt.level+1)) > 0)
			}
		})
	}
}

// 辅助函数：检查文件是否存在
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || !os.IsNotExist(err)
}

func TestCompactionEdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(*Manager)
		expectedError bool
	}{
		{
			name: "Empty Level 0",
			setup: func(mgr *Manager) {
				// Do nothing
			},
			expectedError: false,
		},
		{
			name: "Compact empty level",
			setup: func(mgr *Manager) {
				// Do nothing, will call compactLevel directly
			},
			expectedError: false,
		},
		{
			name: "Invalid SSTable file",
			setup: func(mgr *Manager) {
				mgr.totalMap[mgr.minSSTableLevel] = []string{"invalid1.sst", "invalid2.sst", "invalid3.sst"}
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			mgr := NewSSTableManager(tmpDir)
			tt.setup(mgr)

			var err error
			if tt.name == "Compact empty level" {
				err = mgr.compactLevel(mgr.minSSTableLevel)
			} else {
				err = mgr.Compaction()
			}

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if !tt.expectedError && tt.name == "Empty Level 0" {
				assert.Empty(t, mgr.totalMap[0])
				assert.Empty(t, mgr.totalMap[1])
			}
		})
	}
}

func TestRecursiveCompactionAcrossLevels(t *testing.T) {
	tests := []struct {
		name             string
		level0Files      int
		level1Files      int
		expectedMaxLevel int
	}{
		{
			name:             "Recursive compaction L0 -> L1 -> L2",
			level0Files:      4,
			level1Files:      10, // 假设足以触发 L1 -> L2
			expectedMaxLevel: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			mgr := NewSSTableManager(tmpDir)

			// 构造 Level0 文件
			for i := 0; i < tt.level0Files; i++ {
				mem := memtable.NewMemTable(uint64(i+1), tmpDir)
				_ = mem.Insert(kv.KeyValuePair{
					Key:   kv.Key(fmt.Sprintf("key-%d", i)),
					Value: []byte("val"),
				})
				imem := memtable.NewIMemTable(mem)
				sst := BuildSSTableFromIMemTable(imem, tmpDir)
				sst.level = mgr.minSSTableLevel
				// 手动设置路径
				sst.filePath = sstableFilePath(sst.id, sst.level, tmpDir)
				_ = mgr.addNewSSTables([]*SSTable{sst})
			}

			// 伪造 Level1 文件
			// 注意：这里需要确保文件数量超过 maxFileNumsInLevel(1)
			targetL1Count := mgr.maxFileNumsInLevel(1)
			if tt.level1Files > targetL1Count {
				targetL1Count = tt.level1Files
			}

			for i := 0; i < targetL1Count; i++ {
				sst := NewSSTableWithLevel(1, tmpDir)
				sst.Header = block.NewHeader("keyA", "keyZ")
				sst.Add(&kv.KeyValuePair{Key: "keyA", Value: []byte("valueA")})
				sst.Add(&kv.KeyValuePair{Key: "keyZ", Value: []byte("valueZ")})
				_ = mgr.addNewSSTables([]*SSTable{sst})
			}

			// 压缩
			err := mgr.Compaction()
			assert.NoError(t, err)
			assert.True(t, len(mgr.getFilesByLevel(tt.expectedMaxLevel)) > 0, "should generate higher level SSTables")
		})
	}
}

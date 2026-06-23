package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"github.com/xmh1011/go-kv/engine/lsm/kv"
	"github.com/xmh1011/go-kv/pkg/log"
)

// ScheduleCompaction runs compaction outside the foreground write/flush path.
// Multiple calls while a worker is active are coalesced into one extra pass.
func (m *Manager) ScheduleCompaction() {
	m.mu.Lock()
	if m.compactionRunning {
		m.compactionQueued = true
		m.mu.Unlock()
		return
	}
	m.compactionRunning = true
	m.compactionWG.Add(1)
	m.mu.Unlock()

	go m.runScheduledCompactions()
}

func (m *Manager) runScheduledCompactions() {
	defer m.compactionWG.Done()

	for {
		if err := m.Compaction(); err != nil {
			log.Errorf("[Compaction] Scheduled compaction error: %v", err)
		}

		m.mu.Lock()
		if !m.compactionQueued {
			m.compactionRunning = false
			m.mu.Unlock()
			return
		}
		m.compactionQueued = false
		m.mu.Unlock()
	}
}

// Compaction 执行 Level0 的同步合并，并触发 Level1 及以上的异步合并。
// 合并流程：
// 1. 收集 Level0 文件，解码其 DataBlock，并统计全局 key 区间。
// 2. 从 Level1 中找出与该区间交集的文件，将其 DataBlock 一并取出。
// 3. 使用归并排序将所有块合并分块，产出新 SSTable（写入 Level1）。
// 4. 先写入新 SSTable，再原子替换旧 Level0/Level1 元数据并清理旧文件。
// 5. 如果 Level1 超限，异步触发后续合并。
// Compaction 执行 Level0 的同步合并，并触发后续异步合并
func (m *Manager) Compaction() error {
	// 等待同一层级的压缩完成
	if err := m.waitCompaction(m.minSSTableLevel); err != nil {
		log.Errorf("[Compaction] Wait compaction for level %d error: %s", m.minSSTableLevel, err.Error())
		return fmt.Errorf("wait compaction error: %w", err)
	}

	// 检查 Level0 是否需要压缩
	if !m.isLevelNeedToBeMerged(m.minSSTableLevel) {
		log.Debug("[Compaction] Level 0 not need to be merged")
		return nil
	}

	log.Debugf("[Compaction] Starting compaction for level %d", m.minSSTableLevel)
	// 开始 Level0 压缩
	if err := m.compactLevel(m.minSSTableLevel); err != nil {
		log.Errorf("[Compaction] Compact level %d error: %s", m.minSSTableLevel, err.Error())
		return fmt.Errorf("compact level %d error: %w", m.minSSTableLevel, err)
	}

	// 触发下一层级异步压缩（如果需要）
	if m.isLevelNeedToBeMerged(m.minSSTableLevel + 1) {
		log.Debugf("[Compaction] Triggering async compaction for level %d", m.minSSTableLevel+1)
		m.compactionWG.Add(1)
		go func() {
			defer m.compactionWG.Done()
			m.asyncCompactLevel(m.minSSTableLevel + 1)
		}()
	}

	return nil
}

// asyncCompactLevel 异步合并指定层级（Level1 及以上）
func (m *Manager) asyncCompactLevel(level int) {
	for {
		// 等待同一层级的压缩完成
		if err := m.waitCompaction(level); err != nil {
			log.Errorf("[Compaction] Wait compaction error: %v", err)
			return
		}

		// 检查是否需要压缩
		if !m.isLevelNeedToBeMerged(level) {
			return
		}

		log.Debugf("[Compaction] Starting async compaction for level %d", level)
		// 执行压缩
		if err := m.compactLevel(level); err != nil {
			log.Errorf("[Compaction] Async compaction at level %d error: %v", level, err)
			return
		}

		// 如果下一层级仍需压缩，继续循环（仅对中间层级）
		if level < m.maxSSTableLevel && m.isLevelNeedToBeMerged(level+1) {
			continue
		}
		return
	}
}

// compactLevel 同步合并指定层级
func (m *Manager) compactLevel(level int) error {
	// 标记当前层级及目标层级开始压缩。合并会同时读取/替换 level
	// 和 level+1 的元数据，因此相邻层级的查询和合并都需要等待。
	compactingLevels := m.compactionLevelsFor(level)
	m.waitAndStartCompaction(compactingLevels)

	// 1. 读取当前层级的所有键值对
	files := m.getFilesByLevel(level)
	// 对于 level 1 及以上的层级
	// 按照时间顺序，只合并超出数量的旧文件
	// files 是按 ID 降序排列的（新 -> 旧）
	// 所以 files[limit:] 是最旧的那些溢出文件
	if level > m.minSSTableLevel {
		limit := m.maxFileNumsInLevel(level)
		if len(files) > limit {
			files = files[limit:]
		}
	}
	allPairs, err := m.loadLevelData(level, files)
	if err != nil {
		m.endCompactionLevels(compactingLevels)
		log.Errorf("[Compaction] Load level %d data error: %s", level, err.Error())
		return fmt.Errorf("load level %d data error: %w", level, err)
	}
	if len(allPairs) == 0 {
		m.endCompactionLevels(compactingLevels)
		log.Debugf("[Compaction] Level %d has no readable data after pruning stale metadata", level)
		return nil
	}

	// 2. 加载重叠文件
	var nextLevelPairs []kv.KeyValuePair
	var oldNextFiles []string
	if level < m.maxSSTableLevel {
		minK, maxK := getGlobalKeyRangeFromPairs(allPairs)
		nextLevelPairs, oldNextFiles, err = m.mergeNextLevelFiles(level+1, minK, maxK)
		if err != nil {
			m.endCompactionLevels(compactingLevels)
			log.Errorf("[Compaction] Merge next level files error: %s", err.Error())
			return fmt.Errorf("merge next level files error: %w", err)
		}
		allPairs = append(allPairs, nextLevelPairs...)
	}

	// 3. 合并并生成新 SSTable
	newTables := m.CompactAndMergeKVs(allPairs, level+1) // 目标层级为当前+1

	// 4. 先把新文件完整写入磁盘，再原子切换内存元数据。
	if err := m.encodeSSTables(newTables); err != nil {
		m.endCompactionLevels(compactingLevels)
		log.Errorf("[Compaction] Encode new SSTables error: %s", err.Error())
		return fmt.Errorf("encode new SSTables error: %w", err)
	}

	removals := make([]sstableRemoval, 0, len(files)+len(oldNextFiles))
	removals = append(removals, sstableRemovals(files, level)...)
	if len(oldNextFiles) > 0 {
		removals = append(removals, sstableRemovals(oldNextFiles, level+1)...)
	}
	m.installCompactedSSTables(removals, newTables)

	log.Debugf("[Compaction] Level %d compaction finished, generated %d new tables", level, len(newTables))

	m.endCompactionLevels(compactingLevels)

	// 6. 如果目标层级仍需压缩，递归处理（仅对中间层级）
	if level < m.maxSSTableLevel && m.isLevelNeedToBeMerged(level+1) {
		return m.compactLevel(level + 1)
	}

	return nil
}

func (m *Manager) compactionLevelsFor(level int) []int {
	levels := []int{level}
	if level < m.maxSSTableLevel {
		levels = append(levels, level+1)
	}
	return levels
}

// waitCompaction 等待指定层级的压缩完成
func (m *Manager) waitCompaction(level int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for m.compactingLevels[level] {
		log.Debugf("[Compaction] Level %d is compacting, waiting...", level)
		m.compactionCond.Wait()
	}
	return nil
}

// waitAndStartCompaction 等待所有相关层级空闲后一次性标记为压缩中。
func (m *Manager) waitAndStartCompaction(levels []int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for {
		blocked := false
		for _, level := range levels {
			if m.compactingLevels[level] {
				blocked = true
				break
			}
		}
		if !blocked {
			break
		}
		m.compactionCond.Wait()
	}

	for _, level := range levels {
		m.compactingLevels[level] = true
	}
}

// endCompactionLevels 标记层级压缩完成并广播通知。
func (m *Manager) endCompactionLevels(levels []int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, level := range levels {
		delete(m.compactingLevels, level)
	}
	m.compactionCond.Broadcast()
}

// loadLevelData 加载指定层级的所有键值对。
// 如果元数据指向的 SSTable 文件已经不存在，说明内存目录和磁盘目录之间
// 出现了陈旧目录项；此时清理该目录项并继续，避免后续 flush 永久卡死。
func (m *Manager) loadLevelData(level int, files []string) ([]kv.KeyValuePair, error) {
	allPairs := make([]kv.KeyValuePair, 0)

	for _, path := range files {
		sst, ok := m.getSSTableByPath(path)
		if !ok {
			if m.pruneMissingSSTableMetadata(path, level) {
				continue
			}
			log.Errorf("[Compaction] Sstable not found for path: %s", path)
			return nil, fmt.Errorf("sstable metadata not found for path %s", path)
		}

		pairs, ok, err := m.loadCompactionDataBlock(sst, path, level)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		allPairs = append(allPairs, pairs...)
	}

	return allPairs, nil
}

// mergeNextLevelFiles 合并下一层级的重叠文件
func (m *Manager) mergeNextLevelFiles(level int, minK, maxK kv.Key) ([]kv.KeyValuePair, []string, error) {
	nextLevelFiles := m.getFilesByLevel(level)
	oldFiles := make([]string, 0)
	allPairs := make([]kv.KeyValuePair, 0)

	for _, path := range nextLevelFiles {
		sst, ok := m.getSSTableByPath(path)
		if !ok {
			if m.pruneMissingSSTableMetadata(path, level) {
				continue
			}
			log.Errorf("[Compaction] Sstable not found for path: %s", path)
			return nil, nil, fmt.Errorf("sstable metadata not found for path %s", path)
		}

		if overlapRange(minK, maxK, sst) {
			pairs, ok, err := m.loadCompactionDataBlock(sst, path, level)
			if err != nil {
				log.Errorf("[Compaction] Load data blocks error: %v", err)
				return nil, nil, err
			}
			if !ok {
				continue
			}
			allPairs = append(allPairs, pairs...)
			oldFiles = append(oldFiles, path)
		}
	}

	return allPairs, oldFiles, nil
}

func (m *Manager) loadCompactionDataBlock(sst *SSTable, path string, level int) ([]kv.KeyValuePair, bool, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			m.pruneMissingSSTableMetadata(path, level)
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("stat sstable file %s failed: %w", path, err)
	}

	pairs, err := sst.GetDataBlockFromFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			m.pruneMissingSSTableMetadata(path, level)
			return nil, false, nil
		}
		log.Errorf("[Compaction] Decode sstable from file %s error: %s", path, err.Error())
		return nil, false, fmt.Errorf("decode sstable from file %s error: %w", path, err)
	}
	return pairs, true, nil
}

func (m *Manager) pruneMissingSSTableMetadata(path string, level int) bool {
	if _, err := os.Stat(path); err == nil {
		return false
	} else if !errors.Is(err, os.ErrNotExist) {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	log.Warnf("[Compaction] Pruning stale SSTable metadata for missing file %s at level %d", path, level)
	m.removeTableMetadataLocked(path, level)
	return true
}

// getGlobalKeyRangeFromPairs 从键值对中计算全局 Key 范围
func getGlobalKeyRangeFromPairs(pairs []kv.KeyValuePair) (kv.Key, kv.Key) {
	if len(pairs) == 0 {
		return "", ""
	}

	minKey, maxKey := pairs[0].Key, pairs[0].Key
	for _, pair := range pairs {
		if pair.Key < minKey {
			minKey = pair.Key
		}
		if pair.Key > maxKey {
			maxKey = pair.Key
		}
	}
	return minKey, maxKey
}

// overlapRange 判断 global range [minKey, maxKey] 是否与 sst 索引区间有交集
func overlapRange(minKey, maxKey kv.Key, sst *SSTable) bool {
	return bytes.Compare([]byte(sst.Header.MinKey), []byte(maxKey)) <= 0 && bytes.Compare([]byte(sst.Header.MaxKey), []byte(minKey)) >= 0
}

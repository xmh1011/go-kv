package lsm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/xmh1011/go-kv/engine/lsm/database"
	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
)

// StateMachineAdapter 实现了 storage.StateMachine 接口，
// 将 Raft 的 Apply 请求适配到底层的 LSM 数据库。
type StateMachineAdapter struct {
	db *database.Database
}

// NewStateMachineAdapter 创建一个新的 LSM 状态机适配器。
func NewStateMachineAdapter(db *database.Database) *StateMachineAdapter {
	return &StateMachineAdapter{
		db: db,
	}
}

// Apply 将 Raft 日志条目应用到 LSM 数据库。
func (lsm *StateMachineAdapter) Apply(entry param.LogEntry) any {
	// 1. 解析命令
	var cmd param.KVCommand
	cmdBytes, ok := entry.Command.([]byte)
	if !ok {
		log.Errorf("[LSMAdapter] Apply failed: command is not []byte, but %T", entry.Command)
		return fmt.Errorf("invalid command format: not []byte")
	}
	if err := json.Unmarshal(cmdBytes, &cmd); err != nil {
		log.Errorf("[LSMAdapter] Apply failed: failed to unmarshal command: %v", err)
		return err
	}

	log.Debugf("[LSMAdapter] Applying command: Op=%d, Key=%s", cmd.Op, cmd.Key)

	// 2. 根据操作类型执行
	switch cmd.Op {
	case param.OpSet:
		err := lsm.db.Put(cmd.Key, []byte(cmd.Value))
		if err != nil {
			log.Errorf("[LSMAdapter] Apply 'set' failed for key '%s': %v", cmd.Key, err)
		}
		return err
	case param.OpDelete:
		err := lsm.db.Delete(cmd.Key)
		if err != nil {
			log.Errorf("[LSMAdapter] Apply 'delete' failed for key '%s': %v", cmd.Key, err)
		}
		return err
	default:
		log.Warnf("[LSMAdapter] Apply received unknown operation: %d", cmd.Op)
		return fmt.Errorf("unknown operation: %d", cmd.Op)
	}
}

// Get 从 LSM 数据库中读取一个键的值。
func (lsm *StateMachineAdapter) Get(key string) (string, error) {
	value, err := lsm.db.Get(key)
	if err != nil {
		log.Errorf("[LSMAdapter] Get failed for key '%s': %v", key, err)
		return "", err
	}
	if value == nil {
		return "", nil // Not found
	}
	return string(value), nil
}

// GetSnapshot 生成状态机的快照。
// 实现策略：
// 1. 强制将所有 MemTable flush 到磁盘。
// 2. 获取所有 SSTable 文件的路径列表。
// 3. 读取所有 SSTable 文件的内容。
// 4. 将文件名（相对路径）和内容打包成 map[string][]byte 并序列化。
func (lsm *StateMachineAdapter) GetSnapshot() ([]byte, error) {
	log.Info("[LSMAdapter] Creating snapshot...")

	// 1. 强制 Flush
	if err := lsm.db.ForceFlush(); err != nil {
		log.Errorf("[LSMAdapter] Force flush failed during snapshot: %v", err)
		return nil, err
	}

	// 2. 获取所有 SSTable 文件列表
	files := lsm.db.GetAllSSTables()
	// dbRoot := lsm.db.Name()
	// SSTable 路径是 dbRoot/sst
	sstRoot := filepath.Join(lsm.db.Name(), "sst")

	// 3. 读取文件内容
	snapshotData := make(map[string][]byte)
	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			log.Errorf("[LSMAdapter] Failed to read file %s for snapshot: %v", file, err)
			return nil, err
		}
		// 计算相对路径，例如 "0-level/1.sst"
		relPath, err := filepath.Rel(sstRoot, file)
		if err != nil {
			log.Errorf("[LSMAdapter] Failed to get relative path for %s: %v", file, err)
			return nil, err
		}
		snapshotData[relPath] = content
	}

	// 4. 序列化
	data, err := json.Marshal(snapshotData)
	if err != nil {
		log.Errorf("[LSMAdapter] Failed to marshal snapshot data: %v", err)
		return nil, err
	}

	log.Infof("[LSMAdapter] Snapshot created with %d files", len(files))
	return data, nil
}

// ApplySnapshot 应用快照来恢复状态机。
// 实现策略：
// 1. 反序列化快照数据。
// 2. 清空当前数据库（包括内存和磁盘）。
// 3. 将快照中的文件写回磁盘。
// 4. 重新加载数据库。
func (lsm *StateMachineAdapter) ApplySnapshot(snapshot []byte) error {
	log.Info("[LSMAdapter] Applying snapshot...")

	// 1. 反序列化
	var snapshotData map[string][]byte
	if err := json.Unmarshal(snapshot, &snapshotData); err != nil {
		log.Errorf("[LSMAdapter] Failed to unmarshal snapshot data: %v", err)
		return err
	}

	dbPath := lsm.db.Name()
	sstPath := filepath.Join(dbPath, "sst")

	// 2. 清空目录 (先关闭 DB)
	if err := lsm.db.Close(); err != nil {
		log.Errorf("[LSMAdapter] Failed to close DB before applying snapshot: %v", err)
		return err
	}

	if err := os.RemoveAll(dbPath); err != nil {
		log.Errorf("[LSMAdapter] Failed to remove DB directory %s: %v", dbPath, err)
		return err
	}
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return err
	}

	// 3. 写回文件
	for relPath, content := range snapshotData {
		// 防止路径遍历攻击
		if strings.Contains(relPath, "..") {
			log.Warnf("[LSMAdapter] Skipping invalid snapshot file path: %s", relPath)
			continue
		}

		fullPath := filepath.Join(sstPath, relPath)

		// 确保子目录存在 (例如 0-level)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			return err
		}

		if err := os.WriteFile(fullPath, content, 0644); err != nil {
			log.Errorf("[LSMAdapter] Failed to write snapshot file %s: %v", fullPath, err)
			return err
		}
	}

	log.Infof("[LSMAdapter] Snapshot applied. %d files restored.", len(snapshotData))

	// 4. 重新加载
	// 重新加载前，确保 WAL 目录存在
	walPath := filepath.Join(dbPath, "wal")
	if err := os.MkdirAll(walPath, 0755); err != nil {
		log.Errorf("[LSMAdapter] Failed to create WAL directory %s: %v", walPath, err)
		return err
	}
	// 重新加载前，确保 SSTable 目录存在
	if err := os.MkdirAll(sstPath, 0755); err != nil {
		log.Errorf("[LSMAdapter] Failed to create SSTable directory %s: %v", sstPath, err)
		return err
	}

	if err := lsm.db.Reload(); err != nil {
		log.Errorf("[LSMAdapter] Failed to reload DB after snapshot: %v", err)
		return err
	}

	return nil
}

// Close 关闭底层的 LSM 数据库
func (lsm *StateMachineAdapter) Close() error {
	return lsm.db.Close()
}

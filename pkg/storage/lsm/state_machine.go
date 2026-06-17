package lsm

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/xmh1011/go-kv/engine/lsm/database"
	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage/kvstore"
)

var ErrKeyNotFound = kvstore.ErrKeyNotFound

var lsmSnapshotMagic = []byte("GOKV-LSM-SNAPSHOT\x00\x01")

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
	cmdBytes, ok := param.UnwrapClientCommand(entry.Command).([]byte)
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
		return "", ErrKeyNotFound
	}
	return string(value), nil
}

// GetSnapshot 生成状态机的快照。
// 实现策略：
// 1. 强制将所有 MemTable flush 到磁盘。
// 2. 获取所有 SSTable 文件的路径列表。
// 3. 读取所有 SSTable 文件的内容。
// 4. 将文件名（相对路径）和内容打包成长度前缀的二进制归档。
func (lsm *StateMachineAdapter) GetSnapshot() ([]byte, error) {
	readSnapshot, err := lsm.PrepareSnapshot()
	if err != nil {
		return nil, err
	}
	return readSnapshot()
}

// PrepareSnapshot performs the short consistency-critical part of snapshot
// creation. The returned function reads immutable SSTable files while they are
// pinned, but it can run after Raft releases stateMachineMu.
func (lsm *StateMachineAdapter) PrepareSnapshot() (func() ([]byte, error), error) {
	log.Debug("[LSMAdapter] Creating snapshot...")

	// 1. 强制 Flush
	if err := lsm.db.ForceFlush(); err != nil {
		log.Errorf("[LSMAdapter] Force flush failed during snapshot: %v", err)
		return nil, err
	}

	// 2. Open all immutable SSTable files under the manager lock, then release
	// the lock before reading file content. Open descriptors pin the immutable
	// files while avoiding long stalls for later memtable flushes.
	files, err := lsm.db.SSTables.OpenFilesSnapshot()
	if err != nil {
		return nil, err
	}
	// dbRoot := lsm.db.Name()
	// SSTable 路径是 dbRoot/sst
	sstRoot := filepath.Join(lsm.db.Name(), "sst")

	return func() ([]byte, error) {
		defer func() {
			for _, file := range files {
				_ = file.File.Close()
			}
		}()

		// 3. 读取文件内容
		snapshotData := make(map[string][]byte)
		for _, file := range files {
			content, err := io.ReadAll(file.File)
			if err != nil {
				log.Errorf("[LSMAdapter] Failed to read file %s for snapshot: %v", file.Path, err)
				return nil, err
			}
			// 计算相对路径，例如 "0-level/1.sst"
			relPath, err := filepath.Rel(sstRoot, file.Path)
			if err != nil {
				log.Errorf("[LSMAdapter] Failed to get relative path for %s: %v", file.Path, err)
				return nil, err
			}
			snapshotData[relPath] = content
		}

		// 4. 序列化。避免 JSON 对 []byte 做 base64 编码，减少快照生成
		// 在状态机锁内的 CPU 和内存放大。
		data, err := encodeSnapshotData(snapshotData)
		if err != nil {
			log.Errorf("[LSMAdapter] Failed to encode snapshot data: %v", err)
			return nil, err
		}

		log.Debugf("[LSMAdapter] Snapshot created with %d files", len(files))
		return data, nil
	}, nil
}

// ApplySnapshot 应用快照来恢复状态机。
// 实现策略：
// 1. 反序列化快照数据。
// 2. 清空当前数据库（包括内存和磁盘）。
// 3. 将快照中的文件写回磁盘。
// 4. 重新加载数据库。
func (lsm *StateMachineAdapter) ApplySnapshot(snapshot []byte) error {
	log.Debug("[LSMAdapter] Applying snapshot...")

	// 1. 反序列化
	snapshotData, err := decodeSnapshotData(snapshot)
	if err != nil {
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

	log.Debugf("[LSMAdapter] Snapshot applied. %d files restored.", len(snapshotData))

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

func encodeSnapshotData(files map[string][]byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(lsmSnapshotMagic)

	if len(files) > int(^uint32(0)) {
		return nil, fmt.Errorf("too many snapshot files: %d", len(files))
	}
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(files))); err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	for _, path := range paths {
		pathBytes := []byte(path)
		content := files[path]
		if len(pathBytes) > int(^uint32(0)) {
			return nil, fmt.Errorf("snapshot path too long: %s", path)
		}
		if err := binary.Write(&buf, binary.BigEndian, uint32(len(pathBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(pathBytes); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, uint64(len(content))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(content); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeSnapshotData(snapshot []byte) (map[string][]byte, error) {
	if !bytes.HasPrefix(snapshot, lsmSnapshotMagic) {
		var legacy map[string][]byte
		if err := json.Unmarshal(snapshot, &legacy); err != nil {
			return nil, err
		}
		return legacy, nil
	}

	reader := bytes.NewReader(snapshot[len(lsmSnapshotMagic):])
	var count uint32
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return nil, err
	}

	files := make(map[string][]byte, int(count))
	for i := uint32(0); i < count; i++ {
		var pathLen uint32
		if err := binary.Read(reader, binary.BigEndian, &pathLen); err != nil {
			return nil, err
		}
		if pathLen == 0 || uint64(pathLen) > uint64(reader.Len()) {
			return nil, fmt.Errorf("invalid snapshot path length %d", pathLen)
		}
		pathBytes := make([]byte, int(pathLen))
		if _, err := io.ReadFull(reader, pathBytes); err != nil {
			return nil, err
		}

		var contentLen uint64
		if err := binary.Read(reader, binary.BigEndian, &contentLen); err != nil {
			return nil, err
		}
		if contentLen > uint64(reader.Len()) {
			return nil, fmt.Errorf("invalid snapshot content length %d", contentLen)
		}
		content := make([]byte, int(contentLen))
		if _, err := io.ReadFull(reader, content); err != nil {
			return nil, err
		}
		files[string(pathBytes)] = content
	}

	if reader.Len() != 0 {
		return nil, fmt.Errorf("snapshot has %d trailing bytes", reader.Len())
	}
	return files, nil
}

// Close 关闭底层的 LSM 数据库
func (lsm *StateMachineAdapter) Close() error {
	return lsm.db.Close()
}

// WAL design for each memtable
// The Write-Ahead Log (WAL) is a fundamental mechanism used to ensure durability and crash recovery
// in storage systems like databases and LSM-tree-based key-value stores.
// When a write operation (such as inserting or deleting a key-value pair) occurs,
// the data is first appended to the WAL file on disk before being applied to the in-memory structure (e.g., MemTable).
// This append-only log ensures that, in the event of a crash or system failure,
// the system can replay the WAL to restore the in-memory state to its last consistent point.
// Since the WAL is written sequentially, it offers high write throughput and minimal I/O overhead.
// Once the data in memory (MemTable) is flushed to disk in a more structured format (like an SSTable),
// the corresponding WAL file can be safely deleted. In LSM-based systems,
// WAL plays a crucial role in achieving durability, fault tolerance, and write efficiency.

package wal

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/xmh1011/go-kv/engine/lsm/kv"
	"github.com/xmh1011/go-kv/pkg/log"
)

const (
	defaultWALFileMode   = 0666
	defaultWALFileSuffix = "wal"
)

// WAL implementation
// 在项目设计中，每个memtable拥有独立WAL且单线程写入，因此不需要考虑加锁的问题
// 如果WAL是单实例、全局持久化日志文件（common to all MemTables），多个协程可能同时写入或重放数据，必须加锁。
type WAL struct {
	file *os.File
	path string
}

// NewWAL creates a new instance of WAL for the specified memtable id and a directory path.
// This implementation has WAL for each memtable.
// Every write to memtable involves writing every key/value pair from the batch to WAL.
// This implementation writes every key/value pair from the batch to WAL individually.
func NewWAL(id uint64, dirPath string) (*WAL, error) {
	// Ensure the WAL directory exists
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		log.Errorf("[WAL] Failed to create WAL directory %s: %s", dirPath, err.Error())
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	wal := &WAL{
		path: CreateWalPath(id, dirPath),
	}
	var err error
	wal.file, err = os.OpenFile(wal.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, defaultWALFileMode)
	if err != nil {
		log.Errorf("[WAL] Failed to open WAL file: %s", err.Error())
		return nil, err
	}
	log.Debugf("[WAL] Created new WAL file: %s", wal.path)
	return wal, nil
}

// CreateWalPath creates a WAL path for the memtable with id.
func CreateWalPath(id uint64, walDirectoryPath string) string {
	return filepath.Join(walDirectoryPath, fmt.Sprintf("%v.%s", id, defaultWALFileSuffix))
}

// Sync flushes the file to disk.
func (w *WAL) Sync() error {
	if w.file == nil {
		return nil
	}
	return w.file.Sync()
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	if w.file == nil {
		return nil
	}
	log.Debugf("[WAL] Closing WAL file: %s", w.path)
	err := w.file.Close()
	w.file = nil
	return err
}

// DeleteFile deletes the WAL file.
func (w *WAL) DeleteFile() error {
	_ = w.Close()
	log.Debugf("[WAL] Deleting WAL file: %s", w.path)
	return os.Remove(w.path)
}

// Append writes a KeyValuePair in JSON format to the WAL file.
func (w *WAL) Append(pair kv.KeyValuePair) error {
	if w.file == nil {
		return fmt.Errorf("wal file is closed")
	}
	if err := pair.EncodeTo(w.file); err != nil {
		log.Errorf("[WAL] Failed to write wal, key: %s, error: %s", pair.Key, err.Error())
		return fmt.Errorf("failed to write wal, key: %s: %w", pair.Key, err)
	}

	return nil
}

// Recover reads the WAL file and calls the callback function for each KeyValuePair.
func Recover(path string, callback func(pair kv.KeyValuePair)) (*WAL, error) {
	// 使用 O_RDWR 以便恢复后的 MemTable 可以继续写入
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("[WAL] Open wal file failed: %s", err.Error())
		return nil, fmt.Errorf("open wal file failed: %w", err)
	}
	raw, err := io.ReadAll(file)
	if err != nil {
		log.Errorf("[WAL] Read wal file failed: %s", err.Error())
		return nil, fmt.Errorf("read wal file failed: %w", err)
	}

	buf := bytes.NewReader(raw)
	count := 0
	for buf.Len() > 0 {
		var pair kv.KeyValuePair
		err := pair.DecodeFrom(buf)
		if err != nil {
			log.Errorf("[WAL] Failed to read wal %s, error: %s", file.Name(), err.Error())
			return nil, fmt.Errorf("failed to read wal %s: %w", file.Name(), err)
		}

		// 回调处理有效数据
		callback(pair)
		count++
	}

	log.Infof("[WAL] Recovered %d entries from %s", count, path)
	return &WAL{file: file, path: path}, nil
}

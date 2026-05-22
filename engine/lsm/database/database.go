package database

import (
	"path/filepath"

	"github.com/xmh1011/go-kv/engine/lsm/kv"
	"github.com/xmh1011/go-kv/engine/lsm/memtable"
	"github.com/xmh1011/go-kv/engine/lsm/sstable"
	"github.com/xmh1011/go-kv/pkg/log"
)

type Database struct {
	name      string
	MemTables *memtable.Manager
	SSTables  *sstable.Manager
}

func Open(name string) *Database {
	log.Debugf("[Database] Opening database: %s", name)
	walPath := filepath.Join(name, "wal")
	sstPath := filepath.Join(name, "sst")
	return &Database{
		name:      name,
		MemTables: memtable.NewMemTableManager(walPath),
		SSTables:  sstable.NewSSTableManager(sstPath),
	}
}

// Name 返回数据库的名称（路径）
func (d *Database) Name() string {
	return d.name
}

func (d *Database) Get(key string) ([]byte, error) {
	log.Debugf("[Database] Get key: %s", key)
	value, found := d.MemTables.Search(kv.Key(key))
	if found {
		if value == nil {
			log.Debugf("[Database] Key %s deleted in MemTable", key)
			return nil, nil
		}
		log.Debugf("[Database] Key %s found in MemTable", key)
		return value, nil
	}

	value, found, err := d.SSTables.Search(kv.Key(key))
	if err != nil {
		log.Errorf("[Database] Search key %s in sstable error: %s", key, err.Error())
		return nil, err
	}
	if !found {
		log.Debugf("[Database] Key %s not found", key)
		return nil, nil
	}
	if value == nil {
		log.Debugf("[Database] Key %s deleted in SSTable", key)
		return nil, nil
	}

	log.Debugf("[Database] Key %s found in SSTable", key)
	return value, nil
}

func (d *Database) Put(key string, value []byte) error {
	log.Debugf("[Database] Put key: %s", key)
	imem, err := d.MemTables.Insert(kv.KeyValuePair{Key: kv.Key(key), Value: value})
	if err != nil {
		log.Errorf("[Database] Insert key %s error: %s", key, err.Error())
		return err
	}
	d.createNewSSTable(imem)
	return nil
}

func (d *Database) Delete(key string) error {
	log.Debugf("[Database] Delete key: %s", key)
	imem, err := d.MemTables.Delete(kv.Key(key))
	if err != nil {
		log.Errorf("[Database] Delete key %s error: %s", key, err.Error())
		return err
	}
	d.createNewSSTable(imem)
	return nil
}

func (d *Database) Recover() error {
	log.Debug("[Database] Starting recovery...")
	// 1. 恢复内存中的 MemTable
	if err := d.MemTables.Recover(); err != nil {
		log.Errorf("[Database] Recover memtable error: %s", err.Error())
		return err
	}

	// 2. 恢复磁盘中的 SSTable
	if err := d.SSTables.Recover(); err != nil {
		log.Errorf("[Database] Recover sstable error: %s", err.Error())
		return err
	}

	log.Debug("[Database] Recovery completed successfully")
	return nil
}

// ForceFlush 强制将当前的 MemTable 转换为 Immutable MemTable 并刷新到 SSTable。
// 这通常用于快照操作。
func (d *Database) ForceFlush() error {
	log.Debug("[Database] Force flushing MemTable to SSTable")
	// 1. 强制 Promote
	imem := d.MemTables.ForcePromote()
	if imem == nil {
		log.Debug("[Database] MemTable is empty, nothing to flush")
		return nil
	}

	// 2. 刷新到 SSTable
	if err := d.SSTables.CreateNewSSTable(imem); err != nil {
		d.MemTables.CompleteFlush(imem, false)
		log.Errorf("[Database] Create new sstable error: %s", err.Error())
		return err
	}
	d.MemTables.CompleteFlush(imem, true)

	log.Debug("[Database] Force flush completed")
	return nil
}

// GetAllSSTables 返回当前所有 SSTable 的文件路径列表。
// 这用于快照生成。
func (d *Database) GetAllSSTables() []string {
	return d.SSTables.GetAllFiles()
}

func (d *Database) createNewSSTable(imem *memtable.IMemTable) {
	if imem == nil {
		return
	}
	log.Debug("[Database] MemTable full, flushing to SSTable")
	if err := d.SSTables.CreateNewSSTable(imem); err != nil {
		d.MemTables.CompleteFlush(imem, false)
		log.Errorf("[Database] Create new sstable error: %s", err.Error())
		return
	}
	d.MemTables.CompleteFlush(imem, true)
}

// Close 关闭数据库，释放资源。
func (d *Database) Close() error {
	log.Debug("[Database] Closing database...")
	d.SSTables.WaitForCompactions()
	// 关闭 MemTable Manager (主要是关闭 WAL)
	if err := d.MemTables.Close(); err != nil {
		log.Errorf("[Database] Close MemTable manager error: %s", err.Error())
		return err
	}
	return nil
}

// Reload 关闭并重新打开数据库（用于快照恢复后）
func (d *Database) Reload() error {
	log.Debug("[Database] Reloading database...")
	if err := d.Close(); err != nil {
		return err
	}

	// 重新初始化 Managers
	walPath := filepath.Join(d.name, "wal")
	sstPath := filepath.Join(d.name, "sst")
	d.MemTables = memtable.NewMemTableManager(walPath)
	d.SSTables = sstable.NewSSTableManager(sstPath)

	return d.Recover()
}

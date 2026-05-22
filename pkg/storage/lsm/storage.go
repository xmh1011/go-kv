package lsm

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"strconv"
	"sync"

	"github.com/xmh1011/go-kv/engine/lsm/database"
	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
)

const (
	keyHardState  = "meta:hard_state"
	keyFirstIndex = "meta:first_index"
	keyLastIndex  = "meta:last_index"
	keyLogSize    = "meta:log_size"
	keyLogMeta    = "meta:log_meta" // 合并 firstIndex + lastIndex + logSize
	keySnapshot   = "meta:snapshot"
	logKeyPrefix  = "log:"
)

const (
	logEntryFormatMagic = "GLG1"

	logCommandNil byte = iota
	logCommandBytes
	logCommandString
	logCommandKV
	logCommandConfigChange
	logCommandClient
)

// StorageAdapter 实现了 storage.Storage 接口，
// 使用 LSM 树来存储 Raft 的日志条目和元数据。
type StorageAdapter struct {
	db *database.Database
	mu sync.RWMutex

	// 缓存元数据以提高性能
	firstIndex uint64
	lastIndex  uint64
	logSize    int
}

// NewStorageAdapter 创建一个新的 LSM 存储适配器。
func NewStorageAdapter(db *database.Database) (*StorageAdapter, error) {
	s := &StorageAdapter{db: db}
	if err := s.init(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *StorageAdapter) init() error {
	// 优先从合并 key 读取 (firstIndex + lastIndex + logSize)
	val, err := s.db.Get(keyLogMeta)
	if err != nil {
		return fmt.Errorf("get log meta failed: %w", err)
	}
	if val != nil && len(val) == 24 {
		s.firstIndex = binary.BigEndian.Uint64(val[0:8])
		s.lastIndex = binary.BigEndian.Uint64(val[8:16])
		s.logSize = int(binary.BigEndian.Uint64(val[16:24]))
		log.Debugf("[LSMStorage] Initialized. FirstIndex: %d, LastIndex: %d, LogSize: %d", s.firstIndex, s.lastIndex, s.logSize)
		return nil
	}

	// 兼容旧格式：分别从三个 key 读取
	val, err = s.db.Get(keyFirstIndex)
	if err != nil {
		return fmt.Errorf("get first index failed: %w", err)
	}
	if val != nil {
		if len(val) != 8 {
			return fmt.Errorf("invalid first index data length: %d", len(val))
		}
		s.firstIndex = binary.BigEndian.Uint64(val)
	} else {
		s.firstIndex = 1 // 默认为 1
	}

	val, err = s.db.Get(keyLastIndex)
	if err != nil {
		return fmt.Errorf("get last index failed: %w", err)
	}
	if val != nil {
		if len(val) != 8 {
			return fmt.Errorf("invalid last index data length: %d", len(val))
		}
		s.lastIndex = binary.BigEndian.Uint64(val)
	} else {
		s.lastIndex = 0
	}

	val, err = s.db.Get(keyLogSize)
	if err != nil {
		return fmt.Errorf("get log size failed: %w", err)
	}
	if val != nil {
		if len(val) != 8 {
			return fmt.Errorf("invalid log size data length: %d", len(val))
		}
		s.logSize = int(binary.BigEndian.Uint64(val))
	} else {
		s.logSize = 0
	}

	log.Debugf("[LSMStorage] Initialized. FirstIndex: %d, LastIndex: %d, LogSize: %d", s.firstIndex, s.lastIndex, s.logSize)
	return nil
}

func (s *StorageAdapter) getLogKey(index uint64) string {
	// 手动零填充，避免 fmt.Sprintf 的解析和分配开销
	num := strconv.FormatUint(index, 10)
	var buf [24]byte // "log:" (4) + 20 digits
	copy(buf[:4], logKeyPrefix)
	// 零填充
	padLen := 20 - len(num)
	for i := 0; i < padLen; i++ {
		buf[4+i] = '0'
	}
	copy(buf[4+padLen:], num)
	return string(buf[:24])
}

// SetState 原子地设置 HardState (currentTerm, votedFor)。
func (s *StorageAdapter) SetState(state param.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data := make([]byte, 24)
	binary.BigEndian.PutUint64(data[0:8], state.CurrentTerm)
	binary.BigEndian.PutUint64(data[8:16], state.VotedFor)
	binary.BigEndian.PutUint64(data[16:24], state.CommitIndex)

	if err := s.db.Put(keyHardState, data); err != nil {
		log.Errorf("[LSMStorage] SetState failed: %v", err)
		return err
	}
	return nil
}

// GetState 获取最后保存的 HardState。
func (s *StorageAdapter) GetState() (param.HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var state param.HardState
	val, err := s.db.Get(keyHardState)
	if err != nil {
		return state, err
	}
	if val == nil {
		return state, nil // 返回空状态
	}

	if len(val) != 24 {
		return state, fmt.Errorf("invalid hard state data length: %d", len(val))
	}

	state.CurrentTerm = binary.BigEndian.Uint64(val[0:8])
	state.VotedFor = binary.BigEndian.Uint64(val[8:16])
	state.CommitIndex = binary.BigEndian.Uint64(val[16:24])
	return state, nil
}

// encodeLogEntry uses a compact binary format for Raft log entries and the
// command shapes this storage layer is expected to persist.
func encodeLogEntry(entry *param.LogEntry) ([]byte, error) {
	cmdBytes, err := encodeLogCommand(entry.Command)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 4+8+8+4+len(cmdBytes))
	copy(buf[:4], logEntryFormatMagic)
	binary.BigEndian.PutUint64(buf[4:12], entry.Term)
	binary.BigEndian.PutUint64(buf[12:20], entry.Index)
	binary.BigEndian.PutUint32(buf[20:24], uint32(len(cmdBytes)))
	copy(buf[24:], cmdBytes)
	return buf, nil
}

func encodeLogCommand(command any) ([]byte, error) {
	buf := make([]byte, 0, 64)
	if err := appendLogCommand(&buf, command); err != nil {
		return nil, err
	}
	return buf, nil
}

func appendLogCommand(buf *[]byte, command any) error {
	switch cmd := command.(type) {
	case nil:
		*buf = append(*buf, logCommandNil)
	case []byte:
		*buf = append(*buf, logCommandBytes)
		appendBytes(buf, cmd)
	case string:
		*buf = append(*buf, logCommandString)
		appendString(buf, cmd)
	case param.KVCommand:
		*buf = append(*buf, logCommandKV)
		appendUint64(buf, uint64(cmd.Op))
		appendString(buf, cmd.Key)
		appendString(buf, cmd.Value)
	case param.ConfigChangeCommand:
		*buf = append(*buf, logCommandConfigChange)
		appendUint64(buf, uint64(len(cmd.NewPeerIDs)))
		for _, peerID := range cmd.NewPeerIDs {
			appendUint64(buf, uint64(peerID))
		}
	case param.ClientCommand:
		*buf = append(*buf, logCommandClient)
		appendUint64(buf, uint64(cmd.ClientID))
		appendUint64(buf, uint64(cmd.SequenceNum))
		return appendLogCommand(buf, cmd.Command)
	default:
		return fmt.Errorf("unsupported log command type %T", command)
	}
	return nil
}

func appendUint64(buf *[]byte, value uint64) {
	var scratch [8]byte
	binary.BigEndian.PutUint64(scratch[:], value)
	*buf = append(*buf, scratch[:]...)
}

func appendBytes(buf *[]byte, value []byte) {
	appendUint64(buf, uint64(len(value)))
	*buf = append(*buf, value...)
}

func appendString(buf *[]byte, value string) {
	appendBytes(buf, []byte(value))
}

// decodeLogEntry decodes the current binary log-entry format.
func decodeLogEntry(data []byte) (*param.LogEntry, error) {
	if len(data) >= 4 && string(data[:4]) == logEntryFormatMagic {
		return decodeBinaryLogEntry(data)
	}
	return nil, fmt.Errorf("invalid log entry data: missing %s magic", logEntryFormatMagic)
}

func decodeBinaryLogEntry(data []byte) (*param.LogEntry, error) {
	if len(data) < 24 {
		return nil, fmt.Errorf("invalid binary log entry data: too short (%d bytes)", len(data))
	}

	cmdLen := binary.BigEndian.Uint32(data[20:24])
	if uint32(len(data)-24) < cmdLen {
		return nil, fmt.Errorf("invalid binary log entry data: command truncated")
	}

	command, err := decodeLogCommand(data[24 : 24+cmdLen])
	if err != nil {
		return nil, err
	}
	return &param.LogEntry{
		Term:    binary.BigEndian.Uint64(data[4:12]),
		Index:   binary.BigEndian.Uint64(data[12:20]),
		Command: command,
	}, nil
}

func decodeLogCommand(data []byte) (any, error) {
	cursor := logCommandCursor{data: data}
	command, err := cursor.readCommand()
	if err != nil {
		return nil, err
	}
	if cursor.remaining() != 0 {
		return nil, fmt.Errorf("invalid log command data: %d trailing bytes", cursor.remaining())
	}
	return command, nil
}

type logCommandCursor struct {
	data []byte
	off  int
}

func (c *logCommandCursor) remaining() int {
	return len(c.data) - c.off
}

func (c *logCommandCursor) readCommand() (any, error) {
	if c.remaining() < 1 {
		return nil, fmt.Errorf("invalid log command data: missing type")
	}
	commandType := c.data[c.off]
	c.off++

	switch commandType {
	case logCommandNil:
		return nil, nil
	case logCommandBytes:
		return c.readBytes()
	case logCommandString:
		value, err := c.readString()
		if err != nil {
			return nil, err
		}
		return value, nil
	case logCommandKV:
		op, err := c.readUint64()
		if err != nil {
			return nil, err
		}
		key, err := c.readString()
		if err != nil {
			return nil, err
		}
		value, err := c.readString()
		if err != nil {
			return nil, err
		}
		return param.KVCommand{Op: param.OpType(op), Key: key, Value: value}, nil
	case logCommandConfigChange:
		count, err := c.readUint64()
		if err != nil {
			return nil, err
		}
		if count > uint64(c.remaining()/8) {
			return nil, fmt.Errorf("invalid config change command: peer count %d exceeds payload", count)
		}
		peerIDs := make([]int, 0, int(count))
		for i := uint64(0); i < count; i++ {
			peerID, err := c.readUint64()
			if err != nil {
				return nil, err
			}
			peerIDs = append(peerIDs, int(peerID))
		}
		return param.ConfigChangeCommand{NewPeerIDs: peerIDs}, nil
	case logCommandClient:
		clientID, err := c.readUint64()
		if err != nil {
			return nil, err
		}
		sequenceNum, err := c.readUint64()
		if err != nil {
			return nil, err
		}
		nested, err := c.readCommand()
		if err != nil {
			return nil, err
		}
		return param.NewClientCommand(int64(clientID), int64(sequenceNum), nested), nil
	default:
		return nil, fmt.Errorf("unknown log command type %d", commandType)
	}
}

func (c *logCommandCursor) readUint64() (uint64, error) {
	if c.remaining() < 8 {
		return 0, fmt.Errorf("invalid log command data: truncated uint64")
	}
	value := binary.BigEndian.Uint64(c.data[c.off : c.off+8])
	c.off += 8
	return value, nil
}

func (c *logCommandCursor) readBytes() ([]byte, error) {
	length, err := c.readUint64()
	if err != nil {
		return nil, err
	}
	if length > uint64(c.remaining()) {
		return nil, fmt.Errorf("invalid log command data: bytes truncated")
	}
	value := make([]byte, int(length))
	copy(value, c.data[c.off:c.off+int(length)])
	c.off += int(length)
	return value, nil
}

func (c *logCommandCursor) readString() (string, error) {
	value, err := c.readBytes()
	if err != nil {
		return "", err
	}
	return string(value), nil
}

// AppendEntries 追加一批日志条目。
// 优化：只在批量结束时更新元数据，减少写入次数
func (s *StorageAdapter) AppendEntries(entries []param.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range entries {
		data, err := encodeLogEntry(&entry)
		if err != nil {
			return err
		}

		key := s.getLogKey(entry.Index)
		oldData, err := s.db.Get(key)
		if err != nil {
			log.Errorf("[LSMStorage] Get existing entry %d failed before append: %v", entry.Index, err)
			return err
		}
		if oldData != nil {
			s.logSize -= len(oldData)
		}

		if err := s.db.Put(key, data); err != nil {
			log.Errorf("[LSMStorage] Append entry %d failed: %v", entry.Index, err)
			return err
		}
		s.logSize += len(data)

		// 更新 LastIndex
		if entry.Index > s.lastIndex {
			s.lastIndex = entry.Index
		}
	}

	// 批量写入完成后，一次性保存所有 metadata
	if err := s.saveLogMeta(); err != nil {
		return err
	}

	return nil
}

// GetEntry 获取指定索引的日志条目。
func (s *StorageAdapter) GetEntry(index uint64) (*param.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < s.firstIndex || index > s.lastIndex {
		return nil, nil
	}

	val, err := s.db.Get(s.getLogKey(index))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	return decodeLogEntry(val)
}

// TruncateLog 删除从 fromIndex (包含) 到日志末尾的所有条目。
func (s *StorageAdapter) TruncateLog(fromIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if fromIndex < s.firstIndex {
		fromIndex = s.firstIndex
	}
	if fromIndex > s.lastIndex {
		return nil
	}

	// 逐个删除
	for i := fromIndex; i <= s.lastIndex; i++ {
		key := s.getLogKey(i)
		// 获取旧值以更新 size
		val, _ := s.db.Get(key)
		if val != nil {
			s.logSize -= len(val)
		}
		if err := s.db.Delete(key); err != nil {
			return err
		}
	}

	s.lastIndex = fromIndex - 1
	if s.lastIndex < s.firstIndex-1 {
		s.lastIndex = s.firstIndex - 1
	}

	return s.saveMetadata()
}

// FirstLogIndex 返回日志中的第一条条目的索引。
func (s *StorageAdapter) FirstLogIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.firstIndex, nil
}

// LastLogIndex 返回日志中的最后一条条目的索引。
func (s *StorageAdapter) LastLogIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndex, nil
}

// LogSize 返回日志的大小。
func (s *StorageAdapter) LogSize() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.logSize, nil
}

// SaveSnapshot 原子地保存快照数据和元数据。
func (s *StorageAdapter) SaveSnapshot(snapshot *param.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshot); err != nil {
		return err
	}
	if err := s.db.Put(keySnapshot, buf.Bytes()); err != nil {
		log.Errorf("[LSMStorage] SaveSnapshot failed: %v", err)
		return err
	}
	return nil
}

// ReadSnapshot 读取最后保存的快照。
func (s *StorageAdapter) ReadSnapshot() (*param.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, err := s.db.Get(keySnapshot)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	var snapshot param.Snapshot
	if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

// CompactLog 永久性地删除指定索引（包含）之前的所有日志。
func (s *StorageAdapter) CompactLog(upToIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if upToIndex < s.firstIndex {
		return nil
	}

	oldFirstIndex := s.firstIndex
	oldLastIndex := s.lastIndex
	deleteTo := min(upToIndex, oldLastIndex)

	s.firstIndex = upToIndex + 1
	if upToIndex >= s.lastIndex {
		s.lastIndex = upToIndex
		s.logSize = 0
	} else if oldLastIndex >= oldFirstIndex {
		totalEntries := oldLastIndex - oldFirstIndex + 1
		compactedEntries := deleteTo - oldFirstIndex + 1
		if totalEntries > 0 && compactedEntries > 0 {
			compactedBytes := int((int64(s.logSize) * int64(compactedEntries)) / int64(totalEntries))
			s.logSize -= compactedBytes
			if s.logSize < 0 {
				s.logSize = 0
			}
		}
	}

	return s.saveMetadata()
}

// Close 关闭数据库连接。
func (s *StorageAdapter) Close() error {
	return s.db.Close()
}

func (s *StorageAdapter) saveMetadata() error {
	return s.saveLogMeta()
}

func (s *StorageAdapter) saveLogMeta() error {
	data := make([]byte, 24)
	binary.BigEndian.PutUint64(data[0:8], s.firstIndex)
	binary.BigEndian.PutUint64(data[8:16], s.lastIndex)
	binary.BigEndian.PutUint64(data[16:24], uint64(s.logSize))
	return s.db.Put(keyLogMeta, data)
}

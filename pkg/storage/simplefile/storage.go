package simplefile

import (
	"encoding/gob"
	"errors"
	"os"
	"sync"

	"github.com/xmh1011/go-kv/pkg/param"
)

var (
	ErrLogNotFound      = errors.New("log entry not found")
	ErrIndexOutOfBounds = errors.New("index is out of bounds")
)

// Storage implements a simple file-based storage.
// It persists the entire state to a file on every write operation using encoding/gob.
// This is a simple alternative to LSM trees or B-Trees, suitable for small datasets or educational purposes.
type Storage struct {
	mu       sync.RWMutex
	filePath string

	// In-memory cache of the state
	hardState param.HardState
	snapshot  *param.Snapshot
	log       []param.LogEntry
	logOffset uint64
}

// persistentData is the structure used for serialization.
type persistentData struct {
	HardState param.HardState
	Snapshot  *param.Snapshot
	Log       []param.LogEntry
	LogOffset uint64
}

// NewStorage creates a new simple file storage.
func NewStorage(filePath string) (*Storage, error) {
	s := &Storage{
		filePath:  filePath,
		log:       make([]param.LogEntry, 1), // log[0] is dummy
		logOffset: 0,
	}

	if err := s.load(); err != nil {
		// If file does not exist, initialize it
		if os.IsNotExist(err) {
			if err := s.persist(); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return s, nil
}

func (s *Storage) load() error {
	f, err := os.Open(s.filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	var data persistentData
	if err := gob.NewDecoder(f).Decode(&data); err != nil {
		return err
	}

	s.hardState = data.HardState
	s.snapshot = data.Snapshot
	s.log = data.Log
	s.logOffset = data.LogOffset
	return nil
}

func (s *Storage) persist() error {
	data := persistentData{
		HardState: s.hardState,
		Snapshot:  s.snapshot,
		Log:       s.log,
		LogOffset: s.logOffset,
	}

	// Write to temp file and rename for atomicity
	tmpPath := s.filePath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	encoder := gob.NewEncoder(f)
	if err := encoder.Encode(data); err != nil {
		f.Close()
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, s.filePath)
}

// --- HardState Operations ---

func (s *Storage) SetState(state param.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hardState = state
	return s.persist()
}

func (s *Storage) GetState() (param.HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hardState, nil
}

// --- Log Entry Operations ---

func (s *Storage) AppendEntries(entries []param.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, entries...)
	return s.persist()
}

func (s *Storage) GetEntry(index uint64) (*param.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < s.logOffset+1 || index >= s.logOffset+uint64(len(s.log)) {
		return nil, ErrLogNotFound
	}

	return &s.log[index-s.logOffset], nil
}

func (s *Storage) TruncateLog(fromIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if fromIndex < s.logOffset {
		return ErrIndexOutOfBounds
	}
	if fromIndex >= s.logOffset+uint64(len(s.log)) {
		return nil
	}

	s.log = s.log[:fromIndex-s.logOffset]
	return s.persist()
}

// --- Log Metadata Operations ---

func (s *Storage) FirstLogIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.logOffset + 1, nil
}

func (s *Storage) LastLogIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.logOffset + uint64(len(s.log)) - 1, nil
}

func (s *Storage) LogSize() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.log), nil
}

// --- Snapshot Operations ---

func (s *Storage) SaveSnapshot(snapshot *param.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot = snapshot
	return s.persist()
}

func (s *Storage) ReadSnapshot() (*param.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot, nil
}

func (s *Storage) CompactLog(upToIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if upToIndex < s.logOffset {
		return nil
	}

	lastIndex := s.logOffset + uint64(len(s.log)) - 1
	if upToIndex > lastIndex {
		return ErrIndexOutOfBounds
	}

	sliceIndexToKeep := upToIndex - s.logOffset + 1
	newLog := make([]param.LogEntry, 1, 1+len(s.log)-int(sliceIndexToKeep))
	newLog = append(newLog, s.log[sliceIndexToKeep:]...)

	s.log = newLog
	s.logOffset = upToIndex
	return s.persist()
}

func (s *Storage) Close() error {
	return nil
}

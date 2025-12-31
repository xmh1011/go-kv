package sstable

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xmh1011/go-kv/engine/lsm/kv"
	"github.com/xmh1011/go-kv/engine/lsm/sstable/block"
)

func setupTestEnv(t *testing.T) string {
	tempDir, err := os.MkdirTemp("", "sstable_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return tempDir
}

func cleanupTestEnv(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		t.Logf("Warning: Failed to clean up temp dir: %v", err)
	}
}

// createSampleSSTable correctly builds an SSTable in memory for testing purposes.
func createSampleSSTable(level int, rootPath string, pairs []*kv.KeyValuePair) *SSTable {
	table := NewSSTableWithLevel(level, rootPath)
	var currentOffset uint64

	for _, p := range pairs {
		// Manually add to DataBlock
		table.DataBlock.Entries = append(table.DataBlock.Entries, p.Value)

		// Manually add to IndexBlock
		table.IndexBlock.Indexes = append(table.IndexBlock.Indexes, &block.IndexEntry{
			Key:    p.Key,
			Offset: int64(currentOffset),
		})

		// Manually add to FilterBlock
		table.FilterBlock.Add([]byte(p.Key))

		// Calculate next offset: 4 bytes for length prefix + length of value data
		currentOffset += 4 + uint64(len(p.Value))
	}

	// Setup Header
	if len(pairs) > 0 {
		table.Header = &block.Header{
			MinKey: pairs[0].Key,
			MaxKey: pairs[len(pairs)-1].Key,
		}
	} else {
		table.Header = &block.Header{}
	}

	return table
}

func TestNewSSTable(t *testing.T) {
	table := NewSSTable()
	assert.NotZero(t, table.id)
	assert.NotNil(t, table.IndexBlock)
	assert.NotNil(t, table.FilterBlock)
	assert.NotNil(t, table.Footer)
	assert.NotNil(t, table.DataBlock)
}

func TestNewSSTableWithLevel(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	table := NewSSTableWithLevel(1, tempDir)
	assert.Equal(t, 1, table.level)
	assert.NotEmpty(t, table.filePath)
}

func TestEncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		pairs []*kv.KeyValuePair
	}{
		{
			name: "With data",
			pairs: []*kv.KeyValuePair{
				{Key: "key1", Value: []byte("value1")},
				{Key: "key2", Value: []byte("value2")},
			},
		},
		{
			name:  "Empty sstable",
			pairs: []*kv.KeyValuePair{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := setupTestEnv(t)
			defer cleanupTestEnv(t, tempDir)

			// Create and encode
			table := createSampleSSTable(0, tempDir, tt.pairs)
			err := table.EncodeTo(table.filePath)
			assert.NoError(t, err)
			assert.FileExists(t, table.filePath)

			// Decode
			newTable := NewRecoverSSTable(0)
			err = newTable.DecodeFrom(table.filePath)
			assert.NoError(t, err)

			// Verify decoded data
			assert.Equal(t, table.Header.MinKey, newTable.Header.MinKey)
			assert.Equal(t, table.Header.MaxKey, newTable.Header.MaxKey)
			assert.Equal(t, len(table.IndexBlock.Indexes), len(newTable.IndexBlock.Indexes))
			assert.Empty(t, newTable.DataBlock.Entries) // DataBlock should be empty after decoding meta
		})
	}
}

func TestDecodeBlocks(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	pairs := []*kv.KeyValuePair{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
	}
	table := createSampleSSTable(0, tempDir, pairs)
	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)

	file, err := os.Open(table.filePath)
	assert.NoError(t, err)
	defer func(file *os.File) {
		err := file.Close()
		assert.NoError(t, err)
	}(file)

	t.Run("DecodeFooter", func(t *testing.T) {
		newTable := NewRecoverSSTable(0)
		err := newTable.DecodeFooterFrom(file)
		assert.NoError(t, err)
		assert.NotZero(t, newTable.Footer.IndexHandle.Offset)
		assert.NotZero(t, newTable.Footer.IndexHandle.Size)
	})

	t.Run("DecodeDataBlock", func(t *testing.T) {
		newTable := NewRecoverSSTable(0)
		// First decode footer to get data block position
		err := newTable.DecodeFooterFrom(file)
		assert.NoError(t, err)

		// Reset file pointer
		_, err = file.Seek(0, 0)
		assert.NoError(t, err)

		// Decode data block
		err = newTable.DecodeDataBlock(file)
		assert.NoError(t, err)
		assert.Equal(t, len(table.DataBlock.Entries), len(newTable.DataBlock.Entries))
	})
}

func TestGetDataBlockFromFile(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	pairs := []*kv.KeyValuePair{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
	}
	table := createSampleSSTable(0, tempDir, pairs)
	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)

	// Simulate a table where only metadata is loaded
	metaLoadedTable := NewRecoverSSTable(0)
	err = metaLoadedTable.DecodeFrom(table.filePath)
	assert.NoError(t, err)
	assert.Empty(t, metaLoadedTable.DataBlock.Entries)

	// Now get the full data
	loadedPairs, err := metaLoadedTable.GetDataBlockFromFile(table.filePath)
	assert.NoError(t, err)
	assert.Equal(t, len(pairs), len(loadedPairs))
	for i, p := range pairs {
		assert.Equal(t, p.Key, loadedPairs[i].Key)
		assert.Equal(t, p.Value, loadedPairs[i].Value)
	}
}

func TestGetKeyValuePairs(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	tests := []struct {
		name          string
		setup         func() *SSTable
		expectedPairs int
		expectError   bool
	}{
		{
			name: "Normal case",
			setup: func() *SSTable {
				return createSampleSSTable(0, tempDir, []*kv.KeyValuePair{
					{Key: "key1", Value: []byte("value1")},
					{Key: "key2", Value: []byte("value2")},
				})
			},
			expectedPairs: 2,
			expectError:   false,
		},
		{
			name: "Mismatched lengths",
			setup: func() *SSTable {
				table := createSampleSSTable(0, tempDir, []*kv.KeyValuePair{
					{Key: "key1", Value: []byte("value1")},
					{Key: "key2", Value: []byte("value2")},
				})
				table.DataBlock.Entries = table.DataBlock.Entries[:1] // Mismatch
				return table
			},
			expectedPairs: 0,
			expectError:   true,
		},
		{
			name: "Empty case",
			setup: func() *SSTable {
				return createSampleSSTable(0, tempDir, []*kv.KeyValuePair{})
			},
			expectedPairs: 0,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := tt.setup()
			pairs, err := table.GetKeyValuePairs()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, pairs, tt.expectedPairs)
			}
		})
	}
}

func TestGetValueByOffset(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	pairs := []*kv.KeyValuePair{
		{Key: "key1", Value: []byte("value1")},
	}
	table := createSampleSSTable(0, tempDir, pairs)
	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)

	tests := []struct {
		name        string
		offset      int64
		expectValue kv.Value
		expectError bool
	}{
		{
			name:        "Valid Offset",
			offset:      table.IndexBlock.Indexes[0].Offset,
			expectValue: kv.Value("value1"),
			expectError: false,
		},
		{
			name:        "Invalid Offset",
			offset:      999999,
			expectValue: nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := table.GetValueByOffset(tt.offset)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectValue, value)
			}
		})
	}
}

func TestMayContain(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)
	pairs := []*kv.KeyValuePair{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key3", Value: []byte("value3")},
	}
	table := createSampleSSTable(0, tempDir, pairs)

	tests := []struct {
		name     string
		key      kv.Key
		expected bool
	}{
		{
			name:     "Contained Key",
			key:      "key1",
			expected: true,
		},
		{
			name:     "Another Contained Key",
			key:      "key3",
			expected: true,
		},
		{
			name:     "Key within range but not exist",
			key:      "key2",
			expected: false,
		}, // Filter should reject this
		{
			name:     "Key outside range (before)",
			key:      "key0",
			expected: false,
		},
		{
			name:     "Key outside range (after)",
			key:      "key4",
			expected: false,
		},
		{
			name:     "Non-existent key",
			key:      "nonexistent",
			expected: false,
		},
		{
			name:     "Empty key",
			key:      "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, table.MayContain(tt.key))
		})
	}
}

func TestIdAndFilePath(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)
	table := NewSSTableWithLevel(1, tempDir)
	assert.NotZero(t, table.ID())
	expectedPath := filepath.Join(tempDir, "1-level", strconv.FormatUint(table.ID(), 10)+".sst")
	assert.Equal(t, expectedPath, table.FilePath())
}

func TestRemove(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(t *testing.T) *SSTable
		expectError bool
	}{
		{
			name: "Remove existing file",
			setup: func(t *testing.T) *SSTable {
				tempDir := setupTestEnv(t)
				table := createSampleSSTable(0, tempDir, []*kv.KeyValuePair{{Key: "a", Value: []byte("b")}})
				err := table.EncodeTo(table.filePath)
				assert.NoError(t, err)
				assert.FileExists(t, table.filePath)
				return table
			},
			expectError: false,
		},
		{
			name: "Remove non-existing file",
			setup: func(t *testing.T) *SSTable {
				table := NewSSTable()
				table.filePath = "/nonexistent/file.sst"
				return table
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := tt.setup(t)
			err := table.Remove()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NoFileExists(t, table.filePath)
			}
		})
	}
}

func TestPathHelpers(t *testing.T) {
	tests := []struct {
		name     string
		id       uint64
		level    int
		root     string
		expected string
	}{
		{
			name:     "SSTableFilePath",
			id:       123,
			level:    2,
			root:     "/test/path",
			expected: filepath.Join("/test/path", "2-level", "123.sst"),
		},
		{
			name:     "SSTableLevelPath",
			id:       0,
			level:    3,
			root:     "/test/path",
			expected: filepath.Join("/test/path", "3-level"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var path string
			if tt.name == "SSTableFilePath" {
				path = sstableFilePath(tt.id, tt.level, tt.root)
			} else {
				path = sstableLevelPath(tt.level, tt.root)
			}
			assert.Equal(t, tt.expected, path)
		})
	}
}

func TestEncodeDecodeErrors(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	tests := []struct {
		name        string
		setup       func() (string, *SSTable)
		action      func(string, *SSTable) error
		expectError bool
		errorMsg    string
	}{
		{
			name: "Encode to invalid dir",
			setup: func() (string, *SSTable) {
				table := NewSSTableWithLevel(0, "/nonexistent/path")
				table.filePath = "/nonexistent/path/123.sst"
				return table.filePath, table
			},
			action: func(path string, table *SSTable) error {
				return table.EncodeTo(path)
			},
			expectError: true,
			errorMsg:    "create directory failed",
		},
		{
			name: "Decode from non-existent file",
			setup: func() (string, *SSTable) {
				return "/nonexistent/file.sst", NewRecoverSSTable(0)
			},
			action: func(path string, table *SSTable) error {
				return table.DecodeFrom(path)
			},
			expectError: true,
			errorMsg:    "open file error",
		},
		{
			name: "Decode corrupted file",
			setup: func() (string, *SSTable) {
				filePath := filepath.Join(tempDir, "corrupted.sst")
				_ = os.WriteFile(filePath, []byte("invalid data"), 0644)
				return filePath, NewRecoverSSTable(0)
			},
			action: func(path string, table *SSTable) error {
				return table.DecodeFrom(path)
			},
			expectError: true,
			errorMsg:    "decode Header failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, table := tt.setup()
			err := tt.action(path, table)
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConcurrentAccess(t *testing.T) {
	tempDir := setupTestEnv(t)
	defer cleanupTestEnv(t, tempDir)

	pairs := []*kv.KeyValuePair{
		{Key: "key1", Value: []byte("value1")},
	}
	table := createSampleSSTable(0, tempDir, pairs)
	err := table.EncodeTo(table.filePath)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	numGoroutines := 5
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			newTable := NewRecoverSSTable(0)
			err := newTable.DecodeFrom(table.filePath)
			assert.NoError(t, err)
		}()
	}

	wg.Wait()
}

# LSM-tree Storage Design

Chinese version: [LSM.zh-CN.md](LSM.zh-CN.md)

This document explains the LSM-tree engine used by `go-kv`. It is written for
readers who are new to storage engines, so it explains both the data structures
and the reason each module exists.

Related source files:

- `engine/lsm/database`
- `engine/lsm/kv`
- `engine/lsm/memtable`
- `engine/lsm/sstable`
- `engine/lsm/wal`
- `pkg/storage/lsm`

## 1. Why Use An LSM-tree

Raft creates a write-heavy workload. Every committed operation becomes a log
entry, and every node must persist those entries. LSM-trees fit this workload
because they turn many small random writes into sequential writes.

The core idea is simple:

```text
write to WAL
        |
        v
write to in-memory sorted table
        |
        v
flush full memory table to sorted disk file
        |
        v
merge disk files in the background
```

This design is common in systems such as LevelDB and RocksDB.

## 2. Module Layout

| Module | Path | Responsibility |
|---|---|---|
| Database facade | `engine/lsm/database` | Public `Put`, `Get`, `Delete`, recovery, flush coordination. |
| KV encoding | `engine/lsm/kv` | Key/value representation, binary encoding, tombstone marker. |
| WAL | `engine/lsm/wal` | Append-only recovery log for the active memtable. |
| MemTable | `engine/lsm/memtable` | In-memory sorted data using a skiplist. |
| SSTable | `engine/lsm/sstable` | Immutable sorted files, indexes, Bloom filters, compaction. |
| Raft adapter | `pkg/storage/lsm` | Uses LSM as Raft stable storage and state-machine storage. |

## 3. Database Directory Layout

Each LSM database owns a directory:

```text
lsm_database/
├── wal/
│   ├── 1.wal
│   └── 2.wal
└── sst/
    ├── 0-level/
    │   ├── 10.sst
    │   └── 11.sst
    ├── 1-level/
    │   └── 7.sst
    └── 6-level/
```

For an LSM-backed Raft node, there are two databases:

```text
data/node-1/
├── lsm_raftlog/
└── lsm_statemachine/
```

Keeping these separate avoids mixing Raft metadata with user key/value data.

## 4. Data Model

The basic record is:

```go
type Key string
type Value []byte

type KeyValuePair struct {
    Key   Key
    Value Value
}
```

Records are encoded with length prefixes:

```text
KeyLen | KeyBytes | ValueLen | ValueBytes
```

This format is simple, deterministic, and easy to recover from disk.

## 5. Deletes And Tombstones

An LSM-tree does not immediately remove old values. A delete writes a special
tombstone value:

```go
KeyValuePair{Key: key, Value: kv.DeletedValue}
```

Why this matters:

```text
Level 0: delete(k)      newest
Level 2: k = old-value  older
```

If the tombstone disappears too early, `old-value` becomes visible again. The
engine therefore keeps tombstones until compaction reaches a level where older
versions cannot still exist.

## 6. Write Path

The write path for `Put` and `Delete` is:

```text
Database.Put/Delete
        |
        v
MemTableManager.Insert/Delete
        |
        v
active MemTable writes WAL
        |
        v
active MemTable inserts into skiplist
        |
        v
if active MemTable is full:
        promote it to immutable MemTable
        create a new active MemTable
        schedule an old immutable MemTable for flush
```

The WAL write happens before the skiplist update. If the process crashes after
the WAL write, recovery can replay the WAL and rebuild the memtable.

## 7. MemTable And Immutable MemTable

The active MemTable accepts writes. Internally it uses a skiplist so keys remain
sorted and range iteration is cheap.

When the active MemTable grows too large:

1. it becomes an immutable memtable;
2. a new active MemTable starts accepting writes;
3. a flush worker writes an immutable memtable to Level 0 as an SSTable.

Important invariant:

```text
A flushing immutable memtable remains searchable until its SSTable is published.
```

Otherwise a read could miss data that has left the active memtable but has not
yet reached the SSTable metadata.

## 8. WAL

The WAL is an append-only file for one memtable. Each mutation is written to the
WAL before being inserted into the skiplist.

On recovery:

1. Only committed WAL files named `{id}.wal` are selected. Directories, temp
   files such as `3.wal.tmp`, and unrelated files are ignored.
2. The selected WAL files are read by ID order.
3. Older WAL files become immutable memtables.
4. The newest WAL file becomes the active memtable.
5. Tombstones are replayed like normal records.

A file that matches the committed WAL contract but contains corrupt bytes is
still a hard recovery error. Ignoring non-WAL directory entries is a filesystem
hygiene rule; it is not a fallback for damaged committed data.

```go
files = filterWALFiles(files)
sort.Slice(files, func(i, j int) bool {
    return utils.ExtractID(files[i].Name()) < utils.ExtractID(files[j].Name())
})
```

The WAL protects recent writes that have not yet been flushed to SSTables.

## 9. SSTable Format

An SSTable is immutable and sorted by key. The current implementation uses one
logical data area and one index area per file. The physical order on disk is:

```text
+---------+--------------+-------------+-------------+--------+
| Header  | FilterBlock  | Data values | IndexBlock  | Footer |
+---------+--------------+-------------+-------------+--------+
```

The important detail is that the `DataBlock` stores values only. The
`IndexBlock` stores the sorted keys and the byte offsets of their values. A
key/value pair is reconstructed by pairing `IndexBlock[i].Key` with
`DataBlock[i]`.

The Bloom filter quickly answers:

```text
This key is definitely not here
```

or:

```text
This key may be here
```

Bloom filters can have false positives, but not false negatives. A negative
Bloom-filter result means the SSTable can be skipped without opening its data
area.

## 10. Read Path

Reads search from newest data to oldest data:

```text
Database.Get(key)
        |
        v
active MemTable
        |
        v
immutable MemTables, newest first
        |
        v
Level 0 SSTables, newest first
        |
        v
Level 1..N SSTables by sparse index
```

Level 0 files may overlap because they come directly from memtable flushes.
Therefore Level 0 must be searched newest-first.

Level 1 and deeper files are produced by compaction and should have non-overlap
within the same level. That allows faster candidate selection.

## 11. Compaction

Compaction merges SSTables from one level into the next level.

Why compaction exists:

- removes overwritten old versions;
- eventually removes tombstones;
- reduces read amplification;
- keeps level sizes bounded.

The merge rule is:

```text
For the same key, the newest record wins.
```

If the newest record is a tombstone at the maximum level, the engine may drop
the tombstone after it suppresses all older values.

SSTable metadata updates must be atomic from a reader's point of view. Readers
should see either the old set of files or the new set of files, not a half
updated mixture.

Compaction also distinguishes stale metadata from real corruption. If catalog
metadata references a file that no longer exists, the manager prunes that stale
entry and continues. If the file still exists but cannot be decoded, compaction
returns an error. This keeps the engine strict about corrupt data while allowing
self-healing for stale file catalog entries.

## 12. Raft Log Storage Adapter

`pkg/storage/lsm/storage.go` implements `storage.Storage` on top of the LSM
database.

It stores:

- hard state;
- Raft log entries;
- log metadata (`firstIndex`, `lastIndex`, `logSize`);
- snapshots.

Raft log entries use a compact binary format:

```text
GLG1 | term | index | command_length | encoded_command
```

Supported command types are encoded with explicit tags:

- nil command;
- byte slice;
- string;
- KV command;
- config-change command;
- client command wrapper.

Unknown old gob log data is not treated as compatible Raft log data. Missing or
invalid magic fails fast.

## 13. State Machine Adapter

`pkg/storage/lsm/state_machine.go` implements the key-value state machine.

Apply flow:

```text
Raft committed LogEntry
        |
        v
unwrap ClientCommand if present
        |
        v
decode KVCommand
        |
        v
Database.Put/Delete
```

Read flow:

```text
Raft ReadIndex confirms safety
        |
        v
StateMachine.Get(key)
        |
        v
Database.Get(key)
```

State-machine snapshots are encoded as a binary archive of LSM files. The
archive stores filenames and raw bytes with length prefixes.

Snapshot export pins the SSTable files it plans to copy by opening them while
holding the SSTable manager read lock. The exporter then reads from those open
file descriptors. This protects snapshot creation from concurrent compaction:
even if compaction removes a directory entry later, the already-open file
descriptor still points at the bytes selected for the snapshot.

## 14. Recovery

Recovery happens in layers:

1. Recover memtables from WAL.
2. Recover SSTable metadata from disk.
3. Recover Raft hard state and log metadata.
4. Recover the state machine from its LSM directory or from a Raft snapshot.

Important recovery rules:

- Level 0 SSTables must be searched newest-first after recovery.
- Tombstones must be recovered and flushed like normal records.
- Log metadata must hide compacted or truncated indexes.
- A reappended log entry must replace the previous value at that index.

## 15. Configuration

Important LSM settings:

| Config | Meaning |
|---|---|
| `lsm.max_mem_table_size` | Active memtable size before promotion. |
| `lsm.max_sstable_size` | Target SSTable size before builder flush. |
| `lsm.max_imem_table_count` | Number of immutable memtables before flush pressure. |
| `lsm.min_sstable_level` | Lowest SSTable level, usually 0. |
| `lsm.max_sstable_level` | Deepest compaction level. |
| `lsm.level_size_base` | Growth factor for level capacity. |

Small values make tests trigger flush and compaction quickly. Larger values
reduce background work but use more memory and disk before compaction.

## 16. Concrete SSTable Layout

The implementation-level file layout is important because many storage bugs are
caused by confusing metadata with payload data.

```text
offset 0
|
v
Header
FilterBlock
Data values
IndexBlock
Footer                         fixed 32 bytes at the end
```

The main structures are:

| Part | Code | Stored data | Why it exists |
|---|---|---|---|
| `Header` | `engine/lsm/sstable/block/header.go` | minimum key and maximum key | Fast range exclusion and sparse-index ordering. |
| `FilterBlock` | `engine/lsm/sstable/bloom` | Bloom-filter bits | Avoid reading index/data when the key is definitely absent. |
| `DataBlock` | `engine/lsm/sstable/block/data.go` | value bytes only | Keeps value payloads compact. |
| `IndexBlock` | `engine/lsm/sstable/block/index.go` | sorted key plus value offset | Reconstructs key/value pairs and supports seek. |
| `Footer` | `engine/lsm/sstable/block/footer.go` | `DataHandle` and `IndexHandle` | Lets recovery find data and index regions from the end of the file. |

`Footer` is always 32 bytes:

```text
DataHandle.Offset  uint64 little-endian
DataHandle.Size    uint64 little-endian
IndexHandle.Offset uint64 little-endian
IndexHandle.Size   uint64 little-endian
```

`SSTable.DecodeFrom` intentionally loads only metadata: header, filter, footer,
and index. It does not load values. Values are loaded lazily through
`DecodeDataBlock`, which seeks to `Footer.DataHandle.Offset` and decodes exactly
`Footer.DataHandle.Size` bytes.

That lazy load has one crucial rule:

```go
t.DataBlock = block.NewDataBlock()
```

must happen before decoding data values. Without the reset, repeated reads append
new decoded values to the old in-memory slice, so `DataBlock` and `IndexBlock`
eventually have different lengths.

## 17. In-memory SSTable Metadata

`engine/lsm/sstable.Manager` is the owner of the in-memory file catalog. Its
state is protected by `Manager.mu`.

| Field | Meaning |
|---|---|
| `levels [][]*SSTable` | Tables grouped by level. Level 0 is searched newest first. |
| `fileIndex map[string]*SSTable` | Direct lookup by file path. |
| `totalMap map[int][]string` | File paths grouped by level. |
| `sparseIndexes [][]*SSTable` | Level 1 and deeper tables sorted by minimum key. |
| `compactingLevels map[int]bool` | Levels currently being compacted. |

The manager treats metadata publication as the boundary where a flushed SSTable
becomes visible to reads:

```text
immutable memtable
        |
        v
BuildSSTableFromIMemTable
        |
        v
EncodeTo(temp file in the same directory)
        |
        v
fsync + close + rename to final .sst
        |
        v
addTable(sst) publishes metadata under Manager.mu
        |
        v
imem.Clean() deletes the old WAL only after success
```

This is why a flushing immutable memtable must remain searchable until the
SSTable has been encoded and published. If the memtable disappeared before
`addTable`, a reader could miss a key that is between memory and disk.

Recovery also has an ordering rule. Level 0 tables can overlap, so the newest
table must be searched first. `Recover` sorts files by ascending ID and then
uses `addTable`, which inserts at the front. The result after recovery is still
newest-first lookup.

SSTable recovery follows the same committed-file contract as WAL recovery:

- only final `.sst` files with parseable IDs are loaded;
- uncommitted temp files are ignored;
- legacy empty SSTables with no data/index payload are removed;
- a non-empty corrupt `.sst` remains a hard error.

An empty SSTable is not decoded by passing size `0` into the generic
`DataBlock.DecodeFrom` API, because that lower-level API treats size `0` as
"unlimited". The SSTable layer owns the file-format meaning:
`Footer.DataHandle.Size == 0` means there is no data block to read.

`EncodeTo` treats footer handles and per-entry offsets as derived file-layout
metadata. They must be reset before every encode pass:

```go
t.Footer.DataHandle = block.NewHandle(0, 0)
t.Footer.IndexHandle = block.NewHandle(0, 0)
for _, entry := range t.IndexBlock.Indexes {
    entry.Offset = 0
}
```

Without this reset, rewriting the same in-memory SSTable object can serialize
stale footer sizes into a fully published file. Atomic rename protects readers
from partial files; it does not protect them from internally inconsistent
metadata in a completed file.

## 18. Raft Log Keyspace In LSM

The Raft storage adapter stores consensus metadata and log entries as normal LSM
keys. The keyspace is intentionally small and explicit.

| Key | Value format |
|---|---|
| `meta:hard_state` | 24 bytes: `currentTerm`, `votedFor`, `commitIndex`, big-endian uint64 values. |
| `meta:log_meta` | 24 bytes: `firstIndex`, `lastIndex`, `logSize`, big-endian uint64 values. |
| `meta:snapshot` | gob-encoded `param.Snapshot`. |
| `log:00000000000000000001` | binary Raft log entry. |

Log keys are fixed-width strings:

```text
"log:" + zero-padded 20-digit index
```

That preserves numeric ordering in lexicographic storage. A Raft log entry is:

```text
4 bytes   magic "GLG1"
8 bytes   term, big-endian
8 bytes   index, big-endian
4 bytes   command length, big-endian
N bytes   tagged command payload
```

Commands use one-byte tags for nil, bytes, string, key/value commands,
configuration changes, and wrapped client commands. The log decoder requires the
`GLG1` magic; unknown old gob log entries are treated as invalid log data. Raft
snapshots still use gob because they are a separate value under `meta:snapshot`.

The adapter caches `firstIndex`, `lastIndex`, and `logSize` under its own
`StorageAdapter.mu`. These numbers define the logical log window:

```text
firstIndex <= visible log index <= lastIndex
```

`GetEntry` returns nil outside that window even if old physical keys still exist
below the LSM. `AppendEntries`, `TruncateLog`, and `CompactLog` must update this
window consistently with the physical LSM operations.

## 19. Bug-prone Edges And Guardrails

The hardest LSM bugs in this project are not usually caused by the basic skiplist
or file encoding. They come from boundaries between modules:

| Boundary | Failure mode | Guardrail |
|---|---|---|
| Memtable flush | Key disappears while the immutable memtable is being written. | Keep flushing immutable memtables searchable until the SSTable is published. |
| Level 0 recovery | Older table shadows newer data after restart. | Restore newest-first Level 0 lookup order. |
| Tombstone compaction | Deleted key reappears from an older level. | Keep tombstones until older versions are impossible. |
| SSTable lazy data decode | Repeated reads append duplicate decoded values. | Reset `DataBlock` before every decode. |
| Raft truncate/reappend | Old log payload and new log payload share an index. | Replacing an existing log key subtracts old size and writes the new value. |
| Raft compaction | Apply loop asks for a compacted committed entry. | Raft must either skip through a covering snapshot or fail loudly if no snapshot covers it. |
| Compaction catalog cleanup | Metadata references a file that was already removed. | Prune missing-file metadata, but still fail existing corrupt files. |
| WAL recovery hygiene | Temp files or notes exist beside committed WAL files. | Replay only `{id}.wal`; ignore non-WAL entries and fail corrupt committed WALs. |
| SSTable publication | Recovery sees a partially written table. | Publish via temp file, fsync, close, and rename before metadata publication. |
| SSTable rewrite | A reused in-memory table carries stale footer sizes into a new file. | Reset all derived layout metadata before each encode pass. |
| Snapshot apply | Malformed snapshot path clears the local state machine. | Validate every snapshot path before closing or deleting the current DB. |

These guardrails should be mentioned in code reviews. They are correctness
requirements, not performance details.

## 20. Invariants For LSM Changes

When modifying the LSM code, keep these invariants true:

- WAL append happens before mutating the active memtable.
- WAL recovery replays only committed `{id}.wal` files.
- A flushing immutable memtable remains searchable until flush success.
- Level 0 lookup order is newest to oldest.
- Tombstones hide older values until it is safe to drop them.
- SSTable decode resets reusable structures before filling them.
- SSTable publication is temp-file based and metadata is published only after
  the final `.sst` exists.
- SSTable encoding derives footer handles and index offsets from the current
  write pass; stale layout metadata must never be reused.
- Compaction metadata updates are protected by manager locks.
- Missing SSTable metadata can be pruned only after the physical file is
  confirmed absent; existing corrupt files remain hard errors.
- Raft log reads respect the logical `[firstIndex, lastIndex]` window.
- Snapshot export and apply must not race with state-machine writes.
- Snapshot apply must validate the full file manifest before destructive
  replacement starts; invalid paths are hard errors, not skipped files.

Most storage bugs are violations of one of these invariants.

For recent concrete failures and fixes, read
[BUG_FIX_RETROSPECTIVE.md](BUG_FIX_RETROSPECTIVE.md).

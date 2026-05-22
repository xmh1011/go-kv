# go-kv System Design

Chinese version: [DESIGN.zh-CN.md](DESIGN.zh-CN.md)

This document explains the whole `go-kv` system module by module. It is written
for readers who are new to distributed storage systems, so it starts from the
request flow and then drills into each subsystem.

## 1. System Goal

`go-kv` provides a replicated key-value store. A user can write a key on the
cluster leader and later read that key from the leader with linearizable
semantics.

The system solves four problems:

1. **Replication**: every committed write must be copied to enough nodes.
2. **Fault tolerance**: the cluster should keep serving requests if a minority
   of nodes fail.
3. **Persistence**: committed data and Raft metadata must survive process
   restarts.
4. **Compaction**: logs and on-disk data must not grow forever.

## 2. Main Modules

| Module | Path | Responsibility |
|---|---|---|
| Server entry point | `cmd/server` | Loads config, creates storage and transport, starts Raft. |
| Client entry point | `cmd/client` | Parses CLI commands and sends them to the cluster. |
| Client library | `pkg/client` | Retries requests, follows leader hints, assigns client sequence numbers. |
| Configuration | `pkg/config` | Loads YAML and environment overrides into `config.Conf`. |
| Logging | `pkg/log` | Provides leveled logging with a quiet default mode. |
| Shared parameters | `pkg/param` | Defines commands, log entries, snapshots, and RPC messages. |
| Transport | `pkg/transport` | Abstracts gRPC, TCP, and in-memory RPC. |
| Raft | `raft` | Owns leader election, log replication, commit, apply, reads, snapshots. |
| Storage abstraction | `pkg/storage` | Defines stable Raft storage and state-machine interfaces. |
| LSM engine | `engine/lsm` | Provides WAL, memtables, SSTables, Bloom filters, compaction. |
| Tests | `tests` | Exercises real multi-node clusters and performance scenarios. |

## 3. Write Request Flow

A write goes through the full Raft path:

```text
kv-client set k v
        |
        v
pkg/client sends ClientRequest
        |
        v
leader raft.ClientRequest
        |
        v
wrap command as ClientCommand(ClientID, SequenceNum, payload)
        |
        v
append LogEntry to stable storage
        |
        v
replicate AppendEntries to followers
        |
        v
quorum acknowledges the entry
        |
        v
leader advances commitIndex
        |
        v
apply loop applies the command to the state machine
        |
        v
client waiter receives the apply result
```

The important beginner idea is that a write is not complete when the leader
receives it. It is complete only after Raft commits it and the local state
machine applies it.

## 4. Read Request Flow

A read should not return stale data. `go-kv` therefore protects reads with
ReadIndex or lease confirmation:

```text
kv-client get k
        |
        v
leader raft.ClientRequest
        |
        v
confirm this node is still leader
        |
        v
record readIndex = current commitIndex
        |
        v
wait until lastApplied >= readIndex
        |
        v
stateMachine.Get(k)
```

This means the leader must first prove it is still valid and then ensure its
state machine has applied everything that was committed before the read began.

## 5. Server Lifecycle

`cmd/server/main.go` wires the runtime together:

1. Load YAML config through `pkg/config`.
2. Initialize logging.
3. Build the peer map from `raft.peers`.
4. Create storage through `storage.NewStorage`.
5. Create transport through `transport.NewTransport`.
6. Create a `raft.Raft` node.
7. Register the Raft node with the transport.
8. Start the transport listener and Raft event loop.
9. Drain committed entries from `commitChan`.
10. Stop Raft, transport, and storage on process signal.

The server does not directly apply committed entries from `commitChan`. The Raft
module already applies entries before sending commit notifications. The channel
is used for observation and to avoid blocking internal commit delivery.

## 6. Client Module

The CLI in `cmd/client` converts user commands into `param.KVCommand` values:

```go
type KVCommand struct {
    Op    OpType `json:"op"`
    Key   string `json:"key"`
    Value string `json:"value"`
}
```

`pkg/client` is responsible for cluster-facing behavior:

- pick a target node;
- send the request through the selected transport;
- follow `NotLeader` replies and leader hints;
- retry transient failures;
- attach a stable client ID and increasing sequence number.

The sequence number matters because clients may retry after a timeout. Raft can
contain duplicate log entries for a logical request, but the state machine must
apply that logical request only once.

## 7. Configuration And Logging

Configuration is loaded by Viper in `pkg/config`.

Default values are set in code, and YAML files can override them. Environment
variables use the `GO_KV_` prefix and replace dots with underscores:

```bash
GO_KV_LOG_LEVEL=debug
GO_KV_RAFT_READ_INDEX_MODE=heartbeat
```

The default log level is `warn`. High-frequency operational messages, such as
heartbeat progress and compaction progress, should normally stay at `debug`.
Warnings and errors should describe conditions that require attention.

## 8. Transport Module

Raft does not call gRPC or TCP directly. It uses this interface:

```go
type Transport interface {
    Addr() string
    SetPeers(peers map[int]string)
    RegisterRaft(raftInstance api.RaftService)
    Start() error
    Close() error
    SendRequestVote(target string, req *param.RequestVoteArgs, resp *param.RequestVoteReply) error
    SendAppendEntries(target string, req *param.AppendEntriesArgs, resp *param.AppendEntriesReply) error
    SendInstallSnapshot(target string, req *param.InstallSnapshotArgs, resp *param.InstallSnapshotReply) error
    SendClientRequest(target string, req *param.ClientArgs, resp *param.ClientReply) error
}
```

This keeps the Raft logic independent from the wire protocol. Tests can use the
in-memory transport, while local clusters use gRPC by default.

## 9. Raft Module

Raft owns the replicated log. Its main files are:

- `raft/raft.go`: node state, client requests, batching, ReadIndex helpers;
- `raft/election.go`: PreVote and RequestVote handling;
- `raft/replication.go`: AppendEntries, progress tracking, commit/apply;
- `raft/snapshot.go`: local snapshots and InstallSnapshot RPC.

The most important state variables are:

- `currentTerm` and `votedFor`: persisted election state;
- `commitIndex`: highest log index known to be committed;
- `lastApplied`: highest log index applied to the state machine;
- `nextIndex[peer]`: next log index to send to a follower;
- `matchIndex[peer]`: highest log index confirmed on a follower.

Raft persists data through the `storage.Storage` interface. It does not know
whether the implementation is in-memory, file-based, or LSM-backed.

## 10. Storage Abstraction

`pkg/storage/storage.go` defines two important interfaces:

- `Storage`: durable Raft metadata, log entries, and snapshots.
- `StateMachine`: application data that receives committed commands.

When `raft.engine = "lsm"`, each node creates two separate LSM databases:

```text
data/node-1/
├── lsm_raftlog/        # Raft HardState, log entries, snapshots
└── lsm_statemachine/   # user key-value data
```

This separation avoids mixing consensus metadata with user key/value data and
makes snapshot and recovery logic easier to reason about.

## 11. LSM Engine Module

The LSM engine is located under `engine/lsm`.

The write path is:

```text
Database.Put/Delete
        |
        v
MemTable.Insert writes WAL first
        |
        v
insert into skiplist
        |
        v
promote full MemTable to immutable MemTable
        |
        v
flush immutable MemTable to Level-0 SSTable
        |
        v
background compaction moves data to lower levels
```

The read path is newest-to-oldest:

1. active MemTable;
2. immutable MemTables from newest to oldest;
3. Level-0 SSTables from newest to oldest;
4. Level-1 and deeper SSTables by sparse index and Bloom filter.

Deletes are represented as tombstones. A tombstone must hide older values until
compaction can prove that no older value can reappear.

## 12. Persistence And Recovery

Raft needs to recover from process crashes. Recovery uses three layers:

1. **Raft HardState**: current term, vote, and commit index.
2. **Raft log entries**: persisted through the selected storage backend.
3. **State-machine snapshot/data**: the key-value state machine state.

For LSM-backed Raft logs, log entries use a compact binary format with a magic
header. This keeps the hot log path deterministic and avoids gob reflection on
every append/read.

## 13. Snapshots

Snapshots keep Raft logs bounded.

The safe snapshot order is:

1. Raft observes that persisted log size crossed the threshold.
2. Raft captures `lastApplied` and the term at that index.
3. The state machine exports a snapshot for that exact applied state.
4. Raft persists the snapshot.
5. Raft compacts log entries covered by the snapshot.
6. Lagging followers can receive the snapshot through InstallSnapshot.

The key safety rule is that Raft must not compact entries that are not already
represented by a durable state-machine snapshot.

## 14. Testing Strategy

The repository uses several levels of tests:

- package unit tests for data structures and edge cases;
- Raft tests with mocked storage and transport;
- storage tests with real LSM files;
- integration tests for real clusters;
- E2E performance tests;
- long-running E2E tests with restarts, snapshots, and consistency checks.

For day-to-day changes, run:

```bash
make test
make integration-test
```

For storage/Raft changes, also run the focused race suite:

```bash
go test -race ./pkg/storage/lsm ./raft ./engine/lsm/... ./pkg/storage/... ./pkg/param
```

For production-style validation, run long E2E scenarios:

```bash
make long-test
```

## 15. Design Invariants

These invariants are useful when reading or modifying the code:

- `lastApplied` must advance only after the state machine has applied the
  corresponding command.
- Reads must wait for `lastApplied >= readIndex`.
- A client command identified by `(ClientID, SequenceNum)` must apply at most
  once.
- A follower that is behind the compacted log must receive a snapshot instead
  of missing log entries.
- A flushing immutable memtable must remain searchable until its SSTable is
  safely published.
- A tombstone must suppress older values until compaction can safely discard it.
- SSTable metadata updates must be atomic from the reader's point of view.

Keeping these invariants true is more important than micro-optimizing a single
code path.

## 16. Implementation Detail Map

This system design document is intentionally broad. When debugging core storage
or consensus problems, use the implementation-focused design documents:

| Topic | Document | What to read there |
|---|---|---|
| Raft state ownership | [RAFT.md](RAFT.md) | Which fields are paper state, which fields are implementation guardrails, and which lock protects each field. |
| AppendEntries and apply | [RAFT.md](RAFT.md) | The follower AppendEntries phases, leader progress handling, committed-entry apply flow, and ReadIndex wait rules. |
| Snapshot compaction | [RAFT.md](RAFT.md) | The exact lock order for local snapshots and InstallSnapshot. |
| SSTable file format | [LSM.md](LSM.md) | The physical file order, footer layout, lazy DataBlock loading, and index/value pairing. |
| LSM file catalog | [LSM.md](LSM.md) | How Level 0 ordering, sparse indexes, compaction metadata, and recovery interact. |
| Raft log storage in LSM | [LSM.md](LSM.md) | The `meta:*` keys, fixed-width `log:*` keys, and `GLG1` binary log format. |

If an E2E test exposes a consistency bug, start from these maps before changing
code. Most serious failures come from a broken boundary between Raft progress,
snapshot compaction, and LSM visibility.

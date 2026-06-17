# go-kv

`go-kv` is a distributed, fault-tolerant key-value store written in Go. It uses
the Raft consensus algorithm to replicate commands across a cluster and an
LSM-tree storage engine to persist both user data and Raft log data efficiently.

Chinese documentation: [README.zh-CN.md](README.zh-CN.md)

## What This Project Implements

`go-kv` is intentionally small enough to study, but it contains the core pieces
that appear in real distributed databases:

- A replicated Raft cluster with leader election, log replication, snapshots,
  ReadIndex reads, and joint-consensus membership changes.
- A pluggable storage layer with in-memory, simple-file, and LSM-backed
  implementations.
- A custom LSM-tree engine with WAL, memtables, immutable memtables, SSTables,
  Bloom filters, sparse indexes, and compaction.
- A transport abstraction with gRPC, TCP, and in-memory transports.
- End-to-end tests that run real multi-node clusters with gRPC and LSM storage.

## Documentation Map

Start with the high-level design document if you are new to the codebase.

| Topic | English | Chinese |
|---|---|---|
| Overall system design | [docs/DESIGN.md](docs/DESIGN.md) | [docs/DESIGN.zh-CN.md](docs/DESIGN.zh-CN.md) |
| Raft design | [docs/RAFT.md](docs/RAFT.md) | [docs/RAFT.zh-CN.md](docs/RAFT.zh-CN.md) |
| LSM-tree design | [docs/LSM.md](docs/LSM.md) | [docs/LSM.zh-CN.md](docs/LSM.zh-CN.md) |
| Usage guide | [docs/USAGE.md](docs/USAGE.md) | [docs/USAGE.zh-CN.md](docs/USAGE.zh-CN.md) |
| Go SDK reference | [docs/SDK.md](docs/SDK.md) | [docs/SDK.zh-CN.md](docs/SDK.zh-CN.md) |
| Performance report | [docs/PERFORMANCE.md](docs/PERFORMANCE.md) | [docs/PERFORMANCE.zh-CN.md](docs/PERFORMANCE.zh-CN.md) |
| Bug fix retrospective | [docs/BUG_FIX_RETROSPECTIVE.md](docs/BUG_FIX_RETROSPECTIVE.md) | [docs/BUG_FIX_RETROSPECTIVE.zh-CN.md](docs/BUG_FIX_RETROSPECTIVE.zh-CN.md) |

## Architecture At A Glance

The system has five main layers:

```text
CLI client / server
        |
        v
Client command and configuration layer
        |
        v
Raft consensus module
        |
        +--------------------+
        |                    |
        v                    v
Stable Raft storage      State machine
        |                    |
        +---------+----------+
                  v
             LSM engine
```

The server receives client commands, the Raft leader replicates write commands
to a quorum, and committed commands are applied to a key-value state machine.
Reads use ReadIndex or lease-based confirmation so the leader does not return
stale data.

## Repository Layout

```text
go-kv/
├── cmd/
│   ├── client/             # CLI client entry point
│   └── server/             # server entry point and lifecycle wiring
├── conf/                   # example YAML configs for local clusters
├── docs/                   # design docs and performance report
├── engine/lsm/             # custom LSM-tree storage engine
│   ├── database/           # public database facade
│   ├── kv/                 # key/value encoding and tombstones
│   ├── memtable/           # active and immutable memtables
│   ├── sstable/            # SSTable files, Bloom filters, compaction
│   └── wal/                # write-ahead log
├── pkg/
│   ├── client/             # retrying client logic
│   ├── config/             # Viper-based configuration
│   ├── log/                # logging wrapper
│   ├── param/              # shared RPC and command types
│   ├── storage/            # Raft storage and state-machine interfaces
│   └── transport/          # gRPC, TCP, and in-memory transports
├── raft/                   # Raft election, replication, snapshot logic
├── scripts/                # helper scripts
└── tests/                  # integration, E2E, benchmark, and long-running tests
```

## Requirements

- Go 1.25 or newer is recommended.
- `make` for the standard build and test commands.
- `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc` only if you need to
  regenerate gRPC protobuf files.

## Build

```bash
make build
```

This builds:

- `kv-server`
- `kv-client`

## Run A Local 3-Node Cluster

Start the default local cluster:

```bash
make cluster
```

The command starts three server processes using:

- `conf/config-1.yaml`
- `conf/config-2.yaml`
- `conf/config-3.yaml`

Stop the cluster:

```bash
make stop-cluster
```

Clean generated binaries, logs, test artifacts, and local data:

```bash
make clean
```

## Use The CLI Client

The client reads the cluster peer list from `conf/config.yaml` by default.

Set a key:

```bash
./kv-client set mykey "hello world"
```

Get a key:

```bash
./kv-client get mykey
```

Delete a key:

```bash
./kv-client delete mykey
```

Use another config file:

```bash
./kv-client --config conf/config-1.yaml get mykey
```

## Configuration

The main configuration sections are:

```yaml
log:
  level: "warn"

raft:
  id: 1
  transport: "grpc"
  engine: "lsm"
  data_dir: "./data"
  heartbeat_timeout: 50ms
  election_timeout: 200ms
  snapshot_threshold: 8192
  read_index_mode: "lease"
  peers:
    - id: 1
      address: "127.0.0.1:8001"

lsm:
  max_mem_table_size: 2097152
  max_sstable_size: 2097152
```

Environment variables can override configuration keys. For example:

```bash
GO_KV_LOG_LEVEL=debug make e2e-test
```

The default log level is `warn`. Use `debug` when investigating Raft election,
replication, ReadIndex, LSM flush, compaction, or transport behavior.

## Test Commands

Run unit tests outside the `tests/` package:

```bash
make test
```

Run integration tests:

```bash
make integration-test
```

Run the standard E2E performance suite:

```bash
make e2e-test
```

Run long-running E2E tests:

```bash
make long-test
```

Run benchmarks:

```bash
make bench-test
```

## Storage Modes

The Raft storage factory supports three storage modes:

| Mode | Purpose |
|---|---|
| `inmemory` | Fast tests. Data is lost when the process stops. |
| `simplefile` | Small persistent test backend using local files. |
| `lsm` | Main production-like backend using the custom LSM engine. |

When `raft.engine` is `lsm`, each node gets two separate LSM databases:

- one for Raft metadata, log entries, and snapshots;
- one for the key-value state machine.

This separation keeps consensus metadata independent from user data.

## Transport Modes

| Mode | Purpose |
|---|---|
| `grpc` | Default cross-process transport. |
| `tcp` | Alternative RPC transport. |
| `inmemory` | Test-only transport that does not cross process boundaries. |

The standalone CLI client cannot use the `inmemory` transport because it runs in
a different process from the servers.

## Important Consistency Guarantees

- Writes are acknowledged only after the leader appends the command, replicates
  it to a quorum, commits it, and applies it to the state machine.
- Reads are routed through the leader and protected by ReadIndex or lease
  confirmation.
- Client write retries carry `(ClientID, SequenceNum)` metadata so duplicate
  retries do not apply the same state-machine command twice.
- Snapshots compact Raft logs only after the state machine snapshot is durable.
- LSM deletes use tombstones so older values do not reappear during compaction
  or restart.

## Learning Path

If you are new to the project, read the docs in this order:

1. [Overall system design](docs/DESIGN.md)
2. [Raft design](docs/RAFT.md)
3. [LSM-tree design](docs/LSM.md)
4. [Usage guide](docs/USAGE.md)
5. [Go SDK reference](docs/SDK.md)
6. [Performance report](docs/PERFORMANCE.md)
7. [Bug fix retrospective](docs/BUG_FIX_RETROSPECTIVE.md)
8. The tests in `tests/integration_test.go` and `tests/long_running_e2e_test.go`

The tests are often the easiest way to understand how the modules work together.

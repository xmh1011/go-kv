# Usage Guide

Chinese version: [USAGE.zh-CN.md](USAGE.zh-CN.md)

This guide explains how to build, run, test, and release `go-kv`.

## 1. Requirements

- Go 1.25 or newer.
- `make`.
- `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc` only when regenerating
  the gRPC protobuf files.

Install dependencies and verify the module graph:

```bash
make deps
```

## 2. Build

Build the server and client:

```bash
make build
```

The command creates:

- `kv-server`
- `kv-client`

## 3. Run A Local Cluster

Start the default 3-node cluster:

```bash
make cluster
```

This uses:

- `conf/config-1.yaml`
- `conf/config-2.yaml`
- `conf/config-3.yaml`

Each file sets a different `raft.id` and shares the same peer list. The default
transport is gRPC and the default storage engine is LSM.

`make cluster` waits until ports `8001`, `8002`, and `8003` are listening
before reporting success. Per-node logs are written to `raft-node-N.log`; early
process output is captured in `raft-node-N.out`.

Run the full startup and CLI smoke path:

```bash
make cluster-smoke
```

Stop the cluster:

```bash
make stop-cluster
```

Clean local data, logs, binaries, and test artifacts:

```bash
make clean
```

## 4. Use The CLI Client

The client reads peers from `conf/config.yaml` unless `--config` is provided.

Set a key:

```bash
./kv-client set user:1 "alice"
```

Read a key:

```bash
./kv-client get user:1
```

Delete a key:

```bash
./kv-client delete user:1
```

Use another config:

```bash
./kv-client --config conf/config-1.yaml get user:1
```

The client retries on transient transport errors and follows Raft leader hints
from `NotLeader` replies. Each client request carries a stable `ClientID` and a
monotonic `SequenceNum`, so a retried logical request is applied at most once by
the state machine.

## 5. Configuration

Important fields:

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
  peers:
    - id: 1
      address: "127.0.0.1:8001"

lsm:
  max_mem_table_size: 2097152
  max_sstable_size: 2097152
```

`raft.id` must match one entry in `raft.peers`. For local multi-process testing,
each server process should use a different config file with a different
`raft.id`.

Environment variables can override configuration values. For example:

```bash
GO_KV_LOG_LEVEL=debug ./kv-server --config conf/config-1.yaml
```

Use `warn` for normal runs. Use `debug` when investigating Raft election,
replication, ReadIndex, snapshots, LSM flushes, or LSM compaction.

## 6. Storage Modes

| Mode | Use case |
|---|---|
| `inmemory` | Fast tests only. Data is lost when the process stops. |
| `simplefile` | Small persistent test backend. |
| `lsm` | Main production-like backend. |

When `raft.engine` is `lsm`, each node creates two separate LSM databases:

```text
data/node-1/
├── lsm_raftlog/
└── lsm_statemachine/
```

The split keeps consensus metadata separate from user key/value data.

## 7. Test Commands

Run unit tests outside the `tests/` package and generate coverage:

```bash
make test
```

Run integration tests:

```bash
make integration-test
```

Run standard E2E performance tests:

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

For day-to-day PR checks:

```bash
GO_KV_LOG_LEVEL=warn go test -short ./... -count=1 -timeout=15m
make test
```

For Raft or LSM correctness changes, run a focused long E2E case first, then
run the full long E2E regression:

```bash
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=25m ./tests/long_running_e2e_test.go \
  -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' \
  -count=1

GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests/long_running_e2e_test.go \
  -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' \
  -count=1
```

## 8. Troubleshooting

| Symptom | First check |
|---|---|
| Client reports failed command | Confirm the cluster has a leader and the client config contains all peers. |
| Repeated `NotLeader` replies | Check logs for election churn or stale leader hints. |
| Read timeout | Inspect ReadIndex mode and whether `lastApplied` is catching up. |
| Apply timeout | Check leader commit progress, follower replication, and LSM write latency. |
| Snapshot or compaction warning | Check whether the warning is a recovered stale metadata entry or a real file corruption error. |
| CI has no Codecov comment | Ensure `CODECOV_TOKEN` is configured and `coverage.txt` is non-empty. |

## 9. Release Flow

The release workflow publishes a GitHub Release when a tag matching `v*.*.*` is
pushed. It can also be started manually from GitHub Actions for an existing
version tag.

Create a release tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

To publish an existing tag manually, run the `Release` workflow from GitHub
Actions and set:

- `version`: the existing tag to publish, for example `v0.1.0`.
- `prerelease`: whether to mark the GitHub Release as a prerelease.

The workflow runs short tests, builds cross-platform server and client
binaries, generates checksums, and attaches artifacts to a GitHub release.

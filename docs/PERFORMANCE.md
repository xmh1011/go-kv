# Performance And Long-Running E2E Report

Chinese version: [PERFORMANCE.zh-CN.md](PERFORMANCE.zh-CN.md)

This report records the latest production-style validation for `go-kv`. The
goal of these runs is not only throughput measurement. The long-running suites
exercise Raft leader changes, restarts, snapshots, log compaction, LSM flushes,
LSM compaction, client retries, and final data consistency.

## Latest Verification

| Field | Value |
|---|---|
| Date | 2026-06-23 |
| Machine | macOS Darwin 25.5.0, Apple Silicon |
| Go | 1.25.5 |
| Transport | gRPC for long E2E; focused integration also covers TCP |
| Storage | LSM-backed Raft log plus LSM-backed state machine |
| Log level | `GO_KV_LOG_LEVEL=warn` |

## Commands That Passed

| Purpose | Command | Result |
|---|---|---|
| LSM/WAL recovery regression | `GO_KV_LOG_LEVEL=warn go test ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=10m` | Passed |
| LSM compaction scheduling regression | `GO_KV_LOG_LEVEL=warn go test ./engine/lsm/sstable -run '^(TestCreateNewSSTableSkipsCompactionWhenBelowThreshold|TestSSTableManagerOpenFilesSnapshotReleasesManagerLock)$' -count=10 -timeout=2m` | Passed |
| SSTable package stability loop | `GO_KV_LOG_LEVEL=warn go test ./engine/lsm/sstable -count=100 -timeout=5m` | Passed in 114.758s |
| LSM/storage race gate | `GO_KV_LOG_LEVEL=warn go test -race ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=12m` | Passed |
| Focused cluster regression loop | `GO_KV_LOG_LEVEL=warn go test ./tests -run '^(TestCluster_ConcurrentClientRequests|TestCluster_TakeSnapshot|TestCluster_InstallSnapshot|TestCluster_FullClusterRestart|TestCluster_LeaderFailover)$' -count=3 -timeout=12m` | Passed in 527.110s |
| Full short unit/integration gate | `GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m` | Passed; latest `tests` package 977.155s |
| Single 10-minute write-heavy trigger scenario | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=20m ./tests -run '^TestLongRunning_10Min_WriteHeavy$' -count=1` | Passed in 613.049s after async compaction scheduling |
| Full long-running E2E regression | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' -count=1` | Passed in 3657.132s |

The short-mode behavior is now explicit: the 10-minute E2E tests skip when
`testing.Short()` is enabled. That keeps `go test -short ./...` usable for PR
coverage while preserving the real 10-minute scenarios for explicit long-test
runs.

## Final 10-Minute E2E Results

All scenarios ran with the race detector enabled. A successful write is counted
only after the Raft leader commits the entry and the state machine applies it.
Scenarios with restarts also wait for a final cluster barrier before comparing
node data.

| Scenario | Total ops | Failed ops | Throughput | P50 | P95 | P99 | Leader changes | Snapshot nodes | Max snapshot index | Consistency |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Comprehensive | 655,382 | 0 | 1,092.30 ops/s | 3.457458ms | 12.997042ms | 40.629916ms | 4 | 3 | 482,995 | Passed, 1,998 keys |
| WriteHeavy | 512,565 | 0 | 854.27 ops/s | 3.519625ms | 14.7735ms | 44.532042ms | 16 | 3 | 507,421 | Passed, 2,000 keys |
| MixedWithFailures | 948,673 | 0 | 1,581.12 ops/s | 1.426167ms | 4.114709ms | 11.605334ms | 1 | 3 | 659,881 | Passed, 3,600 node-key checks |
| ConsistencyWithRestartsAndSnapshots | 1,299,458 | 0 | 2,165.76 ops/s | 1.554667ms | 3.969875ms | 8.658667ms | 5 | 3 | 908,296 | Passed, 3,600 node-key checks |
| ReadHeavy | 50,159,859 | 0 | 83,599.76 ops/s | 17.25us | 331.375us | 897.375us | 0 | 0 | 0 | Passed, 2,000 keys |
| DeleteStress | 714,117 | 0 | 1,190.19 ops/s | 2.473625ms | 9.325959ms | 25.642416ms | 13 | 3 | 695,088 | Passed, 3,600 node-key checks |

The latest run fixes the previous ReadIndex and apply-timeout regressions
tracked by [issue #113](https://github.com/xmh1011/go-kv/issues/113),
[issue #116](https://github.com/xmh1011/go-kv/issues/116), and
[issue #117](https://github.com/xmh1011/go-kv/issues/117). The important
change for write-heavy stability is that SSTable compaction is no longer run
synchronously in the foreground Raft apply path. MemTable flush still publishes
durable Level-0 SSTables before returning, but compaction is scheduled on a
coalesced background worker and can be joined with `WaitForCompactions()` during
shutdown or tests. Follow-up issue #119 tightened this further: below-threshold
flushes now return without starting a no-op compaction worker, so ordinary small
flushes do not create avoidable goroutines or contend on `Manager.mu`.

## Post-#109 Focused Restart/Snapshot Replay

After fixing SSTable rewrite metadata reset, the restart/snapshot trigger
scenario was rerun under the race detector:

| Field | Value |
|---|---:|
| Duration | 10m0s |
| Total ops | 1,392,428 |
| Failed ops | 0 |
| Throughput | 2,320.71 ops/s |
| P50 | 1.696541ms |
| P95 | 3.19325ms |
| P99 | 7.467875ms |
| Leader changes | 84 |
| Snapshot nodes | 3 |
| Max snapshot index | 974,207 |
| Final barrier | Passed |
| Strict consistency | Passed, 3,600 node-key checks |

This focused replay does not replace the six-scenario full regression above. It
is the latest targeted evidence for the SSTable metadata fix because that bug
affects the LSM file-layout boundary used by snapshot and restart paths.

## Post-#111 Focused Restart/Snapshot Replay

After fixing LSM snapshot path validation, the same restart/snapshot trigger
scenario was rerun under the race detector:

| Field | Value |
|---|---:|
| Duration | 10m0s |
| Total ops | 1,104,337 |
| Failed ops | 0 |
| Throughput | 1,840.56 ops/s |
| P50 | 1.579459ms |
| P95 | 5.621625ms |
| P99 | 16.051875ms |
| Leader changes | 65 |
| Snapshot nodes | 3 |
| Max snapshot index | 759,795 |
| Final barrier | Passed |
| Strict consistency | Passed, 3,600 node-key checks |

This is the latest targeted evidence for the snapshot apply validation fix. The
test exercises normal snapshot export/install and restart behavior; the new
unit regression covers malformed snapshot manifests directly.

## Correctness Gates

The current long-running suites enforce these gates:

- zero failed operations for the latest successful run;
- final cluster barrier after clients stop, so in-flight requests are drained;
- strict node-by-node data comparison after restart-heavy workloads;
- explicit metrics validation so consistency failures cannot be hidden by
  successful request counts;
- no silent fallback for invalid Raft log encoding;
- long E2E scenarios are skipped in short mode instead of running shortened
  pseudo-long tests.

## How To Reproduce

Run one scenario first when debugging a specific failure:

```bash
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=25m ./tests \
  -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' \
  -count=1
```

After a code or test-logic fix, run the full long E2E regression:

```bash
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests \
  -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' \
  -count=1
```

For PR coverage and Codecov:

```bash
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=25m
make test
```

## Current Bottleneck Reading

The latest runs show that write-heavy workloads are stable but still much slower
than read-heavy workloads. That is expected for this architecture because every
write must:

1. enter the leader log;
2. persist through the LSM-backed Raft log;
3. replicate to a quorum;
4. wait for commit;
5. apply to the LSM-backed state machine;
6. become visible to the client.

The latest performance fixes moved LSM compaction out of the foreground flush
path and made background scheduling threshold-gated. The next meaningful
performance work should target batch sizing, follower catch-up, lower
write-amplification in the Raft log adapter, and backpressure metrics for the
background compaction worker. These optimizations should not weaken the
consistency gates above.

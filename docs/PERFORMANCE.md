# Performance And Long-Running E2E Report

Chinese version: [PERFORMANCE.zh-CN.md](PERFORMANCE.zh-CN.md)

This report records the latest production-style validation for `go-kv`. The
goal of these runs is not only throughput measurement. The long-running suites
exercise Raft leader changes, restarts, snapshots, log compaction, LSM flushes,
LSM compaction, client retries, and final data consistency.

## Latest Verification

| Field | Value |
|---|---|
| Date | 2026-06-22 |
| Machine | macOS Darwin 26.3.1, Apple Silicon |
| Go | 1.25.5 |
| Transport | gRPC for long E2E; focused integration also covers TCP |
| Storage | LSM-backed Raft log plus LSM-backed state machine |
| Log level | `GO_KV_LOG_LEVEL=warn` |

## Commands That Passed

| Purpose | Command | Result |
|---|---|---|
| LSM/WAL recovery regression | `GO_KV_LOG_LEVEL=warn go test ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=5m` | Passed |
| Full short unit/integration gate | `GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=25m` | Passed; latest `tests` package 776.861s |
| Single 10-minute restart/snapshot trigger scenario | `GO_KV_LOG_LEVEL=warn go test -race -v ./tests -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' -count=1 -timeout=25m` | Latest post-#111 run passed in 607.722s |
| Full long-running E2E regression | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' -count=1` | Latest run passed in 3684.450s |

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
| Comprehensive | 502,428 | 0 | 837.38 ops/s | 3.5285ms | 25.564834ms | 66.73125ms | 23 | 3 | 371,551 | Passed, 1,994 keys |
| WriteHeavy | 326,298 | 0 | 543.83 ops/s | 3.841292ms | 24.972667ms | 70.632083ms | 35 | 3 | 326,489 | Passed, 2,000 keys |
| MixedWithFailures | 464,357 | 0 | 773.93 ops/s | 2.070792ms | 13.270042ms | 35.819375ms | 31 | 3 | 311,698 | Passed, 3,600 node-key checks |
| ConsistencyWithRestartsAndSnapshots | 681,847 | 0 | 1,136.41 ops/s | 2.518375ms | 10.924792ms | 28.547208ms | 41 | 3 | 462,404 | Passed, 3,600 node-key checks |
| ReadHeavy | 16,729,989 | 0 | 27,883.31 ops/s | 29.5us | 467.625us | 2.115667ms | 0 | 0 | 0 | Passed, 2,000 keys |
| DeleteStress | 539,492 | 0 | 899.15 ops/s | 2.93875ms | 11.89325ms | 29.039792ms | 58 | 3 | 536,558 | Passed, 3,600 node-key checks |

The latest full run preserved all correctness gates, but it also exposed a
performance and ReadIndex availability signal now tracked in
[issue #113](https://github.com/xmh1011/go-kv/issues/113). Several scenarios
emitted ReadIndex timeout warnings while still completing with zero failed
operations:

```text
[ReadIndex] Node 3 timed out waiting for heartbeat quorum.
[ReadIndex] Node 3 timed out waiting for lastApplied to reach 234016 (current: 233759)
[ReadIndex] Node 2 timed out waiting for heartbeat quorum.
[ReadIndex] Node 1 timed out waiting for heartbeat quorum.
```

The request streams still completed with zero failed operations, and final
consistency passed. The warning is useful performance signal: write-heavy
compaction, snapshot pressure, or ReadIndex heartbeat scheduling can temporarily
delay linearizable reads. Compared with the previous report, WriteHeavy and
ReadHeavy throughput are materially lower, so #113 should be treated as a
follow-up performance bug rather than a correctness failure.

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

The next meaningful performance work should target batch sizing, follower catch
up, LSM compaction scheduling, and lower write-amplification in the Raft log
adapter. These optimizations should not weaken the consistency gates above.

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
| Full short unit/integration gate | `GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=25m` | Passed; latest `tests` package 774.370s |
| Single 10-minute restart/snapshot trigger scenario | `GO_KV_LOG_LEVEL=warn go test -race -v ./tests -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' -count=1 -timeout=25m` | Latest post-#109 run passed in 606.997s |
| Full long-running E2E regression | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' -count=1` | Passed in 3672.630s |

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
| Comprehensive | 741,998 | 0 | 1,236.66 ops/s | 2.947709ms | 13.335041ms | 29.132709ms | 34 | 3 | 557,264 | Passed, 1,994 keys |
| WriteHeavy | 651,910 | 0 | 1,086.52 ops/s | 2.319708ms | 6.559708ms | 14.27275ms | 76 | 3 | 634,129 | Passed, 2,000 keys |
| MixedWithFailures | 667,024 | 0 | 1,111.71 ops/s | 1.45125ms | 6.477083ms | 19.660041ms | 34 | 3 | 467,029 | Passed, 3,600 node-key checks |
| ConsistencyWithRestartsAndSnapshots | 649,820 | 0 | 1,083.03 ops/s | 2.218459ms | 12.986833ms | 35.574416ms | 44 | 3 | 445,972 | Passed, 3,600 node-key checks |
| ReadHeavy | 29,051,853 | 0 | 48,419.75 ops/s | 24.667us | 472.458us | 1.695083ms | 0 | 0 | 0 | Passed, 2,000 keys |
| DeleteStress | 397,732 | 0 | 662.89 ops/s | 4.177875ms | 20.314792ms | 58.492375ms | 30 | 3 | 397,665 | Passed, 3,600 node-key checks |

`Comprehensive` emitted one ReadIndex wait warning while the cluster was under
snapshot and compaction load:

```text
[ReadIndex] Node 1 timed out waiting for lastApplied to reach 446053 (current: 445844)
```

The request stream still completed with zero failed operations, and final
consistency passed. The warning is useful performance signal: write-heavy
compaction or snapshot pressure can temporarily delay `lastApplied` catch-up,
but the latest run did not expose data loss or divergence.

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

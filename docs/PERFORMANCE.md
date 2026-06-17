# Performance And Long-Running E2E Report

Chinese version: [PERFORMANCE.zh-CN.md](PERFORMANCE.zh-CN.md)

This report records the latest production-style validation for `go-kv`. The
goal of these runs is not only throughput measurement. The long-running suites
exercise Raft leader changes, restarts, snapshots, log compaction, LSM flushes,
LSM compaction, client retries, and final data consistency.

## Latest Verification

| Field | Value |
|---|---|
| Date | 2026-06-17 |
| Machine | macOS Darwin 26.3.1, Apple Silicon |
| Go | 1.25.5 |
| Transport | gRPC for long E2E; focused integration also covers TCP |
| Storage | LSM-backed Raft log plus LSM-backed state machine |
| Log level | `GO_KV_LOG_LEVEL=warn` |

## Commands That Passed

| Purpose | Command | Result |
|---|---|---|
| Focused TCP/LSM restart regression | `GO_KV_LOG_LEVEL=warn go test -v ./tests -run '^TestCluster_Persistence_Restart$/^tcp_lsm$' -count=10 -timeout=3m` | Passed in 40.689s |
| Full short unit/integration gate | `GO_KV_LOG_LEVEL=warn go test -short ./... -count=1 -timeout=15m` | Passed in 772.798s |
| Single 10-minute trigger scenario | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=25m ./tests/long_running_e2e_test.go -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' -count=1` | Passed in 608.749s |
| Full long-running E2E regression | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests/long_running_e2e_test.go -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' -count=1` | Passed in 3688.472s |

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
| Comprehensive | 1,187,811 | 0 | 1,979.68 ops/s | 2.079625ms | 4.845958ms | 11.733209ms | 33 | 3 | 891,649 | Passed, 1,996 keys |
| WriteHeavy | 711,886 | 0 | 1,186.48 ops/s | 1.9855ms | 5.132084ms | 11.577292ms | 57 | 3 | 706,490 | Passed, 2,000 keys |
| MixedWithFailures | 998,188 | 0 | 1,663.65 ops/s | 1.276875ms | 3.291916ms | 7.843792ms | 39 | 3 | 698,422 | Passed, 3,600 node-key checks |
| ConsistencyWithRestartsAndSnapshots | 1,304,041 | 0 | 2,173.40 ops/s | 1.468917ms | 3.905167ms | 8.690958ms | 72 | 3 | 908,358 | Passed, 3,600 node-key checks |
| ReadHeavy | 61,771,824 | 0 | 102,953.04 ops/s | 15.542us | 327.333us | 681.125us | 0 | 0 | 0 | Passed, 2,000 keys |
| DeleteStress | 714,646 | 0 | 1,191.08 ops/s | 2.107375ms | 5.763834ms | 13.462959ms | 50 | 3 | 714,968 | Passed, 3,600 node-key checks |

`MixedWithFailures` emitted one warning from the new LSM compaction recovery
guard:

```text
[Compaction] Pruning stale SSTable metadata for missing file .../node-2/lsm_raftlog/sst/1-level/515.sst at level 1
```

That warning is expected for the repaired path. The metadata entry was stale,
the physical file was already gone, and compaction pruned the catalog entry
instead of failing the whole storage engine. The scenario continued and passed
strict final consistency.

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
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=25m ./tests/long_running_e2e_test.go \
  -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' \
  -count=1
```

After a code or test-logic fix, run the full long E2E regression:

```bash
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests/long_running_e2e_test.go \
  -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' \
  -count=1
```

For PR coverage and Codecov:

```bash
GO_KV_LOG_LEVEL=warn go test -short ./... -count=1 -timeout=15m
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

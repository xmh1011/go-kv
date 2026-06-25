# Performance And Long-Running E2E Report

Chinese version: [PERFORMANCE.zh-CN.md](PERFORMANCE.zh-CN.md)

This report records the latest production-style validation for `go-kv`. The
goal of these runs is not only throughput measurement. The long-running suites
exercise Raft leader changes, restarts, snapshots, log compaction, LSM flushes,
LSM compaction, client retries, and final data consistency.

## Latest Verification

| Field | Value |
|---|---|
| Date | 2026-06-25 |
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
| LSM snapshot reload race regression | `GO_KV_LOG_LEVEL=warn go test -race -run '^TestApplySnapshotDoesNotRaceWithConcurrentReads$' ./pkg/storage/lsm -count=1` | Passed after reproducing the pre-fix `Database.Reload` / `Database.Get` race |
| LSM snapshot reload package race gate | `GO_KV_LOG_LEVEL=warn go test -race ./engine/lsm/... ./pkg/storage/lsm -count=1` | Passed after the database lifecycle lock and atomic state-machine replacement fix |
| LSM-backed Raft log physical compaction regression | `GO_KV_LOG_LEVEL=warn go test ./pkg/storage/lsm -run '^(TestStorageAdapterCompactLogDeletesPhysicalLogKeys|TestStorageAdapter_Snapshot|TestStorageAdapter_CompactBeyondLastIndexFromSnapshot|TestStorageAdapter_LogEntries|TestStorageAdapter_ReappendAfterTruncateSurvivesFlushCompactionAndRestart)$' -count=1 -timeout=5m` | Passed in 2.753s |
| LSM package regression after physical log tombstones | `GO_KV_LOG_LEVEL=warn go test ./pkg/storage/lsm ./engine/lsm/... -count=1 -timeout=12m` | Passed |
| LSM/storage race gate after physical log tombstones | `GO_KV_LOG_LEVEL=warn go test -race ./pkg/storage/lsm ./engine/lsm/... -count=1 -timeout=12m` | Passed; slowest package `engine/lsm/database` 33.343s |
| Snapshot/restart cluster loop after physical log tombstones | `GO_KV_LOG_LEVEL=warn go test ./tests -run '^(TestCluster_TakeSnapshot|TestCluster_InstallSnapshot|TestCluster_FullClusterRestart)$' -count=3 -timeout=12m` | Passed in 301.753s |
| Deterministic waitForAppliedLog timeout-recheck regression | `GO_KV_LOG_LEVEL=warn go test -race ./raft -run '^TestWaitForAppliedLogRechecksLastAppliedOnTimeout$' -count=100 -timeout=3m` | Passed in 11.291s |
| Raft package race gate after deterministic test fix | `GO_KV_LOG_LEVEL=warn go test -race ./raft -count=1 -timeout=8m` | Passed in 14.295s |
| Race-load leader discovery regression | `GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_MembershipChange$/^grpc_simplefile$' -count=5 -timeout=12m` | Passed in 71.951s |
| Full membership-change matrix after leader discovery fix | `GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_MembershipChange$' -count=1 -timeout=15m` | Passed in 72.375s |
| Race-load network partition leader detection regression | `GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_NetworkPartition$/^tcp_inmemory$' -count=5 -timeout=12m` | Passed in 61.821s |
| Full network-partition matrix after candidate-scoped leader discovery | `GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_NetworkPartition$' -count=1 -timeout=15m` | Passed in 68.575s |
| Focused cluster regression loop | `GO_KV_LOG_LEVEL=warn go test ./tests -run '^(TestCluster_ConcurrentClientRequests|TestCluster_TakeSnapshot|TestCluster_InstallSnapshot|TestCluster_FullClusterRestart|TestCluster_LeaderFailover)$' -count=3 -timeout=12m` | Passed in 527.110s |
| Full short unit/integration gate | `GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m` | Passed; latest `tests` package 1005.048s after issues #122, #123, and #124 |
| Single 10-minute write-heavy trigger scenario | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=20m ./tests -run '^TestLongRunning_10Min_WriteHeavy$' -count=1` | Passed in 613.049s after async compaction scheduling |
| Post-#121 10-minute restart/snapshot consistency scenario | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=25m ./tests -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' -count=1` | Passed in 611.921s with 0 failed operations |
| Mixed-failure issued-request retry regression | `GO_KV_LOG_LEVEL=warn go test -race -v -timeout=20m ./tests -run '^TestLongRunning_10Min_MixedWithFailures$' -count=1` | Passed in 619.602s with 666,692 operations, 0 failures, final barrier true, and strict consistency true |
| gRPC InstallSnapshot term regression | `GO_KV_LOG_LEVEL=warn go test -race ./pkg/transport/grpc -run 'TestSendInstallSnapshot|TestInstallSnapshotStream' -count=1` | Passed after adding a regression for follower higher-term snapshot replies |
| Post-#164 mixed-failure replay | `GO_KV_LOG_LEVEL=warn go test -race -v ./tests -run '^TestLongRunning_10Min_MixedWithFailures$' -count=1 -timeout=15m` | Passed in 606.751s with 633,195 operations, 0 failures, final barrier true, and strict consistency true |
| Static and unit gates after #164 | `/Users/xiaominghao/go/bin/staticcheck ./...`, `~/go/bin/errcheck -ignoretests ./...`, `go vet ./...`, `GO_KV_LOG_LEVEL=warn make test` | Passed |
| Integration regression after #164 | `GO_KV_LOG_LEVEL=warn make integration-test` | Passed in 512.324s |
| End-to-end regression after #164 | `GO_KV_LOG_LEVEL=warn make e2e-test` | Passed in 455.655s |
| WAL torn-tail regression | `GO_KV_LOG_LEVEL=warn go test ./engine/lsm/wal -run '^TestRecoverTruncatesTornTailAfterValidRecords$' -count=1` | Failed before #166 with `decode key: unexpected EOF`; passed after truncating incomplete WAL tails |
| SSTable non-blocking compaction test stability | `GO_KV_LOG_LEVEL=warn go test ./engine/lsm/sstable -run '^TestCreateNewSSTableDoesNotBlockBehindCompaction$' -count=100 -timeout=5m` | Passed after #168 replaced the fixed 100ms completion deadline with condition-based publication checks |
| LSM/WAL race gate after #166 | `GO_KV_LOG_LEVEL=warn go test -race ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=15m` | Passed; slowest package `engine/lsm/database` 10.065s |
| Raft race/shuffle probe after #166 | `GO_KV_LOG_LEVEL=warn go test -race -shuffle=on ./raft -count=50 -timeout=40m` | Passed in 659.257s |
| Static and unit gates after #166 | `/Users/xiaominghao/go/bin/staticcheck ./...`, `~/go/bin/errcheck -ignoretests ./...`, `go vet ./...`, `GO_KV_LOG_LEVEL=warn make test` | Passed |
| Integration regression after #166 | `GO_KV_LOG_LEVEL=warn make integration-test` | Passed in 506.003s |
| End-to-end regression after #166 | `GO_KV_LOG_LEVEL=warn make e2e-test` | Passed in 452.782s |
| Full long-running E2E regression | `GO_KV_LOG_LEVEL=warn make long-test` | Passed in 3656.928s across all six 10-minute scenarios |

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
| Comprehensive | 1,250,938 | 0 | 2,084.90 ops/s | 2.00875ms | 4.304916ms | 11.241458ms | 0 | 3 | 928,693 | Passed, 1,996 keys |
| WriteHeavy | 866,100 | 0 | 1,443.50 ops/s | 2.046125ms | 5.075375ms | 11.049291ms | 14 | 3 | 850,768 | Passed, 2,000 keys |
| MixedWithFailures | 1,136,462 | 0 | 1,894.10 ops/s | 1.26675ms | 2.13075ms | 4.634417ms | 1 | 3 | 794,687 | Passed, final barrier true, 3,600 node-key checks |
| ConsistencyWithRestartsAndSnapshots | 1,613,118 | 0 | 2,688.53 ops/s | 1.407208ms | 2.084625ms | 3.763667ms | 2 | 3 | 1,122,910 | Passed, final barrier true, 3,600 node-key checks |
| ReadHeavy | 61,731,897 | 0 | 102,886.49 ops/s | 16.625us | 294.25us | 710us | 0 | 0 | 0 | Passed, 2,000 keys |
| DeleteStress | 859,815 | 0 | 1,433.03 ops/s | 1.98525ms | 4.57375ms | 11.266458ms | 12 | 3 | 853,481 | Passed, final barrier true, 3,600 node-key checks |

The latest run validates the previous ReadIndex and apply-timeout fixes tracked
by [issue #113](https://github.com/xmh1011/go-kv/issues/113),
[issue #116](https://github.com/xmh1011/go-kv/issues/116), and
[issue #117](https://github.com/xmh1011/go-kv/issues/117),
[issue #150](https://github.com/xmh1011/go-kv/issues/150), and
[issue #151](https://github.com/xmh1011/go-kv/issues/151), and
[issue #164](https://github.com/xmh1011/go-kv/issues/164), and it validates the
WAL recovery boundary tracked by
[issue #166](https://github.com/xmh1011/go-kv/issues/166). The important change
for write-heavy stability is that SSTable compaction is no longer run
synchronously in the foreground Raft apply path. MemTable flush still publishes
durable Level-0 SSTables before returning, but compaction is scheduled on a
coalesced background worker and can be joined with `WaitForCompactions()` during
shutdown or tests. Follow-up issue #119 tightened this further: below-threshold
flushes now return without starting a no-op compaction worker, so ordinary small
flushes do not create avoidable goroutines or contend on `Manager.mu`.
Follow-up issue #121 tightened snapshot-driven Raft log compaction: `CompactLog`
now tombstones compacted physical `log:<index>` keys before advancing the
logical window, so long-running nodes can reclaim obsolete log payloads through
normal LSM compaction. Issue #122 fixed a race-mode test precondition in
`TestWaitForAppliedLogRechecksLastAppliedOnTimeout`: the test now waits for the
apply waiter to register before setting `lastApplied`, so it verifies the
timeout-path recheck instead of depending on a short scheduler race. Issue #123
fixed the integration leader-discovery helper so race-mode membership tests scan
for local leader candidates before issuing ReadIndex probes, avoiding slow
serial probes against every follower. Issue #124 reused that helper inside the
network-partition test's majority partition, removing a second hand-written
fixed-sleep probe loop.

Issue #150 fixed the LSM state-machine snapshot replacement boundary. Snapshot
apply can close and replace the database directory, so `Database.Get`, `Put`,
`Delete`, `Recover`, `ForceFlush`, `Reload`, and `Close` now share a database
lifecycle lock. Snapshot generation also opens a pinned SSTable snapshot before
copying bytes. The result is that concurrent reads cannot observe a half-closed
database while a Raft InstallSnapshot path replaces local state.

Issue #151 fixed the mixed-failure long-test harness. Already-issued commands
use stable `(ClientID, SequenceNum)` identity and are safe to retry, but the old
30-second issued-request retry window could expire while the cluster was still
recovering from leader re-election and snapshot catch-up. The window is now a
bounded 90 seconds: stuck commands still fail, but normal Raft recovery no
longer appears as a false failed operation after the final barrier succeeds.

Issue #164 fixed the gRPC streaming InstallSnapshot reply boundary. The Raft
RPC contract requires every reply to carry the follower's current term so a
leader can step down if it is stale. The streaming transport previously
installed the snapshot correctly but returned the request term to the caller,
which meant `processSnapshotReply` could treat a higher-term follower as a
successful same-term snapshot ACK. The transport now writes the follower term
into a gRPC trailer after snapshot installation and `SendInstallSnapshot`
propagates that term into `InstallSnapshotReply`.

Issue #166 fixed the LSM WAL recovery boundary. Recovery previously decoded
records until EOF and treated any decode error as fatal. A crash or interrupted
write can leave an incomplete final WAL record, so the correct invariant is to
replay every complete prefix record, truncate only the torn tail, and keep
structural corruption such as impossible length fields fatal. `Recover` now
tracks the last complete record offset, truncates incomplete tails to that
offset, seeks the writable WAL handle back to EOF, and continues to reject
non-tail corruption.

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

## Post-#121 Focused Physical Log Compaction Validation

Issue #121 did not appear as a user-visible consistency failure because
`GetEntry` correctly hid compacted indexes after `firstIndex` moved forward. The
failure was lower in the storage stack: old physical `log:<index>` keys remained
in the LSM tree and had no tombstones, so normal LSM compaction could not reclaim
their payload bytes.

The focused validation therefore checks both layers:

| Check | Signal |
|---|---|
| Direct physical-key regression | `TestStorageAdapterCompactLogDeletesPhysicalLogKeys` confirms compacted keys return `nil` through the raw LSM lookup path while the first retained key still exists. |
| Snapshot/log adapter compatibility | Existing snapshot, compact-beyond-last-index, log-entry, and truncate/reappend tests still pass with the new tombstone writes. |
| LSM package regression | `./pkg/storage/lsm ./engine/lsm/...` passes after the change, covering flush, compaction, WAL recovery, SSTable reads, and restart-adjacent storage behavior. |
| Snapshot/restart integration | `TestCluster_TakeSnapshot`, `TestCluster_InstallSnapshot`, and `TestCluster_FullClusterRestart` pass with `-count=3`, proving the new physical deletions do not break snapshot creation, snapshot install, or durable restart recovery. |

This validation is intentionally separate from throughput numbers. The fix adds
one tombstone write per compacted Raft log key, which is a correctness tradeoff:
the logical log window and physical keyspace must agree before performance work
can safely optimize range deletion or batching.

The targeted 10-minute restart/snapshot E2E replay after #121 produced:

| Field | Value |
|---|---:|
| Duration | 10m0s |
| Total ops | 518,144 |
| Failed ops | 0 |
| Throughput | 863.57 ops/s |
| P50 | 2.64ms |
| P95 | 16.902334ms |
| P99 | 40.707ms |
| Leader changes | 5 |
| Restart count | 3 |
| Snapshot nodes | 3 |
| Max snapshot index | 363,305 |
| Final barrier | Passed |
| Strict consistency | Passed, 3,600 node-key checks |

This replay directly exercises the fixed `CompactLog` path because snapshots
advance the Raft log window while restarts force persisted LSM state to be
reopened.

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

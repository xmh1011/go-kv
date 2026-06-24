# Bug Fix Retrospective: Raft And LSM Long E2E Failures

Chinese version: [BUG_FIX_RETROSPECTIVE.zh-CN.md](BUG_FIX_RETROSPECTIVE.zh-CN.md)

This document explains the main bugs found by the long-running E2E campaign and
the kernel-level principles behind the fixes. It is intentionally practical:
each section describes the symptom, root cause, affected invariant, fix design,
and test signal.

Related issues:

- #88 Long comprehensive E2E hides ReadIndex apply timeouts
- #89 Restart and snapshot long E2E exposes apply timeout
- #90 Restart and snapshot E2E can diverge final values after successful writes
- #91 Drain in-flight long E2E operations before stopping clients
- #92 Refresh election last-log metadata across snapshot compaction
- #93 Keep LSM compaction metadata consistent when SSTable files disappear
- #94 Skip long-running E2E scenarios in short mode
- #95 Preserve LSM data across TCP leader restart
- #113 Investigate ReadIndex timeouts and long E2E throughput regression
- #115 Concurrent gRPC LSM writes can fail and leave missing keys
- #116 Long mixed-failure E2E exhausts retries during leader changes
- #117 Mixed-failure long E2E can return apply timeouts despite final consistency
- #119 Avoid unnecessary LSM compaction scheduling below threshold
- #121 LSM CompactLog leaves compacted log keys on disk
- #122 Make waitForAppliedLog timeout recheck test deterministic
- #123 Make integration leader discovery reliable under race load
- #124 Make network partition leader detection race-load safe
- #142 Bound mixed workload benchmark concurrency
- #143 Close LSM benchmark databases
- #145 Stabilize benchmark leader readiness
- #146 Propagate benchmark test failures
- #150 Prevent LSM snapshot reload races
- #151 Extend mixed-failure issued-request retry budget

## 1. The Debugging Rule That Changed The Result

The early tests counted successful RPCs, but that was not enough. A replicated
database can accept many requests while still hiding a deeper failure:

- reads may time out while writes look healthy;
- a client can stop after sending a request but before observing the result;
- a follower can be slightly behind when a fixed sleep ends;
- compaction can leave stale metadata that only appears under long churn;
- a leader can commit an entry but delay the heartbeat that carries the new
  commit index to followers.

The long E2E tests now use three correctness gates:

1. stop clients only before issuing a new request, not while a request is
   already in flight;
2. wait for a final cluster barrier after clients stop;
3. compare node data after restarts and snapshots.

This changed the tests from throughput samples into consistency tests.

## 2. Raft Principle: A New Leader Must Commit An Entry In Its Own Term

Raft leaders cannot safely commit old-term entries only by counting replicas.
The Raft paper requires a leader to commit at least one entry from its own term.
After that, older entries before it become committed indirectly.

The fix adds a no-op command when a node becomes leader:

```go
func (r *Raft) transitionToLeader() {
    ...
    r.proposeLeaderNoopLocked()
}
```

The no-op is not a user write. It is a leadership barrier. It gives the new
leader a current-term log entry, which helps unblock commit/apply progress after
leader changes and restarts.

Affected issues:

- #89 exposed apply timeouts during restart/snapshot churn.
- #90 exposed final value divergence when successful writes and leader changes
  overlapped.

## 3. Raft Principle: Commit Advancement Must Wake Replication

An AppendEntries RPC has two pieces of useful information:

- entries, which copy log data to the follower;
- `LeaderCommit`, which tells the follower what is committed.

Before the fix, a successful AppendEntries response could advance the leader
commit index, but the follower that just responded might not receive the updated
`LeaderCommit` until a later heartbeat. Under load, that delay amplified apply
timeouts.

The replication path now treats commit advancement as a signal:

```go
advanced := r.updateCommitIndex()
if advanced {
    r.notifyApply()
}
```

After commit advances, the leader schedules more replication work so followers
receive the latest commit index promptly.

Affected issues:

- #88 made ReadIndex apply timeouts visible instead of hiding them.
- #89 and #90 relied on faster propagation of commit progress.

## 4. Raft Principle: Same-Term Leader Hints Must Still Demote Stale Roles

Raft terms order leadership. A node that learns about a valid leader in its
current term must stop acting as candidate or stale leader. A higher term is not
the only demotion trigger.

The fix centralizes current-term leader acceptance:

```go
func (r *Raft) acceptLeaderForCurrentTermLocked(leaderID int) {
    r.leaderID = leaderID
    if r.state != Follower {
        r.setState(Follower)
    }
    r.abortPendingClientRequestsLocked()
}
```

This prevents stale leaders from keeping client waiters alive after another node
has already become the valid leader for the same term.

Affected issues:

- #89 reduced apply timeout loops after restart.
- #90 reduced duplicate or stale leader handling during failover.

## 5. Raft Principle: Election Freshness Must See Snapshot-Compacted Logs

RequestVote compares the candidate log against the voter's last log term and
index. If a node caches `cachedLastLogIndex` and compaction removes that physical
entry, election freshness can accidentally compare against stale metadata.

The fix refreshes the local last-log information when the cached entry no longer
exists:

```go
lastIndex, lastTerm := r.localLastLogInfoLocked()
return r.isCandidateLogUpToDate(candidateIndex, candidateTerm, lastIndex, lastTerm)
```

If the entry is gone because a snapshot covers it, the comparison falls back to
the snapshot boundary. That preserves the Raft safety rule without treating a
normal compacted entry as data loss.

Affected issue:

- #92 refreshes election metadata across snapshot compaction.

## 6. Client Principle: Idempotence Needs Stable Request Identity

Retries are normal in a distributed system. A client can time out even if the
leader later commits the request. Without stable request identity, retrying can
apply the same logical write twice or make the test expect the wrong final
value.

The client command wrapper carries:

```go
type ClientCommand struct {
    ClientID    int64
    SequenceNum int64
    Command     any
}
```

The long E2E workload now uses explicit per-client IDs and monotonic sequence
numbers. The state machine uses that pair to detect duplicates and resolve
waiters consistently.

Affected issues:

- #90 final values diverged after successful writes in restart-heavy runs.
- #91 made the test harness stop only before issuing new requests.

## 7. Test Harness Principle: Do Not Cancel Requests After They Were Issued

The earlier long E2E stop gate could stop a worker while a request was already
sent but before the retry loop observed the final result. That created ambiguous
expected state: the cluster might legitimately apply the request, while the test
harness had already stopped tracking it.

The fix separates two states:

```go
requestIssued := false

if shouldStopBeforeRequest() {
    return
}

requestIssued = true
reply, err := sendRequest(...)
```

After `requestIssued` becomes true, the helper drains the logical request to a
terminal result. It can still retry on `NotLeader`, but it no longer drops the
expected final value.

Affected issue:

- #91 drains in-flight long E2E operations before stopping clients.

## 8. LSM Principle: Snapshot Export Must Pin The Files It Copies

An LSM snapshot copies SSTable files. Compaction can concurrently replace those
files. If the snapshot exporter lists filenames and opens them later, a compacted
file can disappear in the middle of snapshot creation.

The fix opens the SSTable files while holding the manager read lock and returns
open file descriptors:

```go
files, closeSnapshot, err := manager.OpenFilesSnapshot()
defer closeSnapshot()
```

On Unix-like filesystems, an open file descriptor remains readable even if the
directory entry is later removed. That gives snapshot export a stable view of
the bytes it decided to copy.

Affected issue:

- #88 included snapshot safety work for LSM-backed state-machine snapshots.

## 9. LSM Principle: Missing Metadata Is Different From Corrupt Data

Compaction metadata should point to real SSTable files, but long restart and
compaction churn exposed a stale metadata case where the catalog still named a
file that had already been removed. Treating this as fatal made the storage
engine fail even though the missing file was not readable data anymore.

The fix distinguishes two cases:

- missing file: prune stale manager metadata and continue;
- existing corrupt file: return an error.

The key behavior is:

```go
if errors.Is(err, os.ErrNotExist) {
    m.removeTableMetadataLocked(level, table)
    return true, nil
}
return false, err
```

This preserves strictness for real corruption while making metadata cleanup
self-healing.

Affected issue:

- #93 keeps LSM compaction metadata consistent when SSTable files disappear.

## 10. Test Discipline: Short Mode Must Not Pretend To Be Long E2E

Running a 10-minute scenario for one minute under `testing.Short()` created
confusing signals. It was neither a fast unit test nor the real long-running
scenario.

The fix makes the behavior explicit:

```go
func skipLongRunningE2EInShortMode(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping 10-minute long-running E2E test in short mode")
    }
}
```

Now `go test -short ./...` remains useful for PR checks, while long E2E tests
must be run intentionally.

Affected issues:

- #94 fixes short-mode behavior.
- #68 is also addressed by keeping generic short runs from executing long E2E.

## 11. Test Principle: Fixed Sleeps Are Not Cluster Synchronization

The TCP/LSM restart test used a fixed sleep and then read from a restarted node.
That made the test flaky: the write was correct, but the restarted node had not
always caught up before the assertion.

The fix polls for the exact state-machine value:

```go
c.waitForStateMachineValue(t, nodeIndex, key, expected, 5*time.Second)
```

This checks the real condition the test cares about instead of guessing a sleep
duration.

Affected issue:

- #95 preserves LSM data across TCP leader restart by fixing the test assertion
  to wait for the actual replicated value.

## 12. Verification Summary

After the latest code and test changes:

- focused TCP/LSM restart regression passed 10 consecutive runs;
- `go test -short ./...` passed;
- the trigger 10-minute restart/snapshot E2E passed;
- all six 10-minute long E2E scenarios passed under the race detector.

The full long-running result is documented in [PERFORMANCE.md](PERFORMANCE.md).

## 13. Lessons For Future Core Changes

- A successful request counter is not a consistency proof.
- Every Raft leadership change should be checked against the current-term commit
  rule.
- Snapshot compaction bugs often show up as missing log or missing SSTable
  edges, not as obvious panics.
- LSM metadata and LSM physical files must be updated as one logical catalog.
- Tests should wait for state, not time.
- After any Raft or LSM code change, run the triggering long E2E scenario first,
  then run the full long E2E regression.

## 14. 2026-06-22 Core Recovery And Apply Fixes

The second deep-testing pass focused on failures that only appear when Raft
restart, snapshot compaction, LSM flush, and LSM recovery overlap. These issues
are newer than the original #88-#95 campaign and are tracked separately:

- #102 Prevent lossy Raft commit notifications.
- #103 Align performance harness commit channel consumers.
- #104 Publish LSM SSTables atomically and handle empty-table metadata.
- #105 Keep LSM table and WAL IDs local to each database.
- #106 Restore LSM node state after TCP restart by restoring durable
  `commitIndex`.
- #107 Ignore non-WAL directory entries during MemTable recovery.

### #102: Commit Notifications Are Part Of The Apply Boundary

Symptom: under load, `commitChan` could become full. The old code used a
non-blocking send and silently dropped the notification:

```go
select {
case commitChan <- entry:
default:
    log.Warnf("commitChan full, skipping")
}
```

That made observers believe an entry had not been applied even though Raft had
already applied it to the state machine. The fix treats `commitChan` as a
backpressured stream:

```go
select {
case commitChan <- commitEntry:
case <-shutdownChan:
}
```

The shutdown escape prevents `Stop()` from hanging forever, while normal running
nodes no longer lose apply notifications.

### #103: The Benchmark Harness Must Not Apply Twice

`commitChan` is emitted after Raft applies the command. Some performance tests
were consuming `commitChan` and applying the command again. That inflated writes
and could hide real state-machine bugs.

The corrected harness only drains the channel:

```go
go func(ch chan param.CommitEntry) {
    for range ch {
        // already applied by Raft
    }
}(commitChan)
```

This keeps benchmarks from creating a second, non-Raft apply path.

### #104: SSTable Publication Has Two Separate Failure Modes

The first hypothesis for #104 was a partially written final file. Long E2E
later exposed a more precise failure: a retained `.sst` file had footer handles
with `DataHandle.Size == 0` and `IndexHandle.Size == 0`. The generic
`DataBlock.DecodeFrom(reader, 0)` API treats size `0` as "unlimited", so the
SSTable layer accidentally tried to decode footer bytes as values and hit EOF.

The final fix has three parts:

```go
tmp, _ := os.CreateTemp(dir, "."+base+".*.tmp")
// encode header/filter/data/index/footer into tmp
tmp.Sync()
tmp.Close()
os.Rename(tmp.Name(), finalPath)
```

- publish SSTables by temp-file, fsync, close, and rename;
- skip empty immutable memtables instead of publishing empty Level-0 tables;
- make `DecodeDataBlock` return immediately when `Footer.DataHandle.Size == 0`.

Recovery now also ignores uncommitted temp files and removes legacy empty
SSTables instead of loading them into the catalog.

### #105: ID Generators Must Be Scoped To A Database Manager

The old recovery path reset package-level ID generators. Recovering one manager
could move the global generator backward while another manager was still live.
That made a live database reuse an SSTable or WAL ID and potentially overwrite
existing files.

The fix moves ID allocation into the manager:

```go
type Manager struct {
    nextID atomic.Uint64
}

func (m *Manager) nextTableID() uint64 {
    return m.nextID.Add(1)
}
```

Recovery advances the local counter to at least the highest recovered ID. It no
longer resets global state that other databases share.

### #106: Durable Commit Index Is An Implementation Guardrail

Raft's paper-level persistent state is term, vote, and log entries. In this
project the state machine is persistent too, so forgetting a durable commit
index after restart can leave committed entries unapplied until another leader
happens to resend commit information.

The fix stores `CommitIndex` in `HardState` and restores it in `NewRaft`:

```go
r.commitIndex = hardState.CommitIndex
if r.commitIndex > r.lastApplied {
    r.startApplyLogsLocked()
}
```

This does not change the quorum commit rule. It preserves restart progress for
entries that were already durable and committed.

### #107: WAL Recovery Needs A Committed-File Contract

MemTable recovery used to replay every directory entry returned by `os.ReadDir`.
A leftover `notes.txt`, `3.wal.tmp`, or directory named `4.wal` could make the
engine fail before it replayed valid WAL files.

The fix filters the recovery set:

```go
if file.IsDir() || filepath.Ext(file.Name()) != ".wal" {
    continue
}
idPart := strings.TrimSuffix(file.Name(), ".wal")
if _, err := strconv.ParseUint(idPart, 10, 64); err != nil {
    continue
}
```

Only `{id}.wal` files are replayed. A committed WAL with corrupt contents still
fails recovery, so the engine remains strict about real data corruption.

### Validation Signal

After these fixes, the focused 10-minute restart/snapshot scenario passed with:

- 797,556 total operations;
- 0 failed operations;
- final cluster barrier success;
- 3,600 node/key consistency checks passed;
- 3 snapshotting nodes and 46 leader changes.

The full package and long-E2E validation commands are recorded in
[PERFORMANCE.md](PERFORMANCE.md).

## 15. 2026-06-22 SSTable Rewrite Metadata Fix

Issue #109 was found during the next deep LSM pass after #102-#107 had been
merged. The target was a narrow but important file-format invariant: encoding an
in-memory SSTable twice should produce a readable SSTable both times.

### Symptom

The regression test rewrites the same table object twice and then reloads it:

```go
table := createSampleSSTable(0, tempDir, pairs)

require.NoError(t, table.EncodeTo(table.filePath))
require.NoError(t, table.EncodeTo(table.filePath))

recovered := NewRecoverSSTable(0)
require.NoError(t, recovered.DecodeFrom(table.filePath))
_, err := recovered.GetDataBlockFromFile(table.filePath)
```

Before the fix, the second read failed:

```text
read value failed: unexpected EOF
decode DataBlock failed: read value data failed: unexpected EOF
```

### Root Cause

`EncodeTo` serialized layout metadata that was stored on the `SSTable` object.
During data-block encoding it added each encoded value size to
`Footer.DataHandle.Size`:

```go
t.Footer.DataHandle.Size += size
```

That is correct for one encode pass, but not for repeated encodes. The second
pass started with the previous size already present, so the footer claimed the
data block was larger than the bytes actually written in the current file.

When recovery later trusted that footer, it read past the data block and into
the index/footer area, where value decoding correctly failed.

### Fix

The encoder now resets all derived file-layout metadata before writing:

```go
func (t *SSTable) resetFileLayout() {
    if t.Footer == nil {
        t.Footer = block.NewFooter()
        return
    }
    t.Footer.DataHandle = block.NewHandle(0, 0)
    t.Footer.IndexHandle = block.NewHandle(0, 0)
    for _, entry := range t.IndexBlock.Indexes {
        entry.Offset = 0
    }
}
```

`EncodeTo` calls this before creating the temporary output file. The footer and
index offsets are therefore derived only from the current write pass.

### Principle

Atomic SSTable publication has two layers:

- publish only complete files, using temp file + fsync + close + rename;
- serialize internally consistent metadata into those complete files.

#104 fixed the first layer. #109 fixes the second layer for rewrite/retry paths.
Even if production normally writes each SSTable object once, the encoder should
remain deterministic and safe under retry, test, and maintenance tooling paths.

### Validation

Validation for #109:

```bash
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/sstable -run TestEncodeToCanRewriteSameTableWithoutStaleFooterState -count=1 -timeout=2m
```

and the wider LSM/storage regression:

```bash
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=5m
```

## 16. 2026-06-22 Snapshot Apply Path Validation Fix

Issue #111 was found while reviewing the LSM snapshot installation path. The
dangerous part was not just path traversal. The bigger correctness bug was that
`ApplySnapshot` validated file paths after it had already closed and removed
the current database.

### Symptom

The failing test installed a malformed snapshot into a database that already
contained a key:

```go
adapter.Apply(param.LogEntry{Command: mustMarshal(param.KVCommand{
    Op: param.OpSet, Key: "keep", Value: "value",
})})

snapData, _ := encodeSnapshotData(map[string][]byte{
    "../escape.sst": []byte("not-a-valid-sstable"),
})

err := adapter.ApplySnapshot(snapData)
```

Before the fix, the adapter logged a warning, returned nil, and the original key
was gone:

```text
[LSMAdapter] Skipping invalid snapshot file path: ../escape.sst
expected error but got nil
key not found
```

### Root Cause

The old implementation checked paths while writing files:

```go
for relPath, content := range snapshotData {
    if strings.Contains(relPath, "..") {
        log.Warnf("Skipping invalid snapshot file path: %s", relPath)
        continue
    }
    fullPath := filepath.Join(sstPath, relPath)
    os.WriteFile(fullPath, content, 0644)
}
```

This had three problems:

- invalid paths were skipped instead of rejected;
- the check ran after `db.Close()` and `os.RemoveAll(dbPath)`;
- `strings.Contains("..")` is not a precise path policy.

### Fix

`ApplySnapshot` now validates the full snapshot manifest before any destructive
operation:

```go
filesToRestore, err := validateSnapshotFiles(sstPath, snapshotData)
if err != nil {
    return err
}

if err := lsm.db.Close(); err != nil {
    return err
}
```

The validator accepts only clean relative paths that stay under the snapshot
SSTable root:

```go
cleanRel := filepath.Clean(relPath)
if cleanRel == "." || cleanRel == ".." ||
    strings.HasPrefix(cleanRel, ".."+string(os.PathSeparator)) {
    return "", fmt.Errorf("invalid snapshot file path")
}
```

It also checks the absolute joined path remains inside the snapshot root.

### Principle

Raft snapshot installation is a local state-machine replacement. It must behave
like a transaction at the validation boundary:

- reject malformed input before touching the current state;
- accept only a complete, validated file manifest;
- never silently skip part of a snapshot and call the install successful.

### Validation

Validation for #111:

```bash
GO_KV_LOG_LEVEL=warn go test ./pkg/storage/lsm -run TestApplySnapshotRejectsInvalidFilePathBeforeClearingDB -count=1 -timeout=2m
GO_KV_LOG_LEVEL=warn go test ./pkg/storage/lsm -count=1 -timeout=3m
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=5m
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=25m
GO_KV_LOG_LEVEL=warn go test -race -v ./tests -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' -count=1 -timeout=25m
```

The focused 10-minute replay completed with 1,104,337 total operations, 0
failed operations, final barrier success, and 3,600 strict node-key consistency
checks passed.

## 17. 2026-06-23 ReadIndex, Election Timeout, And LSM Compaction Fixes

The next long-running pass was focused on kernel-level behavior rather than
small boundary cases. It found a chain of related availability bugs: healthy
heartbeats could outlive ReadIndex confirmation, followers could start elections
before a healthy AppendEntries RPC was allowed to return, long E2E clients could
give up on an already-issued request during leader churn, and foreground LSM
compaction could stall Raft apply long enough to produce client apply timeouts.

### #113: ReadIndex Confirmation Must Respect RPC Budgets

Symptom: long E2E runs completed safely, but emitted ReadIndex quorum timeout
warnings. A focused unit test reproduced the smaller invariant violation:
`confirmLeadership` could time out after `electionTimeout * 2`, even when both
heartbeat acknowledgements were healthy but slower than that local budget.

The fix adds a floor to the ReadIndex heartbeat confirmation timeout:

```go
func readIndexConfirmTimeout(electionTimeout time.Duration) time.Duration {
    timeout := electionTimeout * 2
    if timeout < minReadIndexConfirmTimeout {
        return minReadIndexConfirmTimeout
    }
    return timeout
}
```

This keeps linearizable reads from reporting leadership loss while healthy
AppendEntries replies are still inside the configured transport timeout.

### #115: Election Timeout Must Exceed Healthy AppendEntries Timeout

Symptom: `TestCluster_ConcurrentClientRequests/grpc_lsm` could fail under
package-parallel race testing with missing keys after concurrent writes. The
root cause was a timeout budget mismatch:

```go
DefaultElectionTimeout      = 500 * time.Millisecond
DefaultAppendEntriesTimeout = 2 * time.Second
```

A follower could start a new election while a healthy AppendEntries RPC was
still allowed to be in flight. That created avoidable leader churn under race
detector and package-parallel load.

The default election timeout is now 2.5s, and a config regression test asserts
the invariant:

```go
assert.Greater(t,
    config.DefaultElectionTimeout,
    transportgrpc.DefaultAppendEntriesTimeout,
)
```

This does not change the Raft paper's election rule. It makes the implementation
timeout budget coherent with its transport layer.

### #116: Already-Issued Long E2E Requests Need A Time Budget, Not A Retry Count

Symptom: the 10-minute mixed-failure E2E scenario failed with a few
`not_leader` operations even though the final barrier and strict consistency
checks passed. The request had already been issued, so the cluster might still
commit it, but the test helper gave up after a fixed retry count:

```go
for retry := 0; retry < maxRetries; retry++ {
    ...
}
```

The long-running helper now distinguishes two states:

- before a request is issued, the normal retry limit still applies;
- after a request is issued, retries continue for a bounded wall-clock window.

```go
func shouldContinueLongRunningRetry(retry, maxRetries int, requestIssued bool, requestIssuedAt, now time.Time) bool {
    if !requestIssued {
        return retry < maxRetries
    }
    return now.Sub(requestIssuedAt) < longRunningIssuedRequestRetryTimeout
}
```

This preserves the expected-value model: once a logical client request may have
entered Raft, the test keeps following that same `(ClientID, SequenceNum)` until
it reaches a terminal result.

### #117: Apply Timeout Was A Foreground Compaction Stall

After #116, the mixed-failure scenario exposed a new failure reason:
`apply_timeout`. Later, full long E2E showed the same pattern in Comprehensive
and WriteHeavy runs. The key clue was that WriteHeavy used 8 clients and failed
with exactly 8 apply timeouts in one window. That pointed to a global apply
stall rather than random client failures.

The first fix was to keep pending client requests after a leader-side apply
timeout:

```go
// old behavior removed the pending request on timeout
if !ok && trackClient {
    r.clearPendingClientRequest(index)
}
```

That removal was wrong. A timeout does not prove the original entry failed.
Keeping `pendingClientRequests` lets a retry attach to the original log index
instead of appending duplicate work.

The deeper root cause was in LSM. `CreateNewSSTable` synchronously ran
compaction after every flush:

```go
if err := m.Compaction(); err != nil {
    return fmt.Errorf("compaction failed: %w", err)
}
```

Raft applies committed entries to the LSM-backed state machine while holding
`stateMachineMu`. If a flush triggered a large compaction inside that path, all
later committed entries stopped applying. Client waiters could then exhaust
their apply/retry windows even though the cluster eventually converged.

The fix publishes durable Level-0 SSTables in the foreground and schedules
compaction on a coalesced background worker:

```go
func (m *Manager) CreateNewSSTable(imem *memtable.IMemTable) error {
    ...
    m.addTable(sst)
    if m.isLevelNeedToBeMerged(m.minSSTableLevel) {
        m.ScheduleCompaction()
    }
    imem.Clean()
    return nil
}
```

`ScheduleCompaction` keeps one worker active and merges additional requests into
one extra pass:

```go
if m.compactionRunning {
    m.compactionQueued = true
    return
}
m.compactionRunning = true
go m.runScheduledCompactions()
```

The storage invariant is unchanged: once `CreateNewSSTable` returns, the data is
durable and visible in Level 0. Only the expensive merge into deeper levels was
moved out of the Raft apply critical path.

### Validation

The final validation set was:

```bash
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=10m
GO_KV_LOG_LEVEL=warn go test ./raft -count=1 -timeout=8m
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=20m ./tests -run '^TestLongRunning_10Min_WriteHeavy$' -count=1
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=90m ./tests -run '^TestLongRunning_10Min_(Comprehensive|WriteHeavy|MixedWithFailures|ConsistencyWithRestartsAndSnapshots|ReadHeavy|DeleteStress)$' -count=1
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m
```

The final six-scenario long E2E run completed with zero failed operations in
all scenarios. The strict restart/snapshot scenarios passed their final barrier
and node-by-node consistency checks. The latest numbers are recorded in
[PERFORMANCE.md](PERFORMANCE.md).

## 18. 2026-06-23 Threshold-Gated LSM Compaction Scheduling

Related issue: #119.

### Symptom

The first baseline after #118 failed in the SSTable package:

```bash
GO_KV_LOG_LEVEL=warn go test ./raft ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=12m
```

The failure was:

```text
--- FAIL: TestSSTableManagerOpenFilesSnapshotReleasesManagerLock
    manager_test.go:201: OpenFilesSnapshot kept the manager lock while callers read files
```

A focused rerun reproduced it without the race detector:

```bash
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/sstable -run '^TestSSTableManagerOpenFilesSnapshotReleasesManagerLock$' -count=10 -timeout=2m
```

### Root Cause

#117 correctly moved compaction out of the foreground flush path, but the first
implementation scheduled a background worker after every non-empty flush:

```go
m.addTable(sst)
m.ScheduleCompaction()
imem.Clean()
```

That was too broad. A single new Level-0 SSTable is below the compaction
threshold, so the worker has no merge work to do. It can still briefly take
`Manager.mu` while checking the catalog. The existing snapshot-lock test then
failed because it observed this unrelated no-op worker, not because
`OpenFilesSnapshot` kept the lock after returning.

The production issue is the same: a below-threshold flush should not create a
goroutine and contend on the manager lock just to discover that compaction is
unnecessary.

### Regression Test

The deterministic test blocks Level-0 compaction before creating one SSTable. If
the flush schedules a worker below the threshold, that worker remains visible as
`compactionRunning`:

```go
func TestCreateNewSSTableSkipsCompactionWhenBelowThreshold(t *testing.T) {
    manager := NewSSTableManager(t.TempDir())
    level := manager.minSSTableLevel

    manager.mu.Lock()
    manager.compactingLevels[level] = true
    manager.mu.Unlock()
    defer func() {
        manager.endCompactionLevels([]int{level})
        manager.WaitForCompactions()
    }()

    assert.NoError(t, manager.CreateNewSSTable(testIMemWithPair("key", "value")))

    manager.mu.Lock()
    running := manager.compactionRunning
    queued := manager.compactionQueued
    manager.mu.Unlock()

    assert.False(t, running)
    assert.False(t, queued)
}
```

Before the fix this failed with:

```text
below-threshold flush must not start a no-op compaction worker
```

### Fix

`CreateNewSSTable` now checks the Level-0 threshold after publishing the new
table and schedules background compaction only when there is real work:

```go
m.addTable(sst)
log.Debugf("[SSTableManager] Created new SSTable %s at level %d", sst.FilePath(), sst.level)

if m.isLevelNeedToBeMerged(m.minSSTableLevel) {
    m.ScheduleCompaction()
}

imem.Clean()
```

This preserves the #117 invariant that foreground flush does not wait for
compaction. It adds a second invariant: background compaction should not be
created for below-threshold states.

### Validation

```bash
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/sstable -run '^(TestCreateNewSSTableSkipsCompactionWhenBelowThreshold|TestSSTableManagerOpenFilesSnapshotReleasesManagerLock)$' -count=10 -timeout=2m
GO_KV_LOG_LEVEL=warn go test ./raft ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=12m
GO_KV_LOG_LEVEL=warn go test ./engine/lsm/sstable -count=100 -timeout=5m
GO_KV_LOG_LEVEL=warn go test -race ./engine/lsm/... ./pkg/storage/lsm -count=1 -timeout=12m
GO_KV_LOG_LEVEL=warn go test ./tests -run '^(TestCluster_ConcurrentClientRequests|TestCluster_TakeSnapshot|TestCluster_InstallSnapshot|TestCluster_FullClusterRestart|TestCluster_LeaderFailover)$' -count=3 -timeout=12m
```

## 19. 2026-06-23 Physical Raft Log Tombstones During LSM CompactLog

Related issue: #121.

### Symptom

`StorageAdapter.CompactLog(upToIndex)` looked correct from the normal Raft API:
after compaction, `GetEntry(index)` returned `nil` for compacted indexes because
the adapter had advanced `firstIndex`.

The deeper storage invariant was still broken. The physical LSM keys
`log:<index>` for compacted entries remained present underneath the logical
window. A direct storage lookup could still see the old encoded log payload, and
normal LSM compaction could not reclaim it because no tombstone had ever been
written.

That means a long-running Raft node could keep obsolete log payloads forever even
though snapshots had already made those entries logically unreachable.

### Root Cause

The old implementation was metadata-only:

```go
s.firstIndex = upToIndex + 1
if upToIndex >= s.lastIndex {
    s.lastIndex = upToIndex
    s.logSize = 0
} else if oldLastIndex >= oldFirstIndex {
    totalEntries := oldLastIndex - oldFirstIndex + 1
    compactedEntries := deleteTo - oldFirstIndex + 1
    compactedBytes := int((int64(s.logSize) * int64(compactedEntries)) / int64(totalEntries))
    s.logSize -= compactedBytes
}

return s.saveMetadata()
```

This maintained the logical Raft log window, but it did not maintain the physical
LSM state. The `logSize` update was also only proportional. It could drift from
the true retained encoded bytes because Raft log entries have variable command
sizes.

The important distinction is:

- the logical window (`firstIndex..lastIndex`) controls what Raft may read;
- the physical LSM tree controls what bytes remain durable and reclaimable.

Both must move together. Hiding old keys at the Raft adapter layer is not the
same as deleting those keys from the storage engine.

### Regression Test

The regression test intentionally bypasses `GetEntry` after compaction and checks
the underlying LSM keys directly:

```go
func TestStorageAdapterCompactLogDeletesPhysicalLogKeys(t *testing.T) {
    ...
    assert.NoError(t, adapter.CompactLog(2))

    raw, err = adapter.db.Get(key1)
    assert.NoError(t, err)
    assert.Nil(t, raw, "CompactLog must tombstone compacted physical log key 1")

    raw, err = adapter.db.Get(key2)
    assert.NoError(t, err)
    assert.Nil(t, raw, "CompactLog must tombstone compacted physical log key 2")

    raw, err = adapter.db.Get(key3)
    assert.NoError(t, err)
    assert.NotNil(t, raw, "CompactLog must keep entries after the compacted range")
}
```

Before the fix it failed with encoded `GLG1` bytes still present for keys 1 and
2.

### Fix

`CompactLog` now writes tombstones for the compacted physical key range before it
saves the new logical metadata:

```go
oldFirstIndex := s.firstIndex
oldLastIndex := s.lastIndex
deleteTo := min(upToIndex, oldLastIndex)

if oldLastIndex >= oldFirstIndex {
    for i := oldFirstIndex; i <= deleteTo; i++ {
        key := s.getLogKey(i)
        val, err := s.db.Get(key)
        if err != nil {
            return err
        }
        if val != nil {
            s.logSize -= len(val)
            if s.logSize < 0 {
                s.logSize = 0
            }
        }
        if err := s.db.Delete(key); err != nil {
            return err
        }
    }
}

s.firstIndex = upToIndex + 1
if upToIndex >= s.lastIndex {
    s.lastIndex = upToIndex
    s.logSize = 0
}

return s.saveMetadata()
```

This restores the invariant that snapshot-driven Raft log compaction updates both
planes:

1. the Raft-visible log window no longer exposes compacted entries;
2. the LSM-visible keyspace contains tombstones that allow normal LSM compaction
   to reclaim obsolete log payloads.

The `logSize` accounting is now based on the actual encoded value length being
removed instead of a proportional estimate.

### Validation

```bash
GO_KV_LOG_LEVEL=warn go test ./pkg/storage/lsm -run '^(TestStorageAdapterCompactLogDeletesPhysicalLogKeys|TestStorageAdapter_Snapshot|TestStorageAdapter_CompactBeyondLastIndexFromSnapshot|TestStorageAdapter_LogEntries|TestStorageAdapter_ReappendAfterTruncateSurvivesFlushCompactionAndRestart)$' -count=1 -timeout=5m
GO_KV_LOG_LEVEL=warn go test ./pkg/storage/lsm ./engine/lsm/... -count=1 -timeout=12m
GO_KV_LOG_LEVEL=warn go test -race ./pkg/storage/lsm ./engine/lsm/... -count=1 -timeout=12m
GO_KV_LOG_LEVEL=warn go test ./tests -run '^(TestCluster_TakeSnapshot|TestCluster_InstallSnapshot|TestCluster_FullClusterRestart)$' -count=3 -timeout=12m
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=25m ./tests -run '^TestLongRunning_10Min_ConsistencyWithRestartsAndSnapshots$' -count=1
```

The first command proves the direct physical-key regression. The second command
checks the surrounding LSM packages so the new tombstone writes do not break
flush, compaction, WAL recovery, or restart behavior. The race and cluster
commands cover concurrency, snapshot creation, snapshot install, and durable
restart recovery after the new physical deletions. The final two commands close
the PR-level gate: all short unit/integration tests passed under the race
detector, and the 10-minute restart/snapshot E2E replay completed with zero
failed operations and strict node-by-node consistency.

## 20. 2026-06-23 Deterministic Timeout-Recheck Test For waitForAppliedLog

Related issue: #122.

### Symptom

The full short race gate exposed a Raft test failure:

```bash
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m
```

Failure:

```text
--- FAIL: TestWaitForAppliedLogRechecksLastAppliedOnTimeout
    raft_test.go:1575: Should be true
```

Targeted repeat runs showed that the failure was timing-sensitive rather than a
deterministic production logic failure:

```bash
GO_KV_LOG_LEVEL=warn go test ./raft -run '^TestWaitForAppliedLogRechecksLastAppliedOnTimeout$' -count=20 -timeout=2m
GO_KV_LOG_LEVEL=warn go test -race ./raft -run '^TestWaitForAppliedLogRechecksLastAppliedOnTimeout$' -count=50 -timeout=3m
```

Both targeted commands passed, while the full race gate had already failed under
package-wide load.

### Root Cause

The production code already had the intended timeout-path recheck:

```go
case <-timer.C:
    r.mu.Lock()
    applied := r.lastApplied >= index
    ...
    r.mu.Unlock()
    if applied {
        return nil, true
    }
    return nil, false
```

The test was the fragile part. It used a fixed sleep to update `lastApplied`
before a short timeout:

```go
go func() {
    time.Sleep(5 * time.Millisecond)
    r.mu.Lock()
    r.lastApplied = 7
    r.mu.Unlock()
}()

result, ok := r.waitForAppliedLog(7, 20*time.Millisecond)
```

Under the race detector, the goroutine is not guaranteed to run before the 20 ms
timer fires. When it runs late, the timeout branch correctly sees
`lastApplied < 7` and returns false. That does not disprove the timeout recheck;
it only proves the test had a scheduler-dependent precondition.

### Fix

The test now makes the intended ordering explicit:

1. start `waitForAppliedLog` in a goroutine;
2. wait until the waiter is registered in `notifyApply`;
3. set `lastApplied` under `r.mu` without sending an apply notification;
4. wait for the timeout branch to recheck `lastApplied`;
5. assert success and verify waiter cleanup.

```go
go func() {
    result, ok := r.waitForAppliedLog(7, 100*time.Millisecond)
    results <- waitResult{result: result, ok: ok}
}()

assert.Eventually(t, func() bool {
    r.mu.Lock()
    defer r.mu.Unlock()
    return len(r.notifyApply[7]) == 1
}, time.Second, time.Millisecond)

r.mu.Lock()
r.lastApplied = 7
r.mu.Unlock()
```

This tests the same kernel invariant, but it no longer depends on a helper
goroutine winning a 5 ms versus 20 ms scheduling race.

### Validation

```bash
GO_KV_LOG_LEVEL=warn go test -race ./raft -run '^TestWaitForAppliedLogRechecksLastAppliedOnTimeout$' -count=100 -timeout=3m
GO_KV_LOG_LEVEL=warn go test -race ./raft -count=1 -timeout=8m
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m
```

The focused test passed 100 times under the race detector, and the whole `raft`
package passed under the race detector. The full short race gate then passed
after this fix and the follow-up integration-helper fixes.

## 21. 2026-06-23 Race-Load-Safe Integration Leader Discovery

Related issue: #123.

### Symptom

After #122, the full short race gate progressed past the Raft package but failed
later in the integration suite:

```bash
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m
```

Failure:

```text
--- FAIL: TestCluster_MembershipChange (64.83s)
    --- FAIL: TestCluster_MembershipChange/grpc_simplefile (8.22s)
        integration_test.go:179: Cluster failed to elect a leader within timeout
FAIL github.com/xmh1011/go-kv/tests 1009.098s
```

Focused reruns did not show a deterministic membership-change bug:

```bash
GO_KV_LOG_LEVEL=warn go test ./tests -run '^TestCluster_MembershipChange$/^grpc_simplefile$' -count=5 -timeout=12m
GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_MembershipChange$/^grpc_simplefile$' -count=3 -timeout=12m
```

Both passed, so the investigation moved to the test helper used to discover a
leader.

### Root Cause

`tests.cluster.getLeader` said it waited about 8 seconds, but it actually did
more expensive work. On every attempt it sent a full `ClientRequest` probe to
every running node:

```go
for i := 0; i < 40; i++ {
    time.Sleep(200 * time.Millisecond)
    for _, node := range c.nodes {
        _ = node.ClientRequest(args, reply)
        if !reply.NotLeader && reply.Success {
            return node
        }
        if !reply.NotLeader && reply.Result == "key not found" {
            return node
        }
    }
}
```

`ClientRequest` is not a cheap local state check. For a leader-side read it may
perform ReadIndex leadership confirmation and wait for `lastApplied` for up to
the client apply timeout. In a full `-race` run, this means the helper can spend
most of its budget blocked in read probes and then report "failed to elect a
leader", even though the real problem is that the probe path did not complete in
the helper's fixed window.

The helper also reused a fixed probe client id, which made repeated calls share
client-session state and could blur the meaning of a probe response.

### Fix

Leader discovery is now condition-driven:

```go
deadline := time.Now().Add(30 * time.Second)
for time.Now().Before(deadline) {
    for _, node := range c.nodes {
        if node.IsStopped() || node.State() != raft.Leader {
            continue
        }

        sequenceNum++
        args := &param.ClientArgs{
            ClientID:    probeClientID,
            SequenceNum: sequenceNum,
            Command:     probeCmdBytes,
        }
        ...
    }
    time.Sleep(200 * time.Millisecond)
}
```

The helper now:

- scans local Raft state first and only probes leader candidates;
- uses a unique probe client id per `getLeader` call;
- uses a 30 second deadline for race-mode integration load;
- prints all node states and the last probe response on failure.

This keeps the stale-leader guard for candidates while avoiding serial
ReadIndex/apply waits against every follower.

### Validation

```bash
GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_MembershipChange$/^grpc_simplefile$' -count=5 -timeout=12m
GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_MembershipChange$' -count=1 -timeout=15m
```

The failing `grpc_simplefile` membership-change sub-scenario passed five times
under the race detector after the helper rewrite, and the full membership-change
transport/storage matrix passed under the race detector.

## 22. 2026-06-23 Race-Load-Safe Network Partition Leader Detection

Related issue: #124.

### Symptom

After #123, the full short race gate progressed further but failed in the
network-partition integration test:

```bash
GO_KV_LOG_LEVEL=warn go test -race -short ./... -count=1 -timeout=35m
```

Failure:

```text
--- FAIL: TestCluster_NetworkPartition (70.77s)
    --- FAIL: TestCluster_NetworkPartition/tcp_inmemory (12.03s)
        integration_test.go:413: Leader: Node 3
        integration_test.go:417: Isolating Node 3...
        integration_test.go:436: Waiting for new leader in majority partition...
        integration_test.go:473: Majority partition failed to elect a new leader
FAIL github.com/xmh1011/go-kv/tests 1011.196s
```

Focused reruns again did not show a deterministic Raft partition-election
failure:

```bash
GO_KV_LOG_LEVEL=warn go test ./tests -run '^TestCluster_NetworkPartition$/^tcp_inmemory$' -count=5 -timeout=12m
GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_NetworkPartition$/^tcp_inmemory$' -count=5 -timeout=12m
```

Both passed.

### Root Cause

`TestCluster_NetworkPartition` had its own hand-written leader detection loop
inside the majority partition. It did not use the condition-driven helper added
for #123:

```go
time.Sleep(5 * time.Second)
for i := 0; i < 20 && !foundLeader; i++ {
    time.Sleep(200 * time.Millisecond)
    for _, node := range majorityNodes {
        reply := &param.ClientReply{}
        _ = node.ClientRequest(&param.ClientArgs{Command: probeCmdBytes}, reply)
        ...
    }
}
```

This repeated the same test-design flaw:

- fixed sleeps instead of condition-based waiting;
- full `ClientRequest` probes against every majority node;
- zero-value `ClientID` and `SequenceNum` for every probe;
- no state diagnostics when the helper failed.

The test was supposed to answer "did the majority partition elect a leader?", but
the probe loop could fail because the read-probe path was slow under full
race-mode load.

### Fix

The generic leader helper now accepts a candidate node set:

```go
func (c *cluster) getLeader(t *testing.T) *raft.Raft {
    t.Helper()
    return c.getLeaderFromCandidates(t, c.nodes, 30*time.Second)
}
```

The network-partition test builds `majorityNodes` and reuses the same helper:

```go
newLeader = c.getLeaderFromCandidates(t, majorityNodes, 30*time.Second)
```

This makes partition leader detection follow the same rules as the rest of the
integration suite:

- only nodes whose local state is `Leader` are probed;
- probes use a unique client id and increasing sequence number;
- the deadline is long enough for race-mode TCP and storage overhead;
- failures include node-state and last-probe diagnostics.

### Validation

```bash
GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_NetworkPartition$/^tcp_inmemory$' -count=5 -timeout=12m
GO_KV_LOG_LEVEL=warn go test -race ./tests -run '^TestCluster_NetworkPartition$' -count=1 -timeout=15m
```

The previously failing `tcp_inmemory` network-partition sub-scenario passed five
times under the race detector after the helper reuse, and the full
network-partition transport/storage matrix passed under the race detector.

## 23. 2026-06-24 Benchmark Harness And Long E2E Hardening

Related issues: #142, #143, #145, #146, #150, and #151.

This pass was different from the earlier Raft-only fixes. The visible failures
were mostly in benchmarks and long E2E accounting, but the investigation still
found one real LSM/Raft snapshot race at the storage-engine boundary.

### #142: Benchmark Concurrency Must Match The Cluster Under Test

Symptom: the mixed workload benchmark created too much client pressure for a
small three-node local cluster. That made benchmark output noisy and made it
hard to separate a core bug from a synthetic overload condition.

The principle is simple: a benchmark should stress the subsystem it claims to
measure, not an accidental unbounded client scheduler. For Raft write
benchmarks, each logical write already crosses leader append, stable storage,
quorum replication, commit, and state-machine apply. Unbounded goroutine fanout
can turn a storage benchmark into a client-side queue benchmark.

The fix bounded mixed workload concurrency so the benchmark still creates
pressure but remains explainable. The important test-design rule is:

```text
benchmark concurrency should be explicit input
        |
        v
load should saturate the Raft/LSM path gradually
        |
        v
failures should identify system behavior, not harness overload
```

### #146: Benchmark Failures Must Propagate

Symptom: benchmark helpers could observe internal operation failures while the
outer benchmark command still exited successfully. That is dangerous because it
creates false performance data: a benchmark that silently drops failed writes is
measuring a different system.

The fix made the harness treat hidden operation errors as benchmark failures.
The invariant is:

```text
performance number is valid only if correctness counters are clean
```

This matches the long E2E rule used elsewhere in the project: throughput and
latency are only meaningful when failed operations, final barrier, and strict
consistency checks are all clean.

### #145: Leader Readiness Is A Condition, Not A Sleep

Symptom: benchmark startup could race leader election and report failures that
depended on timing. The root cause was the same pattern as #123 and #124: the
test wanted "a usable leader exists", but the harness used fixed waiting and
weak readiness assumptions.

The fix made benchmark startup wait for a concrete leader-ready condition before
issuing load. This keeps benchmark failures focused on the workload phase. It
also keeps leader election from being accidentally included in steady-state
latency numbers unless that is the scenario being measured.

### #143: Benchmarks Must Close LSM Databases

Symptom: LSM benchmark runs left database instances open. That can leak file
descriptors, background compaction goroutines, and temporary directories across
benchmark iterations.

The principle is that LSM benchmarks are not pure CPU microbenchmarks. They own
files, WALs, SSTables, and compaction workers. A correct benchmark lifecycle is:

```text
create isolated database directory
        |
        v
run workload
        |
        v
wait for or stop background workers
        |
        v
close database
        |
        v
remove test directory
```

Closing matters for correctness too. If one benchmark leaves a database open,
the next benchmark can observe resource contention that is not part of the
scenario.

### #150: Snapshot Apply Must Serialize Database Replacement

Symptom: a focused race test exposed a real data race between `Database.Reload`
and concurrent `Database.Get`. The problematic production-shaped path is Raft
InstallSnapshot. Installing a state-machine snapshot can close and replace the
LSM database while client reads still walk memtables or SSTables.

The old mental model was incomplete:

```text
stateMachineMu protects Raft apply/read/snapshot calls
```

That is true at the Raft adapter boundary, but the LSM database facade also has
direct methods used by tests and storage utilities. The LSM database itself
needed a lifecycle boundary so destructive operations cannot overlap normal
reads and writes.

The fix added a database-level lifecycle `RWMutex`:

```go
func (d *Database) Get(key kv.Key) ([]byte, bool) {
    d.lifecycleMu.RLock()
    defer d.lifecycleMu.RUnlock()
    ...
}

func (d *Database) ReplaceData(fn func(tmpDir string) error) error {
    d.lifecycleMu.Lock()
    defer d.lifecycleMu.Unlock()
    ...
}
```

Snapshot export was also tightened. Instead of listing SSTable names and opening
them later, the state machine asks the database for a flushed, opened SSTable
snapshot:

```go
files, closeSnapshot, err := db.FlushAndOpenSSTableSnapshot()
defer closeSnapshot()
```

Open file descriptors pin the selected bytes on Unix-like filesystems. The
lifecycle lock then protects the opposite direction: applying a snapshot cannot
close or replace the database while a reader is already using the old one.

Validation:

```bash
GO_KV_LOG_LEVEL=warn go test -race -run '^TestApplySnapshotDoesNotRaceWithConcurrentReads$' ./pkg/storage/lsm -count=1
GO_KV_LOG_LEVEL=warn go test -race ./engine/lsm/... ./pkg/storage/lsm -count=1
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=20m ./tests -run '^TestLongRunning_10Min_DeleteStress$' -count=1
GO_KV_LOG_LEVEL=warn make test
```

The pre-fix focused race test reported a race between `Database.Reload` and
`Database.Get`. After the fix it passed, the wider LSM/storage race gate passed,
and the 10-minute delete-stress run completed with 986,369 operations, zero
failures, and strict consistency true.

### #151: Already-Issued Requests Need A Recovery Window

Symptom: the 10-minute mixed-failure workload could report a small number of
`apply_timeout` failures even though the final barrier and strict consistency
both passed. The reproduced failure had this shape:

```text
apply_timeout=4
final barrier: true
strict consistency: true
```

That signal means "the harness gave up on observing four issued commands", not
"the cluster lost committed data".

The Raft principle is that retry identity matters. A client command is wrapped
with stable identity:

```go
type ClientCommand struct {
    ClientID    int64
    SequenceNum int64
    Command     any
}
```

If the first RPC times out, the same logical command can be retried safely. The
state machine uses `(ClientID, SequenceNum)` to apply it at most once and to
return the previously observed result for duplicates.

The old long-test harness used a 30-second retry window after a command had
already been issued. During failure injection, that was too short because the
request can cross several transient windows:

1. a leader-side apply wait can expire;
2. the leader may restart or step down;
3. a new leader must be elected;
4. followers may need snapshot catch-up before normal log replication resumes;
5. the retried request must reattach to the original logical command outcome.

The fix extends the bounded issued-request retry window:

```go
const (
    longRunningSnapshotThreshold = 2 * 1024 * 1024
    longRunningClientRetries     = 20
    // Already-issued commands must survive several server-side apply waits plus
    // leader re-election and snapshot catch-up. If the command is truly stuck,
    // the long-running test still fails after this bounded window.
    longRunningIssuedRequestRetryTimeout = 90 * time.Second
)
```

This does not hide real failures. A command that remains stuck beyond the
bounded window still counts as failed. The change only aligns the harness with
the Raft retry contract: after a command is issued, the test must wait long
enough to distinguish recoverable leadership/snapshot churn from a true stuck
apply path.

Validation:

```bash
GO_KV_LOG_LEVEL=warn go test -race -v -timeout=20m ./tests -run '^TestLongRunning_10Min_MixedWithFailures$' -count=1
GO_KV_LOG_LEVEL=warn make long-test
GO_KV_LOG_LEVEL=warn make test
```

The targeted mixed-failure run passed in 619.602s with 666,692 operations, zero
failures, final barrier true, and strict consistency true. The full long E2E
regression then passed in 3674.858s across all six 10-minute scenarios with
zero failed operations.

### Combined Lesson

This pass reinforced a useful debugging split:

- benchmark harness bugs usually distort the measurement surface;
- long E2E harness bugs usually distort the failure classification surface;
- LSM/Raft snapshot bugs corrupt the actual lifecycle boundary.

The fix strategy should match the layer. Do not hide a storage race by relaxing
the test. Do not rewrite Raft because a benchmark forgot to wait for a leader.
And do not trust performance numbers until the harness proves zero failures and
the data-consistency gates pass.

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

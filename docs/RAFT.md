# Raft Design

Chinese version: [RAFT.zh-CN.md](RAFT.zh-CN.md)

This document explains the Raft implementation in `go-kv`. It focuses on how
the code is organized, how requests move through Raft, and which invariants must
stay true for correctness.

Related source files:

- `raft/raft.go`
- `raft/election.go`
- `raft/replication.go`
- `raft/snapshot.go`
- `pkg/param/*.go`
- `pkg/storage/storage.go`
- `pkg/transport/transport.go`

## 1. Why Raft Exists In This Project

A key-value store has a simple API, but a distributed key-value store has a hard
problem: several machines must agree on the same sequence of writes.

Raft solves that by forcing all writes through a leader:

1. clients send writes to the leader;
2. the leader appends each write to its log;
3. followers copy the leader's log;
4. a log entry is committed after a quorum stores it;
5. every node applies committed entries in log order.

If every healthy node applies the same committed log entries in the same order,
their key-value state machines converge to the same data.

## 2. Module Layout

| File | Main responsibility |
|---|---|
| `raft.go` | Raft struct, node lifecycle, client requests, proposal batching, ReadIndex helpers. |
| `election.go` | election timeouts, PreVote, RequestVote RPC handling, transition to leader. |
| `replication.go` | AppendEntries RPC, follower log repair, leader progress tracking, commit, apply. |
| `snapshot.go` | local snapshot creation, InstallSnapshot RPC, log compaction. |

Raft depends on two abstractions:

- `storage.Storage`: persistent term/vote/log/snapshot storage;
- `transport.Transport`: RPC sending and receiving.

This keeps consensus logic independent from the concrete disk and network
implementations.

## 3. Node States

Each node is always in one of these states:

```go
const (
    Follower State = iota
    Candidate
    Leader
    Dead
)
```

Beginner explanation:

- A **follower** waits for a leader.
- A **candidate** asks other nodes to vote for it.
- A **leader** accepts client writes and replicates logs.
- A **dead** node has been stopped and should not participate.

The current state is stored atomically so cheap checks can avoid taking the main
Raft mutex.

## 4. Persistent And Volatile State

Raft has two kinds of state.

Persistent state must survive crashes:

- `currentTerm`: the highest term this node has seen;
- `votedFor`: the candidate this node voted for in `currentTerm`;
- log entries;
- snapshots.

Volatile state is rebuilt while running:

- `commitIndex`: highest known committed log index;
- `lastApplied`: highest log index applied to the state machine;
- `nextIndex[peer]`: next log index the leader will send to a follower;
- `matchIndex[peer]`: highest log index known to be stored on a follower;
- leadership acknowledgement timestamps used by ReadIndex/lease reads.

The basic rule is:

```text
commitIndex >= lastApplied
```

`lastApplied` must never move past the actual state-machine apply point.

## 5. Election Flow

Election logic lives in `raft/election.go`.

The normal path is:

```text
Follower does not hear from a leader
        |
        v
random election timeout expires
        |
        v
start PreVote
        |
        v
if PreVote wins, increment term and start real election
        |
        v
send RequestVote RPCs
        |
        v
win quorum
        |
        v
transitionToLeader
```

PreVote is important because it avoids unnecessary term bumps. A node that was
partitioned from the cluster should not disturb a healthy leader just because
its local timeout fired.

## 6. Vote Safety

A follower grants a vote only if:

1. the candidate term is valid;
2. the follower has not already voted for someone else in that term;
3. the candidate log is at least as up-to-date as the follower log.

The log freshness check protects committed entries. A candidate that is missing
committed entries should not become leader.

## 7. Leader Initialization

When a node becomes leader, it initializes replication progress:

```text
nextIndex[peer]  = lastLogIndex + 1
matchIndex[peer] = 0
```

The leader then sends heartbeats. Heartbeats are just AppendEntries RPCs with no
entries. They keep followers from starting elections and provide leadership
confirmation for reads.

## 8. Write Path

Writes enter Raft through `ClientRequest`.

```text
ClientRequest
        |
        v
preHandleClientRequest checks leader and duplicate requests
        |
        v
wrap command in ClientCommand when needed
        |
        v
CommitClient
        |
        v
proposalCh
        |
        v
processBatch
        |
        v
store.AppendEntries
        |
        v
broadcast AppendEntries
        |
        v
wait for apply result
```

`ClientCommand` stores:

- `ClientID`
- `SequenceNum`
- the actual command payload

That makes client retries idempotent. If the same logical request appears twice
in the log, the state machine applies it once and resolves the duplicate waiter.

## 9. AppendEntries On Followers

`AppendEntries` does three jobs:

1. act as a heartbeat;
2. append new log entries;
3. repair inconsistent follower logs.

The follower checks:

- leader term is not stale;
- previous log index/term matches;
- conflicting local entries are truncated;
- new leader entries are appended;
- follower `commitIndex` advances up to the leader commit.

The implementation serializes follower-side disk mutation with
`appendEntriesMu`. This prevents two concurrent AppendEntries calls from
truncating and appending the log in conflicting ways.

## 10. Leader Replication Progress

The leader maintains two maps:

```go
nextIndex[peer]  // next index to send
matchIndex[peer] // highest confirmed index
```

On success:

```text
nextIndex = prevLogIndex + len(entries) + 1
matchIndex = nextIndex - 1
```

On failure, the leader uses conflict information to move `nextIndex` backward.
It never moves `nextIndex` below `matchIndex + 1`, because confirmed progress
must be monotonic.

If `nextIndex` points before the local first log index, the follower needs a
snapshot instead of normal log entries.

## 11. Commit Rule

A leader can advance `commitIndex` when a log index is stored on a quorum and
the entry belongs to the leader's current term.

The current-term restriction is a Raft safety rule. It prevents a leader from
incorrectly committing old-term entries by counting replication alone.

After `commitIndex` advances, Raft wakes the apply loop.

## 12. Apply Loop

The apply loop moves committed entries into the state machine:

```text
for index in lastApplied+1 .. commitIndex
        |
        v
load LogEntry(index)
        |
        v
unwrap ClientCommand
        |
        v
skip duplicate client command if already applied
        |
        v
stateMachine.Apply(entry)
        |
        v
lastApplied = index
        |
        v
notify read waiters and client waiters
```

The key invariant:

```text
lastApplied advances after Apply returns, not before.
```

This matters for linearizable reads. A read may proceed when
`lastApplied >= readIndex`, so `lastApplied` must represent real applied state.

## 13. ReadIndex And Lease Reads

Reads do not need to append a log entry, but they still need leadership safety.

The read path is:

```text
record readIndex = commitIndex
        |
        v
confirm leadership
        |
        v
wait until lastApplied >= readIndex
        |
        v
stateMachine.Get(key)
```

`go-kv` supports two modes:

- `heartbeat`: confirm leadership by sending heartbeat RPCs;
- `lease`: reuse recent quorum acknowledgements within a lease window.

During joint consensus, leadership confirmation requires a quorum from both old
and new configurations.

## 14. Snapshots And Log Compaction

Snapshots keep the Raft log bounded.

Local snapshot flow:

```text
log size exceeds threshold
        |
        v
capture snapshotIndex = lastApplied
        |
        v
read term at snapshotIndex
        |
        v
stateMachine.GetSnapshot
        |
        v
store.SaveSnapshot
        |
        v
store.CompactLog(snapshotIndex)
```

InstallSnapshot flow:

```text
leader sends snapshot to lagging follower
        |
        v
follower persists snapshot
        |
        v
follower applies snapshot to state machine
        |
        v
follower compacts covered logs
        |
        v
follower advances commitIndex and lastApplied
```

State-machine snapshot apply/export is serialized with normal apply/read paths
because an LSM snapshot may rewrite the state-machine directory.

## 15. Membership Changes

Membership changes use Raft joint consensus.

The transition has two phases:

1. **Joint config**: both old and new peer sets are active.
2. **Final config**: only the new peer set remains.

During the joint phase, quorum checks must pass in both configurations. This
rule applies to commits and ReadIndex leadership confirmation.

## 16. Concrete State Ownership

The Raft paper describes the logical state. The code adds locks, caches, and
waiter maps so the same state can be used safely by goroutines. Treat the table
below as the ownership map before changing the implementation.

| State | Main fields | Protected by | Notes |
|---|---|---|---|
| Term and vote | `currentTerm`, `votedFor` | `r.mu` plus stable storage writes | Must be persisted before the node relies on the new term or vote. |
| Node role | `state` | atomic value, usually changed while holding `r.mu` | Fast checks may read it without locking. Transitions still need normal Raft locking. |
| Log bounds | `commitIndex`, `lastApplied`, `cachedLastLogIndex` | `r.mu`; apply is serialized by `applyMu` | `lastApplied` only advances after state-machine apply or snapshot-covered skip. |
| Follower log mutation | `store.TruncateLog`, `store.AppendEntries` on follower | `appendEntriesMu` | Prevents concurrent AppendEntries calls from interleaving truncate and append. |
| Leader progress | `nextIndex`, `matchIndex`, `lastAck` | `r.mu` | `matchIndex` and successful `nextIndex` movement are monotonic. |
| State machine | `stateMachine.Apply`, `Get`, `GetSnapshot`, `ApplySnapshot` | `stateMachineMu` | Snapshot install can rewrite the LSM directory, so it must not overlap with apply or read. |
| Client dedupe | `clientSessions`, `pendingClientRequests`, `pendingLogClients` | `r.mu` | Provides at-most-once execution for retried client commands. |
| Read waiters | `lastAppliedCond`, `notifyApply` | `r.mu` | Wakes ReadIndex and write waiters after apply progress. |

Several fields are not in the Raft paper because they are implementation
bookkeeping: `cachedLastLogIndex`, `applyMu`, `appendEntriesMu`,
`stateMachineMu`, `notifyApply`, and the pending client maps. They are not new
consensus rules. They exist to preserve the paper's rules in a concurrent Go
implementation.

## 17. AppendEntries Phase Diagram

Follower-side `AppendEntries` is split into phases so disk I/O does not hold the
main Raft lock for a long time:

| Phase | Lock state | Work |
|---|---|---|
| Phase 0 | hold `appendEntriesMu` | Serialize this RPC against other follower log mutations. |
| Phase 1 | hold `r.mu` briefly | Check term, update follower state, reset election timer, handle heartbeat fast path. |
| Phase 2 | no `r.mu`, still hold `appendEntriesMu` | Read local log, detect conflicts, truncate conflicting entries, append new entries. |
| Phase 3 | hold `r.mu` briefly | Verify term did not change, update `cachedLastLogIndex`, advance follower commit index. |

The key reason for this shape is the truncate/append window. Without
`appendEntriesMu`, one goroutine could truncate entries while another goroutine
or apply loop reads the same range. The apply path therefore also acquires
`appendEntriesMu` before collecting committed entries from storage.

Conflict handling follows Raft Section 5.3:

```text
for each incoming entry:
    if local entry is missing:
        append incoming entries from here
    if local term differs:
        truncate from this index
        append incoming entries from here
    otherwise:
        keep the existing matching entry
```

This matters because appending blindly after every successful consistency check
would duplicate or overwrite entries that are already correct. Truncating too
early can temporarily remove committed entries that the apply loop is about to
read.

## 18. Leader Replication Internals

A leader chooses between normal log replication and snapshot installation for
each follower:

```text
nextIndex[peer] < first local log index
        |
        v
send snapshot

otherwise
        |
        v
send AppendEntries with at most MaxEntriesPerAppendEntries entries
```

`prepareAppendEntriesArgs` is careful about three separate cases:

1. The requested previous log index is covered by the current snapshot. The
   follower needs an InstallSnapshot RPC.
2. The local store has a sparse gap or unavailable tail. The leader refreshes
   `cachedLastLogIndex`, clamps `nextIndex`, and retries later instead of
   misclassifying the peer as snapshot-bound.
3. The peer is simply behind but the leader still has the needed entries. The
   leader sends a bounded batch of log entries.

AppendEntries replies are also term-checked. If the reply belongs to an old
leader term or the node is no longer leader, it is ignored. If the reply carries
a higher term, the node steps down. A successful reply only moves progress
forward:

```text
newNextIndex  = prevLogIndex + len(entries) + 1
newMatchIndex = newNextIndex - 1
```

Failure replies move `nextIndex` backward using conflict information, but never
below `matchIndex + 1`.

## 19. Apply, Client Retry, And ReadIndex Internals

The apply path has two stages.

First, `fetchEntriesToApply` collects committed log entries:

```text
hold appendEntriesMu
hold r.mu
for i = lastApplied+1 .. commitIndex:
    read store.GetEntry(i)
    if entry is missing and a snapshot covers i:
        advance lastApplied to snapshot.LastIncludedIndex
        continue
    if entry is missing and no snapshot covers i:
        fatal, because committed data is unavailable
release locks
```

Second, `dispatchEntries` applies entries:

```text
unwrap ClientCommand
if client request already applied:
    complete waiter without applying again
else if config change:
    update Raft membership state
else:
    hold stateMachineMu
    stateMachine.Apply(entry)
    release stateMachineMu
completeAppliedEntry(index)
```

`completeAppliedEntry` is the only place that advances `lastApplied` for normal
entries. It also updates client dedupe state and notifies any waiter registered
by `waitForAppliedLog`.

ReadIndex depends on this invariant:

```text
lastApplied >= readIndex  means the local state machine contains every write
committed before the read was admitted.
```

That is why `waitForAppliedLog` and read waiters always recheck `lastApplied`
under `r.mu` after registering or timing out. A notification can race with
waiter registration; the recheck closes that race.

## 20. Snapshot And Compaction Lock Order

Snapshots connect the Raft log layer and the LSM state-machine layer. The code
uses a strict lock order to avoid applying data from one point in time while
exporting or installing data from another.

Local snapshot creation:

```text
hold stateMachineMu
hold r.mu
check threshold and isSnapshotting
capture snapshotIndex = lastApplied
read term at snapshotIndex
mark isSnapshotting
release r.mu
export stateMachine.GetSnapshot while stateMachineMu is still held
release stateMachineMu
async: SaveSnapshot
async: hold appendEntriesMu then r.mu
async: publish snapshot reference and CompactLog(snapshotIndex)
```

InstallSnapshot on a follower:

```text
hold r.mu briefly for term and stale-snapshot checks
release r.mu
create snapshot object
hold stateMachineMu
recheck term/index under r.mu
SaveSnapshot
stateMachine.ApplySnapshot
hold appendEntriesMu
CompactLog(snapshot.LastIncludedIndex)
hold r.mu
advance snapshot, commitIndex, lastApplied, cachedLastLogIndex
broadcast lastAppliedCond
```

The important rule is that log compaction is allowed only after a snapshot that
covers the compacted indexes has been persisted or installed. The apply loop may
skip missing entries only when a stored snapshot covers them.

## 21. Important Concurrency Rules

Raft is a concurrent system. The main rules are:

- `r.mu` protects Raft state such as term, vote, indexes, peer sets, and leader
  progress.
- `appendEntriesMu` serializes follower log mutation.
- `stateMachineMu` serializes state-machine apply, snapshot export, and snapshot
  install.
- `lastAppliedCond` wakes read requests waiting for apply progress.
- `snapshotWG` and apply waiters are drained during shutdown.

When modifying code, prefer preserving these lock-order expectations rather than
adding ad-hoc locks around individual symptoms.

## 22. Failure Cases To Understand

The implementation explicitly handles these cases:

- stale RequestVote or AppendEntries terms;
- higher-term replies that force a leader to step down;
- follower logs that conflict with the leader;
- followers that are behind the compacted log and need snapshots;
- missing local log entries after compaction;
- duplicate client retries;
- snapshot-covered entries that are already represented by state-machine data;
- shutdown while clients are waiting for apply results.

## 23. Checklist For Raft Changes

Before changing Raft behavior, ask:

1. Can this move `lastApplied` before the state machine really applied?
2. Can this serve a read without current leadership?
3. Can this break monotonic `matchIndex` or `nextIndex` progress?
4. Can this compact a log entry not covered by a snapshot?
5. Can this apply a duplicate client command twice?
6. Does the change still work during joint consensus?
7. Does it pass race tests and a real multi-node E2E test?

These questions catch most correctness regressions before they reach production
style tests.

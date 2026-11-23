# shardsqlite implementation plan

## Goal

Build a networked, Raft-replicated SQLite system. Raft log is the authority. SQLite WAL is a local, page-level materialization of committed Raft entries. The two are never aliased: Raft stores ordered `TxBatch` values; SQLite WAL stores the page frames those entries contain.

## Decisions

- Single writer: Raft leader only. All writes route through leader VFS.
- Interception: custom SQLite VFS wrapping `-wal` file writes, gating local WAL commit on Raft quorum.
- Log payload: raw WAL frames. No SQL statement replication.
- Language/stack: Go, `ncruces/go-sqlite3` (first-class VFS), `hashicorp/raft`.
- Quorum failure: block COMMIT, return an I/O error. Never commit locally without quorum.
- Reads: stale follower reads only. Linearizable reads are out of scope for now; `hashicorp/raft` v1.8 has no `ReadIndex` API.
- Follower apply: shm-aware live apply into an open DB.
- Snapshot: `sqlite3` online backup plus last applied index; Raft stores snapshot, trims log.
- Checkpoint: local maintenance on each node; autocheckpoint disabled everywhere.
- Applied-index marker: Raft snapshot index is baseline; fsynced sidecar tracks post-snapshot progress.
- Entry shape: one entry per transaction up to a size cap; chunk beyond cap, commit flag on last entry.
- Forbidden via authorizer: `VACUUM`, `page_size`, `journal_mode`.

## Shapes

```go
type NodeID uint64
type RaftIndex uint64
type DBName string
type WALGeneration uint64

type Frame struct {
    PageNo    uint32
    DBSize    uint32 // nonzero only on commit frame
    Salt1     uint32
    Salt2     uint32
    Checksum1 uint32
    Checksum2 uint32
    Page      []byte
}

type TxBatch struct {
    DB     DBName
    Gen    WALGeneration
    Frames []Frame
    Commit bool
}

type SnapshotPayload struct {
    DBFile      []byte
    Applied     RaftIndex
    Gen         WALGeneration
    PageSize    uint32
}

type AppliedMarker struct {
    Index RaftIndex
    Gen   WALGeneration
}
```

Tagged errors: `ErrQuorumLost`, `ErrNotLeader`, `ErrWALMismatch`, `ErrFrameChecksum`, `ErrAppliedAhead`.

## Production call graphs

Writer commit:

```text
App COMMIT
  → raftvfs.File.WriteAt (db-wal)
    → wal.ParseFrame
    → raftvfs.pending.Append
    → wal.IsCommitFrame
    → proposer.Propose(TxBatch)
      → raft.Node.Apply
        → fsm.Apply
          → applier.ApplyFrames
            → wal.VerifyChecksumChain
            → realFile.WriteAt (db-wal)
            → shm.PublishFrameCount
            → applied.Store.Write
    → realFile.Sync
```

Follower apply:

```text
Raft transport.AppendEntries
  → raft.Node
    → fsm.Apply(TxBatch)
      → applied.Store.SkipIfBehind
      → applier.ApplyFrames
        → wal.VerifyChecksumChain
        → vfs.File.WriteAt (db-wal)
        → shm.PublishFrameCount
        → applied.Store.Write
      → reader query (stale)
```

Snapshot:

```text
threshold reached
  → fsm.Snapshot
    → snapshotMu.Lock
    → sqlite.Conn.BackupInit
    → Backup.Step
    → SnapshotPayload{DBFile, Applied, Gen, PageSize}
    → raft snapshot store
    → raft log Trim
    → snapshotMu.Unlock
```

Restore:

```text
fsm.Restore(payload)
  → close all DB connections
  → replace db file with payload.DBFile
  → reset -wal and -shm
  → applied.Store.Write(payload.Applied)
  → reopen apply + read connections
```

Reads:

```text
ReadRequest
  → readRouter
    → stale: sqlite.Conn(mode=ro) on follower
    → linearizable:
        raft.VerifyLeadership
        raft.ReadIndex
        waitApplied(index)
        sqlite.Conn on leader
```

## Errors and requirements

- `ErrQuorumLost`: return `SQLITE_IOERR` from `WriteAt`. No commit frame written locally.
- `ErrNotLeader`: reject write before buffering.
- `ErrFrameChecksum`: quarantine node, restore from snapshot.
- `ErrAppliedAhead`: fail startup; never reapply past frames.
- `ErrWALMismatch`: reset follower WAL and restore from snapshot.
- Raft handles transient network retry internally; no duplicate retry layer.

Per-node requirements (`R`):

- `wal.ParseFrame`: page size from WAL header.
- `proposer.Propose`: raft node, leadership state.
- `applier.ApplyFrames`: VFS handles, shm writer, sidecar store.
- `fsm.Snapshot`: SQLite backup connection, snapshot mutex.
- `readRouter`: applied-index getter, raft handle.

## Execution waves and gates

Wave 1 — foundation.
- `go.mod` with pinned versions.
- Config parser.
- WAL header/frame parsing and checksum verification.
- Sidecar applied-index store.
- Spike: `ncruces/go-sqlite3` VFS wrapper intercepts `-wal` write; confirm shm interfaces.

Gate: unit tests for frame parsing, checksum chain, commit-marker detection, sidecar round-trip.

Wave 2 — leader gate.
- Raft node bootstrap with `raft-boltdb/v2` stable store and log store.
- VFS commit buffering and Raft proposal.
- FSM `Apply` skeleton writing frames locally after quorum.
- Authorizer forbidding `VACUUM`, `page_size`, `journal_mode`.

Gate: single-node integration test. COMMIT succeeds with quorum; with transport stopped, COMMIT returns error and local DB unchanged.

Wave 3 — replication.
- Follower FSM apply via shm-aware live apply.
- Three-node in-process cluster using real raft + real SQLite on temp dirs.
- Read-only follower connections.

Gate: after each committed transaction, compare leader and follower DB file hashes; run reads against followers.

Wave 4 — snapshots, checkpoint, reads.
- SQLite backup snapshot + Raft snapshot/restore.
- Manual checkpoint path, autocheckpoint disabled.
- Read gate: stale reads only; no leadership barrier.

Gate: kill leader, verify follower promotion and no lost commits; force snapshot then restart node from snapshot; verify stale vs linearizable read behavior.

Wave 5 — hardening.
- Crash/restart with sidecar ahead/behind.
- WAL reset and salt change propagation.
- Large transaction chunking.
- Full static analysis.

Gate: full integration suite; formatting and vet clean.

## Verification commands

Run from module root:

```sh
gofmt -l .
go vet ./...
golangci-lint run ./...  # only when installed
go test ./... -race
```

All must be clean. If `golangci-lint` is absent, report that explicitly; do not suppress findings with bare `//nolint`.

## Risks

- shm-aware live apply is the hardest part; publish `mxFrame` under correct `xShmLock` sequence or readers see torn state. Mitigate with byte-for-byte leader/follower comparison after every batch.
- `ncruces/go-sqlite3` runs SQLite in WASM; throughput may be lower than cgo. Measure in wave 2, switch driver only on evidence.
- Blocking inside `File.WriteAt` holds the WAL write lock across a Raft round trip. Acceptable with single writer.
- Snapshot must capture the same index as the DB file bytes. Hold `snapshotMu` across backup and marker capture.
- Raft snapshot index alone cannot deduplicate replay after crash; sidecar is mandatory.

## Keeping the sharding path open

Sharding is not implemented now. These decisions avoid a rewrite later.

- One Raft group owns one shard. A node can host many groups. `Group` and `Shard` IDs exist in `internal/ids` and are carried in every `TxBatch`, even with a single group and shard.
- Every `TxBatch` targets exactly one `DBName` and one `Shard`. Cross-shard atomic transactions are forbidden now; they would need two-phase commit across groups.
- Applied markers, WAL generations, and snapshots are per shard. Store paths are keyed by group and shard, not global.
- Routing from request key to shard lives outside the VFS. The VFS and FSM only see a shard-local `DBName`, so a router can be added later without changing them.
- Read-index and stale reads are per group. No global quorum assumption is baked into the read path.
- Snapshot payload carries shard identity and applied index. A group can be moved or copied without renaming files.
- Membership is per group. Static one-group config is the starting point; dynamic membership is a later change.

## Implementation status

Implemented and verified by `trust-gateway` properties:

- `wal-checksum-chain`: WAL header/frame parsing, checksum chain, commit marker.
- `applied-monotonic`: contiguous applied-index tracking, WAL generation checks, restart persistence.
- `tx-batch-boundary`: chunking, commit flag placement, encode/decode, WAL position preservation.
- `commit-gated-by-quorum`: single-node COMMIT blocked until Raft quorum; no local commit after quorum loss.
- `replication-across-nodes`: three-node Raft cluster; frames replayed on every node; reads see committed data.
- `snapshot-restore-roundtrip`: SQLite backup snapshot carries applied index; restore rebuilds DB and marker.
- `stale-reads-only`: reads are never gated by leadership; staleness is visible through applied index.
- `local-checkpoint`: manual `CHECKPOINT_TRUNCATE` empties WAL without losing data.
- `direct-shm-live-apply`: raw WAL frames plus direct `-shm` wal-index update; a long-lived reader sees applied frames without reopening.
- `restart-recovery`: Raft log store and applied marker survive node restart; committed data still readable.
- `wal-reset-propagation`: WAL header reset after checkpoint is shipped as a Raft batch; follower salt matches leader and sees post-reset write.
- `tla-bend-verification`: Bend port of the bounded TLA+ model; exhaustive reachability check agrees with TLC on 399,906 distinct states.

## TLA+ and Bend state verification

Bounded TLA+ model in [ShardSQLite.tla](/home/peacewalker/.t3/projects/shardsqlite/spec/ShardSQLite.tla), checked by TLC in property `tla-state-verification`. Bounds: 3 nodes, `MaxLog = 3`, `MaxPending = 1`, one transaction value. State variables cover Raft log, per-node commit/applied store, WAL generation, WAL reset base, WAL image, wal-index `mxFrame`, pending VFS writes, leadership, and snapshot.

Invariants: type bounds, applied never exceeds commit, store covers commit, shm matches applied, pending only on leader, at most one leader, committed entries have quorum, WAL generation matches applied reset count, WAL base matches last applied reset, WAL image matches log prefix, snapshot safe.

`spec/ShardSQLite.bend` ports the same constants, state variables, `Next` actions, and invariants into Bend. Its explorer starts from the port of `Init`, applies every enabled action, and checks all invariants on each newly reached state. The check script compiles with `bend ... -o` and runs the native binary. `bend spec/ShardSQLite.bend --verdict` rechecks the port with Bend's proven kernel.

Resolved decision:

- User chose stale reads only. Linearizable reads and read-index are out of scope. Reads may lag applied index; clients requiring read-your-write must route to the leader or wait for applied index externally.

Bootstrap requires a byte-identical main DB file on every node. WAL frames cannot bootstrap an empty database.

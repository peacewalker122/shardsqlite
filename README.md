# shardsqlite

Experimental SQLite replication built on Raft. The Raft log is the source of truth. SQLite WAL frames are the local page-level materialization of committed Raft entries. The two are not treated as the same log.

This project is not production ready. Reads are stale by default. Sharding is not implemented, but shard and group identifiers are already carried in every replicated batch.

Instrumentation: [Indonesian README](README.id.md).

## How it works

1. The app commits a write through a custom SQLite VFS.
2. The VFS buffers WAL frames and blocks the commit.
3. The leader proposes those frames to Raft.
4. After quorum, the FSM writes the frames to every node's `-wal` file.
5. Direct `-shm` wal-index updates make the new frames visible to long-lived readers.
6. Checkpoint and WAL reset stay local, but the new WAL header ships with the next batch.

Only the Raft leader accepts writes. Followers are read-only.

## Components

| Path | Role |
| --- | --- |
| `internal/raftvfs` | SQLite VFS commit gate and WAL frame capture |
| `internal/raftlog` | Replicated batch encoding, chunking, and WAL position |
| `internal/cluster` | Raft node, FSM, snapshot, restore, read gate |
| `internal/replay` | WAL frame replay and direct `-shm` updates |
| `internal/applied` | Durable per-node applied index and WAL generation |
| `internal/checkpoint` | Manual local checkpoint |
| `internal/wal` | WAL header, frame, and checksum handling |
| `cmd/shardsqlited` | Node daemon |
| `spec` | TLA+ state model, TLC config, and Bend port of the model |
| `properties` | Trust-gateway property checks |

## Build and run

Go 1.26 or newer is required.

```sh
go build ./cmd/shardsqlited
./shardsqlited \
  -node-id=1 \
  -data-dir=./data-node-1 \
  -db=./shard-node-1.db \
  -bind=127.0.0.1:9200 \
  -vfs=shardsqlite-node-1
```

For a three-node cluster, pass the same peer list to each daemon:

```sh
./shardsqlited \
  -node-id=1 \
  -data-dir=./data-node-1 \
  -db=./shard-node-1.db \
  -bind=127.0.0.1:9200 \
  -vfs=shardsqlite-node-1 \
  -peers=1@127.0.0.1:9200,2@127.0.0.1:9201,3@127.0.0.1:9202
```

Every node must start from the same main database bytes. WAL frames cannot bootstrap an empty database.

Application connections use the registered VFS:

```text
file:/path/shard.db?vfs=shardsqlite-node-1&_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)&_pragma=synchronous(FULL)
```

## Reads

Reads are stale. A client sees state up to the applied index of the node it reads from. Read-your-write is not guaranteed unless the client routes to the leader or waits for the applied index.

Linearizable reads are out of scope. `hashicorp/raft` v1.8 does not expose a read-index API.

## Verification

`trust-gateway` runs all `properties/<name>/check` scripts:

```sh
trust-gateway
```

Current checks cover WAL checksums, applied-index monotonicity, batch chunking, quorum-gated commit, three-node replication, snapshot and restore, stale reads, local checkpoint, direct `-shm` apply, restart recovery, WAL reset propagation, and a bounded TLA+ model.

Go checks:

```sh
gofmt -l .
go vet ./...
go build ./...
go test ./... -race
```

The TLA+ model lives in `spec/ShardSQLite.tla`. The trust-gateway check uses `spec/ShardSQLiteSmall.cfg` with 3 nodes, `MaxLog = 3`, and `MaxPending = 1`. TLC reports no invariant violation in that bounded model. It is not an unbounded proof.

`spec/ShardSQLite.bend` ports the same bounded model to Bend: the same state variables, all `Next` actions, and all 11 invariants. `properties/tla-bend-verification/check` compiles it and explores the reachable state space. Both checkers agree on 399,906 distinct states. `bend spec/ShardSQLite.bend --verdict` also checks the port with Bend's proven kernel.

## Limits

- One Raft group owns one shard. Sharding logic is not implemented.
- Every batch targets one SQLite database. Cross-shard transactions are not supported.
- `VACUUM`, `page_size` changes, and `journal_mode` changes are not designed for replication.
- Snapshot restore requires careful coordination with open application connections.
- Long-lived readers see new frames only through direct `-shm` updates on the apply path.

See [PLAN.md](PLAN.md) for the design, decisions, and remaining gaps.

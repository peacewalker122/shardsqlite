--------------------------- MODULE ShardSQLite ---------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Nodes, MaxLog, MaxPending, TxVals

EntryType == [kind: {"Tx", "Reset"}, val: 0..MaxLog]
Quorum == {S \in SUBSET Nodes : Cardinality(S) = 2}
NoSnapshot == [idx |-> 0, gen |-> 0, base |-> 0, wal |-> <<>>]

VARIABLES log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot

vars == <<log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot>>

Entry(k, v) == [kind |-> k, val |-> v]
MaxIdx(a, b) == IF a >= b THEN a ELSE b
ProposedLen(n) == Len(log) + Len(pending[n])

RECURSIVE ResetCount(_, _)
ResetCount(s, to) ==
  IF to = 0 THEN 0
  ELSE ResetCount(s, to - 1) + (IF s[to].kind = "Reset" THEN 1 ELSE 0)

ProposalResets(n) == ResetCount(log \o pending[n], Len(log \o pending[n]))

RECURSIVE LastReset(_, _)
LastReset(s, to) ==
  IF to = 0 THEN 0
  ELSE IF s[to].kind = "Reset" THEN to ELSE LastReset(s, to - 1)

RECURSIVE TxSince(_, _, _)
TxSince(s, from, to) ==
  IF from > to THEN <<>>
  ELSE IF s[from].kind = "Tx" THEN <<s[from].val>> \o TxSince(s, from + 1, to)
  ELSE TxSince(s, from + 1, to)

Init ==
  /\ log = <<>>
  /\ commitIdx = [n \in Nodes |-> 0]
  /\ applied = [n \in Nodes |-> 0]
  /\ store = [n \in Nodes |-> 0]
  /\ gen = [n \in Nodes |-> 0]
  /\ base = [n \in Nodes |-> 0]
  /\ wal = [n \in Nodes |-> <<>>]
  /\ shm = [n \in Nodes |-> 0]
  /\ pending = [n \in Nodes |-> <<>>]
  /\ mode = [n \in Nodes |-> IF n = 1 THEN "Leader" ELSE "Follower"]
  /\ snapshot = NoSnapshot

ChangeLeader(n) ==
  /\ mode[n] # "Leader"
  /\ mode' = [m \in Nodes |-> IF m = n THEN "Leader" ELSE "Follower"]
  /\ pending' = [m \in Nodes |-> <<>>]
  /\ UNCHANGED <<log, commitIdx, applied, store, gen, base, wal, shm, snapshot>>

BufferTx(n, v) ==
  /\ mode[n] = "Leader"
  /\ v \in TxVals
  /\ ProposedLen(n) < MaxLog
  /\ Len(pending[n]) < MaxPending
  /\ pending' = [pending EXCEPT ![n] = @ \o <<Entry("Tx", v)>>]
  /\ UNCHANGED <<log, commitIdx, applied, store, gen, base, wal, shm, mode, snapshot>>

AppendReset(n) ==
  /\ mode[n] = "Leader"
  /\ applied[n] = commitIdx[n]
  /\ ProposalResets(n) < MaxLog
  /\ ProposedLen(n) < MaxLog
  /\ Len(pending[n]) < MaxPending
  /\ pending' = [pending EXCEPT ![n] = @ \o <<Entry("Reset", ProposalResets(n) + 1)>>]
  /\ UNCHANGED <<log, commitIdx, applied, store, gen, base, wal, shm, mode, snapshot>>

Replicate(n, q, len) ==
  /\ mode[n] = "Leader"
  /\ q \in Nodes
  /\ q # n
  /\ len \in 1..ProposedLen(n)
  /\ len > store[q]
  /\ store' = [store EXCEPT ![q] = len]
  /\ UNCHANGED <<log, commitIdx, applied, gen, base, wal, shm, pending, mode, snapshot>>

CommitPending(n) ==
  /\ mode[n] = "Leader"
  /\ Len(pending[n]) > 0
  /\ store[n] >= ProposedLen(n)
  /\ \E Q \in Quorum : \A q \in Q : store[q] >= ProposedLen(n)
  /\ log' = log \o pending[n]
  /\ commitIdx' = [commitIdx EXCEPT ![n] = Len(log')]
  /\ pending' = [pending EXCEPT ![n] = <<>>]
  /\ UNCHANGED <<applied, store, gen, base, wal, shm, mode, snapshot>>

NotifyCommit(n, q) ==
  /\ mode[n] = "Leader"
  /\ q \in Nodes
  /\ store[q] >= commitIdx[n]
  /\ commitIdx[q] < commitIdx[n]
  /\ commitIdx' = [commitIdx EXCEPT ![q] = MaxIdx(commitIdx[q], commitIdx[n])]
  /\ UNCHANGED <<log, applied, store, gen, base, wal, shm, pending, mode, snapshot>>

ApplyEntry(n) ==
  LET i == applied[n] + 1
      e == log[i]
  IN /\ i <= commitIdx[n]
     /\ i <= Len(log)
     /\ applied' = [applied EXCEPT ![n] = i]
     /\ shm' = [shm EXCEPT ![n] = i]
     /\ IF e.kind = "Reset"
        THEN /\ gen' = [gen EXCEPT ![n] = e.val]
             /\ base' = [base EXCEPT ![n] = i]
             /\ wal' = [wal EXCEPT ![n] = <<>>]
        ELSE /\ gen' = gen
             /\ base' = base
             /\ wal' = [wal EXCEPT ![n] = wal[n] \o <<e.val>>]
     /\ UNCHANGED <<log, commitIdx, store, pending, mode, snapshot>>

TakeSnapshot(n) ==
  /\ applied[n] > 0
  /\ snapshot' = [idx |-> applied[n], gen |-> gen[n], base |-> base[n], wal |-> wal[n]]
  /\ UNCHANGED <<log, commitIdx, applied, store, gen, base, wal, shm, pending, mode>>

RestoreFromSnapshot(n) ==
  /\ snapshot.idx > 0
  /\ snapshot.idx <= commitIdx[n]
  /\ applied' = [applied EXCEPT ![n] = snapshot.idx]
  /\ gen' = [gen EXCEPT ![n] = snapshot.gen]
  /\ base' = [base EXCEPT ![n] = snapshot.base]
  /\ wal' = [wal EXCEPT ![n] = snapshot.wal]
  /\ shm' = [shm EXCEPT ![n] = snapshot.idx]
  /\ UNCHANGED <<log, commitIdx, store, pending, mode, snapshot>>

StaleRead ==
  UNCHANGED vars

Next ==
  \/ \E n \in Nodes : ChangeLeader(n)
  \/ \E n \in Nodes, v \in TxVals : BufferTx(n, v)
  \/ \E n \in Nodes : AppendReset(n)
  \/ \E n \in Nodes, q \in Nodes, len \in 1..MaxLog : Replicate(n, q, len)
  \/ \E n \in Nodes : CommitPending(n)
  \/ \E n \in Nodes, q \in Nodes : NotifyCommit(n, q)
  \/ \E n \in Nodes : ApplyEntry(n)
  \/ \E n \in Nodes : TakeSnapshot(n)
  \/ \E n \in Nodes : RestoreFromSnapshot(n)
  \/ StaleRead

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ log \in Seq(EntryType)
  /\ commitIdx \in [Nodes -> 0..MaxLog]
  /\ applied \in [Nodes -> 0..MaxLog]
  /\ store \in [Nodes -> 0..MaxLog]
  /\ gen \in [Nodes -> 0..MaxLog]
  /\ base \in [Nodes -> 0..MaxLog]
  /\ wal \in [Nodes -> Seq(0..MaxLog)]
  /\ shm \in [Nodes -> 0..MaxLog]
  /\ pending \in [Nodes -> Seq(EntryType)]
  /\ mode \in [Nodes -> {"Leader", "Follower"}]
  /\ snapshot \in [idx: 0..MaxLog, gen: 0..MaxLog, base: 0..MaxLog, wal: Seq(0..MaxLog)]
  /\ \A n \in Nodes : Len(pending[n]) <= MaxPending

AppliedNeverExceedsCommit ==
  \A n \in Nodes : applied[n] <= commitIdx[n] /\ commitIdx[n] <= Len(log)

StoreCoversCommit ==
  \A n \in Nodes : store[n] >= commitIdx[n]

ShmMatchesApplied ==
  \A n \in Nodes : shm[n] = applied[n]

PendingOnlyLeader ==
  \A n \in Nodes : mode[n] # "Leader" => Len(pending[n]) = 0

AtMostOneLeader ==
  Cardinality({n \in Nodes : mode[n] = "Leader"}) <= 1

CommittedHasQuorum ==
  \A n \in Nodes :
    commitIdx[n] = 0 \/ \E Q \in Quorum : \A q \in Q : store[q] >= commitIdx[n]

GenerationMatchesLog ==
  \A n \in Nodes : gen[n] = ResetCount(log, applied[n])

BaseMatchesLog ==
  \A n \in Nodes : base[n] = LastReset(log, applied[n])

WalMatchesLog ==
  \A n \in Nodes : wal[n] = TxSince(log, base[n] + 1, applied[n])

SnapshotSafe ==
  /\ snapshot.idx <= Len(log)
  /\ snapshot.base <= snapshot.idx
  /\ snapshot.gen = ResetCount(log, snapshot.idx)
  /\ snapshot.wal = TxSince(log, snapshot.base + 1, snapshot.idx)
=============================================================================

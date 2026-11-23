import Base

type Entry is Data:
  Tx{val: Nat}
  Reset{val: Nat}

type Mode is Data:
  Leader{}
  Follower{}

type Snapshot is Data:
  Snapshot{idx: Nat, gen: Nat, base: Nat, wal: List<&2, Nat>}

type Vec3<a, -A: Kind(a)> is Kind(a):
  Vec3{first: A, second: A, third: A}

type State is Data:
  State{
    log: List<&2, Entry>,
    commitIdx: Vec3<&2, Nat>,
    applied: Vec3<&2, Nat>,
    store: Vec3<&2, Nat>,
    gen: Vec3<&2, Nat>,
    base: Vec3<&2, Nat>,
    wal: Vec3<&2, List<&2, Nat>>,
    shm: Vec3<&2, Nat>,
    pending: Vec3<&2, List<&2, Entry>>,
    mode: Vec3<&2, Mode>,
    snapshot: Snapshot
  }

type Action is Data:
  ChangeLeader{node: Nat}
  BufferTx{node: Nat, val: Nat}
  AppendReset{node: Nat}
  Replicate{node: Nat, peer: Nat, len: Nat}
  CommitPending{node: Nat}
  NotifyCommit{node: Nat, peer: Nat}
  ApplyEntry{node: Nat}
  TakeSnapshot{node: Nat}
  RestoreFromSnapshot{node: Nat}
  StaleRead{}

def max_log() -> Nat:
  3n

def max_pending() -> Nat:
  1n

def tx_val() -> Nat:
  1n

def nodes() -> List<&2, Nat>:
  [1n, 2n, 3n]

def node_get(-A: Data, +v: Vec3<&2, A>, node: Nat) -> A:
  match v:
    case Vec3{first, second, third}:
      match node:
        case 0n: first
        case 1n: first
        case 2n: second
        case 3n: third
        case 4n+p: first

def node_set(-A: Data, +v: Vec3<&2, A>, node: Nat, x: A) -> Vec3<&2, A>:
  match v:
    case Vec3{first, second, third}:
      match node:
        case 0n: Vec3{x, second, third}
        case 1n: Vec3{x, second, third}
        case 2n: Vec3{first, x, third}
        case 3n: Vec3{first, second, x}
        case 4n+p: Vec3{x, second, third}

def is_leader(m: Mode) -> Bool:
  match m:
    case Leader{}: True{}
    case Follower{}: False{}

def is_reset(e: Entry) -> Bool:
  match e:
    case Reset{v}: True{}
    case Tx{v}: False{}

def entry_val(e: Entry) -> Nat:
  match e:
    case Reset{v}: v
    case Tx{v}: v

def bool_and(a: Bool, b: Bool) -> Bool:
  Bool.and(a, b)

def bool_not(a: Bool) -> Bool:
  Bool.not(a)

def list_len_entry(xs: List<&2, Entry>) -> Nat:
  List.length(&2, Entry, xs)

def append_entry(+xs: List<&2, Entry>, e: Entry) -> List<&2, Entry>:
  List.append(&2, Entry, xs, [e])

def append_nat(+xs: List<&2, Nat>, v: Nat) -> List<&2, Nat>:
  List.append(&2, Nat, xs, [v])

def reset_add(e: Entry) -> Nat:
  match e:
    case Reset{v}: 1n
    case Tx{v}: 0n

def reset_count_upto(k: Nat, +xs: List<&2, Entry>, acc: Nat) -> Nat:
  match k:
    case 0n: acc
    case 1n+p:
      match xs:
        case Nil{}: acc
        case h <> t:
          reset_count_upto(p, t, (acc + reset_add(h) : Nat))

def reset_count(+xs: List<&2, Entry>, to: Nat) -> Nat:
  reset_count_upto(to, xs, 0n)

def last_reset_go(+xs: List<&2, Entry>, +i: Nat, +best: Nat) -> Nat:
  match xs:
    case Nil{}: best
    case h <> t:
      match h:
        case Reset{v}: last_reset_go(t, (i + 1n : Nat), i)
        case Tx{v}: last_reset_go(t, (i + 1n : Nat), best)

def last_reset(+xs: List<&2, Entry>, to: Nat) -> Nat:
  last_reset_go(List.take(&2, Entry, xs, to), 1n, 0n)

def tx_emit(e: Entry, rest: List<&2, Nat>) -> List<&2, Nat>:
  match e:
    case Tx{v}: v <> rest
    case Reset{v}: rest

def tx_since_go(k: Nat, +xs: List<&2, Entry>, i: Nat) -> List<&2, Nat>:
  match k:
    case 0n: Nil{}
    case 1n+p:
      match xs:
        case Nil{}: Nil{}
        case h <> t:
          tx_emit(h, tx_since_go(p, t, (i + 1n : Nat)))

def tx_since_if(+xs: List<&2, Entry>, +from: Nat, to: Nat, ok: Bool) -> List<&2, Nat>:
  match ok:
    case True{}:
      tail = List.drop(&2, Entry, xs, Nat.sub(from, 1n))
      tx_since_go(Nat.add(Nat.sub(to, from), 1n), tail, from)
    case False{}: Nil{}

def tx_since(+xs: List<&2, Entry>, +from: Nat, +to: Nat) -> List<&2, Nat>:
  tx_since_if(xs, from, to, Nat.is_le(from, to))

def proposed_len(+s: State, node: Nat) -> Nat:
  match s:
    case State{log, _, _, _, _, _, _, _, pending, _, _}:
      Nat.add(list_len_entry(log), list_len_entry(node_get(List<&2, Entry>, pending, node)))

def proposal_resets(+s: State, node: Nat) -> Nat:
  match s:
    case State{log, _, _, _, _, _, _, _, pending, _, _}:
      +combined = List.append(&2, Entry, log, node_get(List<&2, Entry>, pending, node))
      reset_count(combined, list_len_entry(combined))

def nat_list_valid(+xs: List<&2, Nat>) -> Bool:
  match xs:
    case Nil{}: True{}
    case h <> t:
      Bool.and(Nat.is_le(h, max_log()), nat_list_valid(t))

def entry_list_valid(+xs: List<&2, Entry>) -> Bool:
  match xs:
    case Nil{}: True{}
    case h <> t:
      Bool.and(Nat.is_le(entry_val(h), max_log()), entry_list_valid(t))

def vec_valid(+v: Vec3<&2, List<&2, Nat>>) -> Bool:
  match v:
    case Vec3{a, b, c}:
      Bool.and(nat_list_valid(a), Bool.and(nat_list_valid(b), nat_list_valid(c)))

def vec_entries_valid(+v: Vec3<&2, List<&2, Entry>>) -> Bool:
  match v:
    case Vec3{a, b, c}:
      Bool.and(entry_list_valid(a), Bool.and(entry_list_valid(b), entry_list_valid(c)))

def vec_le_max(+v: Vec3<&2, Nat>) -> Bool:
  match v:
    case Vec3{a, b, c}:
      Bool.and(Nat.is_le(a, max_log()), Bool.and(Nat.is_le(b, max_log()), Nat.is_le(c, max_log())))

def pending_bounded(+v: Vec3<&2, List<&2, Entry>>) -> Bool:
  match v:
    case Vec3{a, b, c}:
      Bool.and(Nat.is_le(list_len_entry(a), max_pending()),
        Bool.and(Nat.is_le(list_len_entry(b), max_pending()), Nat.is_le(list_len_entry(c), max_pending())))

def leader_add(m: Mode) -> Nat:
  match m:
    case Leader{}: 1n
    case Follower{}: 0n

def leader_count(+v: Vec3<&2, Mode>) -> Nat:
  match v:
    case Vec3{a, b, c}:
      (leader_add(a) + leader_add(b) + leader_add(c) : Nat)

def quorum_covers(+v: Vec3<&2, Nat>, +idx: Nat) -> Bool:
  match v:
    case Vec3{a, b, c}:
      Bool.or(
        Bool.and(Nat.is_ge(a, idx), Nat.is_ge(b, idx)),
        Bool.or(Bool.and(Nat.is_ge(a, idx), Nat.is_ge(c, idx)),
          Bool.and(Nat.is_ge(b, idx), Nat.is_ge(c, idx))))

def nat_list_eq(+xs: List<&2, Nat>, +ys: List<&2, Nat>) -> Bool:
  match xs ys:
    case Nil{} Nil{}: True{}
    case x <> xt y <> yt:
      Bool.and(Nat.is_eq(x, y), nat_list_eq(xt, yt))
    case _ _: False{}

def entry_eq(a: Entry, b: Entry) -> Bool:
  match a b:
    case Tx{x} Tx{y}: Nat.is_eq(x, y)
    case Reset{x} Reset{y}: Nat.is_eq(x, y)
    case _ _: False{}

def entry_list_eq(+xs: List<&2, Entry>, +ys: List<&2, Entry>) -> Bool:
  match xs ys:
    case Nil{} Nil{}: True{}
    case x <> xt y <> yt:
      Bool.and(entry_eq(x, y), entry_list_eq(xt, yt))
    case _ _: False{}

def snapshot_valid(+snap: Snapshot) -> Bool:
  match snap:
    case Snapshot{idx, gen, base, wal}:
      Bool.and(Nat.is_le(idx, max_log()),
        Bool.and(Nat.is_le(gen, max_log()),
          Bool.and(Nat.is_le(base, max_log()), nat_list_valid(wal))))

def snapshot_safe(+snap: Snapshot, +log: List<&2, Entry>) -> Bool:
  match snap:
    case Snapshot{idx, gen, base, wal}:
      Bool.and(Nat.is_le(idx, list_len_entry(log)),
        Bool.and(Nat.is_le(base, idx),
          Bool.and(Nat.is_eq(gen, reset_count(log, idx)),
            nat_list_eq(wal, tx_since(log, (base + 1n : Nat), idx)))))

def leader_pending_ok(leader: Bool, pending_len: Nat) -> Bool:
  match leader:
    case True{}: True{}
    case False{}: Nat.is_eq(pending_len, 0n)

def commit_quorum_ok(is_zero: Bool, +store: Vec3<&2, Nat>, ci: Nat) -> Bool:
  match is_zero:
    case True{}: True{}
    case False{}: quorum_covers(store, ci)

def node_invariants(+s: State, +node: Nat) -> Bool:
  match s:
    case State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}:
      +ci = node_get(Nat, commitIdx, node)
      +ap = node_get(Nat, applied, node)
      +st = node_get(Nat, store, node)
      +gn = node_get(Nat, gen, node)
      +bs = node_get(Nat, base, node)
      +wl = node_get(List<&2, Nat>, wal, node)
      +sm = node_get(Nat, shm, node)
      +pend = node_get(List<&2, Entry>, pending, node)
      md = node_get(Mode, mode, node)
      pending_ok = leader_pending_ok(is_leader(md), list_len_entry(pend))
      quorum_ok = commit_quorum_ok(Nat.is_eq(ci, 0n), store, ci)
      Bool.and(Nat.is_le(ap, ci),
        Bool.and(Nat.is_le(ci, list_len_entry(log)),
          Bool.and(Nat.is_ge(st, ci),
            Bool.and(Nat.is_eq(sm, ap),
              Bool.and(pending_ok,
                Bool.and(quorum_ok,
                  Bool.and(Nat.is_eq(gn, reset_count(log, ap)),
                    Bool.and(Nat.is_eq(bs, last_reset(log, ap)),
                      nat_list_eq(wl, tx_since(log, (bs + 1n : Nat), ap))))))))))

def all_invariants(+s: State) -> Bool:
  match s:
    case State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}:
      Bool.and(entry_list_valid(log),
        Bool.and(vec_entries_valid(pending),
          Bool.and(vec_valid(wal),
            Bool.and(vec_le_max(commitIdx),
              Bool.and(vec_le_max(applied),
                Bool.and(vec_le_max(store),
                  Bool.and(vec_le_max(gen),
                    Bool.and(vec_le_max(base),
                      Bool.and(vec_le_max(shm),
                        Bool.and(pending_bounded(pending),
                          Bool.and(Nat.is_le(leader_count(mode), 1n),
                            Bool.and(snapshot_valid(snapshot),
                              Bool.and(snapshot_safe(snapshot, log),
                                Bool.and(node_invariants(s, 1n),
                                  Bool.and(node_invariants(s, 2n), node_invariants(s, 3n))))))))))))))))

def init_state() -> State:
  empty_log: List<&2, Entry> = Nil{}
  +zero: Vec3<&2, Nat> = Vec3{0n, 0n, 0n}
  empty_wal: Vec3<&2, List<&2, Nat>> = Vec3{Nil{}, Nil{}, Nil{}}
  empty_pending: Vec3<&2, List<&2, Entry>> = Vec3{Nil{}, Nil{}, Nil{}}
  initial_mode: Vec3<&2, Mode> = Vec3{Leader{}, Follower{}, Follower{}}
  snap: Snapshot = Snapshot{0n, 0n, 0n, Nil{}}
  State{empty_log, zero, zero, zero, zero, zero, empty_wal, zero, empty_pending, initial_mode, snap}

def f_log(+s: State) -> List<&2, Entry>:
  match s:
    case State{log, _, _, _, _, _, _, _, _, _, _}: log

def f_commit(+s: State) -> Vec3<&2, Nat>:
  match s:
    case State{_, commitIdx, _, _, _, _, _, _, _, _, _}: commitIdx

def f_applied(+s: State) -> Vec3<&2, Nat>:
  match s:
    case State{_, _, applied, _, _, _, _, _, _, _, _}: applied

def f_store(+s: State) -> Vec3<&2, Nat>:
  match s:
    case State{_, _, _, store, _, _, _, _, _, _, _}: store

def f_gen(+s: State) -> Vec3<&2, Nat>:
  match s:
    case State{_, _, _, _, gen, _, _, _, _, _, _}: gen

def f_base(+s: State) -> Vec3<&2, Nat>:
  match s:
    case State{_, _, _, _, _, base, _, _, _, _, _}: base

def f_wal(+s: State) -> Vec3<&2, List<&2, Nat>>:
  match s:
    case State{_, _, _, _, _, _, wal, _, _, _, _}: wal

def f_shm(+s: State) -> Vec3<&2, Nat>:
  match s:
    case State{_, _, _, _, _, _, _, shm, _, _, _}: shm

def f_pending(+s: State) -> Vec3<&2, List<&2, Entry>>:
  match s:
    case State{_, _, _, _, _, _, _, _, pending, _, _}: pending

def f_mode(+s: State) -> Vec3<&2, Mode>:
  match s:
    case State{_, _, _, _, _, _, _, _, _, mode, _}: mode

def f_snapshot(+s: State) -> Snapshot:
  match s:
    case State{_, _, _, _, _, _, _, _, _, _, snapshot}: snapshot

def with_log(+s: State, log: List<&2, Entry>) -> State:
  match s:
    case State{_, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}:
      State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}

def with_commit(+s: State, commitIdx: Vec3<&2, Nat>) -> State:
  match s:
    case State{log, _, applied, store, gen, base, wal, shm, pending, mode, snapshot}:
      State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}

def with_applied(+s: State, applied: Vec3<&2, Nat>) -> State:
  match s:
    case State{log, commitIdx, _, store, gen, base, wal, shm, pending, mode, snapshot}:
      State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}

def with_store(+s: State, store: Vec3<&2, Nat>) -> State:
  match s:
    case State{log, commitIdx, applied, _, gen, base, wal, shm, pending, mode, snapshot}:
      State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}

def with_gen(+s: State, gen: Vec3<&2, Nat>) -> State:
  match s:
    case State{log, commitIdx, applied, store, _, base, wal, shm, pending, mode, snapshot}:
      State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}

def with_base(+s: State, base: Vec3<&2, Nat>) -> State:
  match s:
    case State{log, commitIdx, applied, store, gen, _, wal, shm, pending, mode, snapshot}:
      State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}

def with_wal(+s: State, wal: Vec3<&2, List<&2, Nat>>) -> State:
  match s:
    case State{log, commitIdx, applied, store, gen, base, _, shm, pending, mode, snapshot}:
      State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}

def with_shm(+s: State, shm: Vec3<&2, Nat>) -> State:
  match s:
    case State{log, commitIdx, applied, store, gen, base, wal, _, pending, mode, snapshot}:
      State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}

def with_pending(+s: State, pending: Vec3<&2, List<&2, Entry>>) -> State:
  match s:
    case State{log, commitIdx, applied, store, gen, base, wal, shm, _, mode, snapshot}:
      State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}

def with_mode(+s: State, mode: Vec3<&2, Mode>) -> State:
  match s:
    case State{log, commitIdx, applied, store, gen, base, wal, shm, pending, _, snapshot}:
      State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}

def with_snapshot(+s: State, snapshot: Snapshot) -> State:
  match s:
    case State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, _}:
      State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}

def when(cond: Bool, next: State) -> Maybe<&2, State>:
  match cond:
    case True{}: Some{next}
    case False{}: None{}

def empty_pending_vec() -> Vec3<&2, List<&2, Entry>>:
  Vec3{Nil{}, Nil{}, Nil{}}

def step_change_leader(+s: State, +node: Nat) -> Maybe<&2, State>:
  allowed = bool_not(is_leader(node_get(Mode, f_mode(s), node)))
  followers: Vec3<&2, Mode> = Vec3{Follower{}, Follower{}, Follower{}}
  next = with_mode(with_pending(s, empty_pending_vec()), node_set(Mode, followers, node, Leader{}))
  when(allowed, next)

def step_buffer_tx(+s: State, +node: Nat, +val: Nat) -> Maybe<&2, State>:
  mode_ok = is_leader(node_get(Mode, f_mode(s), node))
  val_ok = Nat.is_eq(val, tx_val())
  len_ok = Nat.is_lt(proposed_len(s, node), max_log())
  pend_len = list_len_entry(node_get(List<&2, Entry>, f_pending(s), node))
  pend_ok = Nat.is_lt(pend_len, max_pending())
  allowed = bool_and(mode_ok, bool_and(val_ok, bool_and(len_ok, pend_ok)))
  pend = node_get(List<&2, Entry>, f_pending(s), node)
  new_pend = node_set(List<&2, Entry>, f_pending(s), node, append_entry(pend, Tx{val}))
  when(allowed, with_pending(s, new_pend))

def step_append_reset(+s: State, +node: Nat) -> Maybe<&2, State>:
  mode_ok = is_leader(node_get(Mode, f_mode(s), node))
  applied = node_get(Nat, f_applied(s), node)
  commit = node_get(Nat, f_commit(s), node)
  caught_up = Nat.is_eq(applied, commit)
  +resets = proposal_resets(s, node)
  resets_ok = Nat.is_lt(resets, max_log())
  len_ok = Nat.is_lt(proposed_len(s, node), max_log())
  pend_len = list_len_entry(node_get(List<&2, Entry>, f_pending(s), node))
  pend_ok = Nat.is_lt(pend_len, max_pending())
  allowed = bool_and(mode_ok, bool_and(caught_up, bool_and(resets_ok, bool_and(len_ok, pend_ok))))
  pend = node_get(List<&2, Entry>, f_pending(s), node)
  new_pend = node_set(List<&2, Entry>, f_pending(s), node, append_entry(pend, Reset{(resets + 1n : Nat)}))
  when(allowed, with_pending(s, new_pend))

def step_replicate(+s: State, +node: Nat, +peer: Nat, +len: Nat) -> Maybe<&2, State>:
  mode_ok = is_leader(node_get(Mode, f_mode(s), node))
  distinct = Nat.is_ne(peer, node)
  within = Nat.is_le(len, proposed_len(s, node))
  ahead = Nat.is_gt(len, node_get(Nat, f_store(s), peer))
  allowed = bool_and(mode_ok, bool_and(distinct, bool_and(within, ahead)))
  new_store = node_set(Nat, f_store(s), peer, len)
  when(allowed, with_store(s, new_store))

def step_commit_pending(+s: State, +node: Nat) -> Maybe<&2, State>:
  mode_ok = is_leader(node_get(Mode, f_mode(s), node))
  +pend = node_get(List<&2, Entry>, f_pending(s), node)
  has_pending = Nat.is_gt(list_len_entry(pend), 0n)
  +plen = proposed_len(s, node)
  local_ok = Nat.is_ge(node_get(Nat, f_store(s), node), plen)
  quorum_ok = quorum_covers(f_store(s), plen)
  allowed = bool_and(mode_ok, bool_and(has_pending, bool_and(local_ok, quorum_ok)))
  +new_log = List.append(&2, Entry, f_log(s), pend)
  new_commit = node_set(Nat, f_commit(s), node, list_len_entry(new_log))
  new_pending = node_set(List<&2, Entry>, f_pending(s), node, Nil{})
  next = with_pending(with_commit(with_log(s, new_log), new_commit), new_pending)
  when(allowed, next)

def step_notify_commit(+s: State, +node: Nat, +peer: Nat) -> Maybe<&2, State>:
  mode_ok = is_leader(node_get(Mode, f_mode(s), node))
  +ci = node_get(Nat, f_commit(s), node)
  +peer_ci = node_get(Nat, f_commit(s), peer)
  stored = node_get(Nat, f_store(s), peer)
  stored_ok = Nat.is_ge(stored, ci)
  behind = Nat.is_lt(peer_ci, ci)
  allowed = bool_and(mode_ok, bool_and(stored_ok, behind))
  new_commit = node_set(Nat, f_commit(s), peer, Nat.max(peer_ci, ci))
  when(allowed, with_commit(s, new_commit))

def apply_entry_state(+s: State, +node: Nat, +i: Nat, m: Maybe<&2, Entry>) -> Maybe<&2, State>:
  match m:
    case None{}: None{}
    case Some{e}:
      match e:
        case Reset{v}:
          applied = node_set(Nat, f_applied(s), node, i)
          shm = node_set(Nat, f_shm(s), node, i)
          gen = node_set(Nat, f_gen(s), node, v)
          base = node_set(Nat, f_base(s), node, i)
          wal = node_set(List<&2, Nat>, f_wal(s), node, Nil{})
          Some{with_wal(with_base(with_gen(with_shm(with_applied(s, applied), shm), gen), base), wal)}
        case Tx{v}:
          applied = node_set(Nat, f_applied(s), node, i)
          shm = node_set(Nat, f_shm(s), node, i)
          old_wal = node_get(List<&2, Nat>, f_wal(s), node)
          wal = node_set(List<&2, Nat>, f_wal(s), node, append_nat(old_wal, v))
          Some{with_wal(with_shm(with_applied(s, applied), shm), wal)}

def maybe_when(cond: Bool, m: Maybe<&2, State>) -> Maybe<&2, State>:
  match cond:
    case True{}: m
    case False{}: None{}

def step_apply_entry(+s: State, +node: Nat) -> Maybe<&2, State>:
  +ci = node_get(Nat, f_commit(s), node)
  +ap = node_get(Nat, f_applied(s), node)
  +i = (ap + 1n : Nat)
  log_len = list_len_entry(f_log(s))
  ok = bool_and(Nat.is_le(i, ci), Nat.is_le(i, log_len))
  entry = List.get(&2, Entry, f_log(s), Nat.sub(i, 1n))
  maybe_when(ok, apply_entry_state(s, node, i, entry))

def step_take_snapshot(+s: State, +node: Nat) -> Maybe<&2, State>:
  +ap = node_get(Nat, f_applied(s), node)
  ok = Nat.is_gt(ap, 0n)
  snap: Snapshot = Snapshot{ap, node_get(Nat, f_gen(s), node), node_get(Nat, f_base(s), node),
    node_get(List<&2, Nat>, f_wal(s), node)}
  when(ok, with_snapshot(s, snap))

def restore_from(+s: State, +node: Nat, snap: Snapshot) -> Maybe<&2, State>:
  match snap:
    case Snapshot{+idx, +gen, +base, wal}:
      ok = bool_and(Nat.is_gt(idx, 0n), Nat.is_le(idx, node_get(Nat, f_commit(s), node)))
      applied = node_set(Nat, f_applied(s), node, idx)
      shm = node_set(Nat, f_shm(s), node, idx)
      gen_v = node_set(Nat, f_gen(s), node, gen)
      base_v = node_set(Nat, f_base(s), node, base)
      wal_v = node_set(List<&2, Nat>, f_wal(s), node, wal)
      next = with_wal(with_base(with_gen(with_shm(with_applied(s, applied), shm), gen_v), base_v), wal_v)
      when(ok, next)

def step_restore_snapshot(+s: State, +node: Nat) -> Maybe<&2, State>:
  restore_from(s, node, f_snapshot(s))

def step_stale_read(+s: State) -> Maybe<&2, State>:
  Some{s}

def maybe_to_list(m: Maybe<&2, State>) -> List<&2, State>:
  match m:
    case None{}: Nil{}
    case Some{s}: [s]

def step(action: Action, +s: State) -> List<&2, State>:
  match action:
    case ChangeLeader{node}: maybe_to_list(step_change_leader(s, node))
    case BufferTx{node, val}: maybe_to_list(step_buffer_tx(s, node, val))
    case AppendReset{node}: maybe_to_list(step_append_reset(s, node))
    case Replicate{node, peer, len}: maybe_to_list(step_replicate(s, node, peer, len))
    case CommitPending{node}: maybe_to_list(step_commit_pending(s, node))
    case NotifyCommit{node, peer}: maybe_to_list(step_notify_commit(s, node, peer))
    case ApplyEntry{node}: maybe_to_list(step_apply_entry(s, node))
    case TakeSnapshot{node}: maybe_to_list(step_take_snapshot(s, node))
    case RestoreFromSnapshot{node}: maybe_to_list(step_restore_snapshot(s, node))
    case StaleRead{}: maybe_to_list(step_stale_read(s))

def node_actions(+n: Nat) -> List<&2, Action>:
  [ChangeLeader{n}, BufferTx{n, tx_val()}, AppendReset{n}, CommitPending{n}, ApplyEntry{n},
    TakeSnapshot{n}, RestoreFromSnapshot{n},
    Replicate{n, 1n, 1n}, Replicate{n, 1n, 2n}, Replicate{n, 1n, 3n},
    Replicate{n, 2n, 1n}, Replicate{n, 2n, 2n}, Replicate{n, 2n, 3n},
    Replicate{n, 3n, 1n}, Replicate{n, 3n, 2n}, Replicate{n, 3n, 3n},
    NotifyCommit{n, 1n}, NotifyCommit{n, 2n}, NotifyCommit{n, 3n}]

def all_actions() -> List<&2, Action>:
  +a = List.append(&2, Action, node_actions(1n), node_actions(2n))
  +b = List.append(&2, Action, a, node_actions(3n))
  List.append(&2, Action, b, [StaleRead{}])

def step_all(+s: State, acts: List<&2, Action>) -> List<&2, State>:
  match acts:
    case Nil{}: Nil{}
    case a <> t:
      List.append(&2, State, step(a, s), step_all(s, t))

def show_entry(e: Entry) -> String:
  match e:
    case Tx{v}: "T" ++ Nat.show(v)
    case Reset{v}: "R" ++ Nat.show(v)

def show_mode(m: Mode) -> String:
  match m:
    case Leader{}: "L"
    case Follower{}: "F"

def show_nat_list(xs: List<&2, Nat>) -> String:
  List.show(~(&2), ~(Nat), ~(Nat.show), xs)

def show_entry_list(xs: List<&2, Entry>) -> String:
  List.show(~(&2), ~(Entry), ~(show_entry), xs)

def show_vec_nat(v: Vec3<&2, Nat>) -> String:
  match v:
    case Vec3{a, b, c}: "(" ++ Nat.show(a) ++ "," ++ Nat.show(b) ++ "," ++ Nat.show(c) ++ ")"

def show_vec_mode(v: Vec3<&2, Mode>) -> String:
  match v:
    case Vec3{a, b, c}: "(" ++ show_mode(a) ++ show_mode(b) ++ show_mode(c) ++ ")"

def show_vec_wal(v: Vec3<&2, List<&2, Nat>>) -> String:
  match v:
    case Vec3{a, b, c}: "(" ++ show_nat_list(a) ++ "," ++ show_nat_list(b) ++ "," ++ show_nat_list(c) ++ ")"

def show_vec_entries(v: Vec3<&2, List<&2, Entry>>) -> String:
  match v:
    case Vec3{a, b, c}: "(" ++ show_entry_list(a) ++ "," ++ show_entry_list(b) ++ "," ++ show_entry_list(c) ++ ")"

def show_snapshot(s: Snapshot) -> String:
  match s:
    case Snapshot{idx, gen, base, wal}:
      "S(" ++ Nat.show(idx) ++ "," ++ Nat.show(gen) ++ "," ++ Nat.show(base) ++ "," ++ show_nat_list(wal) ++ ")"

def show_state(s: State) -> String:
  match s:
    case State{log, commitIdx, applied, store, gen, base, wal, shm, pending, mode, snapshot}:
      show_entry_list(log) ++ "|" ++ show_vec_nat(commitIdx) ++ "|" ++ show_vec_nat(applied)
        ++ "|" ++ show_vec_nat(store) ++ "|" ++ show_vec_nat(gen) ++ "|" ++ show_vec_nat(base)
        ++ "|" ++ show_vec_wal(wal) ++ "|" ++ show_vec_nat(shm) ++ "|" ++ show_vec_entries(pending)
        ++ "|" ++ show_vec_mode(mode) ++ "|" ++ show_snapshot(snapshot)



type Acc is Data:
  Acc{work: List<&2, State>, visited: Set(), violation: Maybe<&2, State>}

def acc_work(+a: Acc) -> List<&2, State>:
  match a:
    case Acc{work, _, _}: work

def acc_visited(+a: Acc) -> Set():
  match a:
    case Acc{_, visited, _}: visited

def acc_violation(+a: Acc) -> Maybe<&2, State>:
  match a:
    case Acc{_, _, violation}: violation

def process_seen(seen: Bool, +state: State, rest: List<&2, State>, v1: Set()) -> Acc:
  match seen:
    case True{}: Acc{rest, v1, None{}}
    case False{}:
      v2 = Set.add(v1, show_state(state))
      successors = step_all(state, all_actions())
      Acc{List.append(&2, State, successors, rest), v2, None{}}

def process_has(r: Set() & Bool, +state: State, rest: List<&2, State>) -> Acc:
  (v1, seen) = r
  process_seen(seen, state, rest, v1)

def process_violation(ok: Bool, +state: State, rest: List<&2, State>, visited: Set()) -> Acc:
  match ok:
    case True{}: process_has(Set.has(visited, show_state(state)), state, rest)
    case False{}: Acc{rest, visited, Some{state}}

def process_one(+state: State, rest: List<&2, State>, visited: Set()) -> Acc:
  process_violation(all_invariants(state), state, rest, visited)

type ExploreResult is Data:
  ExploreResult{violation: Maybe<&2, State>, visited: Set(), remaining: List<&2, State>}

def explore(steps: Nat, violation: Maybe<&2, State>, work: List<&2, State>, visited: Set()) -> ExploreResult:
  match steps:
    case 0n: ExploreResult{None{}, visited, work}
    case 1n+k:
      match violation:
        case Some{bad}: ExploreResult{Some{bad}, visited, work}
        case None{}:
          match work:
            case Nil{}: ExploreResult{None{}, visited, Nil{}}
            case state <> rest:
              +acc = process_one(state, rest, visited)
              explore(k, acc_violation(acc), acc_work(acc), acc_visited(acc))

def result_message(result: Maybe<&2, State>) -> String:
  match result:
    case None{}: "ALL PROOFS CHECK: TLA invariants hold for every reachable state"
    case Some{bad}: "INVARIANT VIOLATION: " ++ show_state(bad)

def result_violation(+r: ExploreResult) -> Maybe<&2, State>:
  match r:
    case ExploreResult{violation, _, _}: violation

def result_visited(+r: ExploreResult) -> Set():
  match r:
    case ExploreResult{_, visited, _}: visited

def result_remaining(+r: ExploreResult) -> List<&2, State>:
  match r:
    case ExploreResult{_, _, remaining}: remaining

def list_len_state(xs: List<&2, State>) -> Nat:
  List.length(&2, State, xs)

def report(+r: ExploreResult) -> String:
  result_message(result_violation(r)) ++ " (visited " ++ Nat.show(Set.size(result_visited(r)))
    ++ ", remaining " ++ Nat.show(list_len_state(result_remaining(r))) ++ ")"

def main() -> IO(Unit):
  do IO<Unit>:
    IO.print(report(explore(4000000n, None{}, [init_state()], Set.new())))

# Claims

`Tx.Claim` takes up to `Limit` rows of one schema that match a condition,
marks each one as the caller's with a **claim mutation** (typically a status,
a lease and a generation), and answers the rows as stored. It is the operation
behind a work queue, an outbox relay, a scheduler or a worker pool. Two
concurrent claimers never receive the same row, a row whose lease expired can
be claimed again, and a holder whose lease was taken over cannot complete its
work. With a `Group`, a claim takes only the earliest unresolved row of each
group, for per-key FIFO.

Leases, owners, statuses and generations are ordinary schema attributes the
caller chooses. Forma provides the atomic take; the caller's condition decides
what is claimable, and the caller's completion decides what a claim means.

```go
due := &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{
	&forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "status", Value: `in:["pending","failed"]`},
		&forma.KvCondition{Attr: "next_attempt_at", Value: "lte:$now"},
	}},
	&forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "status", Value: "equals:publishing"},
		&forma.KvCondition{Attr: "locked_until", Value: "lte:$now"},
	}},
}}
res, err := tx.Claim(ctx, &forma.ClaimRequest{
	SchemaName: "workflow_outbox",
	Condition:  due,
	Sort:       []forma.OrderBy{{Attribute: "enqueued_at"}},
	Limit:      50,
	Updates:    map[string]any{"status": "publishing", "locked_by": relayID},
	UpdateOps: []forma.UpdateOp{
		forma.Increment("lock_generation", 1),
		forma.ServerTime("locked_until", 30*time.Second),
	},
})
```

## What a claim takes

A row is **eligible** when it matches `Condition` as the Tx sees it: the
latest committed hot-tier state plus the Tx's own writes. The federated engine
is never called.

Eligible rows are considered in `Sort` order, with the row ID ascending as the
final tiebreak, and at most `Limit` (1 to 1,000) are claimed. Forma selects
twice `Limit` candidates, then takes them in that order:

1. It tries each candidate's per-row lock **without waiting**. On PostgreSQL
   that is `pg_try_advisory_xact_lock` on the same key every Forma write,
   `AssertRow`, `Get` with `ForUpdate`, `UpdateWhere`, `Upsert` and claim
   takes first. A candidate another Forma transaction holds is **skipped**.
2. It re-checks `Condition`, and with a `Group` the head rule, on the
   candidates it got, under their row locks (`FOR UPDATE ... SKIP LOCKED`). A
   candidate that stopped matching after the select, for example because
   another claimer claimed and committed it, is dropped.
3. Each skipped or dropped candidate is replaced by the next one, until
   `Limit` rows are held or the candidates run out.

So on data no one else is writing, a claim answers exactly the first `Limit`
eligible rows, or all of them when fewer are eligible. Under concurrency it can
answer fewer, including none, while eligible rows still exist: they were held
by others, or past the candidates it selected. An empty result is not an
error: a claimer that gets one claims again later.

Each round tries only as many candidates as rows are still wanted, so a claim
holds no row it does not claim, with one exception: a candidate dropped at the
re-check keeps its lock until the Tx ends, because a transaction-scoped lock
cannot be released early. Writers of that row wait for the Tx.

## Group heads: per-group FIFO

With `Group`, a claim takes only the head of each group:

```go
Group: &forma.ClaimGroup{
	Key:   []string{"partition_key"},
	Order: []forma.OrderBy{{Attribute: "enqueued_at"}},
	Live:  &forma.KvCondition{Attr: "status", Value: `not_in:["published","dead_letter"]`},
},
```

Rows with equal `Key` values form a group. A null component equals a null, so
the rows that lack a key attribute form one group. A row is eligible when it
matches `Condition`, matches `Live`, and no other row of its group that
matches `Live` comes before it by `Order`, then row ID ascending. A null
`Order` value sorts last in either direction, as in `Sort`. So at most one row
of a group, its **head**, is claimable at a time, and a claim takes at most one
row per group.

`Live` decides what blocks a group, separately from `Condition`. A failed
message waiting for its retry matches `Live` but not `Condition`, so the
messages after it wait too. A head under an active claim blocks its group the
same way. When its lease expires the head itself is claimed again, not the
next row. Once the head stops matching `Live`, for example when it is
published, the next row becomes the head.

**No fall-through.** A head another transaction holds is skipped like any
held row, and no later row of its group is claimed in its place: the later
rows are not heads. The re-check under the lock tests the head rule again, so
a candidate that stopped being the head after the select is dropped, for
example because an earlier row became live again.

**Group order is the caller's.** Heads are judged on stored values. A row that
enters a group with an `Order` value earlier than a row already claimed does
not revoke that claim, and the two can then be held at once. A caller that
needs strict FIFO assigns `Order` values that only increase within a group,
as LTFlow requires of its enqueue time.

**A claimed row must stay its group's head.** Two rules keep the claim's own
write from handing the head on while the claimed row is still in flight:

- The claim may not move the row within its group. A `Key` or `Order`
  attribute bound to a column Forma stamps on every update
  (`ltbase_updated_at`, `ltbase_updated_by`, `ltbase_deleted_at`,
  `ltbase_deleted_by`) is refused, and so is a claim mutation that writes a
  `Key` or `Order` attribute. Either would let the next row of the group sort
  first and be claimed beside it.
- `Live` must keep matching a claimed row. That one is the caller's to get
  right, as Forma cannot tell what `Live` means: LTFlow's `not_in` published
  and dead-lettered keeps a `publishing` message live. A `Live` that the
  claim mutation stops matching, such as `status in pending, failed`, makes
  the next row the head as soon as the claim commits.

`Key` lists 1 to 4 attributes with no repeats, and `Order` at least one.
Every attribute of `Key`, `Order` and `Live` must be column-bound, because the
head rule compares rows of the schema with each other in `entity_main`. A
request that breaks a rule is refused with `ErrInvalidInput` naming the
attribute, before any SQL.

**Declare an index on the key, then the order**
([Schema indexes](schema-indexes.md)), for example
`{"name": "fifo", "attributes": ["partition_key", "enqueued_at"]}`. The head
rule looks up a row's earlier rows by key, and with the index it reads only
that row's group. A test asserts the plan. Without it, each lookup reads the
schema's whole range.

**What a group claim costs.** The candidate select applies the head rule to
every row that matches `Condition` and `Live` before it sorts and limits, not
only to the 2 × `Limit` it keeps, and each lookup reads the row's whole
group through the index's key prefix: the order comparison is a filter, and
rows no longer live, such as published messages still stored, are read too.
The cost grows with the matching rows times the group size. The rows whose
key is null are read for every lookup, so a schema where many rows lack a key
pays for them on each claim. In one measurement on a development machine,
with every due row in one partition, the candidate select took 106 ms with a
`Group` and 37 ms without at 4,000 due rows, and 1.19 s against 0.19 s at
16,000, where the planner left the index for a sequential scan. A backlog of
that shape builds when a failed head holds back its partition. Deleting
resolved rows keeps groups small; #685 tracks the candidate select's cost.

## Never waiting

A claim does not wait for a lock another Forma transaction holds. Its lock
attempts never wait, the re-check skips row locks held elsewhere, and its
writes touch only rows it holds. Two claimers therefore make progress in
parallel, and a slow holder never makes a claim wait. A try-lock cannot be
part of a deadlock cycle.

A holder can still make a claim answer less. Every claimer selects from the
head of the same backlog, so while open transactions hold the first
2 × `Limit` eligible rows, a claim with the same condition and sort answers
none, however many eligible rows lie beyond them. A committed claim's rows
stop being eligible, so claims that commit promptly keep this short.

Moving a unique key's entry can wait for another transaction writing the
same value ([Row IDs and Unique Keys](unique-keys.md)), so a claim mutation may
not change an attribute of a declared unique key. Such a request is refused
with `ErrInvalidInput` before any SQL.

One wait remains, on CDC rather than on another write, assertion or claim. A
CDC flush marks the `change_log` entries it exported in one statement, and the
claim's write of a row's entry waits while that statement holds the entry. The
statement can itself be waiting for an open transaction that rewrote another
entry of the same flush, so the claim can wait as long as that transaction
stays open. If the claim already wrote an entry the statement then reaches,
PostgreSQL detects a deadlock and aborts one of the two; when it aborts the
claim, the claim answers `ErrRetryableConflict` and poisons its Tx. Every write
of several rows shares this exposure (#686).

## The claim mutation

`Updates`, then `UpdateOps`, are applied to each claimed row through the same
path as `Tx.Update`: the same merge, validation, stamps, EAV rows and
`change_log` row. At least one of them is required. `$now` and `server_time`
read the Tx's clock, so a lease of `ServerTime("locked_until", 30*time.Second)`
and a condition `locked_until lte:$now` name instants on one clock.

**The mutation must make every claimed row stop matching `Condition`.** After
the writes, the claim evaluates `Condition` on the claimed rows again. If any
still matches, it answers `ErrInvalidInput` ("the claim mutation must make a
claimed row stop matching the claim condition") and poisons the Tx, so nothing
it wrote commits. Without this rule a committed claim could be claimed again
at once by the next claimer. With it, a row comes back only when the caller's
own condition makes it eligible again, for example when its lease passes.

`Records` holds each claimed row as stored after the mutation, in selection
order. If the Tx commits and nothing later in it touches those rows, each
record equals the committed row.

## Fencing

A claim fences by **compare-and-set**, not by a lock the holder keeps. The
claim increments a generation attribute the caller chooses; the holder
completes with a `Tx.Update` whose precondition names the generation it
claimed ([Conditional writes](conditional-writes.md)):

```go
_, err := tx.Update(ctx, &forma.EntityOperation{
	EntityIdentifier: forma.EntityIdentifier{SchemaName: "workflow_outbox", RowID: id},
	Preconditions:    &forma.KvCondition{Attr: "lock_generation", Value: fmt.Sprintf("equals:%d", gen)},
	Updates:          map[string]any{"status": "published"},
})
```

If the lease expired and another claimer took the row, the generation moved
on, and the stale holder's completion answers `ErrPreconditionFailed` and
writes nothing. Renewing a lease is the same kind of CAS `Update`. There is no
background expiry: a lease ends when the caller's condition at claim time says
it has.

An update cannot clear an attribute yet (#684), so a completion or failure
cannot set `locked_until` to null. End the lease by moving it instead, for
example with `ServerTime("locked_until", 0)`, and keep `status` as what tells
a held row from a released one.

## Transactions, errors and limits

A claim is part of its Tx. If the Tx rolls back, no claimed row changes: no
lease, no generation, no `change_log` row, and the locks are released, so the
rows are eligible to the next claimer exactly as before.

| Outcome | Poisons the Tx? |
| --- | --- |
| An empty result | no |
| `ErrInvalidInput` refusing the request before any SQL: no schema or an unknown one, a nil, invalid or empty-group `Condition`, `Limit` outside 1..1000, an unknown `Sort` attribute, no `Updates` or `UpdateOps`, update operators `Update` would refuse, a mutation of a unique-key attribute, a `Group` that breaks a rule above | no |
| `ErrInvalidInput` for a mutation that left a claimed row matching `Condition` | yes |
| any failure once a row is held, and every failed statement | yes |

Each claimed row past the first counts as one more operation against the Tx's
operation limit, and the writes count against its row limit
([Limits](transactions.md#limits)). A breach is `ErrInvalidInput` and poisons
the Tx.

A claim holds an advisory lock on every candidate it got, up to 2 × `Limit`,
until its Tx ends. PostgreSQL's lock table has room for about
`max_locks_per_transaction` × (`max_connections` + `max_prepared_transactions`)
locks across all sessions (6,400 with the defaults), and a transaction that
finds it full fails with "out of shared memory". Several concurrent claims of
`Limit: 1000` can approach that; raise `max_locks_per_transaction` before
running them.

The candidate select is the query `Tx.Query` runs for the same condition and
sort, so a candidate is always a row `Tx.Query` would return. That query
orders and counts every matching row before it applies the limit, so a claim
costs time in proportion to the backlog it selects from: about 12 ms with 100
due rows and 135 ms with 10,000, for a 10-row claim without a declared index.
Making it cost in proportion to the limit is #685.

## Aurora DSQL

This section describes the `aurora-dsql` engine (#633,
[Aurora DSQL](aurora-dsql.md)). CI runs it on PostgreSQL emulating DSQL. It
has not yet run on a cluster.

DSQL has no advisory locks and no `SKIP LOCKED`, and its `FOR UPDATE` takes no
lock, so there is nothing to skip. On this engine the take step writes the
guard row of every candidate in the round and keeps all of them. The re-check
reads them without a locking clause. Two claimers that select the same rows
both write their guard rows, and one of them fails with `40001`, which Forma
answers as `ErrRetryableConflict`. Forma does not retry. The caller re-runs
its Tx, and a relay treats the error as "claim again". The take step is
therefore write-and-let-the-commit-decide, and the API is the same. The
outcome contract on DSQL:

- exclusive claims, rollback, fencing and group heads hold as on PostgreSQL;
- a contending claimer receives `ErrRetryableConflict` instead of a smaller
  result;
- never waiting, and skipping held rows, are PostgreSQL-only.

The head rule reads the transaction's snapshot on DSQL. A concurrent commit
that changes whether an earlier row is live is not in the claiming
transaction's write set, so it raises no `40001`, and a claim can be decided
on a head that is stale by that one commit. If the earlier row was just
published, the claim is conservative: it takes nothing from the group, and
the next claim takes the new head. If an earlier row was just made live again,
the group-order obligation above applies, as on PostgreSQL.

The DSQL transaction limit of 3,000 rows bounds `Limit` times the physical
rows each claimed row writes, guard row included. The coordination suite's
claim scenarios run on this engine in CI (`dsql-emulated`).

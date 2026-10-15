# Transactions (`Transactor.InTx`)

`EntityManager` runs every write in its own PostgreSQL transaction, and an
atomic batch covers one operation kind on one schema. `Transactor.InTx` puts
several writes, across schemas and of any kind, into one transaction, and
lets the caller decide each write from the results of the earlier ones (#648,
part of #593). The motivating shape is a workflow step that advances an
instance, appends history and inserts an outbox message: if any part is
missing, the step is wrong.

The API is Go only. An HTTP transaction endpoint is deferred to #656, because
an interactive transaction cannot span HTTP requests without server-side
session state.

```go
txr, err := factory.NewTransactor(em) // em from factory.NewEntityManager*
if err != nil {
	return err
}
err = txr.InTx(ctx, func(ctx context.Context, tx forma.Tx) error {
	instance, err := tx.Update(ctx, advanceInstance)
	if err != nil {
		return fmt.Errorf("advance instance: %w", err)
	}
	if _, err := tx.Create(ctx, historyEvent(instance)); err != nil {
		return fmt.Errorf("append history: %w", err)
	}
	_, err = tx.Create(ctx, outboxMessage(instance))
	return err
})
if forma.IsRetryable(err) {
	// run the whole function again
}
```

`factory.NewTransactor` accepts only a manager the factory built; a decorator
around one is refused with `ErrInvalidInput`, because the transactor needs the
manager's pool and services. The transactor holds no resources of its own.

## Lifecycle

`InTx` begins one READ COMMITTED transaction and calls `fn` exactly once.

| `fn` ends by | Transaction | `InTx` returns |
| --- | --- | --- |
| returning nil, Tx not poisoned, ctx alive | commits | nil, or the classified COMMIT error |
| returning nil over a poisoned Tx | rolls back | an error matching `ErrTxAborted` and the poisoning cause |
| returning an error | rolls back | that error, wrapped: `errors.Is`/`errors.As` still match it, and a 4xx carrier keeps its published message |
| panicking | rolls back | nothing: the panic continues with its original value |
| `runtime.Goexit` (e.g. `t.FailNow`) | rolls back | nothing: the goroutine exits |
| ctx cancelled or past its deadline before COMMIT | rolls back | an error matching `context.Canceled` or `context.DeadlineExceeded` |

Whatever `fn` returns, a rolled-back `InTx` error also carries the poisoning
cause and the context's error. A `fn` that wraps a deadlock into its own error
type, or swallows it and returns something else, still yields an `InTx` error
for which `forma.IsRetryable` is true.

Once COMMIT is sent, #645's classification applies: an unanswered COMMIT is
`ErrCommitOutcomeUnknown` (see [Error Handling](error-handling.md)).

Other rules:

- **Finished Tx.** After `InTx` returns, every `Tx` method returns `ErrTxDone`.
  Do not keep the `Tx` past `fn`.
- **No nesting.** `InTx` with a context derived from the one `fn` received
  returns `ErrInvalidInput` without starting a transaction. There are no
  savepoints.
- **Isolation** is READ COMMITTED, the level `Update` already pins for #457. It
  is not configurable.
- **No retries.** Forma never re-runs `fn`.

## What a Tx write does

`Tx.Create`, `Tx.Update` and `Tx.Delete` take the same `*EntityOperation` as
`EntityManager` and run the manager's own service code against the
transaction. Validation, computed-field stripping, the per-row advisory lock,
`Update`'s merge semantics and strict-update setting, version stamping and the
`change_log` write are the same code path, not a copy. A test compares the
rows the two paths store and finds them identical apart from row IDs and time
stamps.

Inside one transaction:

- A later operation sees earlier writes: a second `Update` of a row merges
  onto the first.
- Repeated writes to one row stamp strictly increasing versions and leave one
  hot `change_log` entry carrying the last stamp.
- The per-row advisory lock is held until commit or rollback. Another writer
  of the same row, inside or outside a Tx, waits for it and then merges onto
  the committed result.
- No other connection sees any write until commit; then all are visible
  together.

Two transactions that lock rows in opposite orders can deadlock. PostgreSQL
aborts one of them, and its `InTx` returns `ErrRetryableConflict`. Writing rows
in one consistent order in every transaction (for example by schema, then row
ID) avoids this kind of deadlock.

`Tx.Update` and `Tx.Delete` honour `Preconditions` and `UpdateOps` exactly
as `EntityManager` does ([Conditional writes](conditional-writes.md)).
`Tx.Upsert` creates a row, or, when a live row holds its row ID or unique-key
target, locks that row and leaves it or updates it under a precondition
([Row IDs and Unique Keys](unique-keys.md#upsert)). `Tx.Claim` takes up to a
limit of rows matching a condition without waiting for rows other
transactions hold, and marks each with a claim mutation ([Claims](claims.md)).
Batch operations inside a Tx are out of scope.

Adding those methods doesn't break a caller's build. `Tx` is sealed, so a type
outside Forma can satisfy it only by embedding a `Tx`, and that type gets each
new method from the embedded value. A wrapper around the `Tx` that `fn`
received forwards a new method straight to it, without running the wrapper's
own code. A test double that embeds a nil `Tx` panics if a new method is
called.

## Reading inside a Tx

`Tx.Get` and `Tx.Query` take the same `*QueryRequest` as `EntityManager.Get`
and `Query` and return the same results: projection, sort, pagination, the
condition DSL and relation enrichment all run the manager's own read code (#650).
Every statement they send, relation enrichment's included, runs on the Tx's
connection, so they see the Tx's own writes: a row created or updated earlier
in `fn` is returned as written, and a row it deleted is `ErrNotFound`. On
committed data a test finds `Tx.Query` and `EntityManager.Query` identical.

What a read sees:

- **Hot tier only.** A request with `Federated.Enabled` is `ErrInvalidInput`
  before any SQL, and no Tx read ever calls the federated engine. The
  PostgreSQL hot tier holds every live row and the Tx's own writes; the
  lakehouse tiers hold neither the uncommitted writes nor a lock.
- **No repeatable read.** An unlocked read runs at READ COMMITTED: each
  statement sees what had committed when it started, plus the Tx's own
  writes. Two reads of one row in one Tx can disagree when another
  transaction commits between them. A decision that must stay true until
  commit needs a lock, below.
- **One clock.** Every `$now` in a Tx's reads, preconditions and
  `server_time` operators names the transaction's start instant
  ([Conditional writes](conditional-writes.md)), however long `fn` runs.
- **Limits.** A read is a `Tx` method call and counts toward the operation
  limit. It has no timeout of its own: `Query.DefaultTimeout` does not apply,
  and the `InTx` deadline bounds it.

### Locking a row: `ForUpdate` and `AssertRow`

`tx.Get(ctx, req, forma.ForUpdate())` takes the row's write lock, then reads
it. The lock is the per-row advisory lock every Forma writer takes, plus a
`FOR UPDATE` row lock on the row's `entity_main` row, both held until the Tx
ends. Until then no other transaction can commit a write to the row.
Attributes that relation enrichment adds come from related rows, which the
lock does not cover. A missing row is `ErrNotFound` and has no `entity_main`
row to lock, but the advisory lock is still held, so any other write of that
row ID waits for this Tx. Today Forma generates every new row ID itself, so
this matters once callers can choose row IDs (#651).

`Query` refuses `ForUpdate` with `ErrInvalidInput`. Locking a result set needs
a lock-then-recheck protocol, which predicate mutation (#652) and claim (#653)
define.

`tx.AssertRow(ctx, schemaName, rowID, preconditions)` takes the same two
locks and evaluates `preconditions` against the locked row, with the rules
`Preconditions` follow on `Update` and `Delete`. It writes nothing:
`entity_main` (no new row version), EAV, `ltbase_updated_at` and `change_log`
are unchanged whatever the outcome.

| Row | `AssertRow` returns |
| --- | --- |
| exists and satisfies `preconditions` (or `preconditions` is nil) | nil |
| missing | `ErrNotFound` |
| exists and fails `preconditions` | `ErrPreconditionFailed` |

This is a version fence, for a step that reads a row and writes only rows that
depend on it:

```go
err = txr.InTx(ctx, func(ctx context.Context, tx forma.Tx) error {
	err := tx.AssertRow(ctx, "workflow_instance", instanceID,
		&forma.KvCondition{Attr: "version", Value: fmt.Sprintf("equals:%d", v)})
	if err != nil {
		return err // ErrPreconditionFailed: the instance moved on
	}
	_, err = tx.Create(ctx, historyEvent(instanceID))
	return err
})
```

The fence contract is an outcome: **if `AssertRow` returned nil and `InTx`
returns nil, no other transaction committed a write to the row between the
assertion and the commit.** An asserter whose assertion a concurrent committed
write invalidated never returns nil. On PostgreSQL:

| Interleaving | Outcome |
| --- | --- |
| The asserter locks first; a writer of the row arrives | The writer waits. The asserter commits, then the writer commits on top of it. |
| A writer commits first; the asserter then asserts the old version | `ErrPreconditionFailed`. If `fn` returns it, nothing the Tx wrote commits. |
| A writer has written a change that falsifies the precondition but not committed; the asserter asserts | The asserter waits, reads the committed version, and gets `ErrPreconditionFailed`. If the writer rolls back instead, the assertion holds. |
| Two asserters of one row | They serialize on the advisory lock, and both commit. |

The locks are taken in the order writers take them, advisory lock first, so a
fence adds no new deadlock: a Tx that asserts A and then updates B, crossed
with one that asserts B and updates A, deadlocks exactly as two crossed
writers do, and PostgreSQL ends one with `ErrRetryableConflict`. The
consistent-order advice above applies to assertions too.

On Aurora DSQL the fence is specified, not verified (#633). `FOR UPDATE` there
takes no lock but registers the row for conflict detection at commit, so an
asserter whose row another transaction changed and committed after its
snapshot fails at COMMIT with `40001`, which is `ErrRetryableConflict`. The
precondition reads the snapshot, so the second interleaving above may end as
`ErrRetryableConflict` instead of `ErrPreconditionFailed`. Both outcomes
satisfy the contract. The DSQL premise is that every Forma write modifies the
row's `entity_main` row: `Create` inserts it, `Update` rewrites it even when
only EAV attributes change (it stamps `ltbase_updated_at`), and `Delete`
removes it. A test pins this. Whether two DSQL asserters of one row conflict
with each other is not documented, so the two-asserters outcome is promised
for PostgreSQL only.

## Predicate mutation: `UpdateWhere` and `DeleteWhere`

`Tx.UpdateWhere` and `Tx.DeleteWhere` change the rows of one schema that match
a condition and report which rows they changed: SQL's `UPDATE … WHERE …
RETURNING` and `DELETE … WHERE … RETURNING` on Forma's storage (#652). They
replace the forbidden pattern of a `Query`, a list of row IDs, and one write
per ID. That pattern writes a row that stopped matching between the read and
the write, and cannot report what it changed.

```go
res, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{
	SchemaName: "workflow_task",
	Condition: &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "instance_id", Value: "equals:" + id},
		&forma.KvCondition{Attr: "status", Value: "equals:pending"},
	}},
	Updates:   map[string]any{"status": "cancelled"},
	UpdateOps: []forma.UpdateOp{forma.ServerTime("resolved_at", 0)},
	Returning: true,
})
// res.RowIDs: the cancelled tasks; res.Records: each as stored
```

A call runs in four steps:

1. **Validate.** The whole request is checked before any statement, and a
   refusal is `ErrInvalidInput` that leaves the Tx usable. The schema must be
   registered and the condition present: a nil condition is refused, and so
   is an empty `and` or `or` anywhere in it (an empty `and` would match every
   row), so there is no unconditioned bulk mutation. The condition follows the
   rules of a precondition (#649): it tests schema attributes only, never raw
   `entity_main` columns such as `ltbase_updated_at`. `Sort` attributes must
   exist. `Limit` is
   at least 0, `MaxAffected` is 1 to 10,000 (0 means 1,000), and `Limit` must
   not exceed `MaxAffected`. `Updates` and `UpdateOps` are checked as `Update`
   checks them (#649), and `UpdateWhere` needs at least one of them.
   `DeleteWhere` accepts neither.
2. **Select the candidates.** These are the rows matching the condition as
   the Tx sees them: the latest committed hot-tier state plus the Tx's own
   writes. They come in `Sort` order, with the row ID ascending as the final
   tiebreak (`Sort` names schema attributes only, so the row ID is never
   repeated). With `Limit` set, the candidates are the first `Limit` rows.
   With `Limit` 0 they are every matching row, and a match of more than
   `MaxAffected` rows is `ErrInvalidInput`. That refusal writes nothing and,
   although the select ran, leaves the Tx usable. The select is `Query`'s own
   SQL, so for any condition both accept, a candidate set is what `Tx.Query`
   with the same condition and sort answers. The federated engine is never
   called.
3. **Lock and re-check.** Every candidate is locked as a writer locks it, in
   row ID order whatever `Sort` says, so two predicate mutations never
   deadlock with each other. Then the condition is evaluated again on the
   locked rows. A candidate that stopped matching, or was deleted, while the
   call waited for its lock is **dropped**: it is not written and not
   returned. Its lock is still held until the Tx ends, so any other write of
   that row waits for this Tx.
   - **No refill.** A dropped candidate is not replaced by the next row in
     sort order, so a call with `Limit` L can affect fewer than L rows.
   - **No phantoms.** A row that starts matching after the candidates were
     selected is never locked, and is not affected.
4. **Write.** Each surviving row, in candidate order, is written by the
   single-row `Update` or `Delete` path: the same merge, validation, version
   stamps, hot `change_log` row and #274 tombstone stamp as `Tx.Update` and
   `Tx.Delete`. Each row costs that write's statements; the call is not one
   set-based statement.

The result's `RowIDs` lists the affected rows in candidate order. No match is
an empty list, not an error. With `Returning`, `Records` holds one record per
affected row, in the same order. For `UpdateWhere` it is the row as stored
after the write. For `DeleteWhere` it is the row as it was just before its
delete. These are the stored attributes, without relation enrichment, as
`Update` answers them. If the Tx commits and nothing later in it touches those
rows, each record is the committed row. Later operations in the Tx see the
mutation: a second `UpdateWhere` whose condition the first one falsified does
not match those rows.

A failure while writing a row after the first one poisons the Tx, whatever its
class, so a Tx never commits part of a predicate mutation. A failure at the
first row, before anything was written, follows the usual
[poisoning](#poisoning) rules. The select and the locks have been sent by then,
so an `ErrInvalidInput` there (an `Increment` of an attribute the row lacks, or
`Updates` the merged document fails validation on) poisons the Tx, as it does
for `Tx.Update`. Only a refusal in step 1 and the `MaxAffected` overflow leave
the Tx usable.

Each affected row counts as one operation against `WithTxMaxOperations` (a
call that affects no rows still counts as one). Its physical rows count
against `WithTxMaxPhysicalRows`. The physical-row limit usually binds first.
An updated entity modifies its main row, its old and new EAV rows and its
`change_log` row, so the default 3,000 admits far fewer than 1,000 updated
entities of any schema with EAV attributes. A breach of either limit poisons
the Tx.

On Aurora DSQL the predicate mutation is specified, not verified (#633). There
the candidate select and the re-check both read the transaction's snapshot,
and the dropping in step 3 does not happen. Instead, a concurrent transaction
that commits a change to a candidate after the snapshot makes this
transaction's COMMIT fail with `40001`, which is `ErrRetryableConflict`. So on
DSQL a result never includes a row that a committed concurrent writer changed
after the transaction's snapshot: the Tx either commits with the result as
returned, or fails. On PostgreSQL such a row is judged again under its lock,
kept if it still matches and written on top of the other writer's commit.
Forma does not retry: the caller may have acted on the returned rows, so a
retry must re-run the whole `fn`.

## Poisoning

An error from a `Tx` method either leaves the Tx usable or **poisons** it.

| Error | Poisons? |
| --- | --- |
| `ErrNotFound` (an unknown schema, or no row to read, assert, update or delete) | no |
| `ErrPreconditionFailed` (an `AssertRow` or conditional write that found the row failing its precondition), `ErrUniqueConflict` (a create onto a live row ID, or a write onto a taken unique-key value: see [Row IDs and Unique Keys](unique-keys.md)), raised without a failed SQL statement | no |
| `ErrInvalidInput` raised before the operation sent any SQL (a malformed operation, a `Create` that fails validation, a federated read, `ForUpdate` on `Query`, a `MutateWhereRequest`, `UpsertRequest` or `ClaimRequest` refused by its validation) | no |
| `ErrInvalidInput` for a predicate mutation that matched more than `MaxAffected` rows, after only its candidate select | no |
| `ErrInvalidInput` raised after the operation sent SQL | yes |
| any error from a predicate mutation after its first row was written, or from a claim once it holds a row | yes |
| any failed SQL statement, including `23505` and every retryable conflict | yes |
| a limit breach, concurrent use, an operation that panicked | yes |
| anything else | yes |

After poisoning, every `Tx` method returns an error matching both
`ErrTxAborted` and the original cause, and `InTx` rolls back even if `fn`
returns nil. A failed statement poisons because PostgreSQL has already aborted
the transaction. An `ErrInvalidInput` after SQL poisons because the operation
may have written part of its rows.

Two cases surprise people:

- **`Update` validates after it reads.** Strict update validation checks the
  merged document, which exists only after the row is locked and read. A
  validation failure there poisons the Tx, while the same failure from
  `Create` does not. The same holds for an update operator judged against the
  row: an `increment` with no current value or past its type's range poisons.
  A failed precondition does not.
- **Concurrent use.** A `Tx` must be used from one goroutine at a time. A
  call that arrives while another is running returns `ErrInvalidInput` and
  poisons the Tx; the running call finishes normally. If `fn` returns while a
  call is still running on another goroutine, that call's statements are
  cancelled and the transaction rolls back.

## Limits

| Limit | Default | Option |
| --- | --- | --- |
| `Tx` method calls | 1,000 | `WithTxMaxOperations(n)` |
| Rows modified: `entity_main`, EAV, `change_log` and unique-key entries | 3,000 | `WithTxMaxPhysicalRows(n)` |
| Wall-clock time for the whole `InTx`, `fn` included | `Config.Transaction.DefaultTimeout` | `WithTxTimeout(d)`; zero removes Forma's deadline |

Options apply in order, and a non-positive count or negative timeout is
refused with `ErrInvalidInput` before `BEGIN`. A breach answers
`ErrInvalidInput` whose message names the limit and its value, and poisons the
Tx.

The operation limit is checked before the call runs, so operation 1,001 sends
no SQL.

The row limit counts what PostgreSQL reports each statement modified. An
`Update` counts its main row, the row's old EAV rows (deleted) and new EAV rows
(inserted), and its `change_log` row. On a schema with unique keys, a write
also counts each key entry it inserts or deletes
([Row IDs and Unique Keys](unique-keys.md)); a write refused with
`ErrUniqueConflict` counts the entries it inserted and deleted again, though
none of them commits. The count is checked after each statement, so the limit
is exact for what a Tx commits, not for what it does:

- A Tx whose count stays at or under the limit can commit.
- The statement that takes the count past the limit has run. It fails, its
  operation returns `ErrInvalidInput`, and the Tx is poisoned: no statement
  runs after it, and nothing the Tx wrote commits.
- The work past the limit is that one statement. For `Create`, `Update` and
  `Delete` it writes or deletes rows of one entity, and never more than the
  same write does outside a Tx.

A check before each statement would hold the work to the limit too, but only
the write path knows a statement's rows in advance: every path would have to
declare them, and a `Delete` would need an extra query to count its EAV rows.
The check after the statement covers every statement a Tx sends without that,
and gives the same rollback guarantee (decided in the review of #673).

The 3,000 default matches Aurora DSQL's documented per-transaction limit, so a
Tx that fits on PostgreSQL also fits there.

The timeout is a context deadline over the whole call. When it passes, the
running statement is cancelled and `InTx` rolls back with
`context.DeadlineExceeded`.

## Retrying

Retry only when `forma.IsRetryable(err)` is true, and re-run the whole `fn`:
the database rolled back everything the attempt did. `IsRetryable` is true for
`ErrRetryableConflict` (deadlock `40P01`, lock timeout `55P03`, serialization
failure `40001`), including when it reaches `InTx` as a poisoning cause behind
`ErrTxAborted`. Make `fn` safe to run again: compute its writes from what it
reads inside the transaction, not from state captured by an earlier attempt.

Never retry blindly on `ErrCommitOutcomeUnknown`: the transaction may have
committed. Re-read and reconcile first. `ErrTxAborted` on its own is not
retryable: it means an earlier operation failed for a reason a retry will not
fix. The general rules are in [Error Handling](error-handling.md#retry-guidance).

## Scope

- **Hot tier only.** A Tx reads and writes `entity_main`, EAV and
  `change_log` in PostgreSQL and never touches the federated engine or S3. Its
  writes reach the lakehouse through CDC like any other write.
  `CrossSchemaSearch` and shared (`FOR SHARE`) locks are not offered inside a
  Tx.
- **DSQL: specified, not verified (#633).** On Aurora DSQL there are no
  advisory locks and the isolation is fixed at REPEATABLE READ with optimistic
  concurrency. A conflicting writer would abort with `40001` at statement or
  commit time instead of waiting and merging. Forma surfaces that as
  `ErrRetryableConflict`, and the retry rule above is unchanged. The locking
  seam that would replace the advisory lock is #633's work and has not been
  run against DSQL.

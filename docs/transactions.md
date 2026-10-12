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

`Tx` has no read methods yet; they arrive with #650. Upsert (#651), predicate
mutation (#652), claim (#653) and preconditions (#649) are later `Tx`
methods. Batch operations inside a Tx are out of scope.

Adding those methods doesn't break a caller's build. `Tx` is sealed, so a type
outside Forma can satisfy it only by embedding a `Tx`, and that type gets each
new method from the embedded value. A wrapper around the `Tx` that `fn`
received forwards a new method straight to it, without running the wrapper's
own code. A test double that embeds a nil `Tx` panics if a new method is
called.

## Poisoning

An error from a `Tx` method either leaves the Tx usable or **poisons** it.

| Error | Poisons? |
| --- | --- |
| `ErrNotFound` (an unknown schema, or no row to update or delete) | no |
| `ErrPreconditionFailed`, `ErrUniqueConflict`, raised without a failed SQL statement | no |
| `ErrInvalidInput` raised before the operation sent any SQL (a malformed operation, a `Create` that fails validation) | no |
| `ErrInvalidInput` raised after the operation sent SQL | yes |
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
  `Create` does not.
- **Concurrent use.** A `Tx` must be used from one goroutine at a time. A
  call that arrives while another is running returns `ErrInvalidInput` and
  poisons the Tx; the running call finishes normally. If `fn` returns while a
  call is still running on another goroutine, that call's statements are
  cancelled and the transaction rolls back.

## Limits

| Limit | Default | Option |
| --- | --- | --- |
| `Tx` method calls | 1,000 | `WithTxMaxOperations(n)` |
| Rows modified: `entity_main`, EAV and `change_log` | 3,000 | `WithTxMaxPhysicalRows(n)` |
| Wall-clock time for the whole `InTx`, `fn` included | `Config.Transaction.DefaultTimeout` | `WithTxTimeout(d)`; zero removes Forma's deadline |

Options apply in order, and a non-positive count or negative timeout is
refused with `ErrInvalidInput` before `BEGIN`. A breach answers
`ErrInvalidInput` whose message names the limit and its value, and poisons the
Tx.

The operation limit is checked before the call runs, so operation 1,001 sends
no SQL.

The row limit counts what PostgreSQL reports each statement modified. An
`Update` counts its main row, the row's old EAV rows (deleted) and new EAV rows
(inserted), and its `change_log` row. The count is checked after each
statement, so the limit is exact for what a Tx commits, not for what it does:

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

- **Hot tier only.** A Tx writes `entity_main`, EAV and `change_log` in
  PostgreSQL and never touches the federated engine or S3. Its writes reach
  the lakehouse through CDC like any other write.
- **DSQL: specified, not verified (#633).** On Aurora DSQL there are no
  advisory locks and the isolation is fixed at REPEATABLE READ with optimistic
  concurrency. A conflicting writer would abort with `40001` at statement or
  commit time instead of waiting and merging. Forma surfaces that as
  `ErrRetryableConflict`, and the retry rule above is unchanged. The locking
  seam that would replace the advisory lock is #633's work and has not been
  run against DSQL.

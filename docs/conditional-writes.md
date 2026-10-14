# Conditional writes (`Preconditions` and `UpdateOps`)

`Update` takes the row's lock, reads the row and merges onto it (#457), but
until #649 it could not be made conditional. "Apply this only if `version` is
still 7" or "add 1 to `attempts`" meant reading the row first and writing
afterwards, which is the lost-update race #593 forbids. Two fields on
`EntityOperation` close that gap:

- `Preconditions` is a condition in the [Condition DSL](condition-dsl.md)
  that must hold on the locked row, or the write writes nothing.
- `UpdateOps` computes values from the locked row (`increment`) or from the
  transaction clock (`server_time`).

They apply to `EntityManager.Update` and `Delete` and to `Tx.Update` and
`Tx.Delete` ([Transactions](transactions.md)). The API is Go only: both fields
are `json:"-"`, and the HTTP carriers are #655.

```go
_, err := tx.Update(ctx, &forma.EntityOperation{
	EntityIdentifier: forma.EntityIdentifier{SchemaName: "outbox_message", RowID: id},
	Updates:          map[string]any{"status": "failed", "last_error": msg},
	Preconditions:    &forma.KvCondition{Attr: "lock_generation", Value: "equals:4"},
	UpdateOps: []forma.UpdateOp{
		forma.Increment("attempts", 1),
		forma.ServerTime("next_attempt_at", backoff),
	},
})
switch {
case errors.Is(err, forma.ErrPreconditionFailed): // fenced out: the row has moved on
case errors.Is(err, forma.ErrNotFound): // the row is gone
}
```

## Evaluation order

1. **Intake, before any SQL.** The fields are checked against the schema
   (rules below). A refusal is `ErrInvalidInput`.
2. **Render, before any SQL.** The precondition is rendered to SQL. A DSL
   error, such as an operator the attribute's type does not take or an empty
   `in` list, is the DSL's own `ErrInvalidInput`.
3. **Lock.** The write begins its transaction and takes the row's advisory
   lock, as every single-row write does.
4. **Check.** One statement, run after the lock is granted, evaluates the
   precondition on the row. No row is `ErrNotFound`. A row that does not
   satisfy it is `ErrPreconditionFailed`. Either way nothing is written: no
   `entity_main`, EAV or `change_log` change, and no version stamp.
5. **Write.** An update reads the row, merges `Updates` and the
   `server_time` values onto it, applies `increment` to the result,
   validates, and writes through the ordinary merge path with its version
   stamp and `change_log` entry. A delete deletes.

   A `server_time` replaces its attribute as a value in `Updates` does: the
   stored value is never decoded, so the operator also rewrites a stored
   value the read refuses (see
   [Schema consistency migration](schema-consistency-migration.md)). An
   `increment` decodes the value it adds to.

An operation without `Preconditions` skips step 4, and one without `UpdateOps`
skips the operators; both behave exactly as before #649.

## Preconditions

- **Same DSL as `Query`.** Leaves, `and`/`or` composites, `in`, `not_in`,
  `is_null`, `is_not_null` and `$now` all work as in a query. `nil` means no
  precondition.
- **Schema attributes only.** A leaf naming anything the schema does not
  define is refused, raw `entity_main` columns included. In particular the
  system stamp `ltbase_updated_at` is not a precondition: optimistic
  concurrency uses an attribute the schema declares, such as `version`. A
  schema attribute whose name is itself an `entity_main` column (an
  attribute called `text_02`, say) is refused too. The DSL reads such a name
  as the column, whatever the schema stores under it, so the precondition
  would test a different value. Registration still admits these names (#470).
- **Matches `Query`.** A precondition holds exactly when a hot-tier `Query`
  with the same condition would return the row at the moment after the lock
  is granted. The check is rendered by the renderer `Query` uses
  (`buildHybridConditions`). It is anchored on the `entity_main` row, where
  `Query` may anchor on `eav_data`. The anchor does not change which rows
  match. `Query` anchors on `eav_data` only when no part of the condition
  can hold for an entity without EAV rows, which that anchor never visits:
  a column-bound leaf, an `is_null` leaf or an empty `and` group (which
  matches every row) forces the `entity_main` anchor.
- **Update and Delete only.** `Preconditions` on a `Create` is refused: there
  is no current row. Existence-conditional create is `Tx.Upsert` (#651).

### NotFound or PreconditionFailed

A missing row is `ErrNotFound`, whatever the precondition says, including a
precondition no row could satisfy. A present row that fails the precondition
is `ErrPreconditionFailed`, never `ErrNotFound`. Callers can therefore tell
"someone else moved the row on" from "the row is gone" without a second read.
The published message is `precondition does not hold for entity (row=<id>)`
(HTTP `412` once #655 routes the fields).

## Update operators

| Operator | Attribute types | New value |
| --- | --- | --- |
| `forma.Increment(attr, by)` | `smallint`, `integer`, `bigint` | the locked value plus `by` |
| `forma.ServerTime(attr, offset)` | `date`, `datetime` | the transaction's `now()`, truncated to milliseconds, plus `offset` |

Refused at intake, before any SQL, with `ErrInvalidInput` naming the
attribute:

- `UpdateOps` on a `Create` or a `Delete`;
- an attribute that is not in the schema, or one under an `x-relation` root
  (derived on read, never written);
- an attribute inside an array the JSON Schema declares, such as
  `items.seenAt` when `items` is an array of objects. An operator writes one
  value, and such an attribute has one per element. The refusal does not
  depend on the row: where the row has no `items`, the operator would create
  an object the schema says is an array. An array the manager cannot see in
  the schema (one behind a `$ref`, or any array when the manager has no
  validator) is judged on the locked row instead;
- `increment` on any other type, or with an `Offset`; `server_time` on any
  other type, with a `By`, or with an offset finer than a millisecond;
- two operators on one attribute;
- an attribute that `Updates` also writes, in either spelling (`"a.b"` or
  `{"a": {"b": …}}`), or through a container `Updates` replaces (`"a": nil`
  overlaps an operator on `a.b`).

An update may carry `UpdateOps` without `Updates`.

Judged against the locked row, after the write has sent SQL, also
`ErrInvalidInput`:

- `increment` on a row with no value for the attribute. There is no implicit
  zero: write an initial value first.
- `increment` whose result leaves the type's range. The message names the
  attribute, the type and the bound, for example `increment by 1 on smallint
  attribute 'n' would take its value 32767 past the smallint maximum 32767`.
- an attribute that `Updates` writes on this row only. An object in
  `Updates` merges into an object the row holds and is placed where the row
  holds nothing, but replaces a list or a scalar, and everything beneath it.
  On a row whose `a` is a list or a scalar, `{"a": {"x": 1}}` therefore
  replaces it and `a.b` with it, and overlaps an operator on `a.b`. The check
  runs before the merge, while the row's value is still there to judge.
- an operator on an attribute inside an array the row holds, where intake
  could not see the array in the schema: it would write one value where the
  row holds one per element.

### Precision of `server_time`

| Attribute | Value written |
| --- | --- |
| `datetime` (EAV, or bound to a `unix_ms` column) | the instant, in milliseconds |
| `datetime` bound to an `iso8601` text column | the instant floored to the second, the precision the column stores |
| `date` | the UTC calendar day of the instant, the day `$now` names on a `date` |

The offset applies before the floor: `ServerTime("day", -9*time.Hour)` at
08:15 UTC writes yesterday's date.

## One clock

`server_time` is PostgreSQL's `now()` in the write's own transaction: the
transaction's start, the same instant `$now` reads in a precondition. In one
`Tx`, every `server_time` and every `$now` resolves to that one instant,
however long the Tx runs, so a `Tx` can write `ServerTime("seen_at", 0)` and
later test `seen_at equals:$now`. The instant never comes from the process
clock.

The version stamp `ltbase_updated_at` is not that instant. It still comes from
the process clock, kept strictly increasing per row (#274), so it may differ
from a `server_time` written by the same update.

## Batches

`BatchCreate`, `BatchUpdate` and `BatchDelete`, atomic or best-effort, refuse
an operation that carries `Preconditions` or `UpdateOps` before any write,
with `ErrInvalidInput` naming the operation's index. Run conditional writes
one at a time, or several in one `Transactor.InTx`. Multi-row conditional
mutation is #652.

## Inside a Tx

`ErrPreconditionFailed` and `ErrNotFound` are expected outcomes produced
without a failed statement, so they leave the `Tx` usable: `fn` may handle
them and continue, or return them to roll back everything. Intake and
rendering refusals also come before any SQL and do not poison. The refusals
judged against the locked row (a missing value, an overflow, an overlap or an
array only the row shows) come after the write has sent SQL, and poison the
Tx like any other `ErrInvalidInput` after SQL. See [Poisoning](transactions.md#poisoning).

## Concurrency on PostgreSQL

Writers of one row queue on its advisory lock. The check runs after the lock
under READ COMMITTED, so its snapshot postdates the previous holder's commit:
a writer that waited judges the row the winner left. Two writers that both
expect `version = 7` and increment it therefore produce one commit and one
`ErrPreconditionFailed`, and `version` ends at 8. Concurrent `increment`s
without a precondition also serialize on the lock and lose nothing.

A conditional `Delete` runs at READ COMMITTED for the same reason. An
unconditional `Delete` keeps the session's default isolation, as before.

## Aurora DSQL

**DSQL: specified, not verified (#633).** DSQL has no advisory locks and runs
at REPEATABLE READ with optimistic concurrency, so a losing writer may
evaluate its precondition on the snapshot from before the winner committed,
see it hold, and write. DSQL then aborts one of the two transactions with
`40001`. The contract is therefore stated as an outcome that holds on both
backends:

- At most one of the conflicting writers commits.
- Every other writer receives `ErrPreconditionFailed` or
  `ErrRetryableConflict`. A retry of an `ErrRetryableConflict` starts from a
  fresh snapshot and then reports `ErrPreconditionFailed`.

Forma does not retry `40001` internally ([Retrying](transactions.md#retrying)).
The precondition and operator semantics are backend-neutral; the locking that
replaces the advisory lock goes through the seam #648 and #633 own. Tests that
assert `ErrPreconditionFailed` for the loser specifically are named
`_PostgresOnly`.

## Verification

| Property | Test |
| --- | --- |
| Precondition matches `Query`, every operator, on column-bound and EAV attributes | `TestPreconditionHoldsExactlyWhenQueryReturnsTheRow` |
| A refused update or delete leaves `entity_main`, EAV and `change_log` unchanged | `TestFailedPreconditionWritesNothing` |
| An object in `Updates` that would replace the row's array refuses the operator beneath it and writes nothing | `TestUpdatesReplacingARowArrayWritesNothing` |
| A missing or deleted row is `ErrNotFound` | `TestMissingRowIsNotFoundWhateverThePrecondition` |
| Two writers with one expected version: one commits (60 iterations, both queued on the lock) | `TestSameExpectedVersionAdmitsOneWriter_PostgresOnly` |
| 32 concurrent increments total exactly | `TestConcurrentIncrementsLoseNothing` |
| Read-then-conditional-write retry loop loses no update | `TestVersionRetryLoopLosesNoUpdate` |
| `server_time` equals the transaction's `now()` | `TestServerTimeIsTheTransactionInstant` |
| `server_time` and `$now` share one instant in a `Tx` | `TestNowPreconditionAndServerTimeShareTheTxInstant` |
| A failed precondition rolls back a `Tx` that returns it, and does not poison one that handles it | `TestConditionalUpdateFailureRollsBackTheTx`, `TestSwallowedPreconditionFailureDoesNotPoisonTheTx` |

# Conditional writes (`Preconditions` and `UpdateOps`)

`Update` takes the row's lock, reads the row and merges onto it (#457), but
until #649 it could not be made conditional. "Apply this only if `version` is
still 7" or "add 1 to `attempts`" meant reading the row first and writing
afterwards, which is the lost-update race #593 forbids. Two fields on
`EntityOperation` close that gap:

- `Preconditions` is a condition in the [Condition DSL](condition-dsl.md)
  that must hold on the locked row, or the write writes nothing.
- `UpdateOps` computes values from the locked row (`increment`) or from the
  transaction clock (`server_time`), or makes an attribute absent (`unset`,
  #938).

`EntityManager.Update` and `Tx.Update` accept both fields. `EntityManager.Delete`
and `Tx.Delete` accept `Preconditions` and refuse `UpdateOps`: a delete
writes no value ([Transactions](transactions.md)). `Tx.Upsert`'s `update`
action accepts both and applies them to the existing row. `UpdateOps` also
apply to the rows `Tx.UpdateWhere` and `Tx.Claim` write. Every one of these
surfaces goes through the same update path ([Unset on the other
surfaces](#unset-on-the-other-surfaces)). The API is Go only: both fields
are `json:"-"`, and the HTTP carriers are #655. `Tx.AssertRow` evaluates a
precondition by the same rules on a row it locks without writing it
([Locking a row](transactions.md#locking-a-row-forupdate-and-assertrow)).

```go
_, err := tx.Update(ctx, &forma.EntityOperation{
	EntityIdentifier: forma.EntityIdentifier{SchemaName: "outbox_message", RowID: id},
	Updates:          map[string]any{"status": "failed", "last_error": msg},
	Preconditions:    &forma.KvCondition{Attr: "lock_generation", Value: "equals:4"},
	UpdateOps: []forma.UpdateOp{
		forma.Increment("attempts", 1),
		forma.ServerTime("next_attempt_at", backoff),
		forma.Unset("locked_by"), // end the lease: the row no longer holds it
		forma.Unset("locked_until"),
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
   error, such as an operator the attribute's type does not take
   (`starts_with` on a `uuid`), an operand that is not of its type (a
   malformed UUID, text with a NUL character or invalid UTF-8) or an empty
   `in` list, is the DSL's own `ErrInvalidInput`.
3. **Lock.** The write begins its transaction and takes the row's advisory
   lock, as every single-row write does.
4. **Check.** One statement, run after the lock is granted, evaluates the
   precondition on the row. No row is `ErrNotFound`. A row that does not
   satisfy it is `ErrPreconditionFailed`. Either way nothing is written: no
   `entity_main`, EAV or `change_log` change, and no version stamp.
5. **Write.** An update reads the row, merges `Updates` and the
   `server_time` values onto it, removes each `unset` target, applies
   `increment` to the result, validates, and writes through the ordinary
   merge path with its version stamp and `change_log` entry. A delete
   deletes.

   A `server_time` replaces its attribute as a value in `Updates` does: the
   stored value is never decoded, so the operator also rewrites a stored
   value the read refuses (see
   [Schema consistency migration](schema-consistency-migration.md)). So
   does an `unset`. An `increment` decodes the value it adds to.

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
  would test a different value. Since #470 no factory-built manager can
  reach this refusal: schema admission refuses these names, and the factory
  refuses them in a caller-supplied `SchemaRegistry` at startup and on every
  attribute-cache read. The refusal stays as defense in depth.
- **Matches `Query`.** A precondition holds exactly when a hot-tier `Query`
  with the same condition would return the row at the moment after the lock
  is granted. The check is rendered by the renderer `Query` uses
  (`buildHybridConditions`). It is anchored on the `entity_main` row, where
  `Query` may anchor on `eav_data`. The anchor does not change which rows
  match. `Query` anchors on `eav_data` only when no part of the condition
  can hold for an entity without EAV rows, which that anchor never visits:
  a column-bound leaf, an `is_null` leaf or an empty `and` group (which
  matches every row) forces the `entity_main` anchor.
- **What a condition means is the DSL's rule, not `Query`'s answer.**
  Matching `Query` makes a precondition exactly as right as `Query`, and no
  more: the two share the renderer, so a defect in it is in both. Before
  #677 a fractional operand against an `integer` column was truncated, so
  `equals:7.9` held on a row holding 7 and authorized the write. The tests
  therefore judge preconditions against exact arithmetic over the stored
  values and Go string matching, never against `Query`; the operand rules
  are in [condition-dsl.md](condition-dsl.md).
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
| `forma.Unset(attr)` | every scalar type, `list` | none: the attribute is absent (see [Unset](#unset)) |

Refused at intake, before any SQL, with `ErrInvalidInput` naming the
attribute:

- `UpdateOps` on a `Create` or a `Delete`;
- an attribute that is not in the schema, or one under an `x-relation` root
  (derived on read, never written);
- an attribute bound to a system column (`ltbase_row_id`, `ltbase_schema_id`,
  `ltbase_created_at`, `ltbase_updated_at` or `ltbase_deleted_at`). Such a
  binding is a read-only alias: the store sets the column itself and drops
  any value an update sends for it, so the operator's result could not be
  stored. A literal value for it in `Updates` is still accepted and ignored,
  as before;
- an attribute inside an array the JSON Schema declares, such as
  `items.seenAt` when `items` is an array of objects. An operator writes one
  value, and such an attribute has one per element. The refusal does not
  depend on the row: where the row has no `items`, the operator would create
  an object the schema says is an array. An array the manager cannot see in
  the schema (one behind a `$ref`, or any array when the manager has no
  validator) is judged on the locked row instead;
- `increment` on any other type, or with an `Offset`; `server_time` on any
  other type, with a `By`, or with an offset finer than a millisecond;
  `unset` with a `By` or an `Offset`, or on a target [Unset](#unset)
  refuses;
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

### Unset

`forma.Unset(attr)` makes `attr` absent, which nothing else can: an explicit
`null` in `Updates` is refused as before (`attribute 'x' cannot be set to
null`), on `Create` and `Update`, nested or as a list element, and over HTTP
too, which has no unset carrier until #655. An empty list is a value, not a
clear: `{"tags": []}` stores the empty-list marker and reads back `[]`, while
`Unset("tags")` removes the elements and the marker, and the row then reads
back without `tags`. No typed nil clears either: `[]any(nil)` is the empty
list, `map[string]any(nil)` changes nothing, and a nil pointer is refused or
leaves the attribute present.

**Targets.** Decided from the registry alone, its attribute metadata and the
schema document it serves, before any SQL. An unset applies to a registered
scalar or list attribute, top-level or nested under ordinary objects, under
the name the attributes file registers (`profile.nick`, and a literal name
such as `price$` too). Refused with `ErrInvalidInput` naming the attribute:

- an attribute the schema does not define, or a raw `entity_main` column name
  (`ltbase_created_at`, `text_01`);
- an attribute bound to a system column, or one under an `x-relation` root,
  the root included;
- an object (`profile`): unset each attribute beneath it instead;
- an array of objects (`items`), and an attribute inside one (`items.sku`),
  which has one value per element. Crossing is read from
  `AttributeMetadata.ArrayAncestors`, so the refusal holds on a manager with
  no validator too; the array paths the JSON Schema declares refuse as well;
- a nested attribute whose `ArrayAncestors` is nil: the registry did not
  derive its ancestry, so where it is placed cannot be established;
- an indexed, wildcard or JSONPath spelling (`tags[0]`, `profile.*`, `$.note`)
  of a name the schema does not register.

An object or an array of objects is recognised by an attribute registered
beneath it or by the schema document's declaration of the target itself, so
a root the attributes file registers as a `list` while the document declares
it an array of objects (`"lines": []` then stores an empty-list marker) is
refused too, with or without a validator. A document that does not reach the
target, as behind an unresolved `$ref`, leaves only the registration to go
by.

The row-level backstop stays: a list the locked row holds on the way to the
target is refused, as for any operator.

**One writer.** An unset is an operator like the others, so the rules above
apply: a second unset of the attribute, an `increment` or `server_time` on
it, or `Updates` writing it in either spelling, or replacing an object above
it (`{"profile": "flat"}` or `{"profile": nil}` beside `Unset("profile.nick")`),
is refused. An object in `Updates` that merges into the row's object writes
only its own members, so `{"profile": {"title": "x"}}` beside
`Unset("profile.nick")` is accepted. Intake refusals send no SQL and leave a
`Tx` usable.

**Merge.** The target is removed from the merged document, and every object
the removal leaves empty goes with it, so the document validated is the
document stored. The stored value is discarded undecoded, as is a bound
column's `rfc3339` or `bool_text` image, which the merge holds unparsed: an
image the read cannot parse fails an update that keeps it, with the read's
consistency error, and never one that unsets or replaces it. A row that holds no
value for the target is updated as usual, version stamp and `change_log`
entry included, and stays without one; repeating an unset recreates nothing.

**Validation.** Nothing is new: the merged document is validated as any
update's is. The attribute metadata's required policies apply in every mode:
a `required_always` attribute, and a `required_if_parent_present` attribute
whose parent the result still holds, are refused. A
requirement only the JSON Schema states follows `ValidateUpdatesStrict`:
refused when strict, accepted with the report-only log line and counter
otherwise, and not checked without a validator. Any document an unset
produces gets the verdict the same document gets when another update reaches
it.

**Storage.** A clear is atomic across both stores. A column-bound target's
column is written `NULL` in the same `UPDATE`: the merged record carries the
columns it clears, because the write otherwise keeps every main column the
record does not set (#651's retained columns), which would silently undo the
clear. The EAV write already replaces every active attribute of the row, so a
cleared EAV value, its empty-list marker, and any `eav_data` row a bound
attribute retained from before its binding are deleted with it.

**Unique keys.** Keys are planned from the final record, so a cleared
component releases its entry in the update's own transaction, and another
entity can take the value at once, in the same `Tx` included. There is no
`NULL` encoding: a key with an absent component has no entry. A clear that
does not commit, refused by its precondition or rolled back with its `Tx`,
keeps the original reservation. `Claim` refuses an unset of a key component,
as it refuses every write of one.

`BatchUpdate` accepts `Unset`, and no other operator, in both modes (#940;
see [Batches](#batches)). Flush, compaction and federated absence are #941.

### Unset on the other surfaces

`Tx.UpdateWhere` and `Tx.Claim` write each row they take with `Tx.Update`'s
path, and `Tx.Upsert`'s `update` action applies its operation to the existing
row with it, so an unset is one implementation everywhere (#939). Each
surface checks `UpdateOps` with `Update`'s intake before any SQL. A target
refused above, or an overlap intake can see (a second operator on the
attribute, or `Updates` writing it or replacing an object above it), is
`ErrInvalidInput` naming it, sends no SQL and leaves the `Tx` usable. An
overlap only the stored row shows, such as an object in `Updates` that
replaces the row's list or scalar above the target, is judged after the row
is locked, as [Update operators](#update-operators) describes, and poisons the `Tx` like
any other `ErrInvalidInput` after SQL.

| Surface | `Unset` |
| --- | --- |
| `Tx.UpdateWhere` | applied to each selected row the re-check keeps under its lock, and to no other |
| `Tx.Claim` | applied in the claim mutation to the claimed rows only, under group exclusivity and generation fencing; an unset of a unique-key component is refused, as every write of one is |
| `Tx.Upsert`, `update` | applied to the existing row |
| `Tx.Upsert`, create branch | not executed: the create runs as given; the operators are still validated, so a refused target fails the upsert before any SQL |
| `Tx.Upsert`, `do_nothing` | `UpdateOps` are refused, an unset included |

A surface stores what `Tx.Update` stores for the same mutation: the same
`entity_main` columns, `eav_data` rows and unique-key entries, with version
and `change_log` advanced. Everything else is that surface's own behaviour,
unchanged: preconditions, predicate selection, locking and re-check,
`MaxAffected`, claim eligibility, and which errors poison the `Tx`. A failed
precondition, a re-check miss and a `MaxAffected` overflow remove nothing.
To end a lease, unset its attributes in the holder's CAS update
([Fencing](claims.md#fencing)); moving it with `ServerTime(attr, 0)` is no
longer the recommended pattern.

### Precision of `server_time`

| Attribute | Value written |
| --- | --- |
| `datetime` (EAV, or bound to a `unix_ms` or `rfc3339` column) | the instant, in milliseconds |
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

The one exception is `Unset` in `BatchUpdate` (#940). It needs no per-row
compute or guard contract, so a batch can carry it; `Increment` and
`ServerTime` compute from the locked row or the transaction clock, which a
batch has none of. An operation may consist of unsets alone, without
`Updates`. The request-level check reads only the operators' kinds: an
`Increment`, a `ServerTime` or any other kind in a `BatchUpdate`, a
precondition in any batch, and any operator in `BatchCreate` or
`BatchDelete`, `Unset` included, still refuse the whole batch before any
write. Each unset is then judged and applied exactly as on `Update`
([Unset](#unset)): the same target rules, the one-writer rule, the merge,
validation, storage and unique-key release.

| | Atomic | Best-effort |
| --- | --- | --- |
| Target or overlap refused at intake (unknown, object, array of objects, a second writer) | the whole batch fails before any row is locked, naming `operation[i]` | that operation fails, the others commit |
| Refused under the lock (validation of the merged document, such as `required_always`; an overlap only the row shows; a missing row) | the whole batch rolls back, naming `operation[i]` | that operation fails, the others commit |
| Unique-key entries of cleared components | released in the batch's transaction; a rolled-back batch keeps them | released in the operation's own transaction |

A best-effort failure is reported in `BatchResult.Failed` with the
operation itself, its `RowID` included, and its log line carries the
operation's index. Nothing is silently ignored.

The budgets are unchanged. `PerformanceConfig.MaxBatchSize` bounds the
operations of either mode. An atomic batch's rows are not metered against a
physical-row limit yet (#692): a clear deletes its `eav_data` rows,
a list's element rows included, inside the batch's one transaction, and
those deletions count wherever #692's meter will count modified rows.

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

This section describes the `aurora-dsql` engine (#633,
[Aurora DSQL](aurora-dsql.md)). CI runs it on PostgreSQL emulating DSQL. It
has not yet run on a cluster. DSQL has no advisory locks and runs at
REPEATABLE READ with optimistic concurrency, so a losing writer may evaluate
its precondition on the snapshot from before the winner committed, see it
hold, and write. Both writers wrote the row's guard row, so DSQL aborts one of
the two transactions with `40001`. The contract is therefore stated as an
outcome that holds on both backends:

- At most one of the conflicting writers commits.
- Every other writer receives `ErrPreconditionFailed` or
  `ErrRetryableConflict`. A retry of an `ErrRetryableConflict` starts from a
  fresh snapshot and then reports `ErrPreconditionFailed`.

Forma does not retry `40001` internally ([Retrying](transactions.md#retrying)).
The precondition and operator semantics are backend-neutral. The guard-row
write replaces the advisory lock behind the locking seam #648 introduced.
Tests that assert `ErrPreconditionFailed` for the loser specifically are named
`_PostgresOnly`.

## Verification

| Property | Test |
| --- | --- |
| Precondition matches `Query`, every operator, on column-bound and EAV attributes | `TestPreconditionHoldsExactlyWhenQueryReturnsTheRow` |
| A refused update or delete leaves `entity_main`, EAV and `change_log` unchanged | `TestFailedPreconditionWritesNothing` |
| An object in `Updates` that would replace the row's array refuses the operator beneath it and writes nothing | `TestUpdatesReplacingARowArrayWritesNothing` |
| An operator on a system-column alias is refused before SQL: no table changes, and a `Tx` stays usable | `TestOperatorOnASystemColumnAliasIsRefusedBeforeAnyStatement` |
| A uuid precondition with a LIKE operator or a malformed operand is refused before SQL, by `Query`, Update and Delete, and a `Tx` stays usable | `TestUUIDPreconditionIsRefusedBeforeAnyStatement` |
| A text precondition with a NUL character or invalid UTF-8, as a scalar, LIKE or list operand, is refused before SQL on both storage kinds: no table changes, and a `Tx` keeps its earlier write and commits a later one | `TestTextPreconditionIsRefusedBeforeAnyStatement` |
| Numeric preconditions agree with exact arithmetic on every operator and operand class (fractions, both signs, beyond the column's width, around 2^53 and the int64 limits) over `smallint`, `integer`, `bigint` and `double` columns and EAV with `DOUBLE PRECISION` and `NUMERIC` `value_numeric`, through Update, Delete, `Tx.Update` and `Tx.Delete`; a refusal writes nothing and a `Tx` that handles it commits a later write | `TestPreconditionNumericOperandsMatchArithmetic`, `TestPreconditionOperandsReproductions` |
| `starts_with` / `contains` preconditions match their operand literally | `TestPreconditionLikeOperandsAreLiteral` |
| `NaN`, `Inf` and a bigint operand beyond int64 are refused before SQL on every route, and a `Tx` stays usable | `TestNonFinitePreconditionIsRefusedBeforeAnyStatement` |
| A missing or deleted row is `ErrNotFound` | `TestMissingRowIsNotFoundWhateverThePrecondition` |
| Two writers with one expected version: one commits (60 iterations, both queued on the lock) | `TestSameExpectedVersionAdmitsOneWriter_PostgresOnly` |
| 32 concurrent increments total exactly | `TestConcurrentIncrementsLoseNothing` |
| Read-then-conditional-write retry loop loses no update | `TestVersionRetryLoopLosesNoUpdate` |
| `server_time` equals the transaction's `now()` | `TestServerTimeIsTheTransactionInstant` |
| `server_time` and `$now` share one instant in a `Tx` | `TestNowPreconditionAndServerTimeShareTheTxInstant` |
| `unset` clears bound and EAV scalars, lists with elements or only their marker, and nested scalars and lists, alone and beside a Set, an `increment` and a `server_time`, through `Update` and `Tx.Update`: the column is `NULL`, no `eav_data` row is left, and version and `change_log` advance | `TestUnsetCommitsTheClear` |
| `unset` of a bound attribute also deletes the `eav_data` rows it retained | `TestUnsetOfABoundAttributeRemovesItsRetainedEAVRows` |
| `unset` refusals and overlaps name their target and send no SQL, relation and underived nested targets included, and a `Tx` stays usable | `TestUnsetRefusalsLeaveATxUsable`, `TestUnsetRelationAndUnderivedRefusalsLeaveATxUsable`, `TestUnsetRefusesItsTargetAtIntake`, `TestUnsetOverlapsAreRefused` |
| A registered root the document declares an array of objects is refused in every mode; a registered literal name (`price$`) is cleared while path spellings stay refused | `TestUnsetRefusesARegisteredContainer`, `TestUnsetTakesARegisteredLiteralName` |
| A bound `rfc3339` or `bool_text` image the read cannot parse is cleared or replaced unparsed, and fails an update that keeps it | `TestUnsetDiscardsAnUnreadableBoundImage` |
| A cleared key component releases its entry; a failed precondition or rolled-back `Tx` keeps it | `TestUnsetReleasesTheUniqueKeyEntry`, `TestRolledBackUnsetKeepsTheReservation`, `TestUnsetThenRetakeInOneTx` |
| The validation verdict on an unset matches the verdict on the same document reached otherwise, in strict, report-only and no-validator modes, and PostgreSQL commits exactly the accepted clears, with report-only's log line and counter | `TestUnsetValidationMatrix`, `TestUnsetValidationMatrixCommits` |
| `UpdateWhere`, `Claim` and `Upsert`'s `update` store what `Tx.Update` stores for the same Set, `increment` and unset of a bound and of an EAV attribute: the same `entity_main` columns and `eav_data` rows, with version and `change_log` advanced | `TestUnsetSurfacesStoreWhatUpdateStores` |
| On a row holding three key entries, the three surfaces commit the same `entity_main` columns, `eav_data` rows and key entries as `Tx.Update` for an unset of a bound and of an EAV non-key attribute (every entry kept), and `UpdateWhere` and `Upsert` do so for an unset of a bound and of an EAV key component (that entry released) | `TestUnsetSurfacesKeepTheKeysUpdateKeeps` |
| Each of the three surfaces refuses, naming it, before any SQL, every unset target `TestUnsetRefusesItsTargetAtIntake` refuses and every overlap `TestUnsetOverlapsAreRefused` refuses (a second operator, `Updates` writing the attribute nested, dotted or as a list, or replacing its parent with a scalar or `null`), and its `Tx` then commits; relation and underived nested targets are pinned on `Update` only | `TestUnsetSurfacesRefuseAtIntake` |
| `UpdateWhere` clears every matched row and no other, alone and beside a Set and an `increment`; a re-check miss is untouched; a `MaxAffected` overflow commits nothing and leaves the `Tx` usable | `TestUpdateWhereUnsetClearsEveryMatchedRowAndNoOther`, `TestUpdateWhereUnsetLeavesARecheckMissUntouched_PostgresOnly`, `TestUpdateWhereUnsetOverMaxAffectedCommitsNothing` |
| A claim's unset clears the claimed rows only, in `Sort` order where insertion and row-ID order run the other way, and, with a `Group`, the heads by group order only; an unset of a unique-key component is refused before SQL | `TestClaimUnsetClearsOnlyTheClaimedRows`, `TestClaimGroupUnsetClearsOnlyTheHeads`, `TestClaimRefusesAnUnsetOfAUniqueKeyComponent` |
| #653's `MarkFailed` (generation precondition, `increment` of `attempts`, `status: failed`, unset of `locked_by` and `locked_until`) ends the lease so that `locked_until is_null` selects the row; a stale generation leaves the lease in place | `TestClaimMarkFailedUnsetsTheLease` |
| `Upsert`'s create branch creates without running the unset and still refuses a refused target before SQL; `do_nothing` refuses an unset; an `update` whose precondition the live row fails clears nothing (row, version, `change_log` and key entries as before) and leaves the `Tx` usable | `TestUpsertCreateBranchDoesNotRunUnset`, `TestUpsertDoNothingRefusesUnset`, `TestUpsertFailedPreconditionUnsetsNothing` |
| A key component cleared through `UpdateWhere` or `Upsert` releases its entry; a rolled-back `Tx` keeps it | `TestUnsetSurfacesReleaseTheUniqueKeyEntry`, `TestUnsetSurfacesRolledBackKeepTheReservation` |
| A failed precondition rolls back a `Tx` that returns it, and does not poison one that handles it | `TestConditionalUpdateFailureRollsBackTheTx`, `TestSwallowedPreconditionFailureDoesNotPoisonTheTx` |

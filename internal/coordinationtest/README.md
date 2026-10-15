# coordinationtest

A test-only adapter that implements LTFlow's repository (`Repository.InTx` and
the stores of its `UnitOfWork`) on Forma's public API, and a contract suite that
holds the adapter to LTFlow's consistency contract on real PostgreSQL (#654).
Each primitive of #593 has tests of its own. This package shows that they
compose: one transaction mixes CAS updates, fences, predicate mutations,
upserts and claims, and concurrent transactions over the same rows neither
deadlock nor lose a write.

The adapter is the package's non-test files. They import only `forma`,
`forma/factory`, the standard library and `github.com/google/uuid`, which
`TestAdapterImportsOnlyThePublicAPI` enforces. The suite is the `_test.go`
files. Its scenario checks of committed state read the tables with direct
SQL. The adapter is never their witness. Tests of the adapter's read methods
(`Get`, `GetByTokenHash`, `ListByFilter`) read back through those methods, and
compare what they answer with what the test wrote.

## Mapping

Each adapter method's doc comment cites the LTFlow method it mirrors, and
`TestEveryMappedOperationHasACitingMethod` checks every row below against
those comments.

| LTFlow operation | Adapter | Forma primitives |
| --- | --- | --- |
| `Repository.InTx` | `Store.InTx` | `Transactor.InTx` (#648), called once. Every error is returned unchanged, `ErrRetryableConflict` and `ErrCommitOutcomeUnknown` (#645) included. No retry. |
| `InstanceStore.Create` | `Create` | `Tx.Create` with a caller row ID (#651). A taken ID is `ErrUniqueConflict`. |
| `InstanceStore.Update(inst, expectedVersion)` | `Update` | `Tx.Update` with `Preconditions: version equals expected` (#649). A moved instance is `ErrPreconditionFailed`. |
| `InstanceStore.FenceVersion` | `FenceVersion` | `Tx.AssertRow(version equals v)` (#650). |
| `InstanceStore.Get`, `ListByFilter` inside `InTx` | `Get`, `ListByFilter` | `Tx.Get` and `Tx.Query` (#650). |
| `TaskStore.Insert`, `GetByTokenHash` | `Insert`, `GetByTokenHash` | `Tx.Create` with the unique key `token_hash` (#651); `Tx.Query`. |
| `TaskStore.UpdateStatus(h, from, to)` | `UpdateStatus` | `Tx.UpdateWhere{token_hash = h AND status = from}` (#652). No match is `ErrPreconditionFailed`. |
| `TaskStore.CancelPendingForInstanceVersion` | `CancelPendingForInstance` | `Tx.UpdateWhere` with `Returning` (#652). LTFlow has since renamed the method, and the adapter follows the current name. |
| `TaskStore.CompleteMatchingAndCancelPending` | `CompleteMatchingAndCancelPending` | Two `Tx.UpdateWhere` calls in one Tx (#652). |
| `IdempotencyStore.Upsert` | `Upsert` | `Tx.Upsert{Target.Key: idempotency, Action: update, Preconditions: status = failed AND request_hash = h}` (#651, #649). |
| `WorkflowLinkStore.Insert`, `UpdateStatus` | `Insert`, `UpdateStatus` | `Tx.Create` with the unique key `parent_task_hash` (#651); `Tx.UpdateWhere` (#652). |
| `OutboxStore.Insert` | `Insert` | `Tx.Create`. |
| `OutboxStore.ClaimBatch`, `OutboxTopicClaimer.ClaimBatchByTopics` | `ClaimBatch`, `ClaimBatchByTopics` | `Tx.Claim` (#653) with `Group{Key: [partition_key], Order: [enqueued_at], Live: status not_in [published, dead_letter]}`, the condition `(status in [pending, failed] AND next_attempt_at lte $now) OR (status = publishing AND locked_until lte $now)` (#646), plus `topic in T` for `ByTopics`, and `UpdateOps: Increment(lock_generation), ServerTime(locked_until, lease)` (#649). |
| `OutboxStore.MarkPublished`, `MarkFailed`, `MarkDeadLetter` | the same | `Tx.Update` with `Preconditions: lock_generation equals g` (#649). `MarkFailed` adds `Increment(attempts)`. |
| `HistoryStore.Insert` | `Insert` | `Tx.Create`. |
| `HistoryStore.UpsertDeduped` | `UpsertDeduped` | `Tx.Upsert{Target.RowID, Action: update, Preconditions: recorded_at gte $now-window, UpdateOps: Increment(dedup_count)}` (#651, #649, #646). `ErrPreconditionFailed` is a silent no-op. |
| `DefinitionStore` (workflow definitions) | `SaveDraft`, `SaveActive`, `GetByVersion` | The unique keys `(name, version)` and `(name, activation_sequence)` (#651). |
| `RetentionStore.DeleteEligibleTasks`, `IdempotencyStore.DeleteEligible` | `DeleteEligible` of each store | `Tx.DeleteWhere{eligible_cleanup_at lte cutoff}` with `Limit` (#652). |
| `RetentionStore.MarkHistoryArchived`, `TaskStore.MarkResolvedForInstanceEligible` | `MarkArchived`, `MarkResolvedForInstanceEligible` | `Tx.UpdateWhere` (#652). |

### Where the adapter departs from LTFlow or from #654's table

- **`created_at` is `enqueued_at` on the outbox and `recorded_at` on history.**
  Forma reserves `created_at` for the lakehouse projection, so a schema cannot
  declare it.
- **The claim's lease is on the transaction's clock.** LTFlow passes an
  absolute `leaseUntil` from the caller's clock and tests leases against the
  database's `now()`. `ServerTime(locked_until, lease)` keeps the lease and the
  claim condition's `$now` on one clock.
- **`MarkFailed` ends the lease instead of clearing `locked_until`.** An update
  cannot remove an attribute yet (#684), so it writes the transaction's clock.
  `status` still tells a failed message from a held one.
- **Definitions are a store of the unit of work.** LTFlow runs each definition
  method in a transaction of its own and serializes activations with an
  advisory lock. Here a concurrent activation that read the same latest
  sequence fails on the `(name, activation_sequence)` key with
  `ErrUniqueConflict`.
- **A deduplicated history event's row ID is derived from its dedup key**
  (`DedupRowID`, a UUIDv5), where LTFlow uses the key as the primary key.
- **The test files import more than `internal/testdb`, the DDL sources and the
  fault proxy.** The fixture also imports `internal` to build the unique keys
  init-db would build, and to install a federated engine wrapper, and
  `internal/model` for the `entity_main` column contract. The wrapper is a
  context-carried seam in `internal` that the factory applies: the suite builds
  its manager through the public factory and still counts every federated call,
  without adding a public option for a test. The adapter itself imports none of
  them.

## Contract and mechanism assertions

A **contract assertion** checks an outcome that must hold on any backend:
atomicity (all of an `InTx`'s writes and their `change_log` rows, or none),
exclusive and fenced claims, version fencing, uniqueness, per-partition FIFO,
and that every record a mutation, claim or upsert returns equals the committed
row. A losing transaction may answer `ErrRetryableConflict` on a backend with
optimistic concurrency. The suite then checks that it left no trace and does
not re-run it.

A **mechanism assertion** checks how PostgreSQL gets there: a blocked writer
seen in `pg_locks`, a claim that skips a held row without waiting, every loser
answering `ErrPreconditionFailed` or `ErrUniqueConflict` rather than
`ErrRetryableConflict`, and no deadlock in the soak or the lock-order test.
Each starts with `requirePostgresMechanisms(t)`, which skips it on any other
backend. Most are subtests named `mechanism: …`. `TestMechanisms`,
`TestClaimPlanUsesTheDeclaredIndexes` (PostgreSQL's plan) and #593's
"concurrent claim tests run against real PostgreSQL" are mechanism tests as a
whole.

Every test also asserts at teardown that the federated engine was never called.
Synchronization uses channels, held transactions and `pg_locks` polling with a
deadline, never a fixed sleep, and no test is skipped under `-short`;
`TestNoFixedSleepsOrShortGates` enforces both.

## Running

The suite runs in CI's Test job (`go test -race ./internal/...`), which
provisions PostgreSQL; `TestSuiteRunsInTheCITestJob` fails if the package moves
out of that job's reach. Locally:

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/forma?sslmode=disable \
  GOTOOLCHAIN=go1.26.0+auto go test -race ./internal/coordinationtest/
```

Without `DATABASE_URL`, `internal/testdb` builds the DSN from `DB_HOST`,
`DB_PORT`, `DB_USER`, `DB_PASSWORD` and `DB_NAME`, and skips when no database
answers (outside CI). Each test creates its own tables under a unique prefix
and drops them at the end.

To run the contract assertions against another engine, name it:

```bash
FORMA_COORDINATION_DSN='postgres://…' FORMA_COORDINATION_BACKEND=dsql \
  GOTOOLCHAIN=go1.26.0+auto go test ./internal/coordinationtest/
```

Any backend name other than `postgres` skips the mechanism assertions. A DSN
without a backend name fails the run.

Tests that claim over a growing outbox analyze the tables as it grows: the
EXPLAIN test after seeding, scenario 6 after each round's seeding, and the
soak after every 50 committed messages. Autovacuum would analyze a
deployment's tables at about those points, but it looks only once a minute,
longer than these tests last. Without fresh statistics PostgreSQL nests the
claim's group-head anti-join in a loop over the hot `change_log` rows. A
claim can then take seconds, and a soak iteration took 5 to 61 s instead of
about 2 s (#685).

**DSQL: specified, not verified (#633).** The contract assertions are written
to hold under DSQL's optimistic concurrency, but no one has run them there.
That run is #633's, with the fixture changes it needs: `CREATE INDEX ASYNC`
for the declared indexes, and another way to inject the storage failure of
scenario 1, which here is a PL/pgSQL trigger on `entity_main`.

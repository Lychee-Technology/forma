# Aurora DSQL

Forma runs on PostgreSQL 16 and on Amazon Aurora DSQL. The two speak the same
wire protocol and SQL dialect, but they keep concurrent writers apart
differently. PostgreSQL makes a writer wait for a lock. DSQL never makes a
statement wait: it lets both writers run and aborts one with SQLSTATE `40001`
at COMMIT. Forma's write path therefore has one implementation per engine
behind one API. It does not detect the engine. You configure it.

## What runs where

| Surface | PostgreSQL 16 | Aurora DSQL |
| --- | --- | --- |
| Entity writes: `Create`, `Update`, `Delete`, batches, `Tx` (CAS, fences, predicate mutation, upsert, claims, unique keys) | Supported | Supported on the `aurora-dsql` engine (#633). CI runs it on PostgreSQL emulating DSQL. It has not yet run against a cluster |
| CDC schema lock (`cdc-flush`, `cdc-init`, `manifest-reconcile`) | Supported | **Not supported yet.** The lock is `pg_try_advisory_lock`, which DSQL does not have. Its replacement is the second half of #633 |
| `forma-tools init-db` | Supported | Not supported: its DDL is not valid on DSQL (#690) |
| DuckDB `postgres` extension (CDC export, hot-tier federated reads) | Supported | Not verified (#691) |

## Choosing the engine

| Entry point | Setting |
| --- | --- |
| Library | `forma.DatabaseConfig.Engine`: `forma.EnginePostgreSQL` (`"postgresql"`, the default when empty) or `forma.EngineAuroraDSQL` (`"aurora-dsql"`) |
| `cmd/server` | `DB_ENGINE` |
| `cmd/lambda` | `DSQL_ENDPOINT` opens the IAM-authenticated DSQL pool and selects `aurora-dsql`. A `DB_ENGINE` that names another engine beside it fails the cold start |
| `forma-tools validate-schema-consistency` | `--db-engine` (default `DB_ENGINE`), used by `-requeue-stale-width-exports` |

An unknown name fails `Config.Validate` and the factory before any
connection is used. Falling back to PostgreSQL's locks on another database
would remove every write exclusion without an error.

**Every writer of one database must run the same engine.** The two engines'
locks do not see each other: a PostgreSQL-engine writer takes an advisory
lock, an `aurora-dsql` writer writes a guard row, and the two never exclude
each other. On a DSQL cluster the PostgreSQL engine cannot run anyway: its
first write fails at the advisory-lock statement.

The engine is configuration because the alternative fails silently. A
hostname test misses PrivateLink and custom endpoints. A PostgreSQL
deployment wrongly detected as DSQL would keep working with weaker
semantics: claims would stop skipping held rows, and waits would become
retryable errors.

## How the aurora-dsql engine excludes writers

Every write and every assertion of an entity row first upserts that row's
**guard row** in `<entity_main>_row_guard`:

```sql
CREATE TABLE <entity_main>_row_guard (
    ltbase_schema_id SMALLINT NOT NULL,
    ltbase_row_id    UUID     NOT NULL,
    generation       BIGINT   NOT NULL,
    PRIMARY KEY (ltbase_schema_id, ltbase_row_id)
);
-- before reading or writing the row
INSERT INTO <entity_main>_row_guard AS g VALUES ($1, $2, 1)
ON CONFLICT (ltbase_schema_id, ltbase_row_id) DO UPDATE SET generation = g.generation + 1;
```

Two transactions that write one guard row cannot both commit. DSQL aborts
the second committer with `40001`. Forma reports that as
`forma.ErrRetryableConflict`, and `forma.IsRetryable` is true for it. Forma
does not retry. The caller re-runs the operation, or the whole `Tx` function,
from a fresh snapshot ([Retrying](transactions.md#retrying)).

The engine's transactions open with a plain `BEGIN`, because DSQL accepts
only `ISOLATION LEVEL REPEATABLE READ`, its fixed level. They send no
advisory-lock function and no `FOR UPDATE`, `FOR SHARE` or `SKIP LOCKED`.
`TestDSQLEngineSendsOnlyStatementsDSQLRuns` checks this over every locking
path. A batch writes its guard rows in one sorted order, at most 1,000 per
statement, and names each row once.

The lock is a guard row rather than the `entity_main` row every write
already touches, for two reasons:

- A row that does not exist has no `entity_main` row to conflict on. An
  assertion that a row is missing, or the claim of a row that a concurrent
  create is writing, needs a row both transactions write. A guard row exists
  for any row ID ever written, and it outlives a delete. That is what makes a
  recreate conflict with the delete it races, which #274 requires.
- CI has only PostgreSQL. Under REPEATABLE READ, PostgreSQL commits an insert
  that follows a concurrent delete of the same key without `40001`, so the
  `entity_main` row alone could not reproduce the #274 interleaving in CI. A
  guard upsert conflicts the same way on both engines.

The cost is one extra modified row for every entity row a transaction
writes. Guard rows are never deleted. The table holds one small row per row
ID ever written, and it is derived state: it is never in `change_log`, never
flushed to S3 and never read by DuckDB. `init-db` creates it on every
PostgreSQL deployment, and the PostgreSQL engine leaves it empty. On DSQL,
where `init-db` cannot run yet (#690), create it with the statement above.
On the `aurora-dsql` engine the server refuses to start while the table is
missing. When `<entity_main>_row_guard` would pass PostgreSQL's 63-byte
identifier limit, the name is shortened as the unique-key tables' names are
([The key tables](unique-keys.md#the-key-tables)); `init-db` prints the name
it creates.

## What a caller sees differently

| Situation | PostgreSQL engine | aurora-dsql engine |
| --- | --- | --- |
| Two concurrent writes of one row | The second waits, then writes on top of the first's commit | One commits; the other gets `ErrRetryableConflict`, and its retry reads the first's commit |
| The loser of a conditional write ([Conditional writes](conditional-writes.md#aurora-dsql)) | `ErrPreconditionFailed` | `ErrPreconditionFailed` or `ErrRetryableConflict` |
| Two `AssertRow`s of one row in concurrent `Tx`s | Both commit | One gets `ErrRetryableConflict`: an assertion writes the guard row |
| A claim of rows another `Tx` holds ([Claims](claims.md#aurora-dsql)) | Skipped without waiting | Not skipped; one claimer gets `ErrRetryableConflict` |
| A predicate mutation's candidate changed concurrently ([Transactions](transactions.md#predicate-mutation-updatewhere-and-deletewhere)) | Judged again under its lock | The COMMIT fails with `ErrRetryableConflict` |
| A unique-key race ([Unique keys](unique-keys.md#aurora-dsql)) | The loser gets `ErrUniqueConflict` | `ErrUniqueConflict` or `ErrRetryableConflict` |
| Crossed writers of two rows | Possible deadlock, `ErrRetryableConflict` | One gets `ErrRetryableConflict`, on DSQL without a deadlock |

The invariants hold on both engines:

- A recreate of a reused row ID stamps a version above every tombstone of
  the row (#274).
- An update's read, merge and write lose no concurrent update (#457).
- Claims are exclusive and keep group order (#653).
- At most one creator of a unique value commits (#651).

## Limits

DSQL limits a transaction to 3,000 modified rows, 10 MiB and five minutes,
and a connection to one hour. The guard row counts toward the rows: a `Tx`
counts it against `WithTxMaxPhysicalRows`, whose 3,000 default matches DSQL's
limit. Atomic batches have no such meter, and the default batch size of 1,000
already reaches the limit on DSQL (#692). Keep the pool's
`ConnMaxLifetime` under an hour. The Lambda's default is five minutes.

## Verification

**In CI, on PostgreSQL 16.** The `aurora-dsql` engine runs on a pool whose
sessions default to REPEATABLE READ. A guard-row conflict there is a wait
and then `40001` at the guard write. DSQL gives the same outcome at COMMIT,
without the wait. What runs:

- `internal/postgres_persistent_repository_dsql_integration_test.go`:
  - #274: a recreate that races an in-flight delete cannot commit, and its
    retry stamps above the tombstone; a sweep of racing delete and recreate
    pairs.
  - #457: concurrent disjoint updates all survive.
  - #554: overlapping atomic batches in opposite orders lose nothing and do
    not deadlock; batch create and delete.
- The coordination suite (`internal/coordinationtest`), a second time with
  `FORMA_COORDINATION_BACKEND=dsql-emulated`. Its contract assertions run
  against guard-row locking: CAS, fences, predicate mutation, upserts,
  claims, unique keys and the engine-shaped soak. Its PostgreSQL mechanism
  assertions skip. The CI Test job has a step for it.

**Against a cluster.** Nothing in this section has run on a cluster yet. To
run the repository tests and the probe against one (pgx reads the token from
`PGPASSWORD`, which expires after 15 minutes by default):

```bash
export PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token --hostname "$ENDPOINT" --region "$REGION")
export FORMA_DSQL_DSN="postgres://admin@$ENDPOINT:5432/postgres?sslmode=require"
GOTOOLCHAIN=go1.26.0+auto go test ./internal -run 'TestDSQL' -v
```

`TestDSQLClusterProbe` prints #633's six-statement check and the two-session
advisory-lock check. It also asserts the one behaviour the engine depends on:
two transactions that upsert one guard row, whether present or not, never
both commit. Post its output on #633. The deterministic #274 test skips on a
cluster, because it waits for the emulation's lock wait. The sweep covers
#274 there.

The coordination suite runs against a cluster with
`FORMA_COORDINATION_DSN` and `FORMA_COORDINATION_BACKEND=dsql`. It has not
yet: its fixture builds indexes with plain `CREATE INDEX`, which #690
replaces, and it injects a storage failure with a PL/pgSQL trigger, which
needs another mechanism there.

## Open questions

- When two transactions insert a guard row that neither snapshot holds, DSQL
  may answer the second with `40001` or with `23505`. PostgreSQL answers
  `40001`. A `23505` would reach the caller as `ErrUniqueConflict` where
  `ErrRetryableConflict` is meant. The probe asserts `40001`, and its result
  settles this.
- Whether a statement on DSQL can fail with `40001` before COMMIT. Forma
  classifies `40001` the same wherever it arrives.

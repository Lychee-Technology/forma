# Aurora DSQL

Forma runs on PostgreSQL 16 and on Amazon Aurora DSQL. The two speak the same
wire protocol and SQL dialect, but they keep concurrent writers apart
differently. PostgreSQL makes a writer wait for a lock. DSQL never makes a
statement wait: it lets both writers run and aborts one with SQLSTATE `40001`
at COMMIT. Forma's write path and the CDC tools' schema lock therefore have
one implementation per engine behind one API. Forma does not detect the
engine. You configure it.

## What runs where

| Surface | PostgreSQL 16 | Aurora DSQL |
| --- | --- | --- |
| Entity writes: `Create`, `Update`, `Delete`, batches, `Tx` (CAS, fences, predicate mutation, upsert, claims, unique keys) | Supported | Supported on the `aurora-dsql` engine (#633). CI runs it on PostgreSQL emulating DSQL. It has not yet run against a cluster |
| CDC schema lock (`cdc-flush`, `cdc-init`, `manifest-reconcile`) | Supported: a session advisory lock | Supported with `--pg-engine aurora-dsql`: a lease row (#633). CI runs it on PostgreSQL. It has not yet run against a cluster |
| `cdc-flush`'s flush mark | Supported: one statement a batch | Supported: statements of at most 1,000 rows, each retried on `40001` (#633). Not yet run against a cluster |
| `forma-tools init-db` | Supported | Not supported: its DDL is not valid on DSQL (#690) |
| DuckDB `postgres` extension (CDC export, hot-tier federated reads) | Supported | Not verified (#691). `cdc-flush` and `cdc-init` export through it, so no CDC run on a cluster can succeed before it is |

## Choosing the engine

| Entry point | Setting |
| --- | --- |
| Library | `forma.DatabaseConfig.Engine`: `forma.EnginePostgreSQL` (`"postgresql"`, the default when empty) or `forma.EngineAuroraDSQL` (`"aurora-dsql"`) |
| `cmd/server` | `DB_ENGINE` |
| `cmd/lambda` | `DSQL_ENDPOINT` opens the IAM-authenticated DSQL pool and selects `aurora-dsql`. The pool signs a fresh IAM token for each connection it opens, so a warm environment keeps connecting after the cold start's token expires. A `DB_ENGINE` that names another engine beside it fails the cold start |
| `forma-tools validate-schema-consistency` | `--db-engine` (default `DB_ENGINE`), still validated; nothing uses it since #729 retired `-requeue-stale-width-exports` |
| `forma-tools cdc-flush`, `cdc-init`, `manifest-reconcile` | `--pg-engine` (default `postgresql`). `cdc-flush --pg-use-iam` authenticates to DSQL, so it implies `aurora-dsql` and refuses another engine beside it |

An unknown name fails `Config.Validate` and the factory before any
connection is used. Falling back to PostgreSQL's locks on another database
would remove every write exclusion without an error.

**Every writer of one database must run the same engine.** The two engines'
locks do not see each other: a PostgreSQL-engine writer takes an advisory
lock, an `aurora-dsql` writer writes a guard row, and the two never exclude
each other. On a DSQL cluster the PostgreSQL engine cannot run anyway: its
first write fails at the advisory-lock statement. The same holds for the
three CDC tools among themselves: on one database they must all run with
the same `--pg-engine`, or a flush and a reconcile of one schema would not
exclude each other.

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
| A write of a row a CDC flush is marking ([The flush mark](#the-flush-mark)) | The mark waits for the writer, and the two can deadlock (#686) | One of the two gets `40001`: the mark is sent again, and a writer gets `ErrRetryableConflict` |

The invariants hold on both engines:

- A recreate of a reused row ID stamps a version above every tombstone of
  the row (#274).
- An update's read, merge and write lose no concurrent update (#457).
- Claims are exclusive and keep group order (#653).
- At most one creator of a unique value commits (#651).

## The CDC schema lock

`cdc-flush`, `cdc-init` and `manifest-reconcile` each hold a per-schema lock
while they work on a schema, so a reconcile never races a flush or an init
of it (#290). On PostgreSQL the lock is `pg_try_advisory_lock(schema_id,
schema_id)` on a pinned session. DSQL has no advisory locks, and no session
a lock could live in. On the `aurora-dsql` engine a holder writes the
schema's row in `<entity_main>_schema_lease` instead:

```sql
CREATE TABLE <entity_main>_schema_lease (
    schema_id  SMALLINT NOT NULL,
    holder     UUID     NOT NULL,
    expires_at BIGINT   NOT NULL,  -- epoch milliseconds, database clock
    PRIMARY KEY (schema_id)
);
```

- **Acquire** is one upsert. It inserts the row, or takes over a row whose
  `expires_at` has passed, and returns nothing while a live lease holds the
  schema. Of two acquirers that race, one commits. The other gets `40001`
  (or `23505`), which counts as "held by another", not as an error.
- **Renew**: every 15 seconds the holder moves `expires_at` to 60 seconds
  past the database's `now()`, provided the row is still its own and live.
- **Release** deletes the row if it is still the holder's.

Expiry is judged on the database clock, so the holders' host clocks do not
matter. A holder that cannot renew stops 45 seconds (the TTL minus one
renewal interval) after the last renewal it sent, timed on its own
monotonic clock. The database set that renewal's expiry no earlier than
when it was sent, so the holder stops at least 15 seconds before another
holder can take the lease. Its work runs on a context that ends then, and
the tool's error carries `cdc.ErrSchemaLockLost` rather than a bare
"context canceled". The acquire sets the expiry as a renewal does, and the
holder times it the same way, from when it was sent. An acquire answered
more than 30 seconds after that, too late for the holder to reach its
first renewal, deletes its row and fails.

The table is named after `entity_main`, not `change_log`, because all three
tools are given `--entity-main-table` and only `cdc-flush` is given
`--change-log-table`. Tools that named the table differently would never
exclude each other.

The lease has no fencing token. A statement already sent when the lease
ends is not recalled, and a holder frozen for longer than the 15-second
margin (a suspended VM or container) can send one more statement after it
resumes. The advisory lock has the same limit: when its pinned connection
drops, PostgreSQL releases the lock, and the holder works on until its next
statement fails.

### The flush mark

On PostgreSQL the flusher marks a whole batch flushed in one statement. DSQL
caps a transaction at 3,000 modified rows, and the default flush batch is
10,000. So on `aurora-dsql` the mark goes out in statements of at most 1,000
rows, each its own transaction. A statement that loses a conflict with a
concurrent writer gets `40001` and is sent again, up to five attempts. The
mark stays exact against the version the flush exported: a row a writer
advanced in the meantime stays dirty for the next flush (#274). When a
statement fails for good, the rows the earlier statements marked stay
marked. They are in the delta the manifest already lists, and the rest stay
dirty, as after a mark that fails once the delta is appended (#252).

A mark statement and a writer of one of its `change_log` entries do not wait
for each other on DSQL, so #686's stall and deadlock do not arise there.
One of the two fails with `40001` instead. When the writer loses, its caller
gets `ErrRetryableConflict`.

### Operating the CDC tools on DSQL

- **Tables.** `init-db` cannot run on DSQL yet (#690). Create
  `<entity_main>_row_guard` and `<entity_main>_schema_lease` with the
  statements above, next to the core tables. On PostgreSQL, `init-db`
  creates both, and they stay empty.
- **Engine.** Run all three tools with `--pg-engine aurora-dsql`.
  `cdc-flush --pg-use-iam` implies it.
- **Authentication.** Only `cdc-flush` has `--pg-use-iam`. Give `cdc-init`
  and `manifest-reconcile` an IAM token as the password (`--pg-password` or
  `PGPASSWORD`), from `aws dsql generate-db-connect-admin-auth-token`. DSQL
  checks a token only when a connection opens, and it closes a connection
  after an hour. A run that outlives its token cannot reconnect, so give
  the token an `--expires-in` that covers the run. When renewals cannot get
  a connection, the lease ends the run with `ErrSchemaLockLost` rather than
  letting it continue unlocked. None of the three tools refreshes its token
  yet (#694). The Lambda's pool does: it signs a token per connection.
- **Inspecting leases.**

  ```sql
  SELECT schema_id, holder,
         expires_at - floor(extract(epoch from now()) * 1000)::bigint AS ms_left
  FROM entity_main_schema_lease;
  ```

  The table is empty when nothing runs. A row with `ms_left` above zero is a
  live holder.
- **After a crash.** A holder that dies leaves its row, which expires within
  60 seconds. The next run takes it over, and nothing needs doing by hand.
  Until then `cdc-flush` and `manifest-reconcile` skip the schema, and
  `cdc-init` fails it with "schema lock contended".
- **Removing a lease by hand.** Deleting a row frees its schema at once. Do
  it only for a holder known to be dead. A live holder finds its row gone at
  its next renewal, within 15 seconds, and stops, but statements it sends
  before then run unlocked.
- **"schema lock lost".** The run stopped because it found another holder
  in its row, or could not renew for 45 seconds. The schema fails even when
  its remaining steps reported no error: a step the tool logs and skips,
  such as a reconcile probe or a `--gc` delete, may have been skipped
  because the loss cancelled it. So `manifest-reconcile` exits 1, not 2.
  Re-run it. A flush or init cut short leaves what one that fails at the
  same point leaves.
- **"too late to reach its first renewal".** The database took more
  than 30 seconds to answer the acquire. The run gave the lease up before
  doing any work, so the schema is free. Re-run it once the cluster answers
  promptly.

## Limits

DSQL limits a transaction to 3,000 modified rows, 10 MiB and five minutes,
and a connection to one hour. The guard row counts toward the rows: a `Tx`
writes each row's guard once, however many of its operations lock the row,
and counts it against `WithTxMaxPhysicalRows`, whose 3,000 default matches
DSQL's limit. Atomic batches have no such meter, and the default batch size
of 1,000 already reaches the limit on DSQL (#692). The CDC flush mark is
chunked to stay under it. Keep the pool's `ConnMaxLifetime` under an hour.
The Lambda's default is five minutes.

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
- `internal/cdc/schema_lease_integration_test.go`, the schema lease:
  exclusion and release; takeover of an expired lease only; renewal past
  the TTL; a holder that finds another's token stops and leaves that lease
  alone; a holder whose renewals are blocked stops while the database still
  counts its lease live; an acquire answered too late to reach its first
  renewal fails and frees the schema. Acquirers racing for a free and for
  an expired lease leave exactly one holder, on READ COMMITTED and
  REPEATABLE READ sessions.
- `internal/cdc/mark_flushed_dsql_test.go`, the flush mark: statements stay
  at 1,000 rows or fewer; on a REPEATABLE READ session, a writer's commit
  fails the mark with `40001`, and the retry marks the rows at their listed
  version and leaves dirty the row the writer advanced.

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

`FORMA_DSQL_DSN` also runs the schema-lease tests against the cluster
(`go test ./internal/cdc -run TestLeaseSchemaLock -v`). The acquire is the
only statement on the DSQL path that pairs `ON CONFLICT ... DO UPDATE ...
WHERE` with `RETURNING`. The exclusion test checks its contract there: an
acquire that a live lease blocks returns no row, so `TryLock` answers "not
acquired" rather than an error. The blocked-renewal and slow-acquire tests
skip on a cluster, since DSQL has no lock wait to block on. A `cdc-flush`
run against a cluster also waits on the DuckDB `postgres` extension (#691).

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

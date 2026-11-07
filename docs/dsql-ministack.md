# MiniStack strict DSQL lane

`make test-dsql-ministack` runs Forma's `aurora-dsql` suites through
[MiniStack](https://ministack.org/docs/services/dsql)'s strict SQL proxy (#739).
It needs Docker and a Go toolchain, and no AWS credentials. It takes about two
minutes. CI's `dsql-ministack` job runs the same target.

The local database service named `aurora-dsql` in `deploy/docker-compose.yml` is
`postgres:16`, and ordinary PostgreSQL accepts statements DSQL refuses. This lane
adds the missing check. It does not replace the PostgreSQL run, the `dsql-emulated`
run or validation on a real cluster.

**A pass here is an emulator result, not Aurora DSQL compatibility.** The proxy
is regex-based validation in front of PostgreSQL 16. MiniStack calls it "a
compatibility gate over Postgres, not a full Aurora DSQL engine".

| Layer | What it shows |
| --- | --- |
| [`make lint-dsql`](dsql-lint.md) | Statements in the corpus pass DSQL's documented static rules |
| This lane | What the suites send passes the proxy's rules and works on PostgreSQL behind it |
| [A real cluster](aurora-dsql.md#verification) | It runs on DSQL. The only authority for the list below |

## What a pass does not establish

Only a real cluster can establish these:

- **Optimistic concurrency and `40001` timing.** Conflicts are PostgreSQL's: a
  writer waits on a row lock, then fails at the statement. DSQL never waits, and
  fails at COMMIT. The lane proves an invariant survives PostgreSQL's conflict
  handling, not DSQL's.
- **IAM.** The proxy accepts any user and password, and the lane sends none.
- **TLS.** The proxy answers `N` to a TLS request, so the DSN uses
  `sslmode=disable`. DSQL requires TLS.
- **Connection lifetime.** Nothing closes a connection after an hour.
- **Transaction limits.** The proxy counts rows from `VALUES` tuples only, so
  `UPDATE`, `DELETE` and `INSERT ... SELECT` are not counted.
- **Catalogs and protocol.** `pg_locks`, `pg_stat_activity` and `pg_index` answer
  as PostgreSQL's do, and `EXPLAIN` shows PostgreSQL's planner.

The proxy's rules are not complete, so a pass does not clear every statement.
Observed on the pinned version: it forwards `pg_advisory_xact_lock` and
`BEGIN ISOLATION LEVEL READ COMMITTED` to PostgreSQL, which runs them.
`TestDSQLEngineSendsOnlyStatementsDSQLRuns` keeps these out of the engine's
statements, not the proxy.

## Coverage matrix

Results on MiniStack 1.5.38. "Pass" means on this lane, with the meaning above.

| Surface | Result | Run by, or why not |
| --- | --- | --- |
| Entity CRUD and batches | Pass | `TestDSQLEngine*` in `internal/`, including the deterministic #274 race, the #274 sweep, #457 and #554 |
| Transactions | Pass | `TestTransactionsRollBack` and the rest of the coordination suite, `TestDSQLEngineTxChargesEachRowGuardOnce` |
| CAS and fences | Pass, some skip | Contract assertions of `TestCASScenario`, `TestFenceScenario`. Mechanism assertions skip: see below |
| Claims | Pass, some skip | Contract assertions of `TestClaimExclusivity`, `TestEngineShapedSoak`. The same skip |
| Unique keys | Pass, some skip | Contract assertions of `TestUniqueKeyScenario`, `TestUniqueness`. The same skip |
| CDC schema lease | Pass, one skip | `TestLeaseSchemaLock*`, including the two lock-wait tests a cluster skips. The READ COMMITTED race skips, since the lane runs only DSQL's level |
| CDC flush mark | Pass | `TestMarkInChunksRetriesARealConflict`. The `cdc-flush` tool itself and its export reads are not in the lane |
| Schema and index DDL: what the fixtures build | Pass | One DDL a transaction, `CREATE INDEX ASYNC` and `sys.wait_for_job`. The ASYNC rewrite is the coordination fixture's own |
| Generation ledger upgrade (#738) | Pass, one skip | `TestLedgerUpgrade*` in `cmd/tools/`: see [below](#the-ledger-upgrade-suite). The PostgreSQL lock-wait mechanism of a concurrent apply skips |
| Schema and index DDL: `init-db` (#690) | Fail, not in the lane | Observed by hand: `only one DDL statement can be run in a transaction` (`25006`) at `ensure entity main table`. Declared indexes use `CREATE INDEX CONCURRENTLY`, which the proxy also refuses |
| DuckDB `postgres` extension: reads and exports (#691) | Fail, not in the lane | Observed by hand: `ATTACH` succeeds, then every scan fails with `COPY is not supported`, because the extension reads through `COPY ... TO STDOUT (FORMAT binary)` |

The two "not in the lane" rows were run once by hand against this pin. They would
only ever fail until #690 and #691 land, and each of those issues should add its
surface to the lane.

The mechanism assertions skip because they describe the PostgreSQL *engine*: a
wait followed by `ErrPreconditionFailed`, `SKIP LOCKED`, no `ErrRetryableConflict`.
The `aurora-dsql` engine differs by design ([Aurora DSQL](aurora-dsql.md#what-a-caller-sees-differently)),
so they do not apply to the engine under test on any target. They run on the
PostgreSQL lane, so nothing goes uncovered.

[`scripts/dsql_ministack_expected_skips.txt`](../scripts/dsql_ministack_expected_skips.txt)
records every expected skip with its reason. The lane fails on a skip the ledger
does not cover and on a ledger line that covers no skip.

### The ledger upgrade suite

`TestLedgerUpgrade*` runs the upgrade in
[Upgrading an existing deployment](schema-evolution.md#upgrading-an-existing-deployment)
on a deployment from before the generation ledger: a registry with two
schemas, built from init-db's own table sources one DDL a transaction, and
stored rows. CI runs the same tests on the integration PostgreSQL in the Test
job, and this lane runs them through the proxy.

| Step | What runs |
| --- | --- |
| Add the ledger | The three `schemaledger.Statements`, each in its own transaction, twice. Before them both tools name the upgrade |
| Baseline, `--no-parquet-tier` | Dry run and real run refuse an EAV value outside the declaration and record nothing for any schema. Once the data is corrected, it records generation 1 of each schema, once. Generation 1 comes from `baseline-schema`, never `ledgertest.Seed` |
| Baseline, parquet tier | `baseline-schema`'s tier from its flags: S3 manifests and objects on MiniStack's S3, decoded by DuckDB over httpfs. DuckDB never reads PostgreSQL here, so #691 does not apply. A live object holding `12.5` for an integer refuses the baseline, and a corrected manifest lets it record |
| `apply-schema` | Dry run, an allowed widening, a rerun that records nothing, and a run whose one narrowing records nothing for either schema |
| Data-plane start | The gated registry load refuses the deployment before the ledger and before the baseline, starts after it, and refuses a restart on a superseded declaration |
| CDC re-read | `schemaledger.RequireUnchanged`, the per-batch check of `cdc-flush` and `cdc-init`, on the started registry before and after an apply |
| Concurrent applies | Two applies judged against generation 1, held until both reach the compare-and-set. Exactly one commits a complete generation 2, and the ledger has no partial or orphan rows |

The concurrency test checks only the committed state, and accepts either
`ErrGenerationConflict` or `40001` from the loser. Behind the proxy the loser
waits on the winner's row lock and then fails with `40001`, because the lane
pins REPEATABLE READ. A real cluster does not wait, and fails the loser at
COMMIT. `TestLedgerUpgradeConcurrentApplyLoserWaitsOnPostgres` asserts the
PostgreSQL mechanism, a wait and then `ErrGenerationConflict` at READ
COMMITTED. It runs on the PostgreSQL lane and skips here.

Beyond the limits above, a pass here does not establish:

- **The DDL on DSQL.** The proxy's rules accepted
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` and both `CREATE TABLE`s, and
  PostgreSQL ran them. Whether a cluster accepts them is #821's to verify.
- **The tools' connection.** The suite calls `baseline-schema` and
  `apply-schema` on a pool, not through their flags. An IAM token as the
  password, and whether a long audit outlives it, are #821's and #743's.
- **S3.** MiniStack's S3 is an emulator: no IAM, and no TLS.

## How it knows it reached the proxy

MiniStack reports a cluster `ACTIVE` in two cases where it has no SQL at all:
without `DSQL_STRICT=1`, and when the backend fails to start, which it logs as a
warning before serving the cluster metadata-only. So `ACTIVE` proves nothing, and
the lane checks three things before any suite runs:

1. The cluster's backend container exists.
2. `TestMiniStackStrictProxy` gets an answer to a query through the endpoint the
   cluster reported, within 90 seconds. Nothing listens on a metadata-only port.
3. The same test sees a plain `CREATE INDEX` on a real table refused with the
   proxy's `0A000` over both wire protocols, then `CREATE INDEX ASYNC` and
   `sys.wait_for_job` succeed, a row round-trips, and the session runs at
   REPEATABLE READ. PostgreSQL would build the plain index and reject the ASYNC one.

`TestProveMiniStackStrictProxyRefusesDirectPostgres` runs this check against the
integration PostgreSQL in CI's Test job, so the check itself stays honest.

If the gate fails, no suite runs and the lane fails.

## Choices worth knowing

**The backend's isolation is set by the lane.** MiniStack starts PostgreSQL at
READ COMMITTED, and the proxy does not pass a client's startup parameters on, so
the lane runs `ALTER DATABASE ... SET default_transaction_isolation` on the backend
directly. At READ COMMITTED a guard-row conflict waits and then commits, so the
#274 tests would fail for a reason that says nothing about Forma. That statement
is the lane's configuration of its emulator, not something Forma sends to DSQL.

**`FORMA_DSQL_TARGET=ministack` tells a test it is not on a cluster.**
`FORMA_DSQL_DSN` alone means a genuine cluster, and tests that need a lock wait
skip there. PostgreSQL behind the proxy has that wait, so without this variable
they would skip here too and quietly lose coverage. A misspelt value, or the value
without a DSN, fails the test.

**Sessions are named after they connect.** The proxy drops `application_name` from
the startup message, and the tests find their own lock waits by that name.

**A run removes only what it created.** It claims the network, then the container
name, both of which Docker refuses to create twice, and only then clears its logs.
A run that loses either claim, to another run or to something unrelated under that
name, exits non-zero and leaves what it found as it was. From then on the run works
through the IDs Docker gave it, so nothing it does can land on a container that
merely has the name.

**Teardown fails if it cannot confirm the result.** Removal is judged by Docker's
listings, not `docker inspect`, whose failure looks the same for a missing container
and an unreachable daemon. Each step still runs after an earlier one fails.

## Running it

```bash
make test-dsql-ministack        # the whole lane, torn down afterwards
make test-dsql-ministack-down   # removes what an interrupted run left behind
```

The down target has no IDs from the run it cleans up after, so it goes by the name
and the `forma.dsql-ministack.project` label the run put on its container and
network. It leaves anything without that label and exits non-zero. Set the same
`MINISTACK_DSQL_PROJECT` and `DSQL_DOCKER_NETWORK` the run had. It cannot tell an
interrupted run from an active one, and removes an active run's resources too.

| Variable | Default | Meaning |
| --- | --- | --- |
| `DSQL_BASE_PORT` | `25432` | SQL port, published on `127.0.0.1` only |
| `MINISTACK_DSQL_PROJECT` | `forma-dsql-ministack` | Container name, default network name, and the logs' directory. It must not already exist as a container |
| `DSQL_DOCKER_NETWORK` | the project name | Network the run creates and removes. It must not already exist |

To run two lanes at once, give each its own `MINISTACK_DSQL_PROJECT` and
`DSQL_BASE_PORT`. A second run with the same project is refused.

Logs and a per-suite summary go to `.artifacts/dsql-ministack/<project>/`, and CI
uploads them when the job fails. These all fail the run rather than skip: no Docker
access, a missing socket, a failed image pull, an occupied port, a container or
network already under the lane's names, a metadata-only cluster, an unreachable
proxy, a suite below its floor of passing tests or with results that cannot be
read, a log that cannot be written, and a cleanup that is incomplete or cannot be
confirmed.

The lane mounts the Docker socket into MiniStack, which then starts its backend
as a sibling container. It has only been run against a Docker daemon, on
linux/amd64. `DOCKER_HOST` must be a local unix socket if it is set.

## Moving the pin

Both images are pinned by digest in `scripts/test_dsql_ministack.sh`. MiniStack's
proxy rules change between releases, so an upgrade can fail the lane because
MiniStack got stricter or because Forma has a real problem. Read the failing
statement before deciding which, and update this page's results with the new
version.

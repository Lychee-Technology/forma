# Production E2E Harness

Reusable end-to-end test harness that exercises the **real production
stack** (#173, epic #172): real containers (Postgres 16 + RustFS via
testcontainers — the official MinIO docker image is archived — plus
in-process DuckDB), real writes through
`internal.NewEntityManager`, the real CDC flusher (`internal/cdc`), the real
federated engine (`internal/federated`), real schema metadata from fixture
JSON, and Protocol V2 publication (`docs/publication-protocol-v2.md`): a
publication record per schema in PostgreSQL naming an immutable manifest
generation in S3. Every query result is checked against an
**independent oracle** that never consults the engine.

Run it:

```bash
make test-e2e-production
# or
go test -v ./internal/e2e_harness/production/ -tags=e2e -timeout=10m
```

Oracle and generator unit tests are untagged and run with `make test`.

## Writing a test

```go
func TestMyCase(t *testing.T) {
    cluster := production.SharedCluster(t)      // containers shared per test binary
    env := production.NewEnv(t, cluster)        // own DB + S3 prefix + DuckDB
    ctx := context.Background()
    wide := production.DefaultSchemaFixtures()[1] // e2e_wide

    events := env.GenerateScript(production.ScriptSpec{Schema: wide, Creates: 20})
    _ = env.ApplyEvents(ctx, events...)         // real EntityManager writes

    _, _ = env.RunInit(ctx, wide)               // cold tier (cdc.RunInit)
    _, _ = env.RunFlush(ctx)                    // warm tier (cdc.Runner)

    env.AssertQueryMatches(ctx, production.Query{
        Schema:  wide,
        Filters: []production.Filter{{Attr: "count", Op: "gte", Value: "100"}},
        Sorts:   []production.Sort{{Attr: "count"}},
        Limit:   10,
    })
}
```

`AssertQueryMatches` runs the engine and the oracle from the same `Query`
spec and compares totals, row IDs (ordered when sorted), and normalized
attribute values. On mismatch it dumps diagnostics and fails with the
artifact path.

## Isolation model

Containers are shared per test binary (`SharedCluster`, skipped when docker
is unavailable). Each `NewEnv`:

- creates its own PostgreSQL **database** with the standard production
  table names (`entity_main`, `eav_data`, `change_log`, `schema_registry`).
  The harness DDL (`ddl.go`) mirrors `cmd/tools/init_db.go` (#174):
  `value_numeric NUMERIC`, per-tier `bigint`/`double` columns, and a nullable
  `change_log.deleted_at`.
  CDC advisory locks are keyed `(schemaID, schemaID)` *per database OID*
  and `RunOnce` scans the whole `change_log`, so a private database gives
  natural isolation and free multi-schema support (#186);
- gets its own S3 prefix `e2e/<runID>/env<N>` and in-memory DuckDB client
  (1 GB default memory limit, `WithDuckMemoryMB` to override);
- registers the fixture schemas (IDs 20-24; registration rejects the
  sqlgen benchmark-reserved range 100-102) and builds the `x-forma-indexes`
  indexes they declare through the function `init-db` calls (#647);
- registers cleanup: close DuckDB/pool, `DROP DATABASE ... WITH (FORCE)`,
  delete the S3 prefix. Tests may use `t.Parallel()`.

**How this maps to #173's "isolated schema ID, tenant, bucket prefix"
constraint:** forma's storage and query model has no first-class tenant —
the DDL carries no tenant column and no API takes a tenant key; deploy-level
tenancy is a database + S3 prefix pair. The per-test database therefore *is*
the tenant boundary, and the fixture schema IDs (20-24), while numerically
identical across tests, live in private databases and private S3 prefixes —
no state, lock, or parquet file is reachable from another test. A test that
needs schema IDs beyond the fixtures can register its own via
`RegisterSchemas` + `WithSchemaDir`.

Options: `WithSeed`, `WithSchemaDir`, `WithFlushThresholds` (#179),
`WithDuckMemoryMB`, `WithBreaker` (#185), `WithDuckMaxConnections`,
`WithRoutingStrategy`, `WithMetricEmitter` (#634).

Provisioning registers the fixture schemas the way `init-db` does, including
each schema's `UNPUBLISHED` publication record (#896). Writing a record is a
transition only `init-db` may make in production, and `internal/publication`'s
owners guard scans every non-test file, so the record initializer lives in a
test file (`publication_records_test.go`) that `provision` calls through a
hook. The engine is assembled like the factory's: the V2 publication reader
(`internal/federatedv2`) with every fixture schema's epoch pinned when the
engine is built. `PublicationReaderWrap` decorates that reader before the
first query, which is how a test pauses a pass between its record read and
the rest of it (`published_pass_race_e2e_test.go`).

## Fixture schemas (`schemas/`)

| Schema | ID | Purpose |
|---|---|---|
| `e2e_simple` | 20 | minimal: bound text + EAV numeric |
| `e2e_wide` | 21 | one attribute per scalar `forma.ValueType`, mixed main-column/EAV storage (#174) |
| `e2e_second` | 22 | second schema for multi-schema isolation (#186) |
| `e2e_nested` | 23 | dotted (nested-object) attribute names, one column-bound and one EAV-only (#260) |
| `e2e_outbox` | 24 | LTFlow-shaped outbox whose status and time attributes sit behind `x-forma-indexes` declarations (#647) |

Fixtures define only the attributes their tests use: the synthetic
`name`/`version` injection in `sqlgen.BuildSchemaProjection` was removed in
#192, so schemas need no workaround attributes. (`e2e_simple` keeps a real,
column-bound `name` attribute — proving a schema that legitimately defines
one still works.)

**Type coverage (#174).** `e2e_wide` now carries one attribute per scalar
`forma.ValueType` — 17 in all, split across column-bound and EAV storage — and
the full-type round-trip (`full_type_roundtrip_e2e_test.go`) drives every one
through hot/warm/cold with exact-value assertions; the pinned 24-column physical
parquet schema is checked bidirectionally by `assertWideParquetSchema` (a
missing *or* unexpected column fails the test). NULL, empty-string, zero, and
boundary values (`boundary_roundtrip_e2e_test.go`) round-trip exactly, with two
documented ceilings: the faithful **bound `bigint`** range is ±2^62 — int64 is
marshalled through float64 on both write and the federated read, so larger
magnitudes lose precision and a stored `MaxInt64` crashes the read (tracked in
#205) — and **EAV integers** are exact to ±2^53 by the float64 value model (by
design; since #590 the write funnel refuses an EAV-only `bigint` past that,
where it used to store the rounded image). `forma.ValueTypeList` does **not** round-trip: the write path rejects
`list` outright at `transform.populateTypedValue`, so the fixture defines a
`tags` list attribute that `list_roundtrip_e2e_test.go` uses to pin the
rejection contract live, with the end-to-end acceptance path written as a
skipped subtest — blocked by #204.

Note: a schema that has never published (`UNPUBLISHED`) answers every query
from PostgreSQL, with no DuckDB statement, whatever the routing (#720, #896).

## Events, CDC, queries

- `ApplyEvents` drives `Create`/`Update`/`Delete` through the real
  EntityManager and reads the wall-clock `changed_at`/`deleted_at` back
  from `change_log` into each event (timestamps cannot be seeded).
  Production deletes **hard-delete** `entity_main`/`eav_data` and leave a
  `change_log` tombstone.
- `GenerateScript` is seed-deterministic; repeated calls continue one
  stream so ordinal-encoded values stay unique per Env.
- `RunFlush`/`RunFlushDry` wrap `cdc.Runner.RunOnce`; reports capture
  unflushed counts, new S3 objects, the published generation of every
  published fixture schema, and the runner's own per-schema report. A dry
  run writes no object, marks nothing and publishes nothing (#180).
- `RunFlushWith(FlushOverrides{S3, Config, DryRun})` runs one flush pass
  with per-run overrides and reports observable state *even when the run
  fails* (report is non-nil unless the pre-run capture fails), so
  failure-boundary tests can assert partial side effects alongside the
  error. Pair it with `FaultInjectingS3` — a decorator over the real S3
  client that fails calls matching an `S3Fault{Op, KeyContains,
  SkipMatches}` — to break exactly one step of the flush pipeline and pin
  the failure matrix (#179).
- `RunInit` wraps `cdc.RunInit`: the V2 `cdc-init` exports the schema's live
  rows to base objects under fresh keys, publishes an `init` generation that
  lists only them (every earlier base and delta leaves the inventory), and
  hands the exported rows' entries off by marking them flushed at their
  export versions. It deletes no object: superseded ones stay on S3, unlisted,
  for garbage collection (#891). The harness keeps each schema's latest init
  report, which the onboarding helpers below check.
- `Env.EntityManager()` is the public API on a deployment that runs DuckDB:
  every non-transactional `Query` enters the federated engine, and the Env's
  routing strategy (`WithRoutingStrategy`) decides whether it reads DuckDB
  or Postgres alone; `hybrid` answers a page under 1000 rows from Postgres.
  Left unset, the strategy is empty, which the engine routes like an unknown
  strategy: every query to DuckDB. Whether a query is federated is the
  deployment's decision, not the request's (#715).
  `Env.DuckDBRoutedEntityManager()` is the same deployment with
  `freshness-first` routing, which sends every query through a DuckDB pass;
  tests that must read the parquet tiers through the public API use it.
  `Env.RoutedEntityManager(strategy)` builds one for any strategy. Each is
  memoized over its own engine and plan cache.
  `Env.PostgresEntityManager()` is the deployment without DuckDB: every
  `Query` runs on Postgres and, asked for the plan, answers none, which is
  how a PostgreSQL control pins its route (`postgresRouteQuery`). A control
  that reads the federated deployment's Postgres route instead uses
  `RoutedEntityManager(hybrid)` and requires a plan with `UsedDuckDB` false.
  `EntityManager()` routes through DuckDB, so it is no PostgreSQL control.
  `TestServerTimeParityAcrossDeploymentsAndTiers`
  (`server_time_parity_e2e_test.go`) compares `$now` answers across a
  Postgres-only deployment and a federated deployment's Postgres and DuckDB
  routes, with rows hot, in delta and in base parquet.
- `Query`/`AssertQueryMatches` translate one spec for both engine and
  oracle. Filters use the public `"op:value"` condition forms (bool values
  are `"1"`/`"0"`; dates accept ISO strings or epoch millis).
- `ExecSQL` is the escape hatch for failure-state injection (#179/#181). No
  helper clears `change_log`: Protocol V2 never does, and the tools take rows
  out of the dirty set themselves. The helpers of `handoff.go` only check that
  they did, named for what the test means: `MarkOnboarded`/
  `MarkRowsOnboarded`/`MarkAllOnboarded` require the latest `RunInit`'s
  handoff to have marked every entry it exported, and
  `RetireFlushed`/`RetireAllFlushed` require a flush to have left no unflushed
  entry. E2E scenarios read the published inventory through
  `manifest_access.go` (`Manifest`, `ManifestEntry`, `TierEntries`,
  `loadSchemaManifest`: a view of the generation the record names, from its
  verified manifest), compare publications through `publication_state.go`
  (`publishesBetween`, `snapshotPublication`/`publicationChanges`, over the
  record) and stage inventory faults through `publication_faults_test.go`
  (`poisonManifestChecksum`, `restampBaseColumns`,
  `RegisterParquetInManifest`, `RunInitKeepingDelta`), each of which commits
  a test-made generation through the record's compare-and-set, since
  generations are immutable. A fabricated object must have a V2 key of the
  schema's epoch (`newTestParquetKey`). Only those helper files and the
  untagged unit tests of the harness's inventory reader and S3 fault
  decorator (`manifest_load_gate_test.go`, `faults_test.go`) import
  `internal/manifestv2` or `internal/publication`, and no test imports the
  V1 `internal/manifest`; `TestE2ETestsReachPublicationOnlyThroughHelpers`
  keeps these rules (#901, #896). It is syntactic: it catches a `change_log`
  delete in a literal (quoted or schema-qualified) and one that takes
  `Tables.ChangeLog` as its target through `+` or `Sprintf`, not SQL
  assembled any other way at run time.
- `InjectRestore` (#175) re-materializes a hard-deleted row under its
  original row_id at the storage layer — production has no restore API
  (Update on a deleted row returns `ErrNotFound`, pinned live) — with the
  restore timestamp forced past the tombstone's `changed_at` so the revival
  wins LWW. Scoped to text/numeric attributes (e2e_simple shapes).
- `RunCompaction`/`RunCompactionWith` run one V2 compaction pass (#890), wired
  as the compactor tool wires it. `Env.reconciler` (`reconcile.go`) wires
  manifest-reconcile's V2 diagnosis and collector (#891) the same way, under
  time parameters short enough to wait out a retention grace.
- `Env.RestartPostgres` (#175) restarts the Postgres container in place
  (docker stop/start, data preserved) and rebinds the pool, CDC config,
  DuckDB client, and lazy engine/manager. Restart tests must own a
  `DedicatedCluster` — restarting the shared cluster would break every
  parallel Env — and are skipped on external infrastructure.
- `Cluster.HaltS3` (#185) stops the S3 container in place (docker stop) so
  the endpoint becomes genuinely unreachable — the honest way to fault the
  DuckDB httpfs read path, which cannot be broken at the Go-client layer.
  Like `RestartPostgres` it mutates cluster-wide state, so only tests owning
  a `DedicatedCluster` may call it (halting the shared cluster's S3 would
  break every parallel Env); external infrastructure cannot be halted. The
  halted container is reaped by the dedicated cluster's normal shutdown.
- `Env.ReopenDuckDB` (#185) replaces the Env's (possibly closed) DuckDB
  client with a fresh one and drops the lazy engine/manager so the next use
  rebinds them. The cached circuit breaker **deliberately survives** the
  rebuild — breaker state must span client rebuilds so recovery scenarios can
  observe the open-to-closed transition against a healthy DuckDB.
- `WithBreaker(maxFailures, cooldown)` (#185) enables the federated engine's
  circuit breaker; `WithDuckMaxConnections(n)` overrides the per-test DuckDB
  pool size (default 2). Since #245 every pooled connection self-configures
  via the driver's per-connection init hook, so this is purely a pool-sizing
  knob — concurrent DuckDB queries need no pinning.
- `WithMetricEmitter(emitter)` (#634) hands the engine a `forma.MetricEmitter`
  so a scenario can assert what it emits; unset, the engine emits nothing.
  `TestCircuitBreaker_OpensAtThresholdAndRecovers` uses it to check the
  `duckdb_circuit_breaker_transition_total` samples of every leg.
  Set `Query.AllowPartialDegradedMode` to turn on the engine's degraded
  fallback, which a deployment sets through
  `DuckDBConfig.AllowPartialDegradedMode` (#715), so a tier outage falls back
  to a postgres-only result (complete in today's PG-retains-all model)
  instead of erroring.

## Diagnostic artifacts

On failure (or `KEEP_E2E_ENV=1`) the Env writes
`.artifacts/e2e/<runID>/<testName>/` (override root with
`E2E_ARTIFACTS_DIR`); the path is printed in the failure message.

In CI, a failed E2E Smoke or nightly production `-race` job uploads this
tree as a workflow artifact (`e2e-harness-artifacts-attempt-<n>` or
`production-race-e2e-artifacts-attempt-<n>`, kept 14 days). The repository
is public, so those artifacts are too: whatever the harness dumps must stay
free of credentials. Today `run.json` carries a redacted DSN and the plan
SQL in `query_<n>.json` has its connection-string password redacted.

| File | Content |
|---|---|
| `run.json` | seed, run ID, test, DB DSN, bucket/prefix, endpoints, git SHA |
| `events.json` | full event log incl. read-back timestamps |
| `change_log.json` | `SELECT * FROM change_log ORDER BY schema_id,row_id,flushed_at` |
| `s3_listing.json` | key/size/etag/last-modified under the prefix |
| `publication_records.json` | each fixture schema's publication record |
| `manifest_<id>.json` | raw bytes of the manifest each published record names |
| `parquet/<key>.{schema,sample}.json` | DuckDB `DESCRIBE` + up to 20 sampled rows |
| `query_<n>.json` | Query spec, FederatedAttributeQuery, full plan (SQL + params + routing + timings), row IDs |
| `diff.json` | expected-vs-actual totals, missing/extra/misordered rows, attribute mismatches |

## Environment variables

| Var | Effect |
|---|---|
| `E2E_SEED` | pins the cluster base seed (per-test seeds derive from it + test name) |
| `E2E_ARTIFACTS_DIR` | artifact root override |
| `E2E_VERBOSE=1` | verbose harness logging |
| `KEEP_E2E_ENV=1` | keep containers/DB/S3 alive and print connection info; the harness disables the testcontainers reaper itself (set `TESTCONTAINERS_RYUK_DISABLED` explicitly to override) |
| `PRODUCTION_E2E_EXTERNAL_PG_DSN` / `..._S3_ENDPOINT` / `..._S3_BUCKET` / `..._S3_REGION` / `..._S3_ACCESS_KEY` / `..._S3_SECRET_KEY` | use external infrastructure instead of containers (the PG user must be able to CREATE DATABASE) |

## Consumer map (epic #172)

| Issue | Building block |
|---|---|
| #174 full-type coverage | `e2e_wide` + `FullTypeProfile` |
| #176 init handoff | `RunInit` + `RunFlush` (`init_handoff_e2e_test.go`, `init_delete_e2e_test.go`, `init_rerun_e2e_test.go`, `init_concurrent_e2e_test.go`) |
| #175 lifecycle sequences | `Event` model + `ApplyEvents` + `InjectRestore` + `RestartPostgres` (`lifecycle_e2e_test.go`, `restart_e2e_test.go`) |
| #179 flush failure matrix | `FaultInjectingS3` + `RunFlushWith` + `ExecSQL` |
| #180 dry-run semantics | `RunFlushDry` |
| #181 failure injection | `ExecSQL` |
| #185 degraded mode & circuit breaker | `WithBreaker`, `Query.AllowPartialDegradedMode`, `Cluster.HaltS3`, `Env.ReopenDuckDB` |
| #186 multi-schema isolation | `e2e_simple`/`e2e_second` + `FaultInjectingS3`/`PausingS3` (`multi_schema_isolation_e2e_test.go`, `concurrent_flush_multi_schema_e2e_test.go`) |
| #250/#896 factory/server publication-reader wiring | `forma.DefaultConfig` + `factory.NewEntityManagerWithConfigContext` over `Env.Pool`/`Tables`/`Registry`/`DuckCfg` + `RegisterParquetInManifest` (`factory_manifest_e2e_test.go`) |
| #647 declared indexes | `e2e_outbox` + `declaredindex` (`declared_index_explain_e2e_test.go`, `declared_index_build_e2e_test.go`) |

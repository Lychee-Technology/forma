# Production E2E Harness

Reusable end-to-end test harness that exercises the **real production
stack** (#173, epic #172): real containers (Postgres 16 + RustFS via
testcontainers — the official MinIO docker image is archived — plus
in-process DuckDB), real writes through
`internal.NewEntityManager`, the real CDC flusher (`internal/cdc`), the real
federated engine (`internal/federated`), real schema metadata from fixture
JSON, and an S3-backed manifest. Every query result is checked against an
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
- registers the fixture schemas (IDs 20-22; registration rejects the
  sqlgen benchmark-reserved range 100-102);
- registers cleanup: close DuckDB/pool, `DROP DATABASE ... WITH (FORCE)`,
  delete the S3 prefix. Tests may use `t.Parallel()`.

**How this maps to #173's "isolated schema ID, tenant, bucket prefix"
constraint:** forma's storage and query model has no first-class tenant —
the DDL carries no tenant column and no API takes a tenant key; deploy-level
tenancy is a database + S3 prefix pair. The per-test database therefore *is*
the tenant boundary, and the fixture schema IDs (20-22), while numerically
identical across tests, live in private databases and private S3 prefixes —
no state, lock, or parquet file is reachable from another test. A test that
needs schema IDs beyond the fixtures can register its own via
`RegisterSchemas` + `WithSchemaDir`.

Options: `WithSeed`, `WithSchemaDir`, `WithFlushThresholds` (#179),
`WithDuckMemoryMB`, `WithBreaker` (#185), `WithDuckMaxConnections`,
`WithRoutingStrategy`, `WithoutManifest`.

## Fixture schemas (`schemas/`)

| Schema | ID | Purpose |
|---|---|---|
| `e2e_simple` | 20 | minimal: bound text + EAV numeric |
| `e2e_wide` | 21 | one attribute per scalar `forma.ValueType`, mixed main-column/EAV storage (#174) |
| `e2e_second` | 22 | second schema for multi-schema isolation (#186) |

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
design). `forma.ValueTypeList` does **not** round-trip: the write path rejects
`list` outright at `transform.populateTypedValue`, so the fixture defines a
`tags` list attribute that `list_roundtrip_e2e_test.go` uses to pin the
rejection contract live, with the end-to-end acceptance path written as a
skipped subtest — blocked by #204.

Note: the DuckDB federated path requires at least one parquet file per
schema (`read_parquet` errors on an empty glob) — matching production,
where cdc-init bootstraps base files before federated reads. Use
`PreferHot: true` for queries before the first flush/init.

## Events, CDC, queries

- `ApplyEvents` drives `Create`/`Update`/`Delete` through the real
  EntityManager and reads the wall-clock `changed_at`/`deleted_at` back
  from `change_log` into each event (timestamps cannot be seeded).
  Production deletes **hard-delete** `entity_main`/`eav_data` and leave a
  `change_log` tombstone.
- `GenerateScript` is seed-deterministic; repeated calls continue one
  stream so ordinal-encoded values stay unique per Env.
- `RunFlush`/`RunFlushDry` wrap `cdc.Runner.RunOnce`; reports capture
  unflushed counts, new S3 objects, and parsed manifests. Note the
  mainline dry-run still writes parquet objects — it skips `flushed_at`
  marking and manifest updates (#180).
- `RunFlushWith(FlushOverrides{S3, Config, DryRun})` runs one flush pass
  with per-run overrides and reports observable state *even when the run
  fails* (report is non-nil unless the pre-run capture fails), so
  failure-boundary tests can assert partial side effects alongside the
  error. Pair it with `FaultInjectingS3` — a decorator over the real S3
  client that fails calls matching an `S3Fault{Op, KeyContains,
  SkipMatches}` — to break exactly one step of the flush pipeline and pin
  the failure matrix (#179).
- `RunInit` wraps `cdc.RunInit` (base file export + manifest base entries).
  Base keys are deterministic (`<prefix>/<id>/<minRowID>_<maxRowID>.parquet`),
  and init replaces the manifest's base tier with each run's output, so
  reruns neither duplicate entries nor leave stale ranges — obsolete S3
  objects remain (glob+LWW keeps queries exact; reconciliation is #203)
  (#176). Init never touches
  `change_log`: backfilled rows stay hot until the operator clears the log
  (onboarding contract, see `TestInitBaseOnlyParity`) or the next flush
  re-exports them into delta (`TestInitFlushOverlapWithoutLogCleanup`).
- `Query`/`AssertQueryMatches` translate one spec for both engine and
  oracle. Filters use the public `"op:value"` condition forms (bool values
  are `"1"`/`"0"`; dates accept ISO strings or epoch millis).
- `ExecSQL` is the escape hatch for failure-state injection (#179/#181),
  e.g. truncating `change_log` to model post-init onboarding.
- `InjectRestore` (#175) re-materializes a hard-deleted row under its
  original row_id at the storage layer — production has no restore API
  (Update on a deleted row returns `ErrNotFound`, pinned live) — with the
  restore timestamp forced past the tombstone's `changed_at` so the revival
  wins LWW. Scoped to text/numeric attributes (e2e_simple shapes).
- `RunCompaction` wraps the real compactor (`compaction.Compactor.RunOnce`)
  over the Env's manifest. Today's compactor is manifest-level only: the
  dirty-ratio rewrite is unimplemented (reports `RewritePending`, manifest
  untouched) and promotion needs ≥1 MB of delta — the lifecycle suite pins
  that contract as a tripwire for #188.
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
  Set `Query.AllowPartialDegradedMode` to forward the public degraded-mode
  flag so a tier outage falls back to a postgres-only result (complete in
  today's PG-retains-all model) instead of erroring.

## Diagnostic artifacts

On failure (or `KEEP_E2E_ENV=1`) the Env writes
`.artifacts/e2e/<runID>/<testName>/` (override root with
`E2E_ARTIFACTS_DIR`); the path is printed in the failure message.

| File | Content |
|---|---|
| `run.json` | seed, run ID, test, DB DSN, bucket/prefix, endpoints, git SHA |
| `events.json` | full event log incl. read-back timestamps |
| `change_log.json` | `SELECT * FROM change_log ORDER BY schema_id,row_id,flushed_at` |
| `s3_listing.json` | key/size/etag/last-modified under the prefix |
| `manifest_<id>.json` | raw manifest bytes per schema |
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

# Production E2E Harness

Reusable end-to-end test harness that exercises the **real production
stack** (#173, epic #172): real containers (Postgres 16 + MinIO via
testcontainers, in-process DuckDB), real writes through
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
  CDC advisory locks are keyed `(schemaID, schemaID)` *per database OID*
  and `RunOnce` scans the whole `change_log`, so a private database gives
  natural isolation and free multi-schema support (#189);
- gets its own S3 prefix `e2e/<runID>/env<N>` and in-memory DuckDB client
  (1 GB default memory limit, `WithDuckMemoryMB` to override);
- registers the fixture schemas (IDs 20-22; registration rejects the
  sqlgen benchmark-reserved range 100-102);
- registers cleanup: close DuckDB/pool, `DROP DATABASE ... WITH (FORCE)`,
  delete the S3 prefix. Tests may use `t.Parallel()`.

Options: `WithSeed`, `WithSchemaDir`, `WithFlushThresholds` (#179),
`WithDuckMemoryMB`, `WithBreaker` (#185), `WithRoutingStrategy`,
`WithoutManifest`.

## Fixture schemas (`schemas/`)

| Schema | ID | Purpose |
|---|---|---|
| `e2e_simple` | 20 | minimal: bound text + EAV numeric |
| `e2e_wide` | 21 | one attribute per scalar `forma.ValueType`, mixed main-column/EAV storage (#174) |
| `e2e_second` | 22 | second schema for multi-schema isolation (#189) |

All fixtures explicitly define `name` and `version` text attributes:
`sqlgen.BuildSchemaProjection` injects synthetic ones into every schema and
real CDC exports cannot satisfy the injected columns (see the follow-ups in
PR for #173). `forma.ValueTypeList` is not covered yet.

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
- `RunInit` wraps `cdc.RunInit` (base file export + manifest base entries).
- `Query`/`AssertQueryMatches` translate one spec for both engine and
  oracle. Filters use the public `"op:value"` condition forms (bool values
  are `"1"`/`"0"`; dates accept ISO strings or epoch millis).
- `ExecSQL` is the escape hatch for failure-state injection (#179/#181),
  e.g. truncating `change_log` to model post-init onboarding.

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
| `KEEP_E2E_ENV=1` | keep containers/DB/S3 alive and print connection info. **Also set `TESTCONTAINERS_RYUK_DISABLED=true`** or the testcontainers reaper still removes containers on exit |
| `PRODUCTION_E2E_EXTERNAL_PG_DSN` / `..._S3_ENDPOINT` / `..._S3_BUCKET` / `..._S3_REGION` / `..._S3_ACCESS_KEY` / `..._S3_SECRET_KEY` | use external infrastructure instead of containers (the PG user must be able to CREATE DATABASE) |

## Consumer map (epic #172)

| Issue | Building block |
|---|---|
| #174 full-type coverage | `e2e_wide` + `FullTypeProfile` |
| #175 lifecycle sequences | `Event` model + `ApplyEvents` |
| #179 flush thresholds | `WithFlushThresholds` + `FlushReport` |
| #180 dry-run semantics | `RunFlushDry` |
| #181 failure injection | `ExecSQL` |
| #185 circuit breaker | `WithBreaker` |
| #189 multi-schema | `e2e_simple`/`e2e_second` + per-test DB |

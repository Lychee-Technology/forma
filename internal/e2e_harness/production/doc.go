// Package production is the reusable E2E test harness that exercises the
// REAL production stack end to end (#173, epic #172):
//
//   - real containers: Postgres 16 + RustFS (S3-compatible) via
//     testcontainers (shared per test binary through SharedCluster), DuckDB
//     in-process per test Env
//   - real writes: events are applied through internal.NewEntityManager,
//     which maintains entity_main, eav_data, and change_log exactly like
//     production (no harness-simulated inserts)
//   - real CDC: RunFlush wraps cdc.Runner.RunOnce and RunInit wraps
//     cdc.RunInit (delta/base parquet export + S3 manifest)
//   - real reads: Query assembles internal/federated.DBFederatedQueryEngine
//     over the per-test Postgres database and S3 prefix
//   - independent oracle: ExpectedStateFromEvents folds the event log into
//     expected query results without consulting the engine
//
// Isolation model: containers are shared per test binary; every test gets
// its own Postgres DATABASE (advisory-lock and change_log-scan isolation),
// its own S3 prefix, and its own in-memory DuckDB client. Tests may use
// t.Parallel().
//
// Diagnostics: on failure (or KEEP_E2E_ENV=1) DumpArtifacts writes a
// run-specific directory with the event log, change_log snapshot, S3
// listing, raw manifests, parquet schema+samples, generated SQL/params/
// execution plans, and expected-vs-actual diffs.
//
// Consumer map (planned follow-up issues):
//
//	#174 full-type coverage        -> schemas/e2e_wide fixture, GenerateScript
//	#175 lifecycle sequences       -> Event model + ApplyEvents
//	#179 flush failure matrix      -> FaultInjectingS3, RunFlushWith, ExecSQL
//	#180 dry-run semantics         -> RunFlushDry, RunInitWith(InitOverrides{DryRun})
//	#181 failure-state injection   -> Env.ExecSQL escape hatch
//	#185 circuit breaker           -> WithBreaker
//	#189 multi-schema isolation    -> schemas/e2e_simple + e2e_second, per-test DB
//
// Environment variables: KEEP_E2E_ENV, E2E_SEED, E2E_ARTIFACTS_DIR,
// E2E_VERBOSE, and PRODUCTION_E2E_EXTERNAL_* (see README.md).
package production

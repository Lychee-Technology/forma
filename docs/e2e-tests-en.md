# Forma E2E Test Matrix

Last updated: 2026-03-09  
Repository: `forma`

## Overview

The repository currently has two E2E tracks:

- `internal/e2e_harness`: Go-based containerized system E2E, treated as the primary test system
- `tests/e2e`: Bun/k6 script-based black-box validation, treated as the supplementary flow and load-test system

Code is the source of truth. README files are navigation aids. The current inventory is:

- Go harness: `68` federated cases plus `3` suite/baseline entries
- Bun/k6: `6` workflow scripts plus `3` k6 load scenarios

Default execution rules:

- Go uses `go test` as the default entrypoint; functional and consistency groups are part of the normal path
- Go performance cases exist in the federated suite, but should be run explicitly with a longer timeout
- Bun default `bun run test` only runs `register-schemas -> gen-data -> cdc-flush -> federated-check`
- `cdc-init`, `compactor`, and k6 scenarios exist but are not part of the default Bun path

## Single-View Matrix

| Case / Script | System | Layer | Goal | Covered Path | Run Command | Default | Prerequisites | Notes / Limits |
|---|---|---|---|---|---|---|---|---|
| `TestE2EHarnessMinimal` | Go E2E Harness | Infrastructure / Smoke | Verify Postgres, S3, and DuckDB can start and communicate | Container startup -> Parquet write -> S3 upload -> DuckDB read | `go test -v ./internal/e2e_harness/... -run TestE2EHarnessMinimal -timeout=5m` | Yes | Go, Docker | Infrastructure smoke only; no three-tier semantics |
| `TestSuite_Smoke` | Go E2E Harness | Infrastructure / Smoke | Verify harness initialization, baseline data operations, and S3 operations | Harness init -> Seed -> Query -> S3 ops | `go test -v ./internal/e2e_harness/federated/... -run TestSuite_Smoke -tags=e2e -timeout=10m` | Yes | Go, Docker | Focuses on environment health, not performance |
| `TestSuite_Integration` | Go E2E Harness | Data Pipeline Consistency | Verify the full lifecycle with no obvious break or duplicates | Seed All Tiers -> Federated Query -> Postgres Query -> CDC Flush -> Compaction -> Verify | `go test -v ./internal/e2e_harness/federated/... -run TestSuite_Integration -tags=e2e -timeout=10m` | Yes | Go, Docker | Good regression entrypoint |
| `TC-01 Three-Tier Data Architecture` | Go E2E Harness | Functional Correctness | Verify Base, Delta, Hot, and merged querying behavior | Base/Delta/Hot -> Federated Query | `go test -v ./internal/e2e_harness/federated/... -run TestDataTier -tags=e2e` | Yes | Go, Docker | 7 tests including empty-tier and pagination cases |
| `TC-02 Merge-on-Read Logic` | Go E2E Harness | Functional Correctness | Verify `UNION ALL`, version precedence, and dirty-ID exclusion | Base + Delta + Hot -> Merge-on-Read | `go test -v ./internal/e2e_harness/federated/... -run TestMergeOnRead -tags=e2e` | Yes | Go, Docker | 7 tests including last-write-wins |
| `TC-03 Global Deduplication` | Go E2E Harness | Functional Correctness | Verify single-tier and cross-tier dedup semantics | Same Tier / Cross Tier -> QUALIFY / ROW_NUMBER | `go test -v ./internal/e2e_harness/federated/... -run TestDeduplication -tags=e2e` | Yes | Go, Docker | 6 tests including 10K-scale bulk coverage |
| `TC-04 Soft Delete Filtering` | Go E2E Harness | Functional Correctness | Verify soft-delete filtering, restore, and row_id reuse | Deleted / Restored Records -> Federated Query | `go test -v ./internal/e2e_harness/federated/... -run TestSoftDelete -tags=e2e` | Yes | Go, Docker | 7 tests including `NULL` / `0` semantics |
| `TC-05 CDC Smart Flushing` | Go E2E Harness | Data Pipeline Consistency | Verify flush triggers, advisory locking, batching, and delta output | Hot Buffer -> CDC Flush -> Delta Parquet | `go test -v ./internal/e2e_harness/federated/... -run TestCDCFlush -tags=e2e` | Yes | Go, Docker | 7 tests focused on flush behavior, not production scheduling |
| `TC-06 Compaction Strategy` | Go E2E Harness | Data Pipeline Consistency | Verify correctness after delta-to-base compaction and no-op paths | Delta Parquet -> Compaction -> Base Parquet | `go test -v ./internal/e2e_harness/federated/... -run TestCompaction -tags=e2e` | Yes | Go, Docker | 9 tests covering low dirty ratio, high dirty ratio, and empty delta |
| `TC-07 Data Consistency` | Go E2E Harness | Data Pipeline Consistency | Verify Postgres and Federated results stay aligned | Postgres Query <-> Federated Query | `go test -v ./internal/e2e_harness/federated/... -run TestConsistency -tags=e2e` | Yes | Go, Docker | 9 tests covering count, checksum, and attribute values |
| `TC-08 Performance Benchmarks` | Go E2E Harness | Performance and Failure Injection | Verify latency thresholds for query, flush, and compaction paths | Federated Query / CDC / Compaction | `go test -v ./internal/e2e_harness/federated/... -run TestPerformance -tags=e2e -timeout=60m` | No | Go, Docker | 8 tests; recommended to run separately |
| `TC-09 Failure Modes` | Go E2E Harness | Performance and Failure Injection | Verify degradation and recovery under S3, Postgres, timeout, and corrupted-parquet failures | Failure Injection -> Federated Query / Recovery | `go test -v ./internal/e2e_harness/federated/... -run TestFailureMode -tags=e2e` | Yes | Go, Docker | 8 tests focused on graceful degradation |
| `register-schemas` | Bun E2E Scripts | Infrastructure / Smoke | Verify `lead`, `visit`, and `log` schemas are queryable or registerable | Schema Files -> API | `cd tests/e2e && bun run register-schemas` | Yes | Bun, running server | First step in the default Bun flow; writes `schema-registration.json` |
| `gen-data` | Bun E2E Scripts | Functional Correctness | Create batch test data via API and preserve cross references | API Write Path -> Postgres Hot Tier | `cd tests/e2e && bun run gen-data -- --schema all --count 10000` | Yes | Bun, running server | Second step in the default Bun flow; writes `data-gen.json` |
| `cdc-init` | Bun E2E Scripts | Data Pipeline Consistency | Initialize base parquet files for existing data | Postgres Main / EAV -> Base Parquet | `cd tests/e2e && bun run cdc-init` | No | Bun, Go tools binary, S3, Postgres | Backfill-only step; not part of `bun run test` |
| `cdc-flush` | Bun E2E Scripts | Data Pipeline Consistency | Run the `tools` binary to flush change-log data into delta parquet | Change Log -> Delta Parquet -> Manifest | `cd tests/e2e && bun run cdc-flush` | Yes | Bun, Go tools binary, S3, Postgres | Third step in the default Bun flow; supports `--dry-run` |
| `compactor` | Bun E2E Scripts | Data Pipeline Consistency | Run the `tools` binary to merge delta into base | Delta Parquet -> Base Parquet | `cd tests/e2e && bun run compactor -- --all` | No | Bun, Go tools binary, S3 | Extended step; not part of `bun run test` |
| `federated-check` | Bun E2E Scripts | Data Pipeline Consistency | Compare Forma API results with direct Postgres results | Forma API <-> Postgres Direct Query | `cd tests/e2e && bun run federated-check -- --schema all --sample-size 100` | Yes | Bun, running server, Postgres | Fourth step in the default Bun flow; supports `--full-scan` |
| `k6 smoke/full/perf` | Bun k6 | Performance and Failure Injection | Validate SLA for pagination, sorting, and `advanced_query` | HTTP Query Load -> API / Federated Query | `cd tests/e2e && bun run build-k6 && bun run k6-full` | No | Bun, running server, k6 or Docker | `smoke=5 VUs/30s`, `full=30 VUs/2m`, `perf=100 VUs/5m` |

## Execution Paths

### Go Primary Path

```bash
# Infrastructure verification
go test -v ./internal/e2e_harness/... -timeout=5m

# Federated functionality and consistency
go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m

# Performance only
go test -v ./internal/e2e_harness/federated/... -run TestPerformance -tags=e2e -timeout=60m
```

### Bun Primary Path

```bash
cd tests/e2e
bun run test
```

`bun run test` expands to:

```bash
bun run register-schemas
bun run gen-data
bun run cdc-flush
bun run federated-check
```

### Bun Extended Steps

```bash
cd tests/e2e
bun run cdc-init
bun run compactor -- --all
bun run build-k6
bun run k6-smoke
bun run k6-full
bun run k6-perf
```

## Coverage Gaps and Notes

- Go harness is the primary E2E system for three-tier semantics, CDC, compaction, failure handling, and performance thresholds.
- Bun is better treated as black-box workflow validation for API data generation, tooling execution, reconciliation, and load testing.
- `tests/e2e/package.json` default `test` does not include `cdc-init`, `compactor`, or k6; those must be run explicitly.
- `tests/e2e/README.md` previously omitted `cdc-init.ts` and `compactor.ts`; the README and this matrix are now aligned.
- `internal/e2e_harness/README.md` remains accurate for the Go harness category index and can continue to be used as the per-suite reference.

## Sources of Truth

- `internal/e2e_harness/README.md`
- `internal/e2e_harness/e2e_test.go`
- `internal/e2e_harness/federated/suite_test.go`
- `internal/e2e_harness/federated/*_test.go`
- `tests/e2e/README.md`
- `tests/e2e/package.json`
- `tests/e2e/scripts/*.ts`
- `tests/e2e/k6/scenarios.ts`

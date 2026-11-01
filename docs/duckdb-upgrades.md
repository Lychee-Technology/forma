# DuckDB upgrades

Forma pins one DuckDB release through `github.com/duckdb/duckdb-go/v2` (see `go.mod` and the README). A new engine release can accept all of Forma's SQL and still change a cast, the handling of NULL or empty lists, the Parquet it writes, a query plan or a result. `SELECT 1`, rendering snapshots and the `-short` e2e lane do not catch that kind of change (#753). This page lists what to run for an upgrade, which tests cover each risk, and how to check that Parquet files stay readable across the upgrade and a rollback.

## The gate

| Command | What it covers | Where it runs |
|---|---|---|
| `make test`, `make fmt-check`, `make lint` | unit and integration tests, including those that open a real in-memory DuckDB | CI on every PR; the test job runs on `ubuntu-24.04-arm` under `-race` |
| `make test-e2e-production` | the production harness: real PostgreSQL, S3 (RustFS), CDC, compaction and the federated engine, each result checked against an independent oracle; it has no `-short` skips | CI on every PR; nightly under `-race` (`nightly-e2e.yml`, `production-race`) |
| `go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=75m` | the full federated suite: tier merge, dedup, soft delete, CDC and compaction through DuckDB; its PostgreSQL comparisons are narrower than they look (see [Known gaps](#known-gaps)) | nightly only (`nightly-e2e.yml`, `federated-full`) |
| `make duckdb-storage-compat` | Parquet written by the current engine reads back in the previous release, and the reverse | CI on every PR (test job, so on arm64) |

CI runs the federated suite with `-short`, which skips the deduplication, merge-on-read, data-tier, soft-delete, `created_at` lifecycle, filter LWW regression and performance tests. An engine upgrade should not merge on CI alone: run the full federated suite and the production suite under `-race` against the new driver first, and record the commands, commit, versions and results in the upgrade PR. Report a test that could not run as not run, never as passed.

## Where each risk is covered

Paths are relative to `internal/`. The production harness tests live in `e2e_harness/production/`. The table says where a risk has tests, not that the risk is fully covered: [Known gaps](#known-gaps) lists what is still missing.

| Risk | Tests |
|---|---|
| Hot, warm and cold tiers, the dirty set, versions, tombstones, dedup | production `three_tier_merge_e2e_test.go`, `three_tier_soft_delete_e2e_test.go`, `three_tier_isolation_e2e_test.go`, `filter_lww*_e2e_test.go`, `tiebreak_lww_e2e_test.go`; federated `deduplication_test.go`, `merge_on_read_test.go`, `soft_delete_test.go`, `data_tier_test.go` (full suite only) |
| Pagination, keyset cursors, totals | production `deep_pagination_e2e_test.go`, `keyset_lww_e2e_test.go`, `keyset_routing_e2e_test.go`, `pagination_mutation_e2e_test.go`, `bigint_keyset_boundary_e2e_test.go` |
| LIST attributes: empty, NULL, element order, reconstruction | production `list_roundtrip_e2e_test.go` |
| Parquet column types and value images | production `full_type_parquet_e2e_test.go` (pins the physical schema), `full_type_roundtrip_e2e_test.go`, `numeric_width_parity_e2e_test.go`, `width_audit*_e2e_test.go`, `bool_bound_parity_e2e_test.go`, `date_parity_e2e_test.go`, `date_image_tiers_e2e_test.go`, `text_uuid_operator_e2e_test.go` |
| Arrays of objects (#704) | production `object_array_e2e_test.go`, `cdc_object_array_skip_e2e_test.go` |
| Mixed Parquet schemas, manifest-driven reads | production `schema_evolution_e2e_test.go`, `compaction_evolution_e2e_test.go`, `parquet_manifest_consistency_e2e_test.go`, `factory_manifest_e2e_test.go` |
| PostgreSQL through DuckDB (`postgres_query`, the attached catalog) | `cdc/duckdb_export_pg_integration_test.go`, `federated/hot_leg_attach_integration_test.go`, production `hot_leg_scan_e2e_test.go` |
| Compaction | production `compaction*_e2e_test.go` |
| Credentials kept out of errors | `federated/credential_scrub_test.go`, `cdc/duckdb_export_pg_integration_test.go` |
| Extension loading, reopen after invalidation | `duckdbinit/required_recovery_test.go`, `federated/duckdb_conn_init_test.go`, `federated/duckdb_recovery_seam_test.go` |
| The parallel window race (#702) | `federated/duckdb_window_race_test.go` |
| Circuit breaker and degraded modes | production `circuit_breaker_e2e_test.go`, `degraded_mode_e2e_test.go`, `postgres_halt_e2e_test.go`, `s3_recovery_e2e_test.go` |
| SQL syntax the new release dropped | `duckdb_lambda_syntax_guard_test.go` (repository root) for lambdas; behaviour changes are found by the suites above, as #752 found the `postgres_scan` removal and the `postgres_query` column order |
| Parquet readable across releases | `make duckdb-storage-compat` (below) |

## Known gaps

These are open under #753 and must not be counted as passing coverage.

**The federated suite's PostgreSQL comparison covers the hot leg only.** `CompareResults` (`e2e_harness/federated/validation.go`) is applied only in `consistency_test.go`, and there only to data that is entirely in the hot tier: DuckDB reads those rows from PostgreSQL through the attached catalog, and no delta or base file is involved. The PostgreSQL side cannot serve as the oracle for S3 tiers: `buildPostgresOnlySelectQuery` reads only unflushed, live `change_log` rows, and it projects `0` as `version`. The one comparison that does involve S3 data, `TestConsistency_MissingRecordDetection`, expects the two sides to differ. Before #753, `CompareResults` also ignored the totals, collapsed duplicate row IDs, ignored numeric values and system columns, and compared a text attribute only when both sides carried it. It now fails on each of those, and `validation_test.go` pins every case without a database. No PostgreSQL oracle exists yet for federated reads over delta and base files. The production harness (`make test-e2e-production`) checks DuckDB results over S3 tiers against an independent oracle.

**Not yet run or audited on DuckDB 2.0** (status on #753):

- CDC flush latency: `TestPerformance_CDCFlush` runs out of its budget while seeding PostgreSQL, before it flushes (#781), on 2.0 and on 1.5.6 alike.
- Linux arm64 beyond `make test` and `make duckdb-storage-compat`: the e2e and production suites and the Lambda layer build have not run there. Darwin and Windows native libraries are unverified.
- The production suite under `-race` on 2.0: its first run is the nightly `production-race` job.
- Required coverage audited against the tests #752 added, including SQL behaviour changes reachable from Forma's generated SQL beyond the lambda syntax guard.

## Storage compatibility across releases

Parquet files in S3 outlive the binary that wrote them. After an upgrade the new engine reads files the old one wrote, and after a rollback the old engine reads files the new one wrote, including base files compaction rewrote in between. `make duckdb-storage-compat` (`scripts/duckdb_storage_compat.sh`) checks both directions with `cmd/duckdbcompat`, a probe built twice: against `go.mod`, and against `scripts/duckdbcompat/previous.mod` through `-modfile`. Each build writes a 150,000-row fixture carrying every column type Forma's exporters write, with the shapes they produce (NULL attributes, empty and NULL lists, integer-width extremes, non-ASCII text, tombstones), using the production COPY options from `internal/parquetformat`. Each build then reads the other's file and requires the same column names and types and exactly the fixture's rows. Last, the current build merges both files into one, as compaction does, and both builds read the result.

The probe checks the file format: types, encodings, row groups and values. It does not run Forma's LWW SQL or read through S3; the suites above cover those within one engine. It checks one pair of releases, the current one and the one `previous.mod` names, which should be the release a rollback would return to.

DuckDB 2.0.0 against 1.5.6, on linux/amd64 (#753): both directions and the merged file pass, and both releases write every column with the same codec (ZSTD) and the same page encodings (`PLAIN`, `RLE_DICTIONARY`, `DELTA_BINARY_PACKED`, `DELTA_LENGTH_BYTE_ARRAY`, `BYTE_STREAM_SPLIT`). Parquet written by 2.0 therefore needs no migration for a rollback to 1.5.6. A DuckDB database file is different: one written by 2.0 cannot be opened by 1.5.6 (design §7.5).

### Moving the pair after an upgrade

When `go.mod` moves to a new driver, point `previous.mod` at the driver `go.mod` named before the move:

1. Replace the `require` lines in `scripts/duckdbcompat/previous.mod` with the old driver and its native library modules from the pre-upgrade `go.mod`, and copy their lines from the pre-upgrade `go.sum` into `scripts/duckdbcompat/previous.sum`.
2. Run `go build -mod=mod -modfile=scripts/duckdbcompat/previous.mod ./cmd/duckdbcompat`, which adds what is still missing, and review what it added.
3. Run `make duckdb-storage-compat` and record the result in the upgrade PR.

`TestStorageCompatPairsTwoDriverReleases` fails while both files name the same driver, so the check never compares a release with itself.

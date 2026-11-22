# Parquet Publication Protocol V2 — EPIC #887 Close-Out Evidence Ledger

This ledger maps every invariant and closure gate of EPIC #887 (`[EPIC] Manifest-only publication protocol: PG-authoritative generations and fail-closed reads`) to the merged pull requests, automated tests, CI lanes, and measured latency/cost deltas delivered across #888–#892.

It separates the **local/CI implementation gate** (PostgreSQL 16, RustFS/S3, DuckDB, `dsql-lint`, and the MiniStack strict SQL proxy — complete) from the **external live Aurora DSQL validation gate** (tracked separately in #893 under #817 — not yet run against a live AWS cluster).

## 1. Delivery Ledger

| Child issue | PR | Main SHA | Scope |
| --- | --- | --- | --- |
| #888 | #894 | `2d66d64` | Protocol V2 specification (`docs/publication-protocol-v2.md`) and `internal/publication` SQL contract (`Read`, `Publish`, `Rebase`, `Resolve`, `HoldLineage`, `Reset`, `Activate`, `RaiseHorizon`, `Retire`, `Forget`, `PlanCollection`). |
| #895 | #897 | `0b92ce3` | `init-db` DDL creation of `<entity_main>_publication`, `<entity_main>_publication_generation`, and `<entity_main>_publication_epoch` tables (with `dsql-lint` and MiniStack coverage for DSQL DDL statements). |
| #898 | #902 | `9952714` | `internal/manifestv2`: immutable manifest documents, deterministic JSON/SHA-256 digest, write-once `If-None-Match: *` S3/local store, and process-wide verifying cache. |
| #901 | #903 | `3bb809f` | Production E2E harness isolation and `TestE2ETestsReachPublicationOnlyThroughHelpers` AST guard banning direct `change_log` clears and raw V1 manifest stand-ins. |
| #899 | #904 | `f1e34a9` | `internal/cdcv2`: V2 flush and init publication state machine, atomic PG publish + marks, DSQL ≤1,000-entry generation splitting, `Resolve`/`Fence` lost-commit handling, and `HoldLineage` handoff. |
| #900 | #905 | `6902a91` | `internal/federatedv2` & `internal/federated` (`pgx_publication.go`): snapshot-bound `publication.Read`, epoch pinning, `R_max` pass deadline (`60s`), `PublicationConsistencyError`, and `ErrPublishedObjectUnverifiable`. |
| #896 | #906 | `1283ec9` | Cutover of `init-db`, `drop-schema`, `cdc-flush`, `cdc-init`, `baseline-schema`, and factory/federated reader to Protocol V2; deletion of V1 resolution, `QuerySource.Fallback`, level-3 glob, `--glob-only`, and `manifest-template` flags; carries #759 and closes #760. |
| #890 | #907 | `0e9e4f4` | V2 `compactor` publishing `KindCompact` generations through `cdcv2.Writer.Compact`, automatic rebase over concurrent flushes, pre-merge SHA-256 verification, and removal of inline deletion and V1 `FileProvider`. |
| #891 | #909 | `365624a` | `internal/publication` hardening: `RetireRecovery`, `ErrHistoryInconsistent`, storage binding (`<entity_main>_publication_storage`), and lifecycle epoch recovery fences. |
| #891 | #910 | `917e36f` | V2 `manifest-reconcile` (`internal/reconcile`, `cmd/tools/manifest_reconcile.go`): lock-free diagnosis, `--gc` horizon/retention/orphan collection, `--bind-storage`, `--verify-checksums`, `ErrRedirectRefused`, and removal of `--repair`. |
| #892 | #912 | (this PR) | Production-shape E2E lifecycle/fault/crash-boundary/concurrency matrix, DSQL `40001` & 3-way concurrency tests, static legacy-V1 code/doc guard, opt-in P95/cost delta harness, operator doc audit, and this ledger. |

## 2. EPIC #887 Invariant-to-Evidence Matrix

| Invariant / Closure Gate | Enforcement & Tests | CI Lane(s) |
| --- | --- | --- |
| **1. PG-authoritative pointer & monotonic epoch/generation** | `internal/publication`: `TestPublish*`, `TestReset*`, `TestActivate*`, `TestALostRecordIsRecoveredAboveEveryEpochItsIdCarried`, `TestOnlyTheProtocolOwnersMoveAPublicationRecord`. `cmd/tools`: `init_db_records_lifecycle_integration_test.go`, `drop_schema_test.go`. `internal/e2e_harness/production`: `TestPublicationMatrix_FullLifecycleAndPagination`, `TestServerStartupUnpinsASchemaRedroppedAfterItsRegistryLoaded`. | `test`, `dsql-ministack`, `e2e-production` |
| **2. Write-once immutable manifests & parquet objects (`If-None-Match: *`)** | `internal/manifestv2`: `manifest_test.go`, `store_test.go`, `cache_test.go`, `s3_store_test.go`. `internal/e2e_harness/production`: `TestPublicationPerfDelta` confirms `s3_list=0`, `s3_copy=0`, `s3_del=0` at the S3 HTTP endpoint for `cdc-init`, `cdc-flush`, and `compactor`. | `test`, `e2e-production` |
| **3. One atomic PG commit for publish + flush marks; DSQL ≤1,000 marks/gen** | `internal/publication`: `publish_integration_test.go`. `internal/cdcv2`: `failpoint_integration_test.go`, `occ_integration_test.go` (`TestPublishResendsOnSerializationConflictAcrossAllWriters`), `race_integration_test.go`. `internal/e2e_harness/production`: `TestFlushFaultMarkFlushed`, `TestPublicationCrashBoundariesAllWriters`. | `test`, `dsql-ministack`, `e2e-production` |
| **4. Reader reads record and dirty set in one `REPEATABLE READ` snapshot; `UNPUBLISHED` skips DuckDB/S3; `R_max` bounds pass** | `internal/federatedv2`: `source_integration_test.go`. `internal/federated`: `pgx_publication_deadline_test.go`. `internal/e2e_harness/production`: `TestNeverFlushedSchemaAnswersOnDuckDB`, `TestPublishedPassRacingAPublication`, `TestPublicationMatrix_FullLifecycleAndPagination`. | `test`, `e2e-production` |
| **5. Unprovable or inconsistent state fails closed (non-degradable, breaker-neutral)** | `internal/federated`: `TestPublishedPassFailsClosedOnAPublicationFault`. `internal/federatedv2`: `source_faults_integration_test.go`, `TestALossBehindAccessDeniedFailsClosedWithoutClaimingLoss`. `internal/e2e_harness/production`: `TestLostManifestOverFlushedRowsStillFails`, `TestPublicationMatrix_FaultsFailClosedAndPreserveBreaker` (missing/corrupt/stale manifest, forbidden empty flush, missing/`AWAITING_INIT`/`DROPPED` record, manifest deleted mid-pass, 403 `AccessDenied` on manifest fetch and `HeadObject` probe — all tested with `AllowPartialDegradedMode` off & on and threshold-1 breaker). | `test`, `e2e-production` |
| **6. Crash injection at every staging, manifest, publish, commit, commit-answer and handoff boundary + 3-way concurrency** | `internal/cdcv2`: `failpoint_integration_test.go`, `init_integration_test.go`, `compact_integration_test.go`, `occ_integration_test.go` (`TestConcurrentFlushInitAndCompactionSerializeSafely`). `internal/cdc`: `wired_flush_integration_test.go`, `wired_init_integration_test.go`, `wired_settle_integration_test.go`. `internal/e2e_harness/production`: `TestPublicationCrashBoundariesAllWriters` (20 subtests across `cdc-flush`, `cdc-init`, `compactor`), `TestPublicationConcurrentWritersAndReaders`. | `test`, `dsql-ministack`, `e2e-production`, `nightly-e2e` |
| **7. Safe GC (`--gc`), storage binding (`--bind-storage`), no `--repair`, no redirect following** | `internal/publication`: `collect_test.go`, `retention_integration_test.go`, `storage_test.go`. `internal/reconcile`: `concurrent_integration_test.go`, `reader_safety_integration_test.go`, `s3_redirect_test.go`. `cmd/tools`: `manifest_reconcile_test.go`. `internal/e2e_harness/production`: `manifest_reconcile_e2e_test.go`, `TestPublicationMatrix_FullLifecycleAndPagination`. | `test`, `dsql-ministack`, `e2e-production` |
| **8. No legacy V1 fallback, level-3 glob, glob-only writer, or obsolete flags remain** | `internal/publication`: `TestNoLegacyV1PathsOrObsoleteFlagsRemain` (AST + doc-code-block guard with planted-violation self-tests). `internal/e2e_harness/production`: `TestE2ETestsReachPublicationOnlyThroughHelpers`. `cmd/tools`: `cdc_flags_test.go`, `compactor_test.go`, `manifest_reconcile_test.go`. | `test` |
| **9. DSQL SQL syntax & proxy compatibility (local emulator gate)** | `make lint-dsql` (AWS `dsql-lint` over SQL corpus including `internal/publication/dsql_corpus_test.go`); `make test-dsql-ministack` (MiniStack strict SQL proxy over `internal/`, `internal/publication`, `internal/cdcv2`, `internal/cdc`, `internal/federated`, `cmd/tools`). | `dsql-lint`, `dsql-ministack` |

## 3. Regression Verification of Prior Contracts

| Prior Contract | Verified Behavior Under Protocol V2 | Tests |
| --- | --- | --- |
| **#720** (never-flushed vs lost manifest) | `UNPUBLISHED` schemas answer from PostgreSQL with 0 DuckDB statements and 0 S3 calls; `PUBLISHED` schemas with a missing manifest fail closed with `ErrPublicationInconsistent` even when parquet objects exist in S3. | `TestNeverFlushedSchemaAnswersOnDuckDB`, `TestLostManifestOverFlushedRowsStillFails`, `TestPublicationMatrix_FaultsFailClosedAndPreserveBreaker/missing_manifest_with_intact_parquets` |
| **#849 / #858** (pgx hot leg & ordering/pagination parity) | Filtered, sorted, offset-paginated, and keyset-paginated queries merge pgx hot keys and DuckDB parquet rows with zero duplicate/dropped rows and no stale/deleted-row resurrection across flushes, inits, and compactions. | `pgx_hot_leg_e2e_test.go`, `pgx_hot_leg_dsql_integration_test.go`, `TestPublicationMatrix_FullLifecycleAndPagination`, `TestPublicationConcurrentWritersAndReaders` |
| **#635** (no S3-only eventual mode; PG outage fails) | A PostgreSQL outage fails federated queries (`ErrPostgresReadFailed`), including under `AllowPartialDegradedMode: true`; no eventual/S3-only fallback exists. | `degraded_mode_e2e_test.go` (`TestDegradedMode_DirtyFetchFailure`), `postgres_outage_e2e_test.go` |
| **#715** (deployment-level federation; no request-level `federated` block) | Federation is controlled solely by deployment config (`DuckDBConfig`); no request-level `s3_parquet_path_template` or `federated` overrides exist. | `factory_manifest_e2e_test.go`, `TestNoLegacyV1PathsOrObsoleteFlagsRemain` |

## 4. Measured P95 Latency and Cost Deltas

Captured with the reproducible harness `TestPublicationPerfDelta` (`internal/e2e_harness/production/publication_perf_delta_e2e_test.go`):

```bash
FORMA_PUBLICATION_PERF_DELTA=1 GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false GOTOOLCHAIN=go1.26.0+auto \
  go test -v -tags=e2e -timeout=5m ./internal/e2e_harness/production -run TestPublicationPerfDelta
```

**Instrumentation & datasets:** Dedicated local Docker `postgres:16` + pinned `rustfs/rustfs` cluster, DuckDB 4 threads (`WithDuckThreads(4)`), `e2e_wide` schema (filtered `count >= 1050`, sorted by `count`, page size `20`). Every S3 request — Go SDK manifest/reconcile clients and DuckDB `httpfs` parquet traffic alike — is counted at a local HTTP reverse proxy in front of the RustFS endpoint (`s3_total = s3_get + s3_head + s3_list + s3_put + s3_copy + s3_del + s3_other`, with `s3_other = 0` on all runs). Read counters reset after warmup (`3` warmups, `n=25` measured samples; `n=10` for cold cache with a fresh DuckDB/manifest cache per sample). Unpublished reads compare on the same `250`-row dataset; published 3-tier reads compare on the same `330`-row dataset (`300` compacted base rows + `20` flushed delta rows across 2 parquet objects + `10` hot rows).

### Read Workloads (Endpoint-Level S3 HTTP Counts)

| Workload | Dataset | n | P50 (ms) | P95 (ms) | P99 (ms) | DuckDB Execs | S3 Total | Manifest GET | Parquet GET | S3 HEAD | S3 LIST | Notes |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `pg_only_baseline_unpublished` | 250 rows (hot) | 25 | 8.54 | 9.29 | 9.55 | 0 | 0 | 0 | 0 | 0 | 0 | OLTP baseline on the unpublished dataset |
| `v2_unpublished_federated` | 250 rows (hot) | 25 | 14.03 | 15.23 | 15.24 | 0 | 0 | 0 | 0 | 0 | 0 | +5.94 ms P95 vs PG-only (`REPEATABLE READ` + `publication.Read` + dirty-set + hot-keys); 0 DuckDB, 0 S3 HTTP requests |
| `pg_only_baseline_published` | 330 rows (3-tier) | 25 | 3.92 | 4.13 | 4.22 | 0 | 0 | 0 | 0 | 0 | 0 | OLTP baseline on the same 330-row published dataset (warm PG buffers/planner) |
| `v2_published_warm_cache` | 330 rows (3-tier) | 25 | 26.47 | 27.94 | 28.04 | 25 | 150 | 0 | 0 | 150 | 0 | 0 manifest GETs and 0 `ListObjectsV2`; all 150 `HEAD`s (6/query across 2 parquet files) come from DuckDB `httpfs` revalidation while cached pages avoid parquet `GET`s |
| `v2_published_cold_cache` | 330 rows (3-tier) | 10 | 32.67 | 33.65 | 33.65 | 10 | 100 | 10 | 30 | 60 | 0 | +5.71 ms P95 vs warm cache; 1 manifest `GET`/query + DuckDB `httpfs` cold parquet reads (3 `GET` + 6 `HEAD`/query across 2 parquet files); 0 `ListObjectsV2` |

### Publishing & Maintenance Workloads (Endpoint-Level S3 HTTP Counts)

| Workload | Duration (ms) | S3 Total | S3 PUT | Manifest GET | Parquet GET | S3 HEAD | S3 LIST | S3 COPY | S3 DEL | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `v2_cdc_init_250rows` | 145.50 | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 1 base parquet + 1 manifest, write-once (`If-None-Match: *`); 0 `LIST`/`COPY`/`DEL` |
| `v2_cdc_flush_50rows` | 111.18 | 3 | 2 | 1 | 0 | 0 | 0 | 0 | 0 | 1 delta parquet + 1 manifest + 1 parent manifest `GET`; 0 `LIST`/`COPY`/`DEL` |
| `v2_compactor_300rows` | 102.12 | 11 | 2 | 1 | 2 | 6 | 0 | 0 | 0 | 1 merged base + 1 manifest + 1 manifest `GET` + 2 parquet `GET`s + 6 DuckDB `httpfs` `HEAD` probes on the 2 input files; 0 `LIST`/`COPY`/`DEL` |
| `v2_reconcile_diagnose` | 11.77 | 4 | 0 | 3 | 0 | 0 | 1 | 0 | 0 | Read-only, lock-free prefix `LIST` + 3 manifest `GET`s; 0 writes/deletes |
| `v2_reconcile_gc_collect` | 84.54 | 7 | 0 | 3 | 0 | 0 | 1 | 0 | 3 | Prefix `LIST` + 3 manifest `GET`s + 3 `DeleteObject` calls collecting expired generation manifests/objects after grace `G` |

**Caveats and scope of comparison:**
- No historical Protocol V1 binary is executed side-by-side here because V1 read/write paths were removed in #906 and #907; latency deltas compare V2 against equivalent PG-only OLTP baselines on identical datasets and between V2 states (unpublished vs published, warm vs cold manifest cache), while S3 request counts capture the complete V2 object-store HTTP traffic surface.
- Measurements reflect local single-host Docker latency (sub-millisecond S3 loopback) on 250–330 rows. On live AWS S3 (~10–40 ms RTT per request), DuckDB `httpfs` `HEAD`/`GET` calls on parquet objects remain part of published-read latency and request cost, while V2 eliminates per-query `ListObjectsV2`, `CopyObject`, and inline writer `DeleteObject` calls.

## 5. External Validation Gate: Live Aurora DSQL (#893 under #817)

The local/CI implementation gate is complete on PostgreSQL 16, `dsql-lint`, and MiniStack (`make test-dsql-ministack`). **Live Aurora DSQL validation is explicitly separated as an external gate tracked by #893 under #817** and has **not** run against a real AWS Aurora DSQL cluster.

Unverified live-cluster assumptions (detailed in `docs/publication-protocol-v2.md#verification` and `docs/dsql-ministack.md#what-a-pass-does-not-establish`):

1. `SELECT ... FOR UPDATE` on `<entity_main>_publication` participates in DSQL OCC conflict detection so `HoldLineage`/`HoldDropped` fences cannot commit concurrently with a publication transition.
2. Concurrent CAS writers fail at `COMMIT` with SQLSTATE `40001` without row-lock blocking (local PostgreSQL/MiniStack blocks then fails).
3. Exact DSQL modified-row accounting for `UPDATE change_log SET flushed_at = ...` (PK column update) under the 1,000-entry generation cap (3,000 modified-row transaction limit).
4. Cluster-wide monotonicity of `now()` across `ReadAt`, `gc_horizon`, and `retired_at` timestamps.
5. `RaiseHorizon` vs concurrent `Publish` serialization on DSQL (`40001` for whichever commits second).
6. Real IAM token auth, TLS enforcement, 1-hour connection lifetimes, and 5-minute transaction ceilings under load.

Related open DSQL operational items tracked outside #892:
- **#690**: `init-db` single-DDL-per-transaction execution on Aurora DSQL.
- **#694**: End-to-end Aurora DSQL integration verification.
- **#911**: DSQL-specific `manifest-reconcile` operational follow-ups.

## 6. Manual EPIC Close-Out Checklist

Maintainers should complete these manual steps only after PR #912 merges and CI is green on `main`:

- [ ] Verify all CI checks on `main` (`lint`, `dsql-lint`, `test` with `-race`, `dsql-ministack`, `e2e-federated`, `e2e-production`, `benchmark-smoke`) and one `nightly-e2e` run pass after #912 merges.
- [ ] Confirm disposition of superseded/sibling tracking issues (#889, #759, #760 — #759 and #760 already landed in #906/#896).
- [ ] Confirm #893 remains linked under #817 as the external live-DSQL validation gate.
- [ ] Manually close EPIC #887 once the above checks and remaining risks are accepted.

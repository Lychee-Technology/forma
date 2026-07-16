# Forma Integration Test Cases

Last updated: 2026-02-16  
Repository: `forma`

## 1. Purpose and Scope

This document lists integration test cases for the current Forma implementation, covering:

- API layer: CRUD, pagination/sorting, advanced query, cross-schema search
- Storage layer: PostgreSQL integration (`entity_main` / `eav_data` / `change_log`)
- Federated query layer: Postgres Hot + S3 Delta/Base + DuckDB Merge-on-Read
- Data pipeline: `cdc-init`, `cdc-flush`, `compactor`
- Deployment modes: local server and AWS Lambda entrypoint
- Performance and failure scenarios

Pure unit-test-level assertions are out of scope.

## 2. Test Environment Baseline

### 2.1 Minimum Dependencies

- Go 1.26+
- Docker / Docker Compose
- PostgreSQL (local default: `localhost:5432/forma`)
- S3-compatible storage (commonly RustFS locally)
- DuckDB (embedded via service/tooling)
- Bun (for `tests/e2e`)
- k6 (for load testing)

### 2.2 Common Gate Criteria

- All P0 automated test cases pass
- All P0 manual test cases pass
- Data consistency: no data loss, no duplicates, correct soft-delete behavior
- Performance thresholds met (at least smoke/full scenarios)
- After failure recovery, read/write remains available and no duplicate/dirty reads occur

## 3. Automated Integration Test Cases

## 3.1 Go Integration Tests (Internal Layer)

Execution entry:

- `go test ./internal -run TestIntegration_`
- `go test ./internal -run 'TestInsertPersistentRecordIntegration|TestChangeLogWritesOnUpdateAndDeleteIntegration|TestRunOptimizedQueryIntegration'`
- `go test ./internal/federated -run 'TestEvaluateRoutingPolicy_VariousStrategies|TestNewDuckDBClient_HealthCheck|TestAppendDirtyExclusion|TestFinalizeDuckDBExecutionPlan_CaptureDisabled|TestBuildDuckDBQuery_AdvancedTemplate|TestExecuteDuckDBFederatedQuery_NilQuery'`
- `go test ./internal/sqlgen -run 'TestBuildDuckDBQuery_PropagatesRenderError|TestBuildDuckDBQuery_KeysetArgsBindLast'`

Test cases:

- `A-GI-001` `TestIntegration_EntityLifecycle`  
  Acceptance: full Create/Get/Update/Query/Delete path succeeds; read-by-row_id fails after delete.
- `A-GI-002` `TestIntegration_AdvancedQuery`  
  Acceptance: advanced query condition is applied; returned records and total are correct.
- `A-GI-003` `TestIntegration_AdvancedQuery_MixedSorting`  
  Acceptance: mixed sorting across main-table and EAV attributes is correct.
- `A-GI-004` `TestInsertPersistentRecordIntegration`  
  Acceptance: successful writes to `entity_main`/`eav_data`; `change_log` row created with `flushed_at=0`.
- `A-GI-005` `TestChangeLogWritesOnUpdateAndDeleteIntegration`  
  Acceptance: `changed_at` updates on update; `deleted_at` is set on delete.
- `A-GI-006` `TestRunOptimizedQueryIntegration`  
  Acceptance: optimized query returns expected records, total, and attributes.
- `A-GI-007` `TestEvaluateRoutingPolicy_VariousStrategies`  
  Acceptance: routing decisions match strategy for `hybrid`, `cost-first`, and disabled config.
- `A-GI-009` `TestNewDuckDBClient_HealthCheck`  
  Acceptance: DuckDB client initializes successfully and health check passes.
- `A-GI-011` `TestAppendDirtyExclusion`  
  Acceptance: dirty-ID exclusion clause is constructed correctly (`NOT IN` present; arg count matches the dirty IDs).
- `A-GI-012` `TestFinalizeDuckDBExecutionPlan_CaptureDisabled`  
  Acceptance: the execution-plan finalize path runs safely without crashing when capture is disabled.
- `A-GI-013` `TestBuildDuckDBQuery_AdvancedTemplate`  
  Acceptance: the advanced query template renders successfully with Limit/Offset params, producing non-empty SQL and non-nil args.
- `A-GI-014` `TestExecuteDuckDBFederatedQuery_NilQuery`  
  Acceptance: nil query input returns explicit error.
- `A-GI-015` `TestBuildDuckDBQuery_PropagatesRenderError` (`internal/sqlgen`)  
  Acceptance: a template render error is propagated (surfaced) by `BuildDuckDBQuery` rather than swallowed.
- `A-GI-016` `TestBuildDuckDBQuery_KeysetArgsBindLast` (`internal/sqlgen`)  
  Acceptance: merged arg order for keyset queries is correct — the keyset value binds last, `?` placeholder count equals arg count, and no `$n` placeholders appear.

## 3.2 Go E2E Harness Baseline Cases

Execution entry:

- `go test -v ./internal/e2e_harness/... -timeout=5m`
- `go test -v ./internal/e2e_harness/federated/... -tags=e2e -run "TestSuite_Smoke|TestSuite_Integration"`

Test cases:

- `A-EH-001` `TestE2EHarnessMinimal`  
  Acceptance: Postgres/S3/DuckDB startup succeeds; Parquet write/upload works; DuckDB can read Parquet from S3.
- `A-EH-002` `TestSuite_Smoke`  
  Acceptance: test infrastructure is healthy; baseline data operations/query/S3 operations work.
- `A-EH-003` `TestSuite_Integration`  
  Acceptance: full pipeline (Seed -> Query -> Flush -> Compaction -> Verify) completes with no duplicates.

## 3.3 Federated E2E (by TC category)

Execution entry:

- Full suite: `go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m`
- By category: `go test -v ./internal/e2e_harness/federated/... -tags=e2e -run "<CategoryPrefix>"`

### TC-01 Three-Tier Data Architecture (Data Tier)

- `A-F01-001` `TestDataTier_S3BaseFilesOnly`  
  Acceptance: federated query returns valid results with Base tier only.
- `A-F01-002` `TestDataTier_S3DeltaFilesOnly`  
  Acceptance: federated query returns valid results with Delta tier only.
- `A-F01-003` `TestDataTier_PostgresHotBufferOnly`  
  Acceptance: query returns real-time data with Hot Buffer only.
- `A-F01-004` `TestDataTier_AllThreeTiers`  
  Acceptance: three-tier federated query succeeds with complete results.
- `A-F01-005` `TestDataTier_TierPriorityOrder`  
  Acceptance: tier priority and merge order match design expectations.
- `A-F01-006` `TestDataTier_EmptyTiers`  
  Acceptance: empty-tier scenario returns empty results without errors.
- `A-F01-007` `TestDataTier_LargeLimitPagination`  
  Acceptance: large pagination parameters return consistent totals with no duplicates/missing rows.

### TC-02 Merge-on-Read

- `A-F02-001` `TestMergeOnRead_UnionAllCorrectness`  
  Acceptance: `UNION ALL` result is correct for non-overlapping data.
- `A-F02-002` `TestMergeOnRead_OverlappingRecords`  
  Acceptance: deduplication is correct for overlapping row_ids across tiers.
- `A-F02-003` `TestMergeOnRead_LastWriteWins`  
  Acceptance: latest `changed_at` version wins for same row_id.
- `A-F02-004` `TestMergeOnRead_DirtyIDExclusion`  
  Acceptance: Dirty Set row_ids are excluded from S3 results.
- `A-F02-005` `TestMergeOnRead_MultipleOverlappingRecords`  
  Acceptance: multiple overlaps are merged without duplicate versions.
- `A-F02-006` `TestMergeOnRead_MixedCleanAndDirty`  
  Acceptance: mixed dirty/clean data remains consistent in output.
- `A-F02-007` `TestMergeOnRead_TimeSlotOrdering`  
  Acceptance: ordering by time/version slot remains stable.

### TC-03 Global Deduplication

- `A-F03-001` `TestDeduplication_SameTier`  
  Acceptance: duplicate records inside one tier are deduplicated.
- `A-F03-002` `TestDeduplication_CrossTier`  
  Acceptance: duplicate records across tiers are deduplicated.
- `A-F03-003` `TestDeduplication_BulkPerformance`  
  Acceptance: deduplication completes under large duplicate volume.
- `A-F03-004` `TestDeduplication_UUIDv7TimeOrdering`  
  Acceptance: UUIDv7 time-order behavior is compatible with dedup result.
- `A-F03-005` `TestDeduplication_NoFalsePositives`  
  Acceptance: distinct row_ids are not falsely deduplicated.
- `A-F03-006` `TestDeduplication_MultipleRowsWithVersions`  
  Acceptance: each row retains only one latest version in multi-row multi-version scenarios.

### TC-04 Soft Delete Filtering

- `A-F04-001` `TestSoftDelete_ExcludeDeleted`  
  Acceptance: soft-deleted records are excluded from query results.
- `A-F04-002` `TestSoftDelete_NullVsZeroDeletedAt`  
  Acceptance: `deleted_at` as `NULL` and `0` are both treated as not deleted.
- `A-F04-003` `TestSoftDelete_RestoreAfterDelete`  
  Acceptance: restored records become queryable again.
- `A-F04-004` `TestSoftDelete_DeleteThenReuse`  
  Acceptance: behavior is correct when row_id is reused after delete.
- `A-F04-005` `TestSoftDelete_AllTiersDeleted`  
  Acceptance: no stale data leaks when deleted across all tiers.
- `A-F04-006` `TestSoftDelete_BulkDeletedExclusion`  
  Acceptance: filtering remains correct under high deleted ratio.
- `A-F04-007` `TestSoftDelete_DeletedAtTimestampPrecision`  
  Acceptance: millisecond precision handling is correct.

### TC-05 CDC Smart Flush

- `A-F05-001` `TestCDCFlush_MinRecordsThreshold`  
  Acceptance: record-count threshold mechanism can trigger flush; flush works above threshold.  
  Note: current harness does not strictly assert "no flush below threshold"; requires manual supplement.
- `A-F05-002` `TestCDCFlush_MaxAgeThreshold`  
  Acceptance: flush triggers when oldest unflushed record exceeds max-age threshold.
- `A-F05-003` `TestCDCFlush_AdvisoryLockPreventsConurrent`  
  Acceptance: advisory lock prevents duplicate concurrent flush processing.
- `A-F05-004` `TestCDCFlush_RecordsMarkedFlushed`  
  Acceptance: `flushed_at` is updated correctly after flush.
- `A-F05-005` `TestCDCFlush_DeltaFileNaming`  
  Acceptance: delta file naming convention and suffix are correct.
- `A-F05-006` `TestCDCFlush_BatchSizeRespected`  
  Acceptance: single flush respects batch size; remaining records are flushable later.
- `A-F05-007` `TestCDCFlush_MultipleFlushesComplete`  
  Acceptance: repeated flushes can eventually drain all unflushed records.

### TC-06 Compaction

- `A-F06-001` `TestCompaction_NewDataAppendsToDeltas`  
  Acceptance: new data lands in Delta first, not direct Base rewrite.
- `A-F06-002` `TestCompaction_LowDirtyRatioSkipsCompaction`  
  Acceptance: low dirty-ratio scenario is recognized as low urgency.  
  Note: current harness does not strictly assert mandatory skip-rewrite; requires manual supplement.
- `A-F06-003` `TestCompaction_HighDirtyRatioTriggersRewrite`  
  Acceptance: high dirty-ratio scenario performs merge and absorbs/cleans Delta.
- `A-F06-004` `TestCompaction_MergesMultipleDeltaFiles`  
  Acceptance: multiple Delta files merge into Base with correct row counts.
- `A-F06-005` `TestCompaction_PreservesDeduplication`  
  Acceptance: dedup semantics are preserved after compaction.
- `A-F06-006` `TestCompaction_FileSizeRotation`  
  Acceptance: large-file metadata can be read and processed correctly.  
  Note: 256MB rotation policy is not strictly verified by current automation and needs manual supplement.
- `A-F06-007` `TestCompaction_PreservesSoftDeletes`  
  Acceptance: soft-deleted records remain correctly filtered after merge.
- `A-F06-008` `TestCompaction_DurationWithinThreshold`  
  Acceptance: compaction for medium dataset completes within threshold (30s).
- `A-F06-009` `TestCompaction_EmptyDeltaNoOp`  
  Acceptance: no-op when Delta is empty; Base is not modified unexpectedly.

### TC-07 Consistency

- `A-F07-001` `TestConsistency_CountMatch`  
  Acceptance: counts align (or in three-tier case, Federated >= Postgres).
- `A-F07-002` `TestConsistency_AttributeValueMatch`  
  Acceptance: no attribute-level mismatches.
- `A-F07-003` `TestConsistency_ChecksumValidation`  
  Acceptance: same data -> same checksum; changed data -> changed checksum.
- `A-F07-004` `TestConsistency_AfterCDCFlush`  
  Acceptance: count/checksum preserved before/after flush; no duplicates.
- `A-F07-005` `TestConsistency_AfterCompaction`  
  Acceptance: count/checksum preserved before/after compaction; no duplicates.
- `A-F07-006` `TestConsistency_RowIDExistence`  
  Acceptance: sampled row_ids are found on both sides.
- `A-F07-007` `TestConsistency_MissingRecordDetection`  
  Acceptance: missing differences (S3-only or Postgres-only) are correctly detected.
- `A-F07-008` `TestConsistency_DeduplicationAcrossComparison`  
  Acceptance: dedup behavior is consistent during comparison.
- `A-F07-009` `TestConsistency_TimestampOrdering`  
  Acceptance: timestamps are valid and ordering semantics are verifiable.

### TC-08 Performance

- `A-F08-001` `TestPerformance_SimplePagination`  
  Acceptance: P95 latency for pagination meets relaxed threshold (baseline *3 in code).
- `A-F08-002` `TestPerformance_ComplexFilter`  
  Acceptance: P95 latency for complex filters meets relaxed threshold.
- `A-F08-003` `TestPerformance_FullTableScan`  
  Acceptance: P95 latency for full scan meets relaxed threshold.
- `A-F08-004` `TestPerformance_ConcurrentQueries`  
  Acceptance: success rate > 90%; throughput and latency meet relaxed gates.
- `A-F08-005` `TestPerformance_CDCFlush`  
  Acceptance: flush latency P95 meets threshold (default 10s).
- `A-F08-006` `TestPerformance_Compaction`  
  Acceptance: compaction file/row counts are correct and duration is within threshold.
- `A-F08-007` `TestPerformance_QueryLatencyDistribution`  
  Acceptance: usable latency distribution report is produced (P50/P95/P99 + buckets).
- `A-F08-008` `TestPerformance_MemoryUsage`  
  Acceptance: no OOM/crash during repeated and streaming queries.

### TC-09 Failure Modes

- `A-F09-001` `TestFailureMode_S3Unavailable`  
  Acceptance: when S3 is unavailable, system degrades gracefully or returns diagnosable errors; recovers correctly.
- `A-F09-002` `TestFailureMode_PostgresUnavailable`  
  Acceptance: Postgres failure path is handled safely; flush failure is visible.
- `A-F09-003` `TestFailureMode_CorruptedParquet`  
  Acceptance: missing/corrupt parquet is detected; query recovers after valid data restoration.
- `A-F09-004` `TestFailureMode_QueryTimeout`  
  Acceptance: timeout context returns `deadline exceeded`; reasonable timeout succeeds.
- `A-F09-005` `TestFailureMode_PartialFailureRecovery`  
  Acceptance: after partial flush failure, retry/recovery can continue without data loss.
- `A-F09-006` `TestFailureMode_GracefulDegradation`  
  Acceptance: Hot Buffer remains queryable when S3 fails; counts stay correct.
- `A-F09-007` `TestFailureMode_ConcurrentFailures`  
  Acceptance: intermittent failures under concurrency do not make all requests fail.
- `A-F09-008` `TestFailureMode_DataIntegrityAfterFailure`  
  Acceptance: no data loss and no duplicates after failure + retry.

## 3.4 Bun/TS E2E and k6 Automated Cases

Execution entry (`tests/e2e/package.json`):

- `bun run register-schemas`
- `bun run gen-data`
- `bun run cdc-init`
- `bun run cdc-flush`
- `bun run compactor`
- `bun run federated-check`
- `bun run test` (pipeline)
- `bun run k6-smoke`
- `bun run k6-full`
- `bun run k6-perf`

Test cases:

- `A-TS-001` `register-schemas`  
  Acceptance: target schemas are `registered` or `already_exists`, with no `error`.
- `A-TS-002` `gen-data`  
  Acceptance: successful writes match requested count; failed count should be 0.
- `A-TS-003` `cdc-init`  
  Acceptance: command exits with code 0; report contains exported rows/files.
- `A-TS-004` `cdc-flush`  
  Acceptance: command exits with code 0; report indicates successful flush and Delta file generation.
- `A-TS-005` `compactor`  
  Acceptance: compaction succeeds for target schemas; failure count is 0.
- `A-TS-006` `federated-check`  
  Acceptance: schema comparisons pass; counts match and attribute mismatch is 0.
- `A-TS-007` `test` (`register-schemas -> gen-data -> cdc-flush -> federated-check`)  
  Acceptance: end-to-end scripted pipeline passes.
- `A-TS-008` `k6-smoke`  
  Acceptance: smoke load meets thresholds and produces report.
- `A-TS-009` `k6-full`  
  Acceptance: full load meets thresholds (p95, success rate, error rate).
- `A-TS-010` `k6-perf`  
  Acceptance: perf load meets thresholds or triggers traceable alerts.

## 3.5 Non-Gating Benchmark Cases

- `BenchmarkFederatedQuery`  
  Purpose: trend baseline for federated query throughput/latency.
- `BenchmarkCDCFlush`  
  Purpose: trend baseline for CDC flush throughput/stability.

## 4. Manual Integration Test Cases (Supplemental)

Note: These manual cases close automation gaps and cover pre-production must-check scenarios.

| ID | Scenario | Key Steps | Acceptance Criteria | Priority |
|---|---|---|---|---|
| `M-001` | Server cold start and schema autoload | Start local server and load schemas from `SCHEMA_DIR` | Service starts successfully; `/api/v1/<schema>` is reachable (not 404) | P0 |
| `M-002` | Lambda routing and health check | Deploy Lambda/API Gateway; call `/health` and API routes | `/health=200`; CRUD/query routes work | P0 |
| `M-003` | Lambda DSQL IAM auth | Configure `DSQL_ENDPOINT` without DB password | IAM-token DB connection succeeds; read/write works | P1 |
| `M-004` | API error-code contract | Send invalid method/path/body/sort/uuid | Correct 400/405 responses with diagnosable messages | P0 |
| `M-005` | Batch create atomicity | Inject one invalid item in batch create | Atomic transaction behavior (all fail or documented partial-fail policy) | P0 |
| `M-006` | Single-delete and batch-delete consistency | Execute both delete modes and verify | Deleted records are not visible; remaining records are visible; counts are correct | P0 |
| `M-007` | `attrs` projection correctness | Query single/list with `attrs=a,b` | Only requested attributes are returned (plus required metadata) | P1 |
| `M-008` | Complex Advanced Query DSL nesting | Build multi-level `and/or` with mixed operators | Result set equals offline-expected set | P0 |
| `M-009` | Cross-schema search filtering | Compare `schemas=lead,visit` vs global search | Schema filter works; pagination/count are correct | P1 |
| `M-010` | Backward-compatible schema evolution | Add optional fields / relax constraints and read/write | Historical data remains readable; new writes succeed without downtime | P0 |
| `M-011` | Reject non-compatible schema changes | Attempt type change/add required/remove field | Change is rejected; existing service/data remain unaffected | P0 |
| `M-012` | Type-fallback precision | Stress fallback path for numeric/date/UUID/bool columns | Query comparisons remain correct; no precision regression | P1 |
| `M-013` | CDC Init dry-run safety | Run `cdc-init --dry-run` | No files written; plan/stats are output only | P0 |
| `M-014` | CDC Init full export | Run `cdc-init` and verify Base + manifest | Base files/manifest generated; exported rows match source data | P0 |
| `M-015` | CDC Flush threshold and idempotency | Test below/above threshold and repeated runs | No flush below threshold; flush above threshold; no duplicate flushing on retries | P0 |
| `M-016` | Compaction threshold policy | Build low/high dirty-ratio datasets | No rewrite under low ratio; merge + Delta cleanup under high ratio | P0 |
| `M-017` | S3 failure recovery chain | Trigger failures during query/flush and retry after recovery | Pipeline resumes; no data loss/duplicates | P0 |
| `M-018` | Observability and log audit | Inspect server/tools logs | Key fields present (schema/row/rows_flushed/duration); no sensitive leakage | P1 |
| `M-019` | Injection and input safety | Inject malicious payload in query DSL/`q`/`sort_by` | Inputs are handled safely; no DB/table damage; service remains healthy | P0 |
| `M-020` | Production performance gate | Run `k6-full`/`k6-perf` with production-like dataset | SLO is met (p95, success rate, failure rate) | P0 |
| `M-021` | Disaster recovery drill | Restore Postgres + S3 metadata and replay queries | Restored system is consistent and can continue write/flush/compaction | P1 |

## 5. Automation Coverage Gaps (Manual Priority)

- `G-001` "No flush below minimum-record threshold" is not strictly asserted by current harness  
  Related manual case: `M-015`
- `G-002` Low dirty-ratio compaction skip-rewrite policy is not strictly asserted by current automation  
  Related manual case: `M-016`
- `G-003` 256MB file rotation policy is not fully simulated in automation  
  Related manual case: `M-016`
- `G-004` True Postgres unavailability (container-level) is insufficiently exercised  
  Related manual case: `M-017`
- `G-005` Lambda + API Gateway + DSQL combined validation is still mostly manual  
  Related manual cases: `M-002`, `M-003`

## 6. Recommended Execution Order

1. Run Go internal integration tests (`A-GI-*`)  
2. Run Go E2E Harness (`A-EH-*` + `A-F**-*`)  
3. Run Bun E2E pipeline (`A-TS-007`)  
4. Run k6 (`A-TS-008/009/010`)  
5. Execute manual P0 cases (`M-001`, `M-002`, `M-004`, `M-005`, `M-006`, `M-008`, `M-010`, `M-011`, `M-013`, `M-014`, `M-015`, `M-016`, `M-017`, `M-019`, `M-020`)

## 7. Reference Implementation Locations

- Go integration: `internal/integration_suite_test.go`  
- Repository integration: `internal/postgres_persistent_repository_integration_test.go`  
- DuckDB/federated integration: `internal/federated/duckdb_federated_integration_test.go`  
- E2E harness: `internal/e2e_harness/e2e_test.go`  
- Federated E2E: `internal/e2e_harness/federated/*_test.go`  
- Bun E2E scripts: `tests/e2e/scripts/*.ts`  
- Load testing: `tests/e2e/k6/scenarios.ts`  
- Service APIs: `internal/httpapi/server.go`, `cmd/lambda/main.go`

# E2E Test Harness

End-to-end test harness for validating the federated query system, including the three-tier data architecture, CDC flushing, compaction, and data consistency.

## Prerequisites

- **Go 1.21+**
- **Docker** - Required for running test containers (Postgres, RustFS/MinIO)
- **Sufficient memory** - Tests may require 2GB+ for container orchestration

## Quick Start

```bash
# Run the minimal harness test (verifies infrastructure)
go test -v ./internal/e2e_harness/... -timeout=5m

# Run all federated E2E tests
go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m

# Skip performance tests (faster CI runs)
go test -v ./internal/e2e_harness/federated/... -tags=e2e -short
```

## Architecture

The E2E harness tests validate a **three-tier federated query architecture**:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Federated Query                              │
│   SELECT * FROM (base UNION ALL delta UNION ALL hot) QUALIFY ...    │
└─────────────────────────────────────────────────────────────────────┘
                                   │
          ┌────────────────────────┼────────────────────────┐
          │                        │                        │
          ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   S3 Base       │    │   S3 Delta      │    │ Postgres Hot    │
│   (Parquet)     │    │   (Parquet)     │    │ Buffer          │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ - Oldest data   │    │ - Recent flushes│    │ - Unflushed     │
│ - Compacted     │    │ - Pre-compaction│    │ - Real-time     │
│ - Read-only     │    │ - Append-only   │    │ - Read-write    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        ▲                      ▲                      │
        │                       │                       │
        └───────────────────────┴───────────────────────┘
                      Compaction        CDC Flush
```

### Tier Descriptions

| Tier | Storage | Purpose | Data Age |
|------|---------|---------|----------|
| **Base** | S3 Parquet | Historical, compacted data | Oldest |
| **Delta** | S3 Parquet | Recently flushed CDC data | Middle |
| **Hot Buffer** | Postgres | Live, unflushed changes | Newest |

### Data Flow

1. **Writes** go to Postgres (Hot Buffer)
2. **CDC Flush** moves data from Hot Buffer to Delta (S3 Parquet)
3. **Compaction** merges Delta files into Base files
4. **Queries** federate across all three tiers with deduplication

## Test Categories

### TC-01: Three-Tier Data Architecture

**File:** `federated/data_tier_test.go`

Validates that queries correctly access Base, Delta, and Hot tiers independently and together.

| Test | Description |
|------|-------------|
| `TestDataTier_S3BaseFilesOnly` | Queries work with only Base files |
| `TestDataTier_S3DeltaFilesOnly` | Queries work with only Delta files |
| `TestDataTier_PostgresHotBufferOnly` | Queries work with only Hot Buffer |
| `TestDataTier_AllThreeTiers` | Correctly merges data from all tiers |
| `TestDataTier_TierPriorityOrder` | Verifies scan order: Base, Delta, Hot |
| `TestDataTier_EmptyTiers` | Handles empty tiers gracefully |
| `TestDataTier_LargeLimitPagination` | Pagination works across tiers |

```bash
go test -v ./internal/e2e_harness/federated/... -run "TestDataTier" -tags=e2e
```

### TC-02: Merge-on-Read Logic

**File:** `federated/merge_on_read_test.go`

Validates UNION ALL and merge behavior across tiers.

| Test | Description |
|------|-------------|
| `TestMergeOnRead_UnionAllCorrectness` | UNION ALL correctly merges non-overlapping data |
| `TestMergeOnRead_OverlappingRecords` | Overlapping row_ids are deduplicated |
| `TestMergeOnRead_LastWriteWins` | Newest version wins based on changed_at |
| `TestMergeOnRead_DirtyIDExclusion` | Dirty IDs excluded from Base/Delta |
| `TestMergeOnRead_MultipleOverlappingRecords` | Handles many overlapping versions |
| `TestMergeOnRead_MixedCleanAndDirty` | Correct handling of mixed clean/dirty data |
| `TestMergeOnRead_TimeSlotOrdering` | Records ordered by changed_at correctly |

```bash
go test -v ./internal/e2e_harness/federated/... -run "TestMergeOnRead" -tags=e2e
```

### TC-03: Global Deduplication

**File:** `federated/deduplication_test.go`

Validates QUALIFY ROW_NUMBER deduplication logic.

| Test | Description |
|------|-------------|
| `TestDeduplication_SameTier` | Deduplication within a single tier |
| `TestDeduplication_CrossTier` | Deduplication across different tiers |
| `TestDeduplication_BulkPerformance` | Performance with 10K records, 30% duplicates |
| `TestDeduplication_UUIDv7TimeOrdering` | UUID v7 time-based ordering |
| `TestDeduplication_NoFalsePositives` | Distinct rows not incorrectly deduplicated |
| `TestDeduplication_MultipleRowsWithVersions` | Multiple rows with multiple versions each |

```bash
go test -v ./internal/e2e_harness/federated/... -run "TestDeduplication" -tags=e2e
```

### TC-04: Soft Delete Filtering

**File:** `federated/soft_delete_test.go`

Validates deleted_at filtering logic.

| Test | Description |
|------|-------------|
| `TestSoftDelete_ExcludeDeleted` | Soft-deleted records are excluded |
| `TestSoftDelete_NullVsZeroDeletedAt` | NULL and 0 both mean "not deleted" |
| `TestSoftDelete_RestoreAfterDelete` | Restored records appear correctly |
| `TestSoftDelete_DeleteThenReuse` | Deleted row_id can be reused |
| `TestSoftDelete_AllTiersDeleted` | Deleted records excluded from all tiers |
| `TestSoftDelete_BulkDeletedExclusion` | Performance with 50% deleted records |
| `TestSoftDelete_DeletedAtTimestampPrecision` | Millisecond precision handling |

```bash
go test -v ./internal/e2e_harness/federated/... -run "TestSoftDelete" -tags=e2e
```

### TC-05: CDC Smart Flushing

**File:** `federated/cdc_flush_test.go`

Tests CDC flush triggers, advisory locks, and batch processing.

| Test | Description |
|------|-------------|
| `TestCDCFlush_MinRecordsThreshold` | Flush triggered when >100 unflushed records |
| `TestCDCFlush_MaxAgeThreshold` | Flush triggered when oldest record >1 hour |
| `TestCDCFlush_AdvisoryLockPreventsConurrent` | Advisory locks prevent concurrent flushes |
| `TestCDCFlush_RecordsMarkedFlushed` | Records marked with flushed_at timestamp |
| `TestCDCFlush_DeltaFileNaming` | Delta files follow naming convention |
| `TestCDCFlush_BatchSizeRespected` | Flush respects configured batch size |
| `TestCDCFlush_MultipleFlushesComplete` | Multiple flushes drain all records |

```bash
go test -v ./internal/e2e_harness/federated/... -run "TestCDCFlush" -tags=e2e
```

### TC-06: Compaction Strategy

**File:** `federated/compaction_test.go`

Tests delta-to-base compaction behavior.

| Test | Description |
|------|-------------|
| `TestCompaction_NewDataAppendsToDeltas` | New data appends to delta, not base |
| `TestCompaction_LowDirtyRatioSkipsCompaction` | Dirty ratio <5% skips compaction |
| `TestCompaction_HighDirtyRatioTriggersRewrite` | Dirty ratio >5% triggers base rewrite |
| `TestCompaction_MergesMultipleDeltaFiles` | Multiple delta files merged |
| `TestCompaction_PreservesDeduplication` | Deduplication maintained after compaction |
| `TestCompaction_FileSizeRotation` | Base files rotate at 256MB threshold |
| `TestCompaction_PreservesSoftDeletes` | Soft-deleted records handled correctly |
| `TestCompaction_DurationWithinThreshold` | Completes within 30 seconds |
| `TestCompaction_EmptyDeltaNoOp` | No-op when no delta files exist |

```bash
go test -v ./internal/e2e_harness/federated/... -run "TestCompaction" -tags=e2e
```

### TC-07: Data Consistency

**File:** `federated/consistency_test.go`

Validates data consistency between Postgres and federated queries.

| Test | Description |
|------|-------------|
| `TestConsistency_CountMatch` | Postgres and Federated return same count |
| `TestConsistency_AttributeValueMatch` | Attribute values consistent across systems |
| `TestConsistency_ChecksumValidation` | Checksum validation between sources |
| `TestConsistency_AfterCDCFlush` | Consistency after CDC flush operations |
| `TestConsistency_AfterCompaction` | Consistency after compaction operations |
| `TestConsistency_RowIDExistence` | Specific row_ids exist in both systems |
| `TestConsistency_MissingRecordDetection` | Correctly detects missing records |
| `TestConsistency_DeduplicationAcrossComparison` | Deduplication consistent |
| `TestConsistency_TimestampOrdering` | Timestamp ordering maintained |

```bash
go test -v ./internal/e2e_harness/federated/... -run "TestConsistency" -tags=e2e
```

### TC-08: Performance Benchmarks

**File:** `federated/performance_test.go`

Benchmarks query latency and throughput at medium scale (10K-20K records for tests, 100K-500K in production).

```bash
go test -v ./internal/e2e_harness/federated/... -run "TestPerformance" -tags=e2e -timeout=60m
```

#### Performance Thresholds

**Simple Pagination Query:**

| Percentile | Threshold |
|------------|-----------|
| p50 | 50ms |
| p95 | 150ms |
| p99 | 300ms |

**Complex Filter Query:**

| Percentile | Threshold |
|------------|-----------|
| p50 | 200ms |
| p95 | 500ms |
| p99 | 1000ms |

**Full Table Scan:**

| Percentile | Threshold |
|------------|-----------|
| p50 | 750ms |
| p95 | 2000ms |
| p99 | 5000ms |

**Concurrent Queries (20 Virtual Users):**

| Percentile | Threshold |
|------------|-----------|
| p50 | 150ms |
| p95 | 300ms |
| p99 | 500ms |
| Min QPS | 50 |

**CDC Flush (5K-20K records):**

| Percentile | Threshold |
|------------|-----------|
| p95 | 10s |
| p99 | 20s |

**Compaction (20K records):**

| Percentile | Threshold |
|------------|-----------|
| p95 | 30s |
| p99 | 60s |

### TC-09: Failure Modes

**File:** `federated/failure_modes_test.go`

Tests graceful degradation and error handling.

| Test | Description |
|------|-------------|
| `TestFailureMode_S3Unavailable` | Fallback to Postgres-only query |
| `TestFailureMode_PostgresUnavailable` | Graceful error handling |
| `TestFailureMode_CorruptedParquet` | Error handling and recovery |
| `TestFailureMode_QueryTimeout` | Proper timeout enforcement |
| `TestFailureMode_PartialFailureRecovery` | Handling mixed success/failure |
| `TestFailureMode_GracefulDegradation` | System degrades gracefully |
| `TestFailureMode_ConcurrentFailures` | Concurrent failure handling |
| `TestFailureMode_DataIntegrityAfterFailure` | No data loss after failures |

```bash
go test -v ./internal/e2e_harness/federated/... -run "TestFailureMode" -tags=e2e
```

## Test Harness Components

### TestHarness (Base)

Located in `harness.go`, provides:
- Postgres container management (via testcontainers)
- S3/RustFS container management
- DuckDB client initialization
- Basic seeding and parquet file operations

### FederatedTestHarness (Extended)

Located in `federated/harness.go`, extends base harness with:
- Three-tier data seeding
- Federated query execution
- CDC flush simulation
- Compaction operations
- Data comparison utilities

## Configuration

Key configuration options in the test harness:

```go
CDCConfig{
    MinRecords:  100,    // Minimum unflushed records to trigger flush
    MaxAgeMs:    60000,  // Maximum age (ms) before flush triggered
    BatchSize:   1000,   // Records per flush batch
    S3Bucket:    "test-bucket",
    S3Prefix:    "test-project",
}
```

## Running Specific Tests

```bash
# Run smoke test only
go test -v ./internal/e2e_harness/federated/... -run "TestSuite_Smoke" -tags=e2e

# Run integration test
go test -v ./internal/e2e_harness/federated/... -run "TestSuite_Integration" -tags=e2e

# Run benchmarks
go test -v ./internal/e2e_harness/federated/... -bench=. -tags=e2e -timeout=30m

# Run with verbose logging
LOG_LEVEL=debug go test -v ./internal/e2e_harness/federated/... -tags=e2e

# Run with race detection
go test -v -race ./internal/e2e_harness/federated/... -tags=e2e -timeout=45m
```

## Troubleshooting

### Container Startup Failures

If containers fail to start:
1. Ensure Docker daemon is running
2. Check for port conflicts on 5432 (Postgres) and 9000 (S3)
3. Increase container startup timeout if needed

### Timeout Issues

Performance tests may timeout on slower machines:
```bash
# Increase timeout
go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=60m
```

### Memory Issues

If tests run out of memory:
1. Run test categories individually instead of all at once
2. Use `-short` flag to skip large-scale performance tests
3. Increase Docker memory allocation

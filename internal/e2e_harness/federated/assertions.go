// Package federated provides custom assertions for E2E testing.
package federated

import (
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Record Assertions
// ============================================================================

// AssertRecordCount verifies the expected number of records.
func AssertRecordCount(t *testing.T, records []*internal.PersistentRecord, expected int, msgAndArgs ...interface{}) {
	t.Helper()
	assert.Len(t, records, expected, msgAndArgs...)
}

// RequireRecordCount verifies the expected number of records and fails immediately if not met.
func RequireRecordCount(t *testing.T, records []*internal.PersistentRecord, expected int, msgAndArgs ...interface{}) {
	t.Helper()
	require.Len(t, records, expected, msgAndArgs...)
}

// AssertNoDuplicates verifies that all records have unique row_ids.
func AssertNoDuplicates(t *testing.T, records []*internal.PersistentRecord) {
	t.Helper()
	seen := make(map[uuid.UUID]bool)
	for _, r := range records {
		if seen[r.RowID] {
			t.Errorf("duplicate row_id found: %s", r.RowID)
		}
		seen[r.RowID] = true
	}
}

// RequireNoDuplicates verifies that all records have unique row_ids, failing immediately if not.
func RequireNoDuplicates(t *testing.T, records []*internal.PersistentRecord) {
	t.Helper()
	seen := make(map[uuid.UUID]bool)
	for _, r := range records {
		require.False(t, seen[r.RowID], "duplicate row_id found: %s", r.RowID)
		seen[r.RowID] = true
	}
}

// AssertNoDeleted verifies that no records are soft-deleted.
func AssertNoDeleted(t *testing.T, records []*internal.PersistentRecord) {
	t.Helper()
	for _, r := range records {
		if r.DeletedAt != nil && *r.DeletedAt > 0 {
			t.Errorf("found deleted record: %s (deleted_at=%d)", r.RowID, *r.DeletedAt)
		}
	}
}

// RequireNoDeleted verifies that no records are soft-deleted, failing immediately if found.
func RequireNoDeleted(t *testing.T, records []*internal.PersistentRecord) {
	t.Helper()
	for _, r := range records {
		if r.DeletedAt != nil {
			require.Zero(t, *r.DeletedAt, "found deleted record: %s", r.RowID)
		}
	}
}

// AssertAllRecordsHaveSchemaID verifies all records have the expected schema ID.
func AssertAllRecordsHaveSchemaID(t *testing.T, records []*internal.PersistentRecord, schemaID int16) {
	t.Helper()
	for _, r := range records {
		assert.Equal(t, schemaID, r.SchemaID, "unexpected schema_id for row %s", r.RowID)
	}
}

// AssertRecordExists verifies that a record with the given row_id exists.
func AssertRecordExists(t *testing.T, records []*internal.PersistentRecord, rowID uuid.UUID) {
	t.Helper()
	for _, r := range records {
		if r.RowID == rowID {
			return
		}
	}
	t.Errorf("record with row_id %s not found", rowID)
}

// AssertRecordNotExists verifies that no record with the given row_id exists.
func AssertRecordNotExists(t *testing.T, records []*internal.PersistentRecord, rowID uuid.UUID) {
	t.Helper()
	for _, r := range records {
		if r.RowID == rowID {
			t.Errorf("record with row_id %s should not exist", rowID)
			return
		}
	}
}

// ============================================================================
// Query Result Assertions
// ============================================================================

// AssertQueryResultMatch verifies that two query results match.
func AssertQueryResultMatch(t *testing.T, expected, actual *QueryResult) {
	t.Helper()
	assert.Equal(t, expected.TotalRecords, actual.TotalRecords, "total records mismatch")
	assert.Len(t, actual.Records, len(expected.Records), "record count mismatch")
}

// AssertTotalRecordsInRange verifies total records is within expected range.
func AssertTotalRecordsInRange(t *testing.T, result *QueryResult, min, max int64) {
	t.Helper()
	assert.GreaterOrEqual(t, result.TotalRecords, min, "total records below minimum")
	assert.LessOrEqual(t, result.TotalRecords, max, "total records above maximum")
}

// ============================================================================
// Comparison Assertions
// ============================================================================

// AssertComparisonMatch verifies that a comparison report shows a match.
func AssertComparisonMatch(t *testing.T, report *ComparisonReport) {
	t.Helper()
	if !report.Match {
		t.Errorf("comparison failed: federated=%d, postgres=%d, missing_in_fed=%d, missing_in_pg=%d, mismatches=%d",
			report.FederatedCount, report.PostgresCount,
			len(report.MissingInFed), len(report.MissingInPG),
			len(report.AttributeMismatches))

		if len(report.MissingInFed) > 0 {
			t.Logf("missing in federated (first 5): %v", firstN(report.MissingInFed, 5))
		}
		if len(report.MissingInPG) > 0 {
			t.Logf("missing in postgres (first 5): %v", firstN(report.MissingInPG, 5))
		}
		if len(report.AttributeMismatches) > 0 {
			for i, m := range report.AttributeMismatches {
				if i >= 5 {
					break
				}
				t.Logf("mismatch: row=%s attr=%s fed=%v pg=%v", m.RowID, m.AttributeName, m.FederatedVal, m.PostgresVal)
			}
		}
	}
}

// RequireComparisonMatch verifies that a comparison report shows a match, failing immediately if not.
func RequireComparisonMatch(t *testing.T, report *ComparisonReport) {
	t.Helper()
	require.True(t, report.Match, "comparison failed: federated=%d, postgres=%d", report.FederatedCount, report.PostgresCount)
}

// AssertChecksumMatch verifies that checksums match.
func AssertChecksumMatch(t *testing.T, report *ComparisonReport) {
	t.Helper()
	assert.Equal(t, report.FederatedChecksum, report.PostgresChecksum, "checksums do not match")
}

// ============================================================================
// Performance Assertions
// ============================================================================

// AssertLatencyUnder verifies query latency is under the specified threshold.
func AssertLatencyUnder(t *testing.T, duration time.Duration, threshold time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	assert.Less(t, duration, threshold, msgAndArgs...)
}

// RequireLatencyUnder verifies query latency is under the specified threshold, failing immediately if not.
func RequireLatencyUnder(t *testing.T, duration time.Duration, threshold time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	require.Less(t, duration, threshold, msgAndArgs...)
}

// AssertP95Latency verifies that the p95 latency is under threshold.
func AssertP95Latency(t *testing.T, latencies []time.Duration, threshold time.Duration) {
	t.Helper()
	p95 := Percentile(latencies, 95)
	assert.Less(t, p95, threshold, "p95 latency %v exceeds threshold %v", p95, threshold)
}

// RequireP95Latency verifies that the p95 latency is under threshold, failing immediately if not.
func RequireP95Latency(t *testing.T, latencies []time.Duration, threshold time.Duration) {
	t.Helper()
	p95 := Percentile(latencies, 95)
	require.Less(t, p95, threshold, "p95 latency %v exceeds threshold %v", p95, threshold)
}

// AssertThroughput verifies that throughput is above minimum QPS.
func AssertThroughput(t *testing.T, requestCount int, totalDuration time.Duration, minQPS float64) {
	t.Helper()
	qps := float64(requestCount) / totalDuration.Seconds()
	assert.Greater(t, qps, minQPS, "throughput %.2f QPS below minimum %.2f QPS", qps, minQPS)
}

// ============================================================================
// Execution Plan Assertions
// ============================================================================

// AssertPlanContainsSource verifies that the execution plan includes a specific source.
func AssertPlanContainsSource(t *testing.T, plan *internal.ExecutionPlan, source string) {
	t.Helper()
	if plan == nil {
		t.Error("execution plan is nil")
		return
	}
	for _, note := range plan.Notes {
		if containsString(note, source) {
			return
		}
	}
	t.Errorf("execution plan does not contain source: %s", source)
}

// AssertPlanContainsNote verifies that the execution plan contains a specific note.
func AssertPlanContainsNote(t *testing.T, plan *internal.ExecutionPlan, noteSubstring string) {
	t.Helper()
	if plan == nil {
		t.Error("execution plan is nil")
		return
	}
	for _, note := range plan.Notes {
		if containsString(note, noteSubstring) {
			return
		}
	}
	t.Errorf("execution plan does not contain note with: %s", noteSubstring)
}

// AssertDirtyIDsExcluded verifies that dirty IDs were excluded in the execution plan.
func AssertDirtyIDsExcluded(t *testing.T, plan *internal.ExecutionPlan, expectedCount int) {
	t.Helper()
	AssertPlanContainsNote(t, plan, "dirty_ids_excluded")
}

// ============================================================================
// CDC/Flush Assertions
// ============================================================================

// AssertFlushTriggered verifies that a CDC flush was triggered.
func AssertFlushTriggered(t *testing.T, result *FlushResult) {
	t.Helper()
	assert.True(t, result.Flushed, "flush was not triggered")
}

// AssertFlushNotTriggered verifies that a CDC flush was not triggered.
func AssertFlushNotTriggered(t *testing.T, result *FlushResult) {
	t.Helper()
	assert.False(t, result.Flushed, "flush was unexpectedly triggered")
}

// AssertRowsFlushed verifies the number of rows flushed.
func AssertRowsFlushed(t *testing.T, result *FlushResult, expected int64) {
	t.Helper()
	assert.Equal(t, expected, result.RowsFlushed, "unexpected number of rows flushed")
}

// AssertFilesCreated verifies that files were created during flush.
func AssertFilesCreated(t *testing.T, result *FlushResult, minFiles int) {
	t.Helper()
	assert.GreaterOrEqual(t, len(result.FilesCreated), minFiles, "not enough files created")
}

// ============================================================================
// Compaction Assertions
// ============================================================================

// AssertCompactionMerged verifies that records were merged during compaction.
func AssertCompactionMerged(t *testing.T, result *CompactionResult) {
	t.Helper()
	assert.Greater(t, result.RowsMerged, int64(0), "no rows were merged")
}

// AssertCompactionReducedFiles verifies that compaction reduced the number of files.
func AssertCompactionReducedFiles(t *testing.T, result *CompactionResult) {
	t.Helper()
	assert.Less(t, result.FilesCreated, result.FilesCompacted, "compaction did not reduce file count")
}

// ============================================================================
// S3/Parquet Assertions
// ============================================================================

// AssertParquetFileExists verifies that a parquet file exists in S3.
func AssertParquetFileExists(t *testing.T, files []string, expectedPath string) {
	t.Helper()
	for _, f := range files {
		if containsString(f, expectedPath) {
			return
		}
	}
	t.Errorf("parquet file not found: %s", expectedPath)
}

// AssertParquetRowCount verifies the row count in a parquet file.
func AssertParquetRowCount(t *testing.T, meta *ParquetMetadata, expected int64) {
	t.Helper()
	assert.Equal(t, expected, meta.RowCount, "parquet row count mismatch")
}

// AssertParquetRowCountInRange verifies the row count is within range.
func AssertParquetRowCountInRange(t *testing.T, meta *ParquetMetadata, min, max int64) {
	t.Helper()
	assert.GreaterOrEqual(t, meta.RowCount, min, "parquet row count below minimum")
	assert.LessOrEqual(t, meta.RowCount, max, "parquet row count above maximum")
}

// ============================================================================
// Helper Functions
// ============================================================================

// Percentile calculates the p-th percentile of durations.
func Percentile(latencies []time.Duration, p int) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	idx := (len(sorted) - 1) * p / 100
	return sorted[idx]
}

// MaxDuration returns the maximum duration from a slice.
func MaxDuration(latencies []time.Duration) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	maxVal := latencies[0]
	for _, l := range latencies[1:] {
		if l > maxVal {
			maxVal = l
		}
	}
	return maxVal
}

// MinDuration returns the minimum duration from a slice.
func MinDuration(latencies []time.Duration) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	minVal := latencies[0]
	for _, l := range latencies[1:] {
		if l < minVal {
			minVal = l
		}
	}
	return minVal
}

// AvgDuration returns the average duration from a slice.
func AvgDuration(latencies []time.Duration) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	var total time.Duration
	for _, l := range latencies {
		total += l
	}
	return total / time.Duration(len(latencies))
}

// LatencyStats holds statistical information about latencies.
type LatencyStats struct {
	Min   time.Duration
	Max   time.Duration
	Avg   time.Duration
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	Count int
}

// CalculateLatencyStats computes statistics from a slice of durations.
func CalculateLatencyStats(latencies []time.Duration) LatencyStats {
	if len(latencies) == 0 {
		return LatencyStats{}
	}

	return LatencyStats{
		Min:   MinDuration(latencies),
		Max:   MaxDuration(latencies),
		Avg:   AvgDuration(latencies),
		P50:   Percentile(latencies, 50),
		P95:   Percentile(latencies, 95),
		P99:   Percentile(latencies, 99),
		Count: len(latencies),
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr) >= 0))
}

func findSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func firstN[T any](slice []T, n int) []T {
	if len(slice) <= n {
		return slice
	}
	return slice[:n]
}

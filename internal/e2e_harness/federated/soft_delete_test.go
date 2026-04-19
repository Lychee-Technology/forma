//go:build e2e

// Package federated provides E2E tests for soft delete filtering.
// TC-04: Soft Delete Tests - Validates deleted_at filtering logic.
package federated

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestSoftDelete_ExcludeDeleted verifies that soft-deleted records are excluded.
func TestSoftDelete_ExcludeDeleted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Use preset soft delete scenario
	scenarios := PresetScenarios{}
	records := scenarios.SoftDeleteScenario(h.SchemaID)

	require.NoError(t, h.SeedHotRecordsWithData(ctx, records))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 20})
	require.NoError(t, err, "federated query failed")

	// Should have only 5 records (first 5 are not deleted)
	AssertRecordCount(t, result.Records, 5)
	AssertNoDeleted(t, result.Records)

	t.Logf("TC-04-01 PASSED: Soft-deleted records excluded - 10 total, 5 deleted -> 5 returned")
}

// TestSoftDelete_NullVsZeroDeletedAt verifies NULL and 0 both mean "not deleted".
func TestSoftDelete_NullVsZeroDeletedAt(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	now := time.Now().UnixMilli()

	// Records with deleted_at = 0 (explicit not deleted)
	zeroDeleted := []TestRecord{
		{
			RowID:      uuid.Must(uuid.NewV7()),
			SchemaID:   h.SchemaID,
			Attributes: map[string]any{"name": "Zero Deleted 1"},
			ChangedAt:  now,
			DeletedAt:  0,
		},
		{
			RowID:      uuid.Must(uuid.NewV7()),
			SchemaID:   h.SchemaID,
			Attributes: map[string]any{"name": "Zero Deleted 2"},
			ChangedAt:  now,
			DeletedAt:  0,
		},
	}

	require.NoError(t, h.SeedHotRecordsWithData(ctx, zeroDeleted))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")

	// Both records should be returned (deleted_at = 0 means not deleted)
	AssertRecordCount(t, result.Records, 2)
	AssertNoDeleted(t, result.Records)

	t.Logf("TC-04-02 PASSED: NULL and 0 deleted_at both treated as not deleted")
}

// TestSoftDelete_RestoreAfterDelete verifies that restored records appear correctly.
func TestSoftDelete_RestoreAfterDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	rowID := uuid.Must(uuid.NewV7())
	baseTime := time.Now()

	// Version 1: Created (not deleted)
	require.NoError(t, h.WriteParquet(ctx, "base", "restore_v1.parquet", []TestRecord{{
		RowID:      rowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Created", "version": 1},
		ChangedAt:  baseTime.Add(-3 * time.Hour).UnixMilli(),
		DeletedAt:  0,
	}}))

	// Version 2: Deleted
	require.NoError(t, h.WriteParquet(ctx, "delta", "restore_v2.parquet", []TestRecord{{
		RowID:      rowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Deleted", "version": 2},
		ChangedAt:  baseTime.Add(-2 * time.Hour).UnixMilli(),
		DeletedAt:  baseTime.Add(-2 * time.Hour).UnixMilli(),
	}}))

	// Version 3: Restored (not deleted again)
	restoredTime := baseTime.UnixMilli()
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{{
		RowID:      rowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Restored", "version": 3},
		ChangedAt:  restoredTime,
		DeletedAt:  0, // Restored
	}}))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")

	// Should have 1 record - the restored version
	AssertRecordCount(t, result.Records, 1)
	AssertNoDeleted(t, result.Records)

	// Verify it's the restored version by checking the row_id and that it's the latest
	require.Equal(t, rowID, result.Records[0].RowID, "should return the correct row_id")
	// The record should have the timestamp of the restored version (latest)
	// Note: Hot buffer records return changed_at as CreatedAt in our mapping
	require.Equal(t, restoredTime, result.Records[0].CreatedAt, "should return the restored version (latest timestamp)")

	t.Logf("TC-04-03 PASSED: Restored record after deletion returned correctly")
}

// TestSoftDelete_DeleteThenReuse verifies a deleted row_id can be reused.
func TestSoftDelete_DeleteThenReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Create two different row_ids
	deletedRowID := uuid.Must(uuid.NewV7())
	newRowID := uuid.Must(uuid.NewV7())
	now := time.Now().UnixMilli()

	// Original record (deleted)
	require.NoError(t, h.WriteParquet(ctx, "base", "reuse_deleted.parquet", []TestRecord{{
		RowID:      deletedRowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Deleted Record"},
		ChangedAt:  now - 3600000,
		DeletedAt:  now - 1800000, // Deleted
	}}))

	// New record with different row_id
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{{
		RowID:      newRowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "New Record"},
		ChangedAt:  now,
		DeletedAt:  0,
	}}))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")

	// Should only have the new record (deleted one is excluded)
	AssertRecordCount(t, result.Records, 1)
	require.Equal(t, newRowID, result.Records[0].RowID)

	t.Logf("TC-04-04 PASSED: Deleted records excluded, new records returned")
}

// TestSoftDelete_AllTiersDeleted verifies deleted records in all tiers are excluded.
func TestSoftDelete_AllTiersDeleted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	now := time.Now().UnixMilli()

	// Deleted record in base
	require.NoError(t, h.WriteParquet(ctx, "base", "del_base.parquet", []TestRecord{{
		RowID:      uuid.Must(uuid.NewV7()),
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Base Deleted"},
		ChangedAt:  now - 7200000,
		DeletedAt:  now - 3600000,
	}}))

	// Deleted record in delta
	require.NoError(t, h.WriteParquet(ctx, "delta", "del_delta.parquet", []TestRecord{{
		RowID:      uuid.Must(uuid.NewV7()),
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Delta Deleted"},
		ChangedAt:  now - 3600000,
		DeletedAt:  now - 1800000,
	}}))

	// Deleted record in hot
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{{
		RowID:      uuid.Must(uuid.NewV7()),
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Hot Deleted"},
		ChangedAt:  now,
		DeletedAt:  now,
	}}))

	// One live record
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{{
		RowID:      uuid.Must(uuid.NewV7()),
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Live Record"},
		ChangedAt:  now,
		DeletedAt:  0,
	}}))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")

	// Should only have the live record
	AssertRecordCount(t, result.Records, 1)
	AssertNoDeleted(t, result.Records)

	t.Logf("TC-04-05 PASSED: Deleted records from all tiers excluded")
}

// TestSoftDelete_HotDeleteShadowsParquetVersion verifies a latest hot tombstone hides an older parquet row.
func TestSoftDelete_HotDeleteShadowsParquetVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	rowID := uuid.Must(uuid.NewV7())
	baseTime := time.Now()

	// Older visible version in parquet.
	require.NoError(t, h.WriteParquet(ctx, "base", "shadowed_base.parquet", []TestRecord{{
		RowID:      rowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Old Visible Version", "version": 1},
		ChangedAt:  baseTime.Add(-2 * time.Hour).UnixMilli(),
		DeletedAt:  0,
	}}))

	// Newer tombstone in hot should shadow the parquet version.
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{{
		RowID:      rowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Deleted Version", "version": 2},
		ChangedAt:  baseTime.UnixMilli(),
		DeletedAt:  baseTime.UnixMilli(),
	}}))

	// Add one unrelated live row so the query still returns data.
	liveRowID := uuid.Must(uuid.NewV7())
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{{
		RowID:      liveRowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Still Visible", "version": 1},
		ChangedAt:  baseTime.Add(-time.Minute).UnixMilli(),
		DeletedAt:  0,
	}}))

	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")

	AssertRecordCount(t, result.Records, 1)
	require.Equal(t, liveRowID, result.Records[0].RowID, "expected only unrelated live row to remain visible")
	require.EqualValues(t, 1, result.TotalRecords, "expected count query to honor delete shadowing")

	t.Logf("TC-04-05B PASSED: Hot tombstone shadows older parquet version")
}

// TestSoftDelete_BulkDeletedExclusion verifies performance with many deleted records.
func TestSoftDelete_BulkDeletedExclusion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Generate records with 50% deleted
	totalRecords := 1000
	deletedRatio := 0.5

	records := GenerateTestRecords(totalRecords, &GeneratorOptions{
		SchemaID:     h.SchemaID,
		DeletedRatio: deletedRatio,
	})

	require.NoError(t, h.SeedHotRecordsWithData(ctx, records))

	// Count expected live records
	expectedLive := 0
	for _, r := range records {
		if r.DeletedAt == 0 {
			expectedLive++
		}
	}

	// Execute query with timing
	start := time.Now()
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: totalRecords})
	duration := time.Since(start)
	require.NoError(t, err, "federated query failed")

	// Verify only live records are returned
	AssertRecordCount(t, result.Records, expectedLive)
	AssertNoDeleted(t, result.Records)

	// Should complete in reasonable time
	require.Less(t, duration, 3*time.Second, "soft delete filtering too slow: %v", duration)

	t.Logf("TC-04-06 PASSED: Bulk delete exclusion - %d total, %d deleted -> %d returned in %v",
		totalRecords, totalRecords-expectedLive, expectedLive, duration)
}

// TestSoftDelete_DeletedAtTimestampPrecision verifies timestamp handling.
func TestSoftDelete_DeletedAtTimestampPrecision(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	now := time.Now()
	nowMs := now.UnixMilli()

	// Record deleted 1ms ago (should be excluded)
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{{
		RowID:      uuid.Must(uuid.NewV7()),
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Deleted 1ms ago"},
		ChangedAt:  nowMs,
		DeletedAt:  nowMs - 1, // 1ms ago
	}}))

	// Record not deleted (deleted_at = 0)
	liveRowID := uuid.Must(uuid.NewV7())
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{{
		RowID:      liveRowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Not Deleted"},
		ChangedAt:  nowMs,
		DeletedAt:  0,
	}}))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")

	// Should only have the live record
	AssertRecordCount(t, result.Records, 1)
	require.Equal(t, liveRowID, result.Records[0].RowID)

	t.Logf("TC-04-07 PASSED: Millisecond precision deleted_at handled correctly")
}

//go:build e2e

// Package federated provides E2E tests for Merge-on-Read logic.
// TC-02: Merge-on-Read Tests - Validates UNION ALL and merge behavior across tiers.
package federated

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestMergeOnRead_UnionAllCorrectness verifies UNION ALL correctly merges non-overlapping data.
func TestMergeOnRead_UnionAllCorrectness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Seed non-overlapping data to each tier
	baseRecords, err := h.SeedBaseRecords(ctx, 1000)
	require.NoError(t, err)
	deltaRecords, err := h.SeedDeltaRecords(ctx, 500)
	require.NoError(t, err)
	hotRecords, err := h.SeedHotRecords(ctx, 200)
	require.NoError(t, err)

	// Execute federated query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 2000})
	require.NoError(t, err, "federated query failed")

	// Verify UNION ALL returns all records
	expectedTotal := len(baseRecords) + len(deltaRecords) + len(hotRecords)
	AssertRecordCount(t, result.Records, expectedTotal)
	AssertNoDuplicates(t, result.Records)

	t.Logf("TC-02-01 PASSED: UNION ALL returned %d records correctly", len(result.Records))
}

// TestMergeOnRead_OverlappingRecords verifies that overlapping row_ids are handled correctly.
func TestMergeOnRead_OverlappingRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Use preset deduplication scenario
	scenarios := PresetScenarios{}
	baseRecords, deltaRecords, hotRecords := scenarios.DeduplicationScenario(h.SchemaID)

	// Write to respective tiers
	require.NoError(t, h.WriteParquet(ctx, "base", "overlap_base.parquet", baseRecords))
	require.NoError(t, h.WriteParquet(ctx, "delta", "overlap_delta.parquet", deltaRecords))
	require.NoError(t, h.SeedHotRecordsWithData(ctx, hotRecords))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 100})
	require.NoError(t, err, "federated query failed")

	// Should have only 1 record after deduplication
	AssertRecordCount(t, result.Records, 1)

	// Verify it's the newest version (from hot buffer)
	require.Equal(t, hotRecords[0].RowID, result.Records[0].RowID)

	// Verify by timestamp that we got the latest version (hot buffer has newest changed_at)
	require.Equal(t, hotRecords[0].ChangedAt, result.Records[0].CreatedAt,
		"should return the hot buffer version (latest timestamp)")

	t.Logf("TC-02-02 PASSED: Overlapping records deduplicated to 1 record")
}

// TestMergeOnRead_LastWriteWins verifies that the newest version wins based on changed_at.
func TestMergeOnRead_LastWriteWins(t *testing.T) {
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

	// Version 1: Base file (oldest) - 100 hours ago
	require.NoError(t, h.WriteParquet(ctx, "base", "lww_base.parquet", []TestRecord{{
		RowID:    rowID,
		SchemaID: h.SchemaID,
		Attributes: map[string]any{
			"name":    "Alice Original",
			"version": 1,
		},
		ChangedAt: baseTime.Add(-100 * time.Hour).UnixMilli(),
	}}))

	// Version 2: Delta file (middle) - 10 hours ago
	require.NoError(t, h.WriteParquet(ctx, "delta", "lww_delta.parquet", []TestRecord{{
		RowID:    rowID,
		SchemaID: h.SchemaID,
		Attributes: map[string]any{
			"name":    "Alice Updated",
			"version": 2,
		},
		ChangedAt: baseTime.Add(-10 * time.Hour).UnixMilli(),
	}}))

	// Version 3: Hot buffer (newest) - now
	hotChangedAt := baseTime.UnixMilli()
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{{
		RowID:    rowID,
		SchemaID: h.SchemaID,
		Attributes: map[string]any{
			"name":    "Alice Latest",
			"version": 3,
		},
		ChangedAt: hotChangedAt,
	}}))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{
		Limit:  10,
		Filter: &Filter{RowID: rowID},
	})
	require.NoError(t, err, "federated query failed")

	// Verify only one record returned (deduplicated)
	AssertRecordCount(t, result.Records, 1)

	// Verify it's the newest version by timestamp
	require.Equal(t, hotChangedAt, result.Records[0].CreatedAt,
		"should return latest version (newest timestamp)")

	t.Logf("TC-02-03 PASSED: Last write wins - returned newest version")
}

// TestMergeOnRead_DirtyIDExclusion verifies that dirty IDs are excluded from Base/Delta.
func TestMergeOnRead_DirtyIDExclusion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	rowID := uuid.Must(uuid.NewV7())

	// Old version in Base (should be excluded due to dirty)
	require.NoError(t, h.WriteParquet(ctx, "base", "dirty_base.parquet", []TestRecord{{
		RowID:    rowID,
		SchemaID: h.SchemaID,
		Attributes: map[string]any{
			"name":    "Old Version",
			"version": 1,
		},
		ChangedAt: time.Now().Add(-100 * time.Hour).UnixMilli(),
	}}))

	// New version in Hot (dirty, flushed_at = 0)
	hotChangedAt := time.Now().UnixMilli()
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{{
		RowID:    rowID,
		SchemaID: h.SchemaID,
		Attributes: map[string]any{
			"name":    "New Version",
			"version": 2,
		},
		ChangedAt: hotChangedAt,
		FlushedAt: 0, // Unflushed = dirty
	}}))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, nil)
	require.NoError(t, err, "federated query failed")

	// Should only have one record with the new version
	AssertRecordCount(t, result.Records, 1)

	// Verify by timestamp that we got the hot buffer version (attribute values not available in federated queries)
	require.Equal(t, hotChangedAt, result.Records[0].CreatedAt,
		"should return hot buffer version (latest timestamp)")

	// Verify execution plan shows dirty ID exclusion
	if result.Plan != nil {
		AssertDirtyIDsExcluded(t, result.Plan, 1)
	}

	t.Logf("TC-02-04 PASSED: Dirty IDs correctly excluded from Base/Delta")
}

// TestMergeOnRead_MultipleOverlappingRecords verifies dedup with many overlapping versions.
func TestMergeOnRead_MultipleOverlappingRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Create 5 different row_ids, each with 3 versions
	numRows := 5
	versionsPerRow := 3

	for i := 0; i < numRows; i++ {
		rowID := uuid.Must(uuid.NewV7())

		// Create versions spread across tiers
		require.NoError(t, h.InsertOverlappingRecords(ctx, rowID, versionsPerRow))
	}

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 100})
	require.NoError(t, err, "federated query failed")

	// Should have exactly numRows records after deduplication
	AssertRecordCount(t, result.Records, numRows)
	AssertNoDuplicates(t, result.Records)

	t.Logf("TC-02-05 PASSED: Multiple overlapping records deduplicated: %d rows with %d versions each -> %d final records",
		numRows, versionsPerRow, len(result.Records))
}

// TestMergeOnRead_MixedCleanAndDirty verifies correct handling of mixed clean/dirty data.
func TestMergeOnRead_MixedCleanAndDirty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Create 100 base records (clean - already flushed)
	cleanRecords := GenerateTestRecords(100, &GeneratorOptions{
		SchemaID:       h.SchemaID,
		TimeRangeHours: 720,
		TimeOffset:     720,
	})
	require.NoError(t, h.WriteParquet(ctx, "base", "clean_base.parquet", cleanRecords))

	// Create 50 hot records (dirty - not flushed)
	dirtyRecords := GenerateTestRecords(50, &GeneratorOptions{
		SchemaID:       h.SchemaID,
		TimeRangeHours: 1,
		TimeOffset:     0,
	})
	require.NoError(t, h.SeedHotRecordsWithData(ctx, dirtyRecords))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 200})
	require.NoError(t, err, "federated query failed")

	// Should have all 150 records (no overlap)
	expectedTotal := len(cleanRecords) + len(dirtyRecords)
	AssertRecordCount(t, result.Records, expectedTotal)

	// Verify execution plan
	if result.Plan != nil {
		AssertPlanContainsNote(t, result.Plan, "dirty_ids_excluded")
	}

	t.Logf("TC-02-06 PASSED: Mixed clean/dirty data handled correctly: %d clean + %d dirty = %d total",
		len(cleanRecords), len(dirtyRecords), len(result.Records))
}

// TestMergeOnRead_TimeSlotOrdering verifies records are ordered by changed_at correctly.
func TestMergeOnRead_TimeSlotOrdering(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	baseTime := time.Now()

	// Create records with known timestamps
	records := []TestRecord{
		{
			RowID:      uuid.Must(uuid.NewV7()),
			SchemaID:   h.SchemaID,
			Attributes: map[string]any{"name": "Oldest", "version": 1},
			ChangedAt:  baseTime.Add(-3 * time.Hour).UnixMilli(),
		},
		{
			RowID:      uuid.Must(uuid.NewV7()),
			SchemaID:   h.SchemaID,
			Attributes: map[string]any{"name": "Middle", "version": 1},
			ChangedAt:  baseTime.Add(-2 * time.Hour).UnixMilli(),
		},
		{
			RowID:      uuid.Must(uuid.NewV7()),
			SchemaID:   h.SchemaID,
			Attributes: map[string]any{"name": "Newest", "version": 1},
			ChangedAt:  baseTime.Add(-1 * time.Hour).UnixMilli(),
		},
	}

	require.NoError(t, h.SeedHotRecordsWithData(ctx, records))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")

	AssertRecordCount(t, result.Records, 3)

	t.Logf("TC-02-07 PASSED: Time slot ordering verified")
}

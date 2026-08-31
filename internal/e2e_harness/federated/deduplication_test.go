//go:build e2e

// Package federated provides E2E tests for global deduplication.
// TC-03: Deduplication Tests - Validates QUALIFY ROW_NUMBER deduplication logic.
package federated

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestDeduplication_SameTier verifies deduplication within a single tier.
func TestDeduplication_SameTier(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.CleanupOrLog(ctx, t)

	rowID := uuid.Must(uuid.NewV7())
	baseTime := time.Now()

	// Multiple versions in the same tier (hot buffer)
	versions := []TestRecord{
		{
			RowID:      rowID,
			SchemaID:   h.SchemaID,
			Attributes: map[string]any{"name": "Version 1", "version": 1},
			ChangedAt:  baseTime.Add(-3 * time.Hour).UnixMilli(),
		},
		{
			RowID:      rowID,
			SchemaID:   h.SchemaID,
			Attributes: map[string]any{"name": "Version 2", "version": 2},
			ChangedAt:  baseTime.Add(-2 * time.Hour).UnixMilli(),
		},
		{
			RowID:      rowID,
			SchemaID:   h.SchemaID,
			Attributes: map[string]any{"name": "Version 3", "version": 3},
			ChangedAt:  baseTime.Add(-1 * time.Hour).UnixMilli(),
		},
	}

	// Insert all versions (simulating multiple updates)
	for _, v := range versions {
		require.NoError(t, h.insertHotRecord(ctx, v))
	}

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")

	// Should have only 1 record after deduplication
	AssertRecordCount(t, result.Records, 1)
	require.Equal(t, rowID, result.Records[0].RowID)

	t.Logf("TC-03-01 PASSED: Same tier deduplication - 3 versions -> 1 record")
}

// TestDeduplication_CrossTier verifies deduplication across different tiers.
func TestDeduplication_CrossTier(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.CleanupOrLog(ctx, t)

	rowID := uuid.Must(uuid.NewV7())
	baseTime := time.Now()

	// One logical row: every version carries the SAME creation stamp and its
	// own version stamp (#460). The winner is identified by UpdatedAt — the
	// LWW version stamp — while CreatedAt is asserted to stay invariant
	// across the fold, which is the property the reader now guarantees.
	createdAt := baseTime.Add(-200 * time.Hour).UnixMilli()

	// Version in Base (oldest)
	require.NoError(t, h.WriteParquet(ctx, "base", "dedup_base.parquet", []TestRecord{{
		RowID:      rowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Base Version", "version": 1},
		CreatedAt:  createdAt,
		ChangedAt:  baseTime.Add(-100 * time.Hour).UnixMilli(),
	}}))

	// Version in Delta (middle)
	require.NoError(t, h.WriteParquet(ctx, "delta", "dedup_delta.parquet", []TestRecord{{
		RowID:      rowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Delta Version", "version": 2},
		CreatedAt:  createdAt,
		ChangedAt:  baseTime.Add(-10 * time.Hour).UnixMilli(),
	}}))

	// Version in Hot (newest)
	require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{{
		RowID:      rowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "Hot Version", "version": 3},
		CreatedAt:  createdAt,
		ChangedAt:  baseTime.UnixMilli(),
	}}))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err, "federated query failed")

	// Should have only 1 record after cross-tier deduplication
	AssertRecordCount(t, result.Records, 1)

	// Hot version should win (highest priority: Hot > Delta > Base), proven by
	// the version stamp rather than the creation stamp.
	require.Equal(t, baseTime.UnixMilli(), result.Records[0].UpdatedAt,
		"should return the hot buffer version (latest version stamp)")
	require.Equal(t, createdAt, result.Records[0].CreatedAt,
		"the creation stamp is version-invariant: folding three versions must not change it (#460)")

	t.Logf("TC-03-02 PASSED: Cross-tier deduplication - Base/Delta/Hot -> Hot wins")
}

// TestDeduplication_BulkPerformance verifies deduplication performance with large datasets.
func TestDeduplication_BulkPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.CleanupOrLog(ctx, t)

	// Generate 10K records with 30% duplicates
	totalRecords := 10000
	duplicateRatio := 0.30
	records := GenerateBulkRecords(totalRecords, h.SchemaID, duplicateRatio)

	// Split across tiers
	baseSize := int(float64(len(records)) * 0.6)
	deltaSize := int(float64(len(records)) * 0.3)

	require.NoError(t, h.WriteParquet(ctx, "base", "bulk_base.parquet", records[:baseSize]))
	require.NoError(t, h.WriteParquet(ctx, "delta", "bulk_delta.parquet", records[baseSize:baseSize+deltaSize]))
	require.NoError(t, h.SeedHotRecordsWithData(ctx, records[baseSize+deltaSize:]))

	// Execute query with timing
	start := time.Now()
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: totalRecords})
	duration := time.Since(start)
	require.NoError(t, err, "federated query failed")

	// Verify deduplication occurred
	AssertNoDuplicates(t, result.Records)

	// Regression tripwire, not a performance target: in isolation this query
	// runs in ~220ms, but the full suite's container churn has pushed it past
	// a 5s ceiling on a loaded host (#434). The ceiling only needs to catch a
	// broken dedup (30s-class); real latency baselining belongs to the
	// benchmark harness.
	require.Less(t, duration, 30*time.Second, "deduplication took too long: %v", duration)

	// Calculate actual dedup ratio
	uniqueCount := len(result.Records)
	actualDedupRatio := 1.0 - float64(uniqueCount)/float64(len(records))

	t.Logf("TC-03-03 PASSED: Bulk deduplication - %d records -> %d unique (%.1f%% deduplicated) in %v",
		len(records), uniqueCount, actualDedupRatio*100, duration)
}

// TestDeduplication_UUIDv7TimeOrdering verifies UUID v7 time-based ordering.
func TestDeduplication_UUIDv7TimeOrdering(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.CleanupOrLog(ctx, t)

	// Generate UUIDs with time gaps
	var uuids []uuid.UUID
	var records []TestRecord

	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps in UUID
		rowID := uuid.Must(uuid.NewV7())
		uuids = append(uuids, rowID)

		records = append(records, TestRecord{
			RowID:      rowID,
			SchemaID:   h.SchemaID,
			Attributes: map[string]any{"name": "Record", "sequence": i},
			ChangedAt:  time.Now().UnixMilli(),
		})
	}

	require.NoError(t, h.SeedHotRecordsWithData(ctx, records))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 20})
	require.NoError(t, err, "federated query failed")

	// Verify all records are returned (no duplicates to dedup)
	AssertRecordCount(t, result.Records, 10)
	AssertNoDuplicates(t, result.Records)

	// Verify UUID v7 temporal ordering (newer UUIDs should have higher values when compared)
	for i := 1; i < len(uuids); i++ {
		// UUID v7 embeds timestamp, so string comparison should preserve order
		require.True(t, uuids[i-1].String() < uuids[i].String(),
			"UUID v7 ordering violated: %s should be < %s", uuids[i-1], uuids[i])
	}

	t.Logf("TC-03-04 PASSED: UUID v7 time ordering verified for %d records", len(result.Records))
}

// TestDeduplication_NoFalsePositives verifies distinct rows are not incorrectly deduplicated.
func TestDeduplication_NoFalsePositives(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.CleanupOrLog(ctx, t)

	// Create 100 unique records (no duplicates)
	uniqueRecords := GenerateTestRecords(100, &GeneratorOptions{
		SchemaID:       h.SchemaID,
		TimeRangeHours: 24,
		Seed:           12345,
	})

	require.NoError(t, h.SeedHotRecordsWithData(ctx, uniqueRecords))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 150})
	require.NoError(t, err, "federated query failed")

	// All 100 unique records should be returned
	AssertRecordCount(t, result.Records, 100)
	AssertNoDuplicates(t, result.Records)

	t.Logf("TC-03-05 PASSED: No false positives - %d unique records preserved", len(result.Records))
}

// TestDeduplication_MultipleRowsWithVersions verifies dedup with multiple rows having versions.
func TestDeduplication_MultipleRowsWithVersions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.CleanupOrLog(ctx, t)

	numDistinctRows := 20
	versionsPerRow := 3
	baseTime := time.Now()

	// Track the expected version stamp of the winning version, and the one
	// creation stamp every version of that row shares (#460).
	expectedLatestTimestamps := make(map[string]int64)
	expectedCreatedAt := make(map[string]int64)

	// Create multiple rows, each with multiple versions
	for i := 0; i < numDistinctRows; i++ {
		rowID := uuid.Must(uuid.NewV7())
		createdAt := baseTime.Add(-time.Duration(versionsPerRow+1) * time.Hour).UnixMilli()
		expectedCreatedAt[rowID.String()] = createdAt

		for v := 0; v < versionsPerRow; v++ {
			changedAt := baseTime.Add(-time.Duration(versionsPerRow-v) * time.Hour).UnixMilli()
			record := TestRecord{
				RowID:    rowID,
				SchemaID: h.SchemaID,
				Attributes: map[string]any{
					"name":    "Record",
					"version": v + 1,
				},
				CreatedAt: createdAt,
				ChangedAt: changedAt,
			}

			if v == 0 {
				// Oldest version in base
				require.NoError(t, h.WriteParquet(ctx, "base",
					"multi_base_"+rowID.String()[:8]+".parquet", []TestRecord{record}))
			} else if v == 1 {
				// Middle version in delta
				require.NoError(t, h.WriteParquet(ctx, "delta",
					"multi_delta_"+rowID.String()[:8]+".parquet", []TestRecord{record}))
			} else {
				// Newest version in hot - track this as the expected latest
				expectedLatestTimestamps[rowID.String()] = changedAt
				require.NoError(t, h.SeedHotRecordsWithData(ctx, []TestRecord{record}))
			}
		}
	}

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 100})
	require.NoError(t, err, "federated query failed")

	// Should have exactly numDistinctRows records after deduplication
	AssertRecordCount(t, result.Records, numDistinctRows)
	AssertNoDuplicates(t, result.Records)

	// The winner is the latest VERSION stamp; the creation stamp is the same
	// for every version and must survive the fold unchanged (#460).
	for _, r := range result.Records {
		expectedTs, exists := expectedLatestTimestamps[r.RowID.String()]
		require.True(t, exists, "record %s should be in expected timestamps map", r.RowID)
		require.Equal(t, expectedTs, r.UpdatedAt,
			"record %s should have latest version timestamp", r.RowID)
		require.Equal(t, expectedCreatedAt[r.RowID.String()], r.CreatedAt,
			"record %s must keep its single creation stamp across all versions (#460)", r.RowID)
	}

	t.Logf("TC-03-06 PASSED: Multiple rows deduplication - %d rows x %d versions -> %d unique",
		numDistinctRows, versionsPerRow, len(result.Records))
}

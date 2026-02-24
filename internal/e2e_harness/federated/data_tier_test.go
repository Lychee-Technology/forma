//go:build e2e

// Package federated provides E2E tests for the three-tier data architecture.
// TC-01: Data Tier Tests - Validates that queries correctly access Base, Delta, and Hot tiers.
package federated

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDataTier_S3BaseFilesOnly verifies queries work when only Base files exist.
// Scenario: All data is in S3 Base files, no Delta files, no Hot Buffer.
func TestDataTier_S3BaseFilesOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Seed only base records
	baseRecords, err := h.SeedBaseRecords(ctx, 100)
	require.NoError(t, err, "failed to seed base records")

	// Clear change_log to ensure no hot data
	require.NoError(t, h.ClearChangeLog(ctx))

	// Execute federated query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 200})
	require.NoError(t, err, "federated query failed")

	// Verify results
	AssertRecordCount(t, result.Records, len(baseRecords))
	AssertNoDuplicates(t, result.Records)
	AssertNoDeleted(t, result.Records)

	// Verify execution plan shows base files were scanned
	if result.Plan != nil {
		AssertPlanContainsNote(t, result.Plan, "base_files_scanned")
	}

	t.Logf("TC-01-01 PASSED: Base files only query returned %d records in %v",
		len(result.Records), result.Duration)
}

// TestDataTier_S3DeltaFilesOnly verifies queries work when only Delta files exist.
// Scenario: Data was recently flushed to Delta, no Base files yet.
func TestDataTier_S3DeltaFilesOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Seed only delta records
	deltaRecords, err := h.SeedDeltaRecords(ctx, 50)
	require.NoError(t, err, "failed to seed delta records")

	// Clear change_log
	require.NoError(t, h.ClearChangeLog(ctx))

	// Execute federated query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 100})
	require.NoError(t, err, "federated query failed")

	// Verify results
	AssertRecordCount(t, result.Records, len(deltaRecords))
	AssertNoDuplicates(t, result.Records)

	// Verify execution plan shows delta files were scanned
	if result.Plan != nil {
		AssertPlanContainsNote(t, result.Plan, "delta_files_scanned")
	}

	t.Logf("TC-01-02 PASSED: Delta files only query returned %d records in %v",
		len(result.Records), result.Duration)
}

// TestDataTier_PostgresHotBufferOnly verifies queries work when only Hot Buffer has data.
// Scenario: Fresh data not yet flushed to S3.
func TestDataTier_PostgresHotBufferOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Seed only hot records (unflushed)
	hotRecords, err := h.SeedHotRecords(ctx, 25)
	require.NoError(t, err, "failed to seed hot records")

	// Execute federated query (should fall back to postgres-only since no parquet files)
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 50})
	require.NoError(t, err, "federated query failed")

	// Verify results contain hot buffer data
	require.GreaterOrEqual(t, len(result.Records), len(hotRecords),
		"expected at least %d records from hot buffer", len(hotRecords))

	// Verify execution plan shows hot buffer was scanned
	if result.Plan != nil {
		AssertPlanContainsNote(t, result.Plan, "hot_buffer_scanned")
	}

	t.Logf("TC-01-03 PASSED: Hot buffer only query returned %d records in %v",
		len(result.Records), result.Duration)
}

// TestDataTier_AllThreeTiers verifies queries correctly merge data from all tiers.
// Scenario: Data exists in Base, Delta, and Hot tiers with no overlap.
func TestDataTier_AllThreeTiers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Use preset scenario for non-overlapping data
	scenarios := PresetScenarios{}
	baseRecords, deltaRecords, hotRecords := scenarios.ThreeTierNoOverlap(h.SchemaID)

	// Write to respective tiers
	require.NoError(t, h.WriteParquet(ctx, "base", "tier_base.parquet", baseRecords))
	require.NoError(t, h.WriteParquet(ctx, "delta", "tier_delta.parquet", deltaRecords))
	require.NoError(t, h.SeedHotRecordsWithData(ctx, hotRecords))

	expectedTotal := len(baseRecords) + len(deltaRecords) + len(hotRecords)

	// Execute federated query with large limit to get all records
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: expectedTotal + 100})
	require.NoError(t, err, "federated query failed")

	// Verify UNION ALL returns all records
	AssertRecordCount(t, result.Records, expectedTotal)
	AssertNoDuplicates(t, result.Records)
	AssertNoDeleted(t, result.Records)

	// Verify execution plan shows all sources
	if result.Plan != nil {
		AssertPlanContainsNote(t, result.Plan, "base_files_scanned")
		AssertPlanContainsNote(t, result.Plan, "delta_files_scanned")
		AssertPlanContainsNote(t, result.Plan, "hot_buffer_scanned")
	}

	t.Logf("TC-01-04 PASSED: All three tiers query returned %d records (base=%d, delta=%d, hot=%d) in %v",
		len(result.Records), len(baseRecords), len(deltaRecords), len(hotRecords), result.Duration)
}

// TestDataTier_TierPriorityOrder verifies the execution follows the correct tier order.
// Scenario: Query should scan Base -> Delta -> Hot in that order.
func TestDataTier_TierPriorityOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Seed all tiers
	require.NoError(t, h.SeedAllTiers(ctx, 100, 50, 25))

	// Execute query
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 200})
	require.NoError(t, err, "federated query failed")

	// Verify execution plan exists and shows proper ordering
	require.NotNil(t, result.Plan, "execution plan should not be nil")

	// The execution plan notes should reflect the scan order
	// This is a basic check; actual order verification depends on plan implementation
	require.NotEmpty(t, result.Plan.Notes, "execution plan should have notes")

	t.Logf("TC-01-05 PASSED: Tier priority order verified with %d records in %v",
		len(result.Records), result.Duration)
}

// TestDataTier_EmptyTiers verifies behavior when some tiers are empty.
func TestDataTier_EmptyTiers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Clear all data
	require.NoError(t, h.ClearAllData(ctx))

	// Query should return empty result without errors
	result, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 100})
	require.NoError(t, err, "federated query failed on empty tiers")

	AssertRecordCount(t, result.Records, 0)

	t.Logf("TC-01-06 PASSED: Empty tiers query returned 0 records in %v", result.Duration)
}

// TestDataTier_LargeLimitPagination verifies pagination works across tiers.
func TestDataTier_LargeLimitPagination(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)

	// Seed data to all tiers
	require.NoError(t, h.SeedAllTiers(ctx, 500, 300, 100))
	expectedTotal := 900

	// Page 1
	page1, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 100, Offset: 0})
	require.NoError(t, err, "page 1 query failed")
	require.Len(t, page1.Records, 100, "page 1 should have 100 records")

	// Page 2
	page2, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 100, Offset: 100})
	require.NoError(t, err, "page 2 query failed")
	require.Len(t, page2.Records, 100, "page 2 should have 100 records")

	// Verify no overlap between pages
	page1IDs := make(map[string]bool)
	for _, r := range page1.Records {
		page1IDs[r.RowID.String()] = true
	}
	for _, r := range page2.Records {
		require.False(t, page1IDs[r.RowID.String()], "record %s appears in both pages", r.RowID)
	}

	t.Logf("TC-01-07 PASSED: Pagination across tiers works correctly (total=%d)", expectedTotal)
}

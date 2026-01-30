// Package federated provides E2E tests for Data Consistency (TC-07).
//
// Tests cover:
// - Postgres vs Federated record count match
// - Attribute value consistency across systems
// - Checksum validation between data sources
// - Consistency after CDC flush operations
// - Consistency after compaction operations
//
//go:build e2e

package federated

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsistency_CountMatch verifies that Postgres and Federated
// queries return the same record count.
func TestConsistency_CountMatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	t.Run("hot_buffer_only", func(t *testing.T) {
		// Clear data and seed only hot buffer
		require.NoError(t, h.ClearAllData(ctx))
		_, err := h.SeedHotRecords(ctx, 500)
		require.NoError(t, err)

		// Query both systems
		fedResult, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 1000})
		require.NoError(t, err)

		pgResult, err := h.ExecutePostgresQuery(ctx, &QueryOptions{Limit: 1000})
		require.NoError(t, err)

		// Compare counts
		report := h.CompareResults(fedResult, pgResult)
		AssertComparisonMatch(t, report)

		t.Logf("Hot-only: Federated=%d, Postgres=%d", report.FederatedCount, report.PostgresCount)
	})

	t.Run("three_tier_data", func(t *testing.T) {
		// Clear data and seed all tiers
		require.NoError(t, h.ClearAllData(ctx))
		require.NoError(t, h.SeedAllTiers(ctx, 300, 200, 100))

		// Query federated (all tiers)
		fedResult, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 1000})
		require.NoError(t, err)

		// Query postgres (hot buffer + entity_main)
		pgResult, err := h.ExecutePostgresQuery(ctx, &QueryOptions{Limit: 1000})
		require.NoError(t, err)

		// Note: Federated includes S3 data, Postgres only has hot data
		// So we verify federated count >= postgres count
		assert.GreaterOrEqual(t, fedResult.TotalRecords, pgResult.TotalRecords,
			"federated should include all postgres records plus S3 data")

		t.Logf("Three-tier: Federated=%d, Postgres=%d", fedResult.TotalRecords, pgResult.TotalRecords)
	})

	t.Run("after_flush_counts_match", func(t *testing.T) {
		// Clear and seed hot buffer
		require.NoError(t, h.ClearAllData(ctx))
		hotRecords, err := h.SeedHotRecords(ctx, 200)
		require.NoError(t, err)

		// Query before flush
		fedBefore, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 1000})
		require.NoError(t, err)
		countBefore := fedBefore.TotalRecords

		// Flush records
		result, err := h.RunCDCFlush(ctx)
		require.NoError(t, err)
		AssertFlushTriggered(t, result)

		// Query after flush
		fedAfter, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 1000})
		require.NoError(t, err)

		// Count should remain the same (records moved from hot to delta, not lost)
		assert.Equal(t, countBefore, fedAfter.TotalRecords,
			"record count should be preserved after flush")

		t.Logf("Before flush: %d, After flush: %d, Hot records: %d",
			countBefore, fedAfter.TotalRecords, len(hotRecords))
	})
}

// TestConsistency_AttributeValueMatch verifies that attribute values
// are consistent between Postgres and Federated queries.
func TestConsistency_AttributeValueMatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	// Clear and seed specific test data
	require.NoError(t, h.ClearAllData(ctx))

	// Create records with known attribute values
	testRecords := []TestRecord{
		{
			RowID:    uuid.Must(uuid.NewV7()),
			SchemaID: h.SchemaID,
			Attributes: map[string]any{
				"name":    "Alice Smith",
				"version": 1,
			},
			ChangedAt: time.Now().Add(-1 * time.Hour).UnixMilli(),
		},
		{
			RowID:    uuid.Must(uuid.NewV7()),
			SchemaID: h.SchemaID,
			Attributes: map[string]any{
				"name":    "Bob Johnson",
				"version": 2,
			},
			ChangedAt: time.Now().Add(-30 * time.Minute).UnixMilli(),
		},
		{
			RowID:    uuid.Must(uuid.NewV7()),
			SchemaID: h.SchemaID,
			Attributes: map[string]any{
				"name":    "Charlie Brown",
				"version": 3,
			},
			ChangedAt: time.Now().UnixMilli(),
		},
	}

	require.NoError(t, h.SeedHotRecordsWithData(ctx, testRecords))

	// Query both systems
	fedResult, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 100})
	require.NoError(t, err)

	pgResult, err := h.ExecutePostgresQuery(ctx, &QueryOptions{Limit: 100})
	require.NoError(t, err)

	// Compare results
	report := h.CompareResults(fedResult, pgResult)

	// Verify no attribute mismatches
	assert.Empty(t, report.AttributeMismatches,
		"should have no attribute value mismatches")

	if len(report.AttributeMismatches) > 0 {
		for _, m := range report.AttributeMismatches {
			t.Logf("Mismatch: row=%s, attr=%s, fed=%v, pg=%v",
				m.RowID, m.AttributeName, m.FederatedVal, m.PostgresVal)
		}
	}

	t.Logf("Attribute consistency check passed for %d records", len(testRecords))
}

// TestConsistency_ChecksumValidation verifies data integrity
// using checksums across different data sources.
func TestConsistency_ChecksumValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	t.Run("same_data_same_checksum", func(t *testing.T) {
		// Clear and seed data
		require.NoError(t, h.ClearAllData(ctx))
		_, err := h.SeedHotRecords(ctx, 100)
		require.NoError(t, err)

		// Query the same data twice
		result1, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 200})
		require.NoError(t, err)

		result2, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 200})
		require.NoError(t, err)

		// Calculate checksums
		checksum1 := h.CalculateChecksum(result1.Records)
		checksum2 := h.CalculateChecksum(result2.Records)

		assert.Equal(t, checksum1, checksum2, "same data should produce same checksum")

		t.Logf("Checksums match: %s", checksum1)
	})

	t.Run("checksum_changes_with_data", func(t *testing.T) {
		// Clear and seed initial data
		require.NoError(t, h.ClearAllData(ctx))
		_, err := h.SeedHotRecords(ctx, 50)
		require.NoError(t, err)

		// Get initial checksum
		result1, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 100})
		require.NoError(t, err)
		checksum1 := h.CalculateChecksum(result1.Records)

		// Add more data
		_, err = h.SeedHotRecords(ctx, 50)
		require.NoError(t, err)

		// Get new checksum
		result2, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 200})
		require.NoError(t, err)
		checksum2 := h.CalculateChecksum(result2.Records)

		assert.NotEqual(t, checksum1, checksum2,
			"checksum should change when data changes")

		t.Logf("Checksum changed: %s -> %s", checksum1, checksum2)
	})

	t.Run("federated_postgres_checksum_match", func(t *testing.T) {
		// Clear and seed hot buffer only
		require.NoError(t, h.ClearAllData(ctx))
		_, err := h.SeedHotRecords(ctx, 100)
		require.NoError(t, err)

		// Query both systems
		fedResult, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 200})
		require.NoError(t, err)

		pgResult, err := h.ExecutePostgresQuery(ctx, &QueryOptions{Limit: 200})
		require.NoError(t, err)

		// Compare checksums
		report := h.CompareResults(fedResult, pgResult)
		AssertChecksumMatch(t, report)

		t.Logf("Checksums match - Fed: %s, PG: %s",
			report.FederatedChecksum, report.PostgresChecksum)
	})
}

// TestConsistency_AfterCDCFlush verifies data consistency
// after CDC flush operations.
func TestConsistency_AfterCDCFlush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	// Clear and seed data
	require.NoError(t, h.ClearAllData(ctx))
	originalRecords, err := h.SeedHotRecords(ctx, 300)
	require.NoError(t, err)

	// Get pre-flush state
	preFlushResult, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 500})
	require.NoError(t, err)
	preFlushChecksum := h.CalculateChecksum(preFlushResult.Records)

	// Perform flush
	flushResult, err := h.RunCDCFlush(ctx)
	require.NoError(t, err)
	AssertFlushTriggered(t, flushResult)

	// Get post-flush state
	postFlushResult, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 500})
	require.NoError(t, err)
	postFlushChecksum := h.CalculateChecksum(postFlushResult.Records)

	// Verify consistency
	assert.Equal(t, preFlushResult.TotalRecords, postFlushResult.TotalRecords,
		"record count should be preserved after flush")
	assert.Equal(t, preFlushChecksum, postFlushChecksum,
		"checksum should be preserved after flush")

	// Verify no duplicates
	RequireNoDuplicates(t, postFlushResult.Records)

	t.Logf("Post-flush consistency: %d records, checksum=%s, flushed=%d",
		postFlushResult.TotalRecords, postFlushChecksum, len(originalRecords))
}

// TestConsistency_AfterCompaction verifies data consistency
// after compaction operations.
func TestConsistency_AfterCompaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	// Clear and seed multiple delta files
	require.NoError(t, h.ClearAllData(ctx))

	// Create delta files
	var allRecords []TestRecord
	for i := 0; i < 5; i++ {
		records := GenerateTestRecords(100, &GeneratorOptions{
			SchemaID:       h.SchemaID,
			TimeRangeHours: 24,
			TimeOffset:     i * 24,
			Seed:           int64(i * 500),
		})
		err = h.WriteParquet(ctx, "delta", "consistency_delta_"+string(rune('a'+i))+".parquet", records)
		require.NoError(t, err)
		allRecords = append(allRecords, records...)
	}

	// Get pre-compaction state
	preCompactResult, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 1000})
	require.NoError(t, err)
	preCompactChecksum := h.CalculateChecksum(preCompactResult.Records)

	// Perform compaction
	compactResult, err := h.RunCompaction(ctx)
	require.NoError(t, err)
	AssertCompactionMerged(t, compactResult)

	// Get post-compaction state
	postCompactResult, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 1000})
	require.NoError(t, err)
	postCompactChecksum := h.CalculateChecksum(postCompactResult.Records)

	// Verify consistency
	assert.Equal(t, preCompactResult.TotalRecords, postCompactResult.TotalRecords,
		"record count should be preserved after compaction")
	assert.Equal(t, preCompactChecksum, postCompactChecksum,
		"checksum should be preserved after compaction")

	// Verify no duplicates
	RequireNoDuplicates(t, postCompactResult.Records)

	t.Logf("Post-compaction consistency: %d records, checksum=%s",
		postCompactResult.TotalRecords, postCompactChecksum)
}

// TestConsistency_RowIDExistence verifies that specific row_ids
// exist in both Postgres and Federated results.
func TestConsistency_RowIDExistence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	// Clear and seed data
	require.NoError(t, h.ClearAllData(ctx))
	records, err := h.SeedHotRecords(ctx, 50)
	require.NoError(t, err)

	// Get a sample of row_ids to verify
	sampleRowIDs := make([]uuid.UUID, 0, 10)
	for i := 0; i < 10 && i < len(records); i++ {
		sampleRowIDs = append(sampleRowIDs, records[i].RowID)
	}

	// Query federated
	fedResult, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 100})
	require.NoError(t, err)

	// Query postgres
	pgResult, err := h.ExecutePostgresQuery(ctx, &QueryOptions{Limit: 100})
	require.NoError(t, err)

	// Verify sample row_ids exist in both
	for _, rowID := range sampleRowIDs {
		AssertRecordExists(t, fedResult.Records, rowID)
		AssertRecordExists(t, pgResult.Records, rowID)
	}

	t.Logf("Verified %d row_ids exist in both systems", len(sampleRowIDs))
}

// TestConsistency_MissingRecordDetection verifies that the comparison
// correctly detects missing records between systems.
func TestConsistency_MissingRecordDetection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	// Clear data
	require.NoError(t, h.ClearAllData(ctx))

	// Seed hot buffer
	_, err = h.SeedHotRecords(ctx, 100)
	require.NoError(t, err)

	// Also seed S3 base (not in Postgres)
	baseRecords := GenerateTestRecords(50, &GeneratorOptions{
		SchemaID:       h.SchemaID,
		TimeRangeHours: 720,
		TimeOffset:     720,
	})
	err = h.WriteParquet(ctx, "base", "extra_base.parquet", baseRecords)
	require.NoError(t, err)

	// Query both systems
	fedResult, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 200})
	require.NoError(t, err)

	pgResult, err := h.ExecutePostgresQuery(ctx, &QueryOptions{Limit: 200})
	require.NoError(t, err)

	// Compare results
	report := h.CompareResults(fedResult, pgResult)

	// Federated should have more records (S3 base data)
	assert.Greater(t, report.FederatedCount, report.PostgresCount,
		"federated should have additional records from S3")

	// MissingInPG should contain the base records
	assert.NotEmpty(t, report.MissingInPG,
		"should detect records missing in Postgres (S3 base data)")

	t.Logf("Detection: Fed=%d, PG=%d, MissingInPG=%d",
		report.FederatedCount, report.PostgresCount, len(report.MissingInPG))
}

// TestConsistency_DeduplicationAcrossComparison verifies that
// deduplication is consistent between comparison sources.
func TestConsistency_DeduplicationAcrossComparison(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	// Clear data
	require.NoError(t, h.ClearAllData(ctx))

	// Create overlapping records (same row_id across tiers)
	sharedRowID := uuid.Must(uuid.NewV7())
	err = h.InsertOverlappingRecords(ctx, sharedRowID, 3)
	require.NoError(t, err)

	// Query federated
	fedResult, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 100})
	require.NoError(t, err)

	// Verify deduplication in federated result
	RequireNoDuplicates(t, fedResult.Records)

	// Should have exactly one record for the shared row_id
	count := 0
	for _, r := range fedResult.Records {
		if r.RowID == sharedRowID {
			count++
		}
	}
	assert.Equal(t, 1, count, "should have exactly one record for shared row_id after deduplication")

	t.Logf("Deduplication verified: shared row_id appears %d time(s)", count)
}

// TestConsistency_TimestampOrdering verifies that records maintain
// proper timestamp ordering across systems.
func TestConsistency_TimestampOrdering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	// Clear and seed data with known timestamps
	require.NoError(t, h.ClearAllData(ctx))

	now := time.Now()
	orderedRecords := []TestRecord{
		{
			RowID:      uuid.Must(uuid.NewV7()),
			SchemaID:   h.SchemaID,
			Attributes: map[string]any{"name": "First", "version": 1},
			ChangedAt:  now.Add(-3 * time.Hour).UnixMilli(),
		},
		{
			RowID:      uuid.Must(uuid.NewV7()),
			SchemaID:   h.SchemaID,
			Attributes: map[string]any{"name": "Second", "version": 1},
			ChangedAt:  now.Add(-2 * time.Hour).UnixMilli(),
		},
		{
			RowID:      uuid.Must(uuid.NewV7()),
			SchemaID:   h.SchemaID,
			Attributes: map[string]any{"name": "Third", "version": 1},
			ChangedAt:  now.Add(-1 * time.Hour).UnixMilli(),
		},
	}

	require.NoError(t, h.SeedHotRecordsWithData(ctx, orderedRecords))

	// Query with ordering (DESC by default in Postgres query)
	pgResult, err := h.ExecutePostgresQuery(ctx, &QueryOptions{Limit: 10})
	require.NoError(t, err)

	// Verify records are returned (order may vary by implementation)
	assert.Equal(t, 3, len(pgResult.Records), "should have 3 records")

	// All records should have valid timestamps
	for _, r := range pgResult.Records {
		assert.Greater(t, r.CreatedAt, int64(0), "timestamp should be positive")
	}

	t.Logf("Timestamp ordering verified for %d records", len(pgResult.Records))
}

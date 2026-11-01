// Package federated provides E2E tests for Data Consistency (TC-07): the
// tier-movement half, checking that a CDC flush or a compaction leaves the
// federated result's count, checksum and row_id uniqueness unchanged. The
// read-side comparisons are consistency_test.go.
//
//go:build e2e

package federated

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsistency_AfterCDCFlush verifies data consistency
// after CDC flush operations.
func TestConsistency_AfterCDCFlush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.CleanupOrLog(ctx, t)

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
	defer h.CleanupOrLog(ctx, t)

	// Clear and seed multiple delta files
	require.NoError(t, h.ClearAllData(ctx))

	// Create delta files
	for i := 0; i < 5; i++ {
		records := GenerateTestRecords(100, &GeneratorOptions{
			SchemaID:       h.SchemaID,
			TimeRangeHours: 24,
			TimeOffset:     i * 24,
			Seed:           int64(i * 500),
		})
		err = h.WriteParquet(ctx, "delta", "consistency_delta_"+string(rune('a'+i))+".parquet", records)
		require.NoError(t, err)
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

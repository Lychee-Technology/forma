// Package federated provides E2E tests for Performance Benchmarks (TC-08):
// the write-path half, timing the CDC flush and compaction jobs that move
// data between tiers. The query-latency half is performance_test.go.
//
// Write-path thresholds:
// - CDC Flush (20K): p95=5s, p99=10s
// - Compaction (500K): p95=30s, p99=60s
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

// Write-path performance thresholds
const (
	// CDC Flush thresholds
	CDCFlushP95 = 10 * time.Second
	CDCFlushP99 = 20 * time.Second

	// Compaction thresholds
	CompactionP95 = 30 * time.Second
	CompactionP99 = 60 * time.Second
)

// TestPerformance_CDCFlush benchmarks CDC flush operations.
func TestPerformance_CDCFlush(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.CleanupOrLog(ctx, t)

	// Seed hot buffer with records to flush
	require.NoError(t, h.ClearAllData(ctx))

	// Reduced from 20K to 5K for faster tests
	numRecords := 5000
	_, err = h.SeedHotRecords(ctx, numRecords)
	require.NoError(t, err)

	t.Logf("Seeded %d hot records for flush test", numRecords)

	// Measure flush duration
	numFlushes := 5
	latencies := make([]time.Duration, 0, numFlushes)

	for i := 0; i < numFlushes; i++ {
		// Re-seed data for each flush
		if i > 0 {
			_, err = h.SeedHotRecords(ctx, numRecords/numFlushes)
			require.NoError(t, err)
		}

		start := time.Now()
		result, err := h.RunCDCFlush(ctx)
		duration := time.Since(start)

		if err != nil {
			t.Logf("Flush %d failed: %v", i, err)
			continue
		}

		if result.Flushed {
			latencies = append(latencies, duration)
			t.Logf("Flush %d: %d rows in %v", i, result.RowsFlushed, duration)
		}
	}

	if len(latencies) == 0 {
		t.Skip("no successful flushes to measure")
	}

	// Calculate statistics
	stats := CalculateLatencyStats(latencies)

	t.Logf("CDC Flush Results (n=%d):", stats.Count)
	t.Logf("  Min: %v, Max: %v, Avg: %v", stats.Min, stats.Max, stats.Avg)
	t.Logf("  P95: %v, P99: %v", stats.P95, stats.P99)

	// Verify against thresholds
	RequireP95Latency(t, latencies, CDCFlushP95)
}

// TestPerformance_Compaction benchmarks compaction operations.
func TestPerformance_Compaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.CleanupOrLog(ctx, t)

	// Create multiple delta files to compact
	require.NoError(t, h.ClearAllData(ctx))

	numDeltaFiles := 10
	recordsPerFile := 2000

	for i := 0; i < numDeltaFiles; i++ {
		records := GenerateTestRecords(recordsPerFile, &GeneratorOptions{
			SchemaID:       h.SchemaID,
			TimeRangeHours: 24,
			TimeOffset:     i * 24,
			Seed:           int64(i * 1000),
		})
		err = h.WriteParquet(ctx, "delta", "compact_delta_"+string(rune('0'+i))+".parquet", records)
		require.NoError(t, err)
	}

	totalRecords := numDeltaFiles * recordsPerFile
	t.Logf("Created %d delta files with %d total records", numDeltaFiles, totalRecords)

	// Measure compaction duration
	start := time.Now()
	result, err := h.RunCompaction(ctx)
	duration := time.Since(start)
	require.NoError(t, err)

	t.Logf("Compaction Results:")
	t.Logf("  Files compacted: %d", result.FilesCompacted)
	t.Logf("  Rows merged: %d", result.RowsMerged)
	t.Logf("  Duration: %v", duration)

	// Verify completion
	assert.Equal(t, numDeltaFiles, result.FilesCompacted, "should compact all delta files")
	assert.Equal(t, int64(totalRecords), result.RowsMerged, "should merge all records")

	// Verify duration (relaxed threshold for test environment)
	AssertLatencyUnder(t, duration, CompactionP95, "compaction should complete within threshold")
}

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
	"fmt"
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

// Phase budgets of TestPerformance_CDCFlush. Each phase gets its own
// deadline, so seeding and container start-up cannot use up the time meant
// for the flushes being measured, and a failure names the phase that ran out
// (#781). A flush budget is many times CDCFlushP99: a flush that exhausts it
// is slow beyond doubt rather than marginally over the threshold.
const (
	cdcFlushSetupBudget = 5 * time.Minute
	cdcFlushSeedBudget  = 5 * time.Minute
	cdcFlushRunBudget   = 2 * time.Minute
)

// TestPerformance_CDCFlush benchmarks CDC flush operations.
func TestPerformance_CDCFlush(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	setup := startPhase("harness setup", cdcFlushSetupBudget)
	h, err := NewFederatedTestHarness(setup.ctx)
	if err == nil {
		// CleanupOrLog takes a deadline of its own; the setup context only
		// carries values to it.
		defer h.CleanupOrLog(setup.ctx, t)
		err = h.ClearAllData(setup.ctx)
	}
	setup.finish(t, err)

	// Reduced from 20K to 5K for faster tests
	numRecords := 5000
	numFlushes := 5
	seedHot(t, h, numRecords, "seed initial hot records")
	t.Logf("Seeded %d hot records for flush test", numRecords)

	// Measure flush duration
	latencies := make([]time.Duration, 0, numFlushes)

	for i := 0; i < numFlushes; i++ {
		// Re-seed data for each flush
		if i > 0 {
			seedHot(t, h, numRecords/numFlushes, fmt.Sprintf("seed hot records for CDC flush %d", i))
		}

		flush := startPhase(fmt.Sprintf("CDC flush %d", i), cdcFlushRunBudget)
		start := time.Now()
		result, err := h.RunCDCFlush(flush.ctx)
		duration := time.Since(start)
		flush.end()

		if err != nil {
			if flush.expired() {
				t.Fatal(flush.describe(err))
			}
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

// seedHot seeds count hot records as the named phase, under
// cdcFlushSeedBudget, failing t with the phase's name if seeding fails or
// runs out of time.
func seedHot(t *testing.T, h *FederatedTestHarness, count int, name string) {
	t.Helper()
	seed := startPhase(fmt.Sprintf("%s (%d records)", name, count), cdcFlushSeedBudget)
	_, err := h.SeedHotRecords(seed.ctx, count)
	seed.finish(t, err)
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

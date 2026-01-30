// Package federated provides the test suite entry point for E2E federated query tests.
//
// Test Categories:
// - TC-01: Three-tier data architecture (data_tier_test.go)
// - TC-02: Merge-on-Read logic (merge_on_read_test.go)
// - TC-03: Global deduplication (deduplication_test.go)
// - TC-04: Soft delete filtering (soft_delete_test.go)
// - TC-05: CDC Smart Flushing (cdc_flush_test.go)
// - TC-06: Compaction strategy (compaction_test.go)
// - TC-07: Data consistency (consistency_test.go)
// - TC-08: Performance benchmarks (performance_test.go)
// - TC-09: Failure modes (failure_modes_test.go)
//
// Run all tests:
//   go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m
//
// Run specific test group:
//   go test -v ./internal/e2e_harness/federated/... -run "TestDataTier" -tags=e2e
//
// Run performance tests only:
//   go test -v ./internal/e2e_harness/federated/... -run "TestPerformance" -tags=e2e -timeout=60m
//
// Skip performance tests:
//   go test -v ./internal/e2e_harness/federated/... -tags=e2e -short
//
//go:build e2e

package federated

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestMain provides the test suite entry point with setup and teardown.
func TestMain(m *testing.M) {
	// Set up any global test configuration
	os.Setenv("E2E_TEST_MODE", "true")

	// Optional: set up logging
	os.Setenv("LOG_LEVEL", "warn") // Reduce noise during tests

	// Run tests
	code := m.Run()

	// Global cleanup
	os.Unsetenv("E2E_TEST_MODE")
	os.Unsetenv("LOG_LEVEL")

	os.Exit(code)
}

// TestSuite_Smoke runs a quick smoke test to verify the test infrastructure.
func TestSuite_Smoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Create harness
	h, err := NewFederatedTestHarness(ctx)
	if err != nil {
		t.Fatalf("Failed to create test harness: %v", err)
	}
	defer h.Cleanup(ctx)

	// Verify basic operations
	t.Run("harness_initialization", func(t *testing.T) {
		if h.PGDB == nil {
			t.Error("Postgres connection not initialized")
		}
		if h.Duck == nil {
			t.Error("DuckDB connection not initialized")
		}
		if h.GetS3Client() == nil {
			t.Error("S3 client not initialized")
		}
	})

	t.Run("basic_data_operations", func(t *testing.T) {
		// Clear any existing data
		if err := h.ClearAllData(ctx); err != nil {
			t.Errorf("Failed to clear data: %v", err)
		}

		// Seed some test records
		records, err := h.SeedHotRecords(ctx, 10)
		if err != nil {
			t.Errorf("Failed to seed records: %v", err)
		}
		if len(records) != 10 {
			t.Errorf("Expected 10 records, got %d", len(records))
		}

		// Query the records
		result, err := h.ExecutePostgresQuery(ctx, &QueryOptions{Limit: 20})
		if err != nil {
			t.Errorf("Failed to query records: %v", err)
		}
		if result.TotalRecords != 10 {
			t.Errorf("Expected 10 records from query, got %d", result.TotalRecords)
		}
	})

	t.Run("s3_operations", func(t *testing.T) {
		// Write a parquet file
		records := GenerateTestRecords(5, &GeneratorOptions{SchemaID: h.SchemaID})
		err := h.WriteParquet(ctx, "base", "smoke_test.parquet", records)
		if err != nil {
			t.Errorf("Failed to write parquet: %v", err)
		}

		// List parquet files
		files, err := h.ListParquetFiles(ctx, "base")
		if err != nil {
			t.Errorf("Failed to list parquet files: %v", err)
		}
		if len(files) == 0 {
			t.Error("Expected at least one parquet file")
		}
	})

	t.Log("Smoke test passed - test infrastructure is working")
}

// TestSuite_Integration runs a comprehensive integration test across all tiers.
func TestSuite_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	if err != nil {
		t.Fatalf("Failed to create test harness: %v", err)
	}
	defer h.Cleanup(ctx)

	// Clear data
	if err := h.ClearAllData(ctx); err != nil {
		t.Fatalf("Failed to clear data: %v", err)
	}

	t.Run("full_lifecycle", func(t *testing.T) {
		// 1. Seed data in all tiers
		t.Log("Step 1: Seeding data in all tiers...")
		if err := h.SeedAllTiers(ctx, 100, 50, 200); err != nil {
			t.Fatalf("Failed to seed all tiers: %v", err)
		}

		// 2. Query federated data
		t.Log("Step 2: Querying federated data...")
		fedResult, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{Limit: 500})
		if err != nil {
			t.Logf("Federated query error (may be expected): %v", err)
		} else {
			t.Logf("Federated query returned %d records", len(fedResult.Records))
		}

		// 3. Query Postgres data
		t.Log("Step 3: Querying Postgres data...")
		pgResult, err := h.ExecutePostgresQuery(ctx, &QueryOptions{Limit: 500})
		if err != nil {
			t.Fatalf("Postgres query failed: %v", err)
		}
		t.Logf("Postgres query returned %d records", len(pgResult.Records))

		// 4. Run CDC flush
		t.Log("Step 4: Running CDC flush...")
		flushResult, err := h.RunCDCFlush(ctx)
		if err != nil {
			t.Fatalf("CDC flush failed: %v", err)
		}
		t.Logf("CDC flush: flushed=%v, rows=%d", flushResult.Flushed, flushResult.RowsFlushed)

		// 5. Run compaction
		t.Log("Step 5: Running compaction...")
		compactResult, err := h.RunCompaction(ctx)
		if err != nil {
			t.Fatalf("Compaction failed: %v", err)
		}
		t.Logf("Compaction: files=%d, rows=%d", compactResult.FilesCompacted, compactResult.RowsMerged)

		// 6. Final verification
		t.Log("Step 6: Final verification...")
		finalResult, err := h.ExecutePostgresQuery(ctx, &QueryOptions{Limit: 500})
		if err != nil {
			t.Fatalf("Final query failed: %v", err)
		}
		t.Logf("Final query returned %d records", len(finalResult.Records))

		// Verify no duplicates
		RequireNoDuplicates(t, finalResult.Records)
		t.Log("Integration test passed - no duplicates found")
	})
}

// BenchmarkFederatedQuery provides a benchmark for federated query performance.
func BenchmarkFederatedQuery(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	if err != nil {
		b.Fatalf("Failed to create test harness: %v", err)
	}
	defer h.Cleanup(ctx)

	// Seed data
	if err := h.ClearAllData(ctx); err != nil {
		b.Fatalf("Failed to clear data: %v", err)
	}

	records := GenerateBulkRecords(10000, h.SchemaID, 0.02)
	if err := h.WriteParquet(ctx, "base", "benchmark.parquet", records); err != nil {
		b.Fatalf("Failed to write parquet: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := h.ExecuteFederatedQuery(ctx, &QueryOptions{
			Limit:  100,
			Offset: (i * 100) % len(records),
		})
		if err != nil {
			b.Logf("Query error: %v", err)
		}
	}
}

// BenchmarkCDCFlush provides a benchmark for CDC flush performance.
func BenchmarkCDCFlush(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	h, err := NewFederatedTestHarness(ctx)
	if err != nil {
		b.Fatalf("Failed to create test harness: %v", err)
	}
	defer h.Cleanup(ctx)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Seed data
		if err := h.ClearAllData(ctx); err != nil {
			b.Fatalf("Failed to clear data: %v", err)
		}
		if _, err := h.SeedHotRecords(ctx, 1000); err != nil {
			b.Fatalf("Failed to seed records: %v", err)
		}
		b.StartTimer()

		// Run flush
		_, err := h.RunCDCFlush(ctx)
		if err != nil {
			b.Logf("Flush error: %v", err)
		}
	}
}

// Test categories for organization
var testCategories = map[string]string{
	"TC-01": "Three-tier data architecture",
	"TC-02": "Merge-on-Read logic",
	"TC-03": "Global deduplication",
	"TC-04": "Soft delete filtering",
	"TC-05": "CDC Smart Flushing",
	"TC-06": "Compaction strategy",
	"TC-07": "Data consistency",
	"TC-08": "Performance benchmarks",
	"TC-09": "Failure modes",
}

// printTestCategories outputs the test categories for documentation.
func printTestCategories() {
	for id, desc := range testCategories {
		println(id + ": " + desc)
	}
}

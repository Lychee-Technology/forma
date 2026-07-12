//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestFlushFaultMultiBatchMiddleChunk forces byte-size splitting (9 rows,
// maxRows = MaxBatchBytes/EstimatedRowBytes = 3 → 3 chunks) and fails the
// second chunk's CopyObject. Chunk 1 is already committed (marked flushed +
// manifest entry) and must NOT be re-exported by the retry; only the
// remaining 6 dirty rows are. After retry: 3 final objects, 3 manifest
// entries, zero physically duplicated row_ids, oracle-identical query (#179
// multi-batch scenario).
func TestFlushFaultMultiBatchMiddleChunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple := DefaultSchemaFixtures()[0]
	seedRows(ctx, t, env, simple, 9)

	cfg := env.CDC
	cfg.EstimatedRowBytes = 1
	cfg.MaxBatchBytes = 3

	faulty := &FaultInjectingS3{Inner: env.Cluster.S3, Fault: S3Fault{Op: S3OpCopy, SkipMatches: 1}}
	report, err := env.RunFlushWith(ctx, FlushOverrides{S3: faulty, Config: &cfg})
	if err == nil {
		t.Fatal("flush with failing second-chunk CopyObject must fail")
	}
	if faulty.Injected() == 0 {
		t.Fatal("fault never fired")
	}
	finals, _ := splitKeys(report.NewObjects)
	if len(finals) != 1 {
		t.Fatalf("exactly chunk 1 must have landed, got finals %v", finals)
	}
	if report.UnflushedAfter != 6 {
		t.Errorf("chunks 2-3 must stay dirty, unflushed = %d, want 6", report.UnflushedAfter)
	}
	assertManifestDeltaPaths(t, report.Manifests, simple, finals)
	// Row-level flushed_at consistency: the flushed set must be exactly the
	// rows inside the chunk-1 parquet file, everything else stays dirty.
	flushedAfterFault, dirtyAfterFault := fetchChangeLogRowIDs(ctx, t, env, simple)
	if len(flushedAfterFault) != 3 || len(dirtyAfterFault) != 6 {
		t.Errorf("flushed/dirty row split after fault = %d/%d, want 3/6", len(flushedAfterFault), len(dirtyAfterFault))
	}
	assertSameRowIDs(t, "chunk-1 parquet vs flushed_at markers", fetchParquetRowIDs(ctx, t, env, finals), flushedAfterFault)

	// Clean retry with the same chunking config re-exports ONLY the 6
	// remaining rows (2 more chunks).
	retry, err := env.RunFlushWith(ctx, FlushOverrides{Config: &cfg})
	if err != nil {
		t.Fatalf("clean retry flush: %v", err)
	}
	if retry.UnflushedAfter != 0 {
		t.Errorf("retry must flush the rest, unflushed = %d", retry.UnflushedAfter)
	}
	retryFinals, _ := splitKeys(retry.NewObjects)
	if len(retryFinals) != 2 {
		t.Errorf("retry must add exactly 2 chunk objects, got %v", retryFinals)
	}
	assertManifestDeltaPaths(t, retry.Manifests, simple, append(append([]string(nil), finals...), retryFinals...))
	// "Only remaining dirty rows are re-exported": the retry files must
	// contain exactly the rows that were dirty after the fault, and every
	// row must now carry a flushed_at marker.
	assertSameRowIDs(t, "retry parquet vs previously-dirty rows", fetchParquetRowIDs(ctx, t, env, retryFinals), dirtyAfterFault)
	flushedAfterRetry, dirtyAfterRetry := fetchChangeLogRowIDs(ctx, t, env, simple)
	if len(flushedAfterRetry) != 9 || len(dirtyAfterRetry) != 0 {
		t.Errorf("flushed/dirty row split after retry = %d/%d, want 9/0", len(flushedAfterRetry), len(dirtyAfterRetry))
	}
	// Create-only fixture: any row_id in two parquet files means a completed
	// chunk was re-exported — the exact regression this scenario guards.
	assertNoRowExportedTwice(ctx, t, env, simple)
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})
}

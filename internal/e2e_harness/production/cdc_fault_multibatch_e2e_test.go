//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestFlushFaultMultiBatchMiddleChunk forces byte-size splitting (9 rows,
// maxRows = MaxBatchBytes/EstimatedRowBytes = 3 → 3 chunks) and fails the
// staging of the second chunk's delta object (its parquet PutObject). A V2
// flush stages every object of its generation before the transaction that
// publishes it and marks its rows, so the failure publishes nothing and marks
// nothing: all 9 rows stay dirty, and chunk 1's staged object stays an
// unlisted orphan for garbage collection (#891). The retry exports all 9 rows
// into 3 fresh objects, the generation lists exactly those, no listed row_id
// is duplicated, and the query is oracle-identical (#179 multi-batch
// scenario). V2 stages no temporary object, so no tmp residue appears.
func TestFlushFaultMultiBatchMiddleChunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple := DefaultSchemaFixtures()[0]
	seedRows(ctx, t, env, simple, 9)

	cfg := env.CDC
	cfg.EstimatedRowBytes = 1
	cfg.MaxBatchBytes = 3

	faulty := &FaultInjectingS3{Inner: env.Cluster.S3, Fault: S3Fault{Op: S3OpPut, KeyContains: ".parquet", SkipMatches: 1}}
	report, err := env.RunFlushWith(ctx, FlushOverrides{S3: faulty, Config: &cfg})
	if err == nil {
		t.Fatal("flush with failing second-chunk parquet PutObject must fail")
	}
	if faulty.Injected() == 0 {
		t.Fatal("fault never fired")
	}
	orphans, tmps := splitKeys(report.NewObjects)
	if len(orphans) != 1 {
		t.Fatalf("exactly chunk 1 must have been staged, got %v", orphans)
	}
	if len(tmps) != 0 {
		t.Errorf("V2 stages no temporary object, got %v", tmps)
	}
	if report.UnflushedAfter != 9 {
		t.Errorf("a failed generation marks nothing, unflushed = %d, want 9", report.UnflushedAfter)
	}
	assertManifestDeltaPaths(t, report.Manifests, simple, nil)
	if flushed, dirty := fetchChangeLogRowIDs(ctx, t, env, simple); len(flushed) != 0 || len(dirty) != 9 {
		t.Errorf("flushed/dirty row split after fault = %d/%d, want 0/9", len(flushed), len(dirty))
	}

	// Clean retry with the same chunking config exports the 9 rows in 3
	// fresh chunks and publishes exactly those.
	retry, err := env.RunFlushWith(ctx, FlushOverrides{Config: &cfg})
	if err != nil {
		t.Fatalf("clean retry flush: %v", err)
	}
	if retry.UnflushedAfter != 0 {
		t.Errorf("retry must flush every row, unflushed = %d", retry.UnflushedAfter)
	}
	retryFinals, _ := splitKeys(retry.NewObjects)
	if len(retryFinals) != 3 {
		t.Errorf("retry must stage exactly 3 chunk objects, got %v", retryFinals)
	}
	assertManifestDeltaPaths(t, retry.Manifests, simple, retryFinals)
	flushedAfterRetry, dirtyAfterRetry := fetchChangeLogRowIDs(ctx, t, env, simple)
	if len(flushedAfterRetry) != 9 || len(dirtyAfterRetry) != 0 {
		t.Errorf("flushed/dirty row split after retry = %d/%d, want 9/0", len(flushedAfterRetry), len(dirtyAfterRetry))
	}
	assertSameRowIDs(t, "retry parquet vs flushed_at markers", fetchParquetRowIDs(ctx, t, env, retryFinals), flushedAfterRetry)
	// Create-only fixture: a row_id in two listed objects means a row was
	// published twice — the regression this scenario guards.
	assertNoRowExportedTwice(ctx, t, env, simple)
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})
}

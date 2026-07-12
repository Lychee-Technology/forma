//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestFlushFaultDirtySelection breaks step 1 (dirty ID selection) by
// pointing the flush config at an unreachable Postgres. The very first
// pipeline step fails, so there must be zero side effects anywhere, and a
// clean retry must converge (#179).
func TestFlushFaultDirtySelection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple := DefaultSchemaFixtures()[0]
	seedRows(ctx, t, env, simple, 3)
	// Baseline the seeded rows on the hot tier. PreferHot keeps the read on
	// Postgres; a default federated read here would hit read_parquet on a
	// schema with zero parquet files (nothing flushed yet) and fail with an
	// IO "no files match" error — see boundary_roundtrip_e2e_test.go for the
	// same hot-only-before-flush pattern.
	env.AssertQueryMatches(ctx, Query{Schema: simple, PreferHot: true, Limit: 100})

	cfg := env.CDC
	cfg.PGHost, cfg.PGPort = "127.0.0.1", 1
	report, err := env.RunFlushWith(ctx, FlushOverrides{Config: &cfg})
	if err == nil {
		t.Fatal("flush with unreachable postgres must fail")
	}
	assertUntouched(t, report, simple, 3)

	assertRetryConverges(ctx, t, env, simple, 3)
}

// TestFlushFaultDuckExport breaks step 2 (DuckDB COPY to the tmp S3 object)
// by pointing the DuckDB httpfs endpoint at an unreachable address. The
// injected-client steps (3+) are never reached; nothing lands in the real
// bucket and change_log stays dirty.
func TestFlushFaultDuckExport(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple := DefaultSchemaFixtures()[0]
	seedRows(ctx, t, env, simple, 3)

	cfg := env.CDC
	cfg.S3Endpoint = "127.0.0.1:1"
	report, err := env.RunFlushWith(ctx, FlushOverrides{Config: &cfg})
	if err == nil {
		t.Fatal("flush with unreachable duckdb s3 endpoint must fail")
	}
	assertUntouched(t, report, simple, 3)

	assertRetryConverges(ctx, t, env, simple, 3)
}

// assertRetryConverges runs a clean flush and asserts full convergence: all
// rows flushed into exactly one final delta object, the manifest tracking
// exactly that object, and the federated result matching the fault-free
// oracle baseline.
func assertRetryConverges(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, wantRows int64) {
	t.Helper()
	retry, err := env.RunFlushWith(ctx, FlushOverrides{})
	if err != nil {
		t.Fatalf("clean retry flush: %v", err)
	}
	if retry.UnflushedBefore != wantRows || retry.UnflushedAfter != 0 {
		t.Errorf("retry unflushed %d -> %d, want %d -> 0", retry.UnflushedBefore, retry.UnflushedAfter, wantRows)
	}
	finals, _ := splitKeys(retry.NewObjects)
	if len(finals) != 1 {
		t.Errorf("retry created %d final objects %v, want 1", len(finals), finals)
	}
	assertManifestDeltaPaths(t, retry.Manifests, schema, finals)
	env.AssertQueryMatches(ctx, Query{Schema: schema, Limit: 100})
}

// TestFlushFaultCopyObject breaks step 3 (S3 CopyObject tmp->final). The
// export succeeded, so the tmp object exists, but no final object, no
// flushed_at update, and no manifest entry may appear. Today the failed
// attempt's tmp object is orphaned permanently (retry uses fresh UUIDs and
// CopyTmpToFinal only deletes its own tmp key) — cleanup is tracked in #226;
// until it lands this test pins the current behavior. Correctness holds
// regardless: the production glob's `*` does not cross `/` and skips _tmp/
// (production/query.go:52-56), and orphans never enter the manifest.
func TestFlushFaultCopyObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple := DefaultSchemaFixtures()[0]
	seedRows(ctx, t, env, simple, 3)

	faulty := &FaultInjectingS3{Inner: env.Cluster.S3, Fault: S3Fault{Op: S3OpCopy}}
	report, err := env.RunFlushWith(ctx, FlushOverrides{S3: faulty})
	if err == nil {
		t.Fatal("flush with failing CopyObject must fail")
	}
	if faulty.Injected() == 0 {
		t.Fatal("fault never fired")
	}
	finals, tmps := splitKeys(report.NewObjects)
	if len(finals) != 0 {
		t.Errorf("no final object may exist after CopyObject failure, got %v", finals)
	}
	if len(tmps) != 1 {
		t.Errorf("expected exactly the orphaned tmp object, got %v", tmps)
	}
	if report.UnflushedAfter != 3 {
		t.Errorf("rows must stay dirty, unflushed = %d, want 3", report.UnflushedAfter)
	}
	assertManifestDeltaPaths(t, report.Manifests, simple, nil)

	// Clean retry converges; the orphaned tmp object stays behind (until
	// #226 adds cleanup) but is invisible to both the manifest and the
	// federated glob.
	assertRetryConverges(ctx, t, env, simple, 3)
}

// TestFlushFaultTempCleanup breaks step 4 (DeleteObject of the tmp object).
// CopyTmpToFinal swallows delete failures by design (helpers.go:165-167), so
// the flush must SUCCEED: rows flushed, manifest updated, query correct —
// with an orphaned tmp object left behind that must not affect anything.
// Removing such orphans is #226's scope; this test pins today's behavior.
func TestFlushFaultTempCleanup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple := DefaultSchemaFixtures()[0]
	seedRows(ctx, t, env, simple, 3)

	faulty := &FaultInjectingS3{Inner: env.Cluster.S3, Fault: S3Fault{Op: S3OpDelete, KeyContains: "/_tmp/"}}
	report, err := env.RunFlushWith(ctx, FlushOverrides{S3: faulty})
	if err != nil {
		t.Fatalf("tmp cleanup failure must be swallowed, got %v", err)
	}
	if faulty.Injected() == 0 {
		t.Fatal("fault never fired")
	}
	finals, tmps := splitKeys(report.NewObjects)
	if len(finals) != 1 || len(tmps) != 1 {
		t.Errorf("want 1 final + 1 orphaned tmp, got finals %v tmps %v", finals, tmps)
	}
	if report.UnflushedAfter != 0 {
		t.Errorf("flush must complete, unflushed = %d, want 0", report.UnflushedAfter)
	}
	assertManifestDeltaPaths(t, report.Manifests, simple, finals)
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})

	// A second flush is a no-op: nothing dirty, no new objects.
	noop, err := env.RunFlushWith(ctx, FlushOverrides{})
	if err != nil {
		t.Fatalf("no-op flush: %v", err)
	}
	if noop.UnflushedBefore != 0 || len(noop.NewObjects) != 0 {
		t.Errorf("no-op flush moved state: unflushed %d, new %v", noop.UnflushedBefore, noop.NewObjects)
	}
}

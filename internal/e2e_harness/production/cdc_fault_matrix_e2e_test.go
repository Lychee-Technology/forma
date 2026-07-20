//go:build e2e

package production

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/reconcile"
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
// export succeeded, so the tmp object exists at failure time, but no final
// object, no flushed_at update, and no manifest entry may appear. A retry
// uses fresh UUIDs, so the failed attempt's tmp would be unreachable
// garbage — #226 makes CopyTmpToFinal best-effort delete its own tmp before
// surfacing the copy error, so no tmp survives the failed attempt either.
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
	if len(tmps) != 0 {
		t.Errorf("in-band cleanup must remove the tmp after copy failure (#226), got %v", tmps)
	}
	if report.UnflushedAfter != 3 {
		t.Errorf("rows must stay dirty, unflushed = %d, want 3", report.UnflushedAfter)
	}
	assertManifestDeltaPaths(t, report.Manifests, simple, nil)

	// Clean retry converges from a bucket free of the failed attempt's tmp.
	assertRetryConverges(ctx, t, env, simple, 3)
}

// TestFlushFaultTempCleanup breaks step 4 (DeleteObject of the tmp object).
// CopyTmpToFinal swallows delete failures by design, so the flush must
// SUCCEED: rows flushed, manifest updated, query correct — with an orphaned
// tmp object left behind that must not affect anything. This swallowed-delete
// residue is the one leak path #226 intentionally leaves to the GC backstop:
// the test closes the loop by driving manifest-reconcile --gc's two-phase
// sighting contract (#188/#203) and asserting the real leaked object is
// reclaimed past the grace window while the final object and manifest
// survive untouched.
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

	// GC backstop, phase 1 (within grace): the run only records the leaked
	// tmp's first-unlisted sighting; nothing may be deleted yet.
	leakedTmp := tmps[0]
	gcOpts := reconcile.Options{GC: true, GCGrace: 15 * time.Minute}
	{
		r, cleanup := newReconcileHarness(t, ctx, env, simple, gcOpts, false)
		defer cleanup()
		gcReport, err := r.Run(ctx)
		if err != nil {
			t.Fatalf("gc run within grace: %v", err)
		}
		s := gcReport.Schemas[0]
		if len(s.Deleted) != 0 {
			t.Fatalf("gc within grace deleted %v, want nothing", s.Deleted)
		}
		if !slices.Contains(s.TmpOrphans, leakedTmp) {
			t.Fatalf("gc must sight the leaked tmp %s, got tmp orphans %v", leakedTmp, s.TmpOrphans)
		}
	}

	// GC backstop, phase 2 (past grace): pretend the run happens an hour
	// later; both GC clocks (sighting age + object age) expire and the
	// leaked tmp is reclaimed. The final delta object and the manifest must
	// survive untouched.
	{
		r, cleanup := newReconcileHarness(t, ctx, env, simple, gcOpts, false)
		defer cleanup()
		r.Now = func() time.Time { return time.Now().Add(time.Hour) }
		gcReport, err := r.Run(ctx)
		if err != nil {
			t.Fatalf("gc run past grace: %v", err)
		}
		s := gcReport.Schemas[0]
		if len(s.Deleted) != 1 || s.Deleted[0] != leakedTmp {
			t.Fatalf("gc past grace deleted %v, want exactly the leaked tmp %s", s.Deleted, leakedTmp)
		}
	}

	keys, err := env.listS3Keys(ctx)
	if err != nil {
		t.Fatalf("list s3 keys after gc: %v", err)
	}
	if slices.Contains(keys, leakedTmp) {
		t.Errorf("leaked tmp %s must be gone after gc", leakedTmp)
	}
	for _, final := range finals {
		if !slices.Contains(keys, final) {
			t.Errorf("final delta object %s must survive gc", final)
		}
	}
	mAfter, _ := loadManifestWithETag(t, ctx, env, simple)
	assertManifestDeltaPaths(t, map[int16]*manifest.Manifest{simple.ID: mAfter}, simple, finals)
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})
}

//go:build e2e

package production

import (
	"context"
	"strings"
	"testing"
)

// TestFlushFaultMarkFlushed breaks the mark-flushed UPDATE via a poison
// trigger. Under the #252 ordering mark runs LAST, so the final S3 object
// landed AND its manifest entry was appended before the failure: the first
// delta is listed with all its rows still dirty (the dirty anti-join keeps
// serving the hot versions). Retry re-exports the same rows into a second
// listed delta; LWW must dedup the identical (row_id, ver_ts) copies across
// the two listed files so the federated result has no duplicates (#179
// known risky boundary 1, relaxed to crash-free runs by #252).
func TestFlushFaultMarkFlushed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple := DefaultSchemaFixtures()[0]
	seedRows(ctx, t, env, simple, 3)

	poisonMarkFlushed(ctx, t, env)
	report, err := env.RunFlushWith(ctx, FlushOverrides{})
	if err == nil {
		t.Fatal("flush with poisoned mark-flushed must fail")
	}
	if !strings.Contains(err.Error(), "mark flushed at snapshot") {
		t.Errorf("failure must surface at the mark-flushed step, got: %v", err)
	}
	firstFinals, _ := splitKeys(report.NewObjects)
	if len(firstFinals) != 1 {
		t.Fatalf("the final object must already exist when mark-flushed fails, got %v", firstFinals)
	}
	if report.UnflushedAfter != 3 {
		t.Errorf("rows must stay dirty, unflushed = %d, want 3", report.UnflushedAfter)
	}
	// #252: the manifest append preceded the failed mark, so the first delta
	// is already listed while its rows stay dirty.
	assertManifestDeltaPaths(t, report.Manifests, simple, firstFinals)

	healMarkFlushed(ctx, t, env)
	retry, err := env.RunFlushWith(ctx, FlushOverrides{})
	if err != nil {
		t.Fatalf("healed retry flush: %v", err)
	}
	if retry.UnflushedAfter != 0 {
		t.Errorf("retry must flush all rows, unflushed = %d", retry.UnflushedAfter)
	}
	secondFinals, _ := splitKeys(retry.NewObjects)
	if len(secondFinals) != 1 {
		t.Fatalf("retry must create exactly one more final object, got %v", secondFinals)
	}
	// Two physical copies of the same rows exist and BOTH are listed; LWW is
	// the only thing keeping the duplicates invisible.
	assertManifestDeltaPaths(t, retry.Manifests, simple,
		append(append([]string(nil), firstFinals...), secondFinals...))
	// The oracle knows 3 rows; duplicates across the two listed copies would
	// surface as extra result rows here.
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})
}

// TestFlushFaultManifestLoad breaks the manifest GetObject and
// TestFlushFaultManifestSave breaks the manifest PutObject. Under the #252
// ordering both land BEFORE mark-flushed, pinning the contract that
// superseded #197: the run fails with the final key embedded in the error
// (observability, not a --repair pointer), the rows stay dirty and fully
// hot-visible, and the retry self-heals — it re-exports the same rows to a
// fresh UUIDv7 key and appends that. The first copied final stays an
// unlisted orphan for manifest-reconcile --gc (ClassDelta covered leftover).
func TestFlushFaultManifestLoad(t *testing.T) {
	t.Parallel()
	testFlushFaultManifest(t, S3OpGet)
}

func TestFlushFaultManifestSave(t *testing.T) {
	t.Parallel()
	testFlushFaultManifest(t, S3OpPut)
}

func testFlushFaultManifest(t *testing.T, op S3Op) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple := DefaultSchemaFixtures()[0]
	seedRows(ctx, t, env, simple, 3)

	faulty := &FaultInjectingS3{Inner: env.Cluster.S3, Fault: S3Fault{Op: op, KeyContains: "manifest/"}}
	report, err := env.RunFlushWith(ctx, FlushOverrides{S3: faulty})
	if err == nil {
		t.Fatalf("flush with failing manifest %s must fail", op)
	}
	if faulty.Injected() == 0 {
		t.Fatal("fault never fired")
	}
	finals, _ := splitKeys(report.NewObjects)
	if len(finals) != 1 {
		t.Fatalf("the final object must exist when the manifest step fails, got %v", finals)
	}
	// The error still names the copied final key for observability.
	if !strings.Contains(err.Error(), "manifest update") || !strings.Contains(err.Error(), finals[0]) {
		t.Errorf("error must point at the copied final key %q, got: %v", finals[0], err)
	}
	// #252: the manifest append precedes mark-flushed, so the failed append
	// leaves every row dirty — nothing was partially committed to Postgres.
	if report.UnflushedAfter != 3 {
		t.Errorf("rows must stay dirty when the manifest step fails, unflushed = %d, want 3", report.UnflushedAfter)
	}
	assertManifestDeltaPaths(t, report.Manifests, simple, nil)
	// Mid-failure visibility: the rows never left the hot tier.
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})

	// Retry self-heals: it re-exports the same rows to a fresh key and appends
	// only that key. The first copied final stays an unlisted orphan.
	retry, err := env.RunFlushWith(ctx, FlushOverrides{})
	if err != nil {
		t.Fatalf("clean retry flush: %v", err)
	}
	if retry.UnflushedBefore != 3 || retry.UnflushedAfter != 0 {
		t.Errorf("retry must drain the dirty rows: unflushed before/after = %d/%d, want 3/0",
			retry.UnflushedBefore, retry.UnflushedAfter)
	}
	retryFinals, _ := splitKeys(retry.NewObjects)
	if len(retryFinals) != 1 {
		t.Fatalf("retry must promote exactly one new final object, got %v", retryFinals)
	}
	if retryFinals[0] == finals[0] {
		t.Errorf("retry must use a fresh key, reused %q", finals[0])
	}
	assertManifestDeltaPaths(t, retry.Manifests, simple, retryFinals)

	// Converged: the listed delta serves the rows, the orphan stays invisible.
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})
}

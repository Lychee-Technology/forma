//go:build e2e

package production

import (
	"context"
	"strings"
	"testing"
)

// TestFlushFaultMarkFlushed breaks step 5 (UPDATE change_log SET flushed_at)
// via a poison trigger, after the final S3 object already landed. Retry
// re-exports the same rows into a second final object; LWW must dedup the
// identical (row_id, ver_ts) copies so the federated result has no
// duplicates (#179 known risky boundary 1). The first final object stays
// outside the manifest forever (its manifest step never ran; retry appends
// only its own key) — inventory drift owned by #203, invisible to reads.
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
	assertManifestDeltaPaths(t, report.Manifests, simple, nil)

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
	// Two physical copies of the same rows exist; only the retry's object is
	// tracked by the manifest.
	assertManifestDeltaPaths(t, retry.Manifests, simple, secondFinals)
	// The oracle knows 3 rows; duplicates across the two parquet copies would
	// surface as extra result rows here.
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})
}

// TestFlushFaultManifestLoad breaks step 6 (manifest GetObject) and
// TestFlushFaultManifestSave breaks step 7 (manifest PutObject). Both land
// after mark-flushed, pinning the corrected #197 contract: the run fails
// with the final key embedded in the error, the retry is a strict no-op
// (rows are flushed; no re-export, no manifest self-repair — a blind
// AppendFile retry would duplicate entries), the delta file stays outside
// the manifest (reconciliation is #203's scope), and federated reads are
// unaffected because the read path never consults the manifest.
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
	// #197 contract: the error names the orphaned final key for the operator.
	if !strings.Contains(err.Error(), "manifest update") || !strings.Contains(err.Error(), finals[0]) {
		t.Errorf("error must point at the orphaned final key %q, got: %v", finals[0], err)
	}
	// Rows are already marked flushed — the partial commit this matrix exists
	// to pin.
	if report.UnflushedAfter != 0 {
		t.Errorf("rows must be marked flushed before the manifest step, unflushed = %d", report.UnflushedAfter)
	}
	assertManifestDeltaPaths(t, report.Manifests, simple, nil)

	// Retry is a strict no-op and does NOT self-repair the manifest.
	retry, err := env.RunFlushWith(ctx, FlushOverrides{})
	if err != nil {
		t.Fatalf("clean retry flush: %v", err)
	}
	if retry.UnflushedBefore != 0 || len(retry.NewObjects) != 0 {
		t.Errorf("retry must be a no-op: unflushed %d, new objects %v", retry.UnflushedBefore, retry.NewObjects)
	}
	assertManifestDeltaPaths(t, retry.Manifests, simple, nil)

	// Reads never consult the manifest; the flushed data stays fully visible.
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})
}

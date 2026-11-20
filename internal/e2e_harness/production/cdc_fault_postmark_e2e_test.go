//go:build e2e

package production

import (
	"context"
	"strings"
	"testing"
)

// TestFlushFaultMarkFlushed breaks the marks via a poison trigger. Protocol
// V2 marks the rows in the transaction that publishes their generation, so
// the failure rolls the publication back with them: the staged delta object
// and its manifest exist, but no generation is published and every row stays
// dirty and hot-visible. The healed retry stages afresh and publishes its own
// delta only; the first object stays an unlisted orphan for garbage
// collection (#891), and nothing reads it.
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
	if !strings.Contains(err.Error(), "mark-flushed poisoned") {
		t.Errorf("failure must surface the poisoned marks, got: %v", err)
	}
	firstFinals, _ := splitKeys(report.NewObjects)
	if len(firstFinals) != 1 {
		t.Fatalf("the staged delta object must exist when the marks fail, got %v", firstFinals)
	}
	if report.UnflushedAfter != 3 {
		t.Errorf("rows must stay dirty, unflushed = %d, want 3", report.UnflushedAfter)
	}
	assertManifestDeltaPaths(t, report.Manifests, simple, nil)
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})

	healMarkFlushed(ctx, t, env)
	retry, err := env.RunFlushWith(ctx, FlushOverrides{})
	if err != nil {
		t.Fatalf("healed retry flush: %v", err)
	}
	if retry.UnflushedAfter != 0 {
		t.Errorf("retry must flush all rows, unflushed = %d", retry.UnflushedAfter)
	}
	secondFinals, _ := splitKeys(retry.NewObjects)
	if len(secondFinals) != 1 || secondFinals[0] == firstFinals[0] {
		t.Fatalf("retry must stage exactly one fresh delta object, got %v (first %v)", secondFinals, firstFinals)
	}
	assertManifestDeltaPaths(t, retry.Manifests, simple, secondFinals)
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})
}

// TestFlushFaultManifestLoad breaks the GetObject of the parent generation's
// manifest, which a flush of a published schema reads to list its files, and
// TestFlushFaultManifestSave breaks the conditional PutObject of the new
// generation's manifest. Both land before the publication transaction: the
// run fails, the staged delta object is an unlisted orphan, the published
// generation is unchanged and every new row stays dirty and hot-visible. The
// retry stages the same rows under a fresh key and publishes it.
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
	first := mustFlush(ctx, t, env)
	published, _ := splitKeys(first.NewObjects)
	seedRows(ctx, t, env, simple, 3)

	faulty := &FaultInjectingS3{Inner: env.Cluster.S3, Fault: S3Fault{Op: op, KeyContains: "/_manifests/"}}
	report, err := env.RunFlushWith(ctx, FlushOverrides{S3: faulty})
	if err == nil {
		t.Fatalf("flush with failing manifest %s must fail", op)
	}
	if faulty.Injected() == 0 {
		t.Fatal("fault never fired")
	}
	finals, _ := splitKeys(report.NewObjects)
	if len(finals) != 1 {
		t.Fatalf("the staged delta object must exist when the manifest step fails, got %v", finals)
	}
	if report.UnflushedAfter != 3 {
		t.Errorf("rows must stay dirty when the manifest step fails, unflushed = %d, want 3", report.UnflushedAfter)
	}
	assertManifestDeltaPaths(t, report.Manifests, simple, published)
	// Mid-failure visibility: the rows never left the hot tier.
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})

	retry, err := env.RunFlushWith(ctx, FlushOverrides{})
	if err != nil {
		t.Fatalf("clean retry flush: %v", err)
	}
	if retry.UnflushedBefore != 3 || retry.UnflushedAfter != 0 {
		t.Errorf("retry must drain the dirty rows: unflushed before/after = %d/%d, want 3/0",
			retry.UnflushedBefore, retry.UnflushedAfter)
	}
	retryFinals, _ := splitKeys(retry.NewObjects)
	if len(retryFinals) != 1 || retryFinals[0] == finals[0] {
		t.Fatalf("retry must stage exactly one fresh delta object, got %v (failed attempt %v)", retryFinals, finals)
	}
	assertManifestDeltaPaths(t, retry.Manifests, simple, append(append([]string(nil), published...), retryFinals...))

	// Converged: the listed deltas serve the rows, the orphan stays invisible.
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})
}

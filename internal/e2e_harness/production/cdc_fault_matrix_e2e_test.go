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
	// Baseline the seeded rows on the hot tier.
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

// Step 2, the DuckDB export, no longer has an S3 fault of its own: since
// #896 it writes a local scratch file, and the object reaches S3 only through
// the conditional put that TestFlushFaultStageObject breaks.

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

// TestFlushFaultStageObject breaks the staging of the flush's delta object:
// the conditional PutObject of its parquet fails. Nothing was published, so
// no generation, no manifest object, no flushed_at update and no delta object
// may appear, and a clean retry converges.
func TestFlushFaultStageObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple := DefaultSchemaFixtures()[0]
	seedRows(ctx, t, env, simple, 3)

	faulty := &FaultInjectingS3{Inner: env.Cluster.S3, Fault: S3Fault{Op: S3OpPut, KeyContains: ".parquet"}}
	report, err := env.RunFlushWith(ctx, FlushOverrides{S3: faulty})
	if err == nil {
		t.Fatal("flush with a failing parquet PutObject must fail")
	}
	if faulty.Injected() == 0 {
		t.Fatal("fault never fired")
	}
	assertUntouched(t, report, simple, 3)

	assertRetryConverges(ctx, t, env, simple, 3)
}

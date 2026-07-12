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

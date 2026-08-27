//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/compaction"
)

// TestCompactionRewriteInFlightReaderRetention is #461's regression: a
// federated reader that resolved its parquet path set from the PRE-swap
// manifest must keep working after the compactor commits the swap. The
// compactor therefore retains the merged sources (unlisted, reclaimed by
// manifest-reconcile --gc past its grace); this test holds the pre-swap path
// set across a real rewrite publish and proves the scan still answers
// correctly. Before the fix the inline post-commit delete made this exact
// read fail with the non-degradable, breaker-worthy
// ParquetSetInconsistentError.
func TestCompactionRewriteInFlightReaderRetention(t *testing.T) {
	cluster := SharedCluster(t)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide
	env := NewEnv(t, cluster)

	creates := seedCompactionBase(ctx, t, env, wide, 5)
	update := UpdateEvent(wide, creates[0].RowID, map[string]any{"title": "inflight-v2"})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	mustFlush(ctx, t, env) // dirty ratio 1/5 = 20% > 5%: rewrite-eligible

	// The in-flight reader's snapshot: the path set resolved from the
	// pre-swap manifest, exactly what resolveScanSources would be holding
	// while the compactor publishes.
	preSwap := loadSchemaManifest(ctx, t, env, wide)
	preSwapKeys := make([]string, 0, len(preSwap.Files))
	preSwapURIs := make([]string, 0, len(preSwap.Files))
	for _, f := range preSwap.Files {
		preSwapKeys = append(preSwapKeys, f.Path)
		preSwapURIs = append(preSwapURIs, fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(f.Path, "/")))
	}
	if len(preSwapURIs) < 2 {
		t.Fatalf("pre-swap manifest lists %d files, want at least base+delta", len(preSwapURIs))
	}

	// The compactor publishes: the manifest swap commits and the sources are
	// spliced out.
	result, err := env.RunCompaction(ctx, wide)
	if err != nil {
		t.Fatalf("compaction: %v", err)
	}
	if result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.RewriteApplied)
	}

	// Writer-side acceptance: every pre-swap source object survives the
	// publish — the zero-grace inline delete was the bug.
	after := map[string]bool{}
	for _, k := range schemaParquetKeys(ctx, t, env, wide) {
		after[k] = true
	}
	for _, k := range preSwapKeys {
		if !after[k] {
			t.Errorf("pre-swap source %s was deleted at publish; an in-flight reader holding the old path set would fail ParquetSetInconsistent", k)
		}
	}

	// Reader-side acceptance, verified empirically through the real engine: a
	// query still holding the pre-swap path set (pinned via the explicit
	// path-list hint — same read_parquet scan shape, same DuckDB open path)
	// must succeed and answer identically to the oracle.
	held := env.AssertQueryMatches(ctx, Query{
		Schema:                wide,
		S3ParquetPathTemplate: strings.Join(preSwapURIs, ","),
		Limit:                 100,
	})
	if held == nil || held.Plan == nil || !held.Plan.Routing.UseDuckDB {
		t.Fatalf("pre-swap path-set query did not route to duckdb")
	}
}

//go:build e2e

package production

import (
	"context"
	"errors"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
)

// TestManifestConsistency_MissingListedParquet pins #187 scenario 2 as a
// contract: deleting a manifest-listed parquet object (5 flushed rows vanish
// from storage while the manifest still lists the file) must classify as
// ErrParquetSetInconsistent naming the missing key — never silently succeed
// with fewer rows, which is what the flat glob did before manifest-driven
// reads: the glob expanded to whatever objects survived, so cold-tier data
// loss was invisible to callers and operators alike.
func TestManifestConsistency_MissingListedParquet(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	keys := seedMultiParquet(ctx, t, env, wide)

	// Precondition: with all objects present the query routes through DuckDB
	// and matches the oracle, so the deletion below is what breaks it.
	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy != nil && !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb: %+v", healthy.Plan.Routing)
	}

	deleteObject(ctx, t, env, keys[1])

	failCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := env.Query(failCtx, Query{Schema: wide, Limit: 20})
	if err == nil {
		t.Fatal("query silently succeeded with a manifest-listed parquet missing from storage (#187 scenario 2)")
	}
	if !errors.Is(err, fedengine.ErrParquetSetInconsistent) {
		t.Fatalf("missing listed parquet must classify as ErrParquetSetInconsistent, got: %v", err)
	}
	var typed *fedengine.ParquetSetInconsistentError
	if !errors.As(err, &typed) {
		t.Fatalf("error chain must carry ParquetSetInconsistentError, got: %v", err)
	}
	if typed.SchemaID != wide.ID {
		t.Errorf("inconsistency names schema %d, want %d", typed.SchemaID, wide.ID)
	}
	if len(typed.MissingKeys) != 1 || typed.MissingKeys[0] != keys[1] {
		t.Errorf("inconsistency names missing keys %v, want [%s]", typed.MissingKeys, keys[1])
	}

	// Non-degradable: AllowPartialDegradedMode exists to absorb transient
	// infrastructure faults, and a Postgres-only fallback here would return
	// exactly the silent-loss answer this classification exists to prevent.
	degradedCtx, cancelDegraded := context.WithTimeout(ctx, 30*time.Second)
	defer cancelDegraded()
	_, err = env.Query(degradedCtx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	if err == nil {
		t.Fatal("degraded mode re-silenced the manifest inconsistency (#187 scenario 2)")
	}
	if !errors.Is(err, fedengine.ErrParquetSetInconsistent) {
		t.Fatalf("degraded-mode error must keep the inconsistency classification, got: %v", err)
	}
}

// TestManifestConsistency_OneGoodOneBadFile pins #187 scenario 7 as the #251
// contract: one valid and one page-corrupt parquet in the same scan set. The
// corrupt file's footer is intact, so the pre-read validator accepts it and
// the failure surfaces mid-scan; per-file verification then attributes it,
// path resolution excludes it (TTL cache), and one retry answers from the
// readable remainder — partial, loud (plan note names the object), and
// breaker-neutral (confirmed corruption is not engine sickness, so the
// DuckDB route survives a permanently corrupt object).
func TestManifestConsistency_OneGoodOneBadFile(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	keys := seedMultiParquet(ctx, t, env, wide)

	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy == nil || !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb")
	}
	healthyIDs := resultRowIDSet(t, healthy)

	// Capture the doomed file's row set BEFORE corrupting it: the batches are
	// disjoint and fully flushed, so the expected partial answer is exactly
	// everything else (no hot version shadows a lost row).
	badIDs := readParquetRowIDs(ctx, t, env, keys[1])
	want := setMinus(healthyIDs, badIDs)

	overwriteObjectBytes(ctx, t, env, keys[1], corruptMidFile)

	partialCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	res, err := env.Query(partialCtx, Query{Schema: wide, Limit: 20})
	if err != nil {
		t.Fatalf("degraded off: partial read must succeed with the corrupt file excluded (#251), got: %v", err)
	}
	assertIDSetEqual(t, resultRowIDSet(t, res), want)
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("partial read must stay duckdb-routed, got: %+v", res.Plan.Routing)
	}
	assertCorruptExclusionNote(t, res, keys[1])

	// Second query answers from the exclusion cache without a failed scan —
	// still partial, still loud.
	res2, err := env.Query(ctx, Query{Schema: wide, Limit: 20})
	if err != nil {
		t.Fatalf("cached-exclusion query failed: %v", err)
	}
	assertIDSetEqual(t, resultRowIDSet(t, res2), want)
	assertCorruptExclusionNote(t, res2, keys[1])

	// A permanently corrupt object must not accumulate breaker failures:
	// well past any failure threshold the route must still be DuckDB.
	for i := 0; i < 8; i++ {
		resN, err := env.Query(ctx, Query{Schema: wide, Limit: 20})
		if err != nil {
			t.Fatalf("query %d failed with a permanently corrupt file present: %v", i, err)
		}
		if !resN.Plan.Routing.UseDuckDB {
			t.Fatalf("query %d lost the duckdb route (breaker tripped on corruption?): %+v", i, resN.Plan.Routing)
		}
	}

	// Degraded mode no longer needs the Postgres-only fallback: the partial
	// parquet read succeeds, so the plan stays DuckDB-routed and partial.
	degraded, err := env.Query(ctx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	if err != nil {
		t.Fatalf("degraded-mode query failed: %v", err)
	}
	if !degraded.Plan.Routing.UseDuckDB {
		t.Fatalf("degraded mode fell back to postgres although the partial read succeeds: %+v", degraded.Plan.Routing)
	}
	assertIDSetEqual(t, resultRowIDSet(t, degraded), want)
}

// TestManifestConsistency_OneGoodOneTruncatedFile is the footer-dead sibling
// of OneGoodOneBadFile: truncation kills the footer, so the failure surfaces
// at Query (bind) time rather than mid-stream. Same #251 contract, other
// error branch.
func TestManifestConsistency_OneGoodOneTruncatedFile(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	keys := seedMultiParquet(ctx, t, env, wide)

	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy == nil || !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb")
	}
	healthyIDs := resultRowIDSet(t, healthy)
	badIDs := readParquetRowIDs(ctx, t, env, keys[1])
	want := setMinus(healthyIDs, badIDs)

	overwriteObjectBytes(ctx, t, env, keys[1], truncateHalf)

	partialCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	res, err := env.Query(partialCtx, Query{Schema: wide, Limit: 20})
	if err != nil {
		t.Fatalf("truncated file: partial read must succeed with it excluded (#251), got: %v", err)
	}
	assertIDSetEqual(t, resultRowIDSet(t, res), want)
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("partial read must stay duckdb-routed, got: %+v", res.Plan.Routing)
	}
	assertCorruptExclusionNote(t, res, keys[1])
}

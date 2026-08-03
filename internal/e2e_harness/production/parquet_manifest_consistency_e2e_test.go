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
//
// The breaker is armed at threshold 1 deliberately: a default Env builds no
// breaker at all (engine.go only constructs one when breakerFailures > 0,
// and every CircuitBreaker method is a nil-receiver no-op), so neutrality
// would be unfalsifiable. At threshold 1 a single RecordFailure opens the
// circuit, which makes the FIRST post-corruption query — the one that
// actually eats the failed scan and the in-request retry — the real
// discriminator, and turns the loop below into a no-stray-failures check.
// The cooldown outlasts the rest of the test, so an opened breaker cannot
// silently heal before the assertions run.
func TestManifestConsistency_OneGoodOneBadFile(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithBreaker(1, time.Minute))
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
	// The count must be partial too: a recount that still sees the excluded
	// file would report the pre-corruption total while the page shows fewer
	// rows (the #181 divergent-total class).
	if res.Total != int64(len(want)) {
		t.Errorf("partial read total = %d, want %d (page and count must agree)", res.Total, len(want))
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
//
// The mode ordering is deliberately inverted relative to OneGoodOneBadFile:
// here the FIRST post-corruption query — the only one that pays a real
// failed scan, before any exclusion cache exists — carries
// AllowPartialDegradedMode. That pins the branch a cache-warm degraded query
// cannot: on its first corrupt encounter degraded mode must NOT take the
// Postgres-only fallback, because the partial parquet read succeeds. Between
// the two tests, both error branches get first-query coverage in both modes.
// The breaker is armed at threshold 1 for the reason documented on
// OneGoodOneBadFile.
func TestManifestConsistency_OneGoodOneTruncatedFile(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithBreaker(1, time.Minute))
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

	// First corrupt encounter, degraded mode ON: the fallback must stay
	// unused — the answer is the partial DuckDB read, not the Postgres-only
	// (oracle-complete) plan degraded mode used to produce here.
	partialCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	res, err := env.Query(partialCtx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	if err != nil {
		t.Fatalf("truncated file: partial read must succeed with it excluded (#251), got: %v", err)
	}
	assertIDSetEqual(t, resultRowIDSet(t, res), want)
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("degraded mode fell back to postgres on the first corrupt encounter although the partial read succeeds: %+v", res.Plan.Routing)
	}
	if res.Total != int64(len(want)) {
		t.Errorf("partial read total = %d, want %d (page and count must agree)", res.Total, len(want))
	}
	assertCorruptExclusionNote(t, res, keys[1])

	// Same partial answer from the exclusion cache with degraded mode OFF:
	// exclusion is a property of the read path, not of the degraded flag.
	res2, err := env.Query(ctx, Query{Schema: wide, Limit: 20})
	if err != nil {
		t.Fatalf("cached-exclusion query (degraded off) failed: %v", err)
	}
	assertIDSetEqual(t, resultRowIDSet(t, res2), want)
	if !res2.Plan.Routing.UseDuckDB {
		t.Fatalf("cached-exclusion query must stay duckdb-routed, got: %+v", res2.Plan.Routing)
	}
	assertCorruptExclusionNote(t, res2, keys[1])
}

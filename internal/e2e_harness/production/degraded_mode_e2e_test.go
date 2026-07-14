//go:build e2e

package production

import (
	"context"
	"strings"
	"testing"
	"time"
)

// seedTwoTiers writes flushed (parquet) plus hot rows for schema so a
// federated query has to touch both Postgres and S3.
func seedTwoTiers(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	flushed := env.GenerateScript(ScriptSpec{Schema: schema, Creates: 5})
	if err := env.ApplyEvents(ctx, flushed...); err != nil {
		t.Fatalf("apply flushed events: %v", err)
	}
	mustFlush(ctx, t, env)
	hot := env.GenerateScript(ScriptSpec{Schema: schema, Creates: 3})
	if err := env.ApplyEvents(ctx, hot...); err != nil {
		t.Fatalf("apply hot events: %v", err)
	}
}

// assertDegradedFallbackPlan asserts the execution plan reflects the
// postgres-only degraded fallback (#185 scenario 6; contract pinned by
// TestDBFederatedQueryEngine_DegradedFallbackRecordsExecutionPlan).
func assertDegradedFallbackPlan(t *testing.T, result *QueryResult) {
	t.Helper()
	if result == nil {
		return
	}
	if result.Plan == nil {
		t.Fatal("degraded query returned no execution plan")
	}
	if result.Plan.Routing.UseDuckDB {
		t.Errorf("degraded fallback plan still claims duckdb: %+v", result.Plan.Routing)
	}
	if !strings.Contains(result.Plan.Routing.Reason, "degraded fallback") {
		t.Errorf("degraded fallback plan reason = %q, want a degraded-fallback marker", result.Plan.Routing.Reason)
	}
}

// TestDegradedMode_S3Unavailable covers #185 degraded scenarios 1, 2 and 6
// with the S3 container really stopped (docker stop — DuckDB httpfs gets
// connection failures, not a harness boolean): degraded-on must fall back to
// postgres-only with complete, oracle-checked results, correct page metadata
// and a fallback-marked execution plan; degraded-off must surface the error
// instead of silently returning partial results. Owns a dedicated cluster:
// halting the shared S3 would break every parallel Env.
func TestDegradedMode_S3Unavailable(t *testing.T) {
	ctx := context.Background()
	cluster := DedicatedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	seedTwoTiers(ctx, t, env, wide)

	// Precondition: with S3 healthy the query routes through DuckDB, so the
	// halt below is what forces the fallback (not the routing policy).
	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy != nil && !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb: %+v", healthy.Plan.Routing)
	}

	if err := cluster.HaltS3(ctx); err != nil {
		t.Fatalf("halt s3 container: %v", err)
	}

	// Scenario 2 (degraded OFF): the DuckDB/S3 failure must surface as an
	// error, bounded by a timeout so a hanging httpfs retry fails the test
	// instead of stalling it.
	failCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := env.Query(failCtx, Query{Schema: wide, Limit: 20}); err == nil {
		t.Fatal("degraded mode off: expected error with S3 halted, got success")
	} else if !strings.Contains(err.Error(), "duckdb federated query") {
		t.Errorf("degraded mode off: error should wrap the duckdb failure, got: %v", err)
	}

	// Scenarios 1+6 (degraded ON): postgres-only fallback returns complete
	// oracle-checked results (totals, row sets, attribute values) plus a
	// fallback-marked plan. No panic is implicit in reaching the assertions.
	degraded := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	assertDegradedFallbackPlan(t, degraded)
}

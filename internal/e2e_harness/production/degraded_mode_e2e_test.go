//go:build e2e

package production

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
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

// TestDegradedMode_DuckDBUnavailable is #185 degraded scenario 3: the
// embedded DuckDB client is really closed (database/sql rejects every call —
// the honest way to "stop" an in-process engine). Degraded-on falls back to
// postgres-only with complete results; degraded-off surfaces the failure.
// Runs on the shared cluster: closing this Env's DuckDB touches nothing
// shared.
func TestDegradedMode_DuckDBUnavailable(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]

	seedTwoTiers(ctx, t, env, wide)

	// Bind the engine (and its executor) to the client before closing it:
	// the executor wraps the same *sql.DB that Close invalidates.
	if _, err := env.Query(ctx, Query{Schema: wide, Limit: 20}); err != nil {
		t.Fatalf("precondition query: %v", err)
	}
	if err := env.Duck.Close(); err != nil {
		t.Fatalf("close duckdb client: %v", err)
	}

	if _, err := env.Query(ctx, Query{Schema: wide, Limit: 20}); err == nil {
		t.Fatal("degraded mode off: expected error with duckdb closed, got success")
	}

	degraded := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	assertDegradedFallbackPlan(t, degraded)
}

// TestDegradedMode_DirtyFetchFailure is #185 degraded scenario 4: the
// dirty-ID anti-join source is broken for real by renaming change_log out
// from under the engine. Degraded-off (DuckDB path) propagates the "fetch
// dirty ids" error.
//
// Deviation from the task brief (outside its Step-2 contingency list;
// flagged for adjudication on #185): the brief assumed the degraded-on half
// would fall back to a *complete* postgres-only page ("postgres is itself
// the dirty-set source so the result stays complete"). That is not what the
// engine does. The postgres OLTP optimized query UNIONs the real-time buffer
// straight from change_log (advanced_query_template.go: `FROM {{.ChangeLogTable}} cl
// ... flushed_at = 0`) whenever tables.ChangeLog is set — which it always is
// in the production harness. So renaming change_log breaks the postgres-only
// fallback too, and AllowPartialDegradedMode does NOT rescue a change_log
// outage: degraded-on surfaces the same missing-relation error rather than a
// clean fallback page. This is a broader break than a pure dirty-ID
// anti-join outage. The honest characterization below asserts that actual
// behavior; the brief's assertDegradedFallbackPlan expectation cannot hold
// here. See the task report / issue for adjudication of whether the harness
// should isolate the dirty-ID source from the real-time buffer scan.
func TestDegradedMode_DirtyFetchFailure(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]

	seedTwoTiers(ctx, t, env, wide)

	if _, err := env.Pool.Exec(ctx, "ALTER TABLE change_log RENAME TO change_log_broken"); err != nil {
		t.Fatalf("rename change_log: %v", err)
	}
	// Registered after NewEnv's cleanups, so it runs first (LIFO) and the
	// artifact dump still sees an intact change_log on failure.
	t.Cleanup(func() {
		_, _ = env.Pool.Exec(context.Background(), "ALTER TABLE change_log_broken RENAME TO change_log")
	})

	_, err := env.Query(ctx, Query{Schema: wide, Limit: 20})
	if err == nil {
		t.Fatal("expected dirty-fetch error with change_log renamed, got success")
	}
	if !strings.Contains(err.Error(), "fetch dirty ids") {
		t.Errorf("error should wrap the dirty-id fetch, got: %v", err)
	}

	// Degraded-on: the postgres-only fallback also scans change_log (real-time
	// buffer UNION), so a change_log outage is non-degradable — the flag does
	// not rescue it. See the function doc for the full deviation rationale.
	_, degradedErr := env.Query(ctx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	if degradedErr == nil {
		t.Fatal("degraded on: expected error (postgres fallback also scans change_log), got success")
	}
	if !strings.Contains(degradedErr.Error(), "change_log") {
		t.Errorf("degraded on: error should reference the missing change_log relation, got: %v", degradedErr)
	}
}

// TestDegradedMode_MissingSchemaMetadataNotDegradable is #185 degraded
// scenario 5: a schema unknown to the load-once metadata snapshot fails with
// ErrSchemaMetadataCacheRequired even under AllowPartialDegradedMode — the
// documented non-degradable configuration error (#151). errors.Is must
// survive the engine's wrapping.
func TestDegradedMode_MissingSchemaMetadataNotDegradable(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]

	seedTwoTiers(ctx, t, env, wide)

	ghost := SchemaRef{ID: 9999, Name: "ghost"}
	_, err := env.Query(ctx, Query{Schema: ghost, Limit: 20, AllowPartialDegradedMode: true})
	if err == nil {
		t.Fatal("expected ErrSchemaMetadataCacheRequired for a schema outside the metadata snapshot, got success")
	}
	if !errors.Is(err, fedengine.ErrSchemaMetadataCacheRequired) {
		t.Errorf("error = %v, want errors.Is(ErrSchemaMetadataCacheRequired)", err)
	}
}

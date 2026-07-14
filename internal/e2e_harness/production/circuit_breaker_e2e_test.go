//go:build e2e

package production

import (
	"context"
	"strings"
	"testing"
	"time"
)

const breakerRejection = "circuit breaker open"

// TestCircuitBreaker_OpensAtThresholdAndRecovers covers #185 breaker
// scenarios 1-3 end to end: exactly N real DuckDB failures (a closed
// database/sql client) open the breaker, an open breaker rejects queries
// before reaching DuckDB (rejection persists across a DuckDB rebuild while
// openDuration lasts), after openDuration the first success closes the
// breaker immediately, and the cleared failure history means one fresh
// failure does not reopen it — the documented immediate-forgiveness design
// with no half-open state accumulation (circuit_breaker.go design note).
func TestCircuitBreaker_OpensAtThresholdAndRecovers(t *testing.T) {
	const threshold = 3
	const cooldown = 5 * time.Second

	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithBreaker(threshold, cooldown))
	wide := DefaultSchemaFixtures()[1]

	seedTwoTiers(ctx, t, env, wide)

	// Healthy precondition binds the engine — and the breaker the Env caches
	// across DuckDB rebuilds — before the client is closed.
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})

	if err := env.Duck.Close(); err != nil {
		t.Fatalf("close duckdb client: %v", err)
	}

	// Scenario 1: the first `threshold` queries reach DuckDB and fail for
	// real; none of them may be breaker rejections.
	for i := 0; i < threshold; i++ {
		_, err := env.Query(ctx, Query{Schema: wide, Limit: 20})
		if err == nil {
			t.Fatalf("failure %d/%d: expected error with duckdb closed, got success", i+1, threshold)
		}
		if strings.Contains(err.Error(), breakerRejection) {
			t.Fatalf("failure %d/%d: breaker opened before the threshold: %v", i+1, threshold, err)
		}
	}
	// ...and the next query is rejected by the breaker, not by DuckDB.
	if _, err := env.Query(ctx, Query{Schema: wide, Limit: 20}); err == nil || !strings.Contains(err.Error(), breakerRejection) {
		t.Fatalf("query after threshold: want breaker rejection, got: %v", err)
	}

	// DuckDB is healthy again, but openDuration has not expired: rejection
	// must come from breaker state, not from the dead client.
	if err := env.ReopenDuckDB(); err != nil {
		t.Fatalf("reopen duckdb: %v", err)
	}
	if _, err := env.Query(ctx, Query{Schema: wide, Limit: 20}); err == nil || !strings.Contains(err.Error(), breakerRejection) {
		t.Fatalf("query while open with healthy duckdb: want breaker rejection, got: %v", err)
	}

	// Scenario 2: after openDuration the first query flows through, succeeds,
	// and closes the breaker (oracle-checked result).
	time.Sleep(cooldown + time.Second)
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})

	// Scenario 3: the success cleared the failure history — a single fresh
	// failure must NOT reopen the breaker (no half-open accumulation).
	if err := env.Duck.Close(); err != nil {
		t.Fatalf("close duckdb client again: %v", err)
	}
	if _, err := env.Query(ctx, Query{Schema: wide, Limit: 20}); err == nil {
		t.Fatal("expected one real failure after re-close, got success")
	} else if strings.Contains(err.Error(), breakerRejection) {
		t.Fatalf("single failure after recovery reopened the breaker (history not cleared): %v", err)
	}
	if err := env.ReopenDuckDB(); err != nil {
		t.Fatalf("reopen duckdb after single failure: %v", err)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
}

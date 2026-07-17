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

// TestS3Restart_CleanRecovery covers #187 scenario 8 end to end: a real
// docker stop of the S3 container (degraded-off error, degraded-on
// oracle-complete fallback — the halted half overlaps #185's
// TestDegradedMode_S3Unavailable but keeps this test self-contained), then
// the restoration #185 could not assert for lack of a restart verb: after
// RestartS3 rebinds the endpoint (the host-mapped port can change), a plain
// query with no degraded flag must route through DuckDB again and match the
// oracle. Runs breaker-free on a dedicated cluster so recovery is
// deterministic — an open breaker would keep rejecting until its cooldown
// elapses regardless of S3 health.
func TestS3Restart_CleanRecovery(t *testing.T) {
	ctx := context.Background()
	cluster := DedicatedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	seedAllTiers(ctx, t, env, wide)
	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy != nil && !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb: %+v", healthy.Plan.Routing)
	}

	if err := cluster.HaltS3(ctx); err != nil {
		t.Fatalf("halt s3 container: %v", err)
	}

	// Degraded OFF: bounded classified failure while S3 is down.
	failCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := env.Query(failCtx, Query{Schema: wide, Limit: 20})
	if err == nil {
		t.Fatal("degraded mode off: expected error with S3 halted, got success")
	}
	if !errors.Is(err, fedengine.ErrFederatedReadFailed) {
		t.Fatalf("S3 outage must classify as ErrFederatedReadFailed, got: %v", err)
	}
	if errors.Is(err, fedengine.ErrParquetSetInconsistent) {
		t.Errorf("an unreachable store cannot prove loss; must not classify as manifest inconsistency: %v", err)
	}
	if !strings.Contains(err.Error(), "duckdb federated query") {
		t.Errorf("error must carry the federated wrap chain, got: %v", err)
	}

	// Degraded ON: oracle-complete Postgres-only fallback while S3 is down.
	degraded := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	assertDegradedFallbackPlan(t, degraded)

	// Restore S3 and assert clean recovery: no degraded flag, DuckDB route
	// back, oracle-complete. The route assertion guards against a stale
	// endpoint silently degrading forever.
	if err := env.RestartS3(ctx); err != nil {
		t.Fatalf("restart s3: %v", err)
	}
	recovered := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if recovered != nil && !recovered.Plan.Routing.UseDuckDB {
		t.Errorf("recovered query must route through DuckDB again: %+v", recovered.Plan.Routing)
	}
}

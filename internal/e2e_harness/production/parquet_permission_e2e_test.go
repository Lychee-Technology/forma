//go:build e2e

package production

import (
	"context"
	"errors"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
)

// TestParquetPermission_BadCredentials covers #187 scenario 3 with the only
// permission-style fault that can reach DuckDB reads on the
// single-credential test store: an httpfs session holding wrong S3
// credentials, rejected by RustFS's signature check on every read. Contract:
// a bounded, classified error (the objects exist and the harness probe uses
// the good credentials, so this must stay ErrFederatedReadFailed — not
// manifest inconsistency), oracle-complete degraded fallback, and full
// recovery once the session is rebuilt with the cluster credentials.
func TestParquetPermission_BadCredentials(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithDuckMaxConnections(1))
	wide := DefaultSchemaFixtures()[1]

	seedTwoTiers(ctx, t, env, wide)
	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy != nil && !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb: %+v", healthy.Plan.Routing)
	}

	if err := env.ReopenDuckDBWithS3Creds("wrongaccess", "wrongsecret"); err != nil {
		t.Fatalf("reopen duckdb with bad creds: %v", err)
	}

	// Degraded OFF: the signature rejection surfaces as a bounded, classified
	// error — not a panic, not a hang, not a silent success.
	failCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := env.Query(failCtx, Query{Schema: wide, Limit: 20})
	if err == nil {
		t.Fatal("degraded mode off: expected error with bad httpfs credentials, got success (does the store enforce SigV4?)")
	}
	if !errors.Is(err, fedengine.ErrFederatedReadFailed) {
		t.Fatalf("credential rejection must classify as ErrFederatedReadFailed, got: %v", err)
	}
	if errors.Is(err, fedengine.ErrParquetSetInconsistent) {
		t.Errorf("objects exist; credential rejection must not classify as manifest inconsistency: %v", err)
	}

	// Degraded ON: Postgres is unaffected by S3 credentials, so the fallback
	// serves the oracle-complete result.
	degraded := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	assertDegradedFallbackPlan(t, degraded)

	// Recovery: restore the cluster credentials and the DuckDB route comes
	// back oracle-complete with no degraded flag needed.
	if err := env.ReopenDuckDBWithS3Creds(cluster.S3AccessKey, cluster.S3SecretKey); err != nil {
		t.Fatalf("reopen duckdb with cluster creds: %v", err)
	}
	recovered := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if recovered != nil && !recovered.Plan.Routing.UseDuckDB {
		t.Errorf("recovered query must route through DuckDB again: %+v", recovered.Plan.Routing)
	}
}

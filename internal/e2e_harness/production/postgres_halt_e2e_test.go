//go:build e2e

package production

import (
	"context"
	"errors"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
)

// TestPostgresHalt_ParquetTiersError covers #187 scenario 9: with Postgres
// really stopped mid-outage (docker stop — the pool and DuckDB's postgres
// attachment point at a dead server, failing with genuine network errors),
// a federated query over fully seeded parquet tiers must fail bounded, and
// AllowPartialDegradedMode must NOT rescue it: the dirty-ID anti-join needs
// Postgres for consistency, and the degraded fallback is itself
// Postgres-only — parquet tiers alone can never serve a correct answer.
// After ResumePostgres the query and a fresh write/read roundtrip prove the
// full rebind. Owns a dedicated cluster: halting the shared Postgres would
// break every parallel Env.
func TestPostgresHalt_ParquetTiersError(t *testing.T) {
	ctx := context.Background()
	cluster := DedicatedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	seedAllTiers(ctx, t, env, wide)
	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy != nil && !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb: %+v", healthy.Plan.Routing)
	}

	if err := env.HaltPostgres(ctx); err != nil {
		t.Fatalf("halt postgres container: %v", err)
	}

	// Every probe must produce the classified Postgres-side failure (#187
	// success criterion: classified, actionable errors — asserted via
	// errors.Is, never network-error text).
	assertPostgresReadFailed := func(label string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: expected error with Postgres halted, got success", label)
		}
		if !errors.Is(err, fedengine.ErrPostgresReadFailed) {
			t.Fatalf("%s: must classify as ErrPostgresReadFailed, got: %v", label, err)
		}
	}

	// Degraded OFF: bounded failure (the dirty-ID fetch hits Postgres first
	// and fails fast; the timeout guards against a hanging retry loop).
	failCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := env.Query(failCtx, Query{Schema: wide, Limit: 20})
	assertPostgresReadFailed("degraded mode off", err)

	// Degraded ON must ALSO fail: the fallback is Postgres-only, so a
	// Postgres outage is not degradable — parquet-only tiers cannot answer.
	degradedCtx, cancelDegraded := context.WithTimeout(ctx, 30*time.Second)
	defer cancelDegraded()
	_, err = env.Query(degradedCtx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	assertPostgresReadFailed("degraded mode on", err)

	// Explicit warm+cold preference changes nothing: the dirty-set
	// consistency barrier still needs Postgres (#184 keeps the anti-join for
	// hot-excluded requests).
	tiersCtx, cancelTiers := context.WithTimeout(ctx, 30*time.Second)
	defer cancelTiers()
	warmCold := []model.DataTier{model.DataTierWarm, model.DataTierCold}
	_, err = env.Query(tiersCtx, Query{Schema: wide, Limit: 20, PreferredTiers: warmCold})
	assertPostgresReadFailed("parquet-only tiers", err)

	// Resume and prove full recovery: oracle-complete federated read plus a
	// fresh write/read roundtrip through the rebuilt handles.
	if err := env.ResumePostgres(ctx); err != nil {
		t.Fatalf("resume postgres: %v", err)
	}
	recovered := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if recovered != nil && !recovered.Plan.Routing.UseDuckDB {
		t.Errorf("recovered query must route through DuckDB again: %+v", recovered.Plan.Routing)
	}
	extra := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 2})
	if err := env.ApplyEvents(ctx, extra...); err != nil {
		t.Fatalf("apply post-resume writes: %v", err)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 30})
}

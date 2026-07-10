//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestPostgresRestartRoundtrip proves the state-preserving Postgres restart
// (#175 scenario 6 plumbing): rows written before the restart must survive
// it, and every rebound handle — pool, CDC config, DuckDB, entity manager,
// federated engine — must work against the restarted server. It owns a
// dedicated cluster because restarting the shared one would break every
// parallel Env.
func TestPostgresRestartRoundtrip(t *testing.T) {
	ctx := context.Background()
	cluster := DedicatedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	events := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})
	if err := env.ApplyEvents(ctx, events...); err != nil {
		t.Fatalf("apply events: %v", err)
	}

	if err := env.RestartPostgres(ctx); err != nil {
		t.Fatalf("restart postgres: %v", err)
	}

	// Hot query through the rebuilt pool/engine: all three rows survived.
	env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})

	// Writes still work after the restart (fresh EntityManager over the new
	// pool), proving the rebind is complete, not just read-capable.
	more := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 1})
	if err := env.ApplyEvents(ctx, more...); err != nil {
		t.Fatalf("apply events after restart: %v", err)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
}

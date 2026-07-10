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

// TestEntityLifecycleCrossRestart is scenario 6 of #175: every lifecycle
// stage must be idempotent across a Postgres restart, because all state that
// matters — entity rows, change_log tombstones, flushed_at markers — lives
// in the persistence layer, never in process memory. The restart points
// bracket the delete-resurrection chain: after the live flush, after the
// unflushed delete, and after the tombstone flush. Serial on a dedicated
// cluster (restarting the shared one would break parallel Envs).
func TestEntityLifecycleCrossRestart(t *testing.T) {
	ctx := context.Background()
	cluster := DedicatedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 1})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	mustFlush(ctx, t, env)

	// Restart point 1: a flushed live row survives.
	if err := env.RestartPostgres(ctx); err != nil {
		t.Fatalf("restart after live flush: %v", err)
	}
	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
	if result != nil && !result.Plan.Routing.UseDuckDB {
		t.Errorf("post-restart query did not route to duckdb: %+v", result.Plan.Routing)
	}

	del := DeleteEvent(wide, creates[0].RowID)
	if err := env.ApplyEvents(ctx, del); err != nil {
		t.Fatalf("apply delete: %v", err)
	}

	// Restart point 2: the unflushed tombstone survives in change_log and
	// keeps masking the flushed live version.
	if err := env.RestartPostgres(ctx); err != nil {
		t.Fatalf("restart after unflushed delete: %v", err)
	}
	assertTombstoneChangeLog(ctx, t, env, wide, del)
	env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})

	flush2 := mustFlush(ctx, t, env)
	assertTombstoneParquet(ctx, t, env, soleParquetKey(t, flush2), del)

	// Restart point 3: with the tombstone flushed, the deletion stays
	// invisible and a retried flush is still a no-op.
	if err := env.RestartPostgres(ctx); err != nil {
		t.Fatalf("restart after tombstone flush: %v", err)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
	retry, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("retry flush after restart: %v", err)
	}
	if retry.UnflushedBefore != 0 || retry.UnflushedAfter != 0 || len(retry.NewObjects) != 0 {
		t.Errorf("post-restart flush retry moved state: unflushed %d -> %d, new objects %v",
			retry.UnflushedBefore, retry.UnflushedAfter, retry.NewObjects)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
}

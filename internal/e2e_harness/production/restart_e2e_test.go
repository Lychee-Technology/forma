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
// in the persistence layer, never in process memory. Restart points cover
// each lifecycle stage: after the live flush, after an unflushed update,
// after the update flush, after the unflushed delete, after the tombstone
// flush (plus flush-retry no-op), and after an injected restore. Serial on a
// dedicated cluster (restarting the shared one would break parallel Envs).
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
	restartPG(ctx, t, env, "after live flush")
	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
	if result != nil && !result.Plan.Routing.UseDuckDB {
		t.Errorf("post-restart query did not route to duckdb: %+v", result.Plan.Routing)
	}

	// Restart point 2: an unflushed update survives in change_log/entity_main
	// and still wins LWW over the flushed original (oracle checks the value).
	update := UpdateEvent(wide, creates[0].RowID, map[string]any{
		"title": "restart-updated",
		"count": float64(424242),
	})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	restartPG(ctx, t, env, "after unflushed update")
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})

	// Restart point 3: with the update flushed there are two live parquet
	// versions of the row; LWW must still resolve to the updated value.
	mustFlush(ctx, t, env)
	restartPG(ctx, t, env, "after update flush")
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})

	del := DeleteEvent(wide, creates[0].RowID)
	if err := env.ApplyEvents(ctx, del); err != nil {
		t.Fatalf("apply delete: %v", err)
	}

	// Restart point 4: the unflushed tombstone survives in change_log and
	// keeps masking the flushed live versions.
	restartPG(ctx, t, env, "after unflushed delete")
	assertTombstoneChangeLog(ctx, t, env, wide, del)
	env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})

	flush2 := mustFlush(ctx, t, env)
	assertTombstoneParquet(ctx, t, env, soleParquetKey(t, flush2), del)

	// Restart point 5: with the tombstone flushed, the deletion stays
	// invisible and a retried flush is still a no-op.
	restartPG(ctx, t, env, "after tombstone flush")
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

	// Restart point 6: an injected restore stays visible across a restart.
	testRestoreAcrossRestart(ctx, t, env)
}

// testRestoreAcrossRestart drives scenario 5's injected revival on the
// simple fixture and restarts after the restore flush: the restored row must
// stay visible, with its single live delta version readable through the
// rebuilt DuckDB client.
func testRestoreAcrossRestart(ctx context.Context, t *testing.T, env *Env) {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]
	screates := env.GenerateScript(ScriptSpec{Schema: simple, Creates: 1})
	if err := env.ApplyEvents(ctx, screates...); err != nil {
		t.Fatalf("apply simple create: %v", err)
	}
	mustFlush(ctx, t, env)
	sdel := DeleteEvent(simple, screates[0].RowID)
	if err := env.ApplyEvents(ctx, sdel); err != nil {
		t.Fatalf("apply simple delete: %v", err)
	}
	mustFlush(ctx, t, env)
	restored := env.InjectRestore(ctx, screates[0], sdel)
	restoreFlush := mustFlush(ctx, t, env)
	restartPG(ctx, t, env, "after restore flush")
	env.AssertQueryMatches(ctx, Query{Schema: simple, PreferHot: true, Limit: 10})
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 10})
	assertSoleLiveVersion(ctx, t, env, soleParquetKey(t, restoreFlush), restored)
}

// restartPG restarts the Env's Postgres and fails the test on error; stage
// names the lifecycle point for the failure message.
func restartPG(ctx context.Context, t *testing.T, env *Env, stage string) {
	t.Helper()
	if err := env.RestartPostgres(ctx); err != nil {
		t.Fatalf("restart postgres %s: %v", stage, err)
	}
}

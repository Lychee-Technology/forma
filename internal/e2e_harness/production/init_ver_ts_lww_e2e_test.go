//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestInitVerTsLWW (#210) pins the cdc-init version-timestamp contract on the
// federated LWW path: init must stamp base rows with the row's true
// latest-version timestamp (ltbase_updated_at AS changed_at), not its
// creation time. Under the pre-#210 contract (ltbase_created_at) any row
// updated after creation left init with a base copy whose ver_ts predated
// every previously flushed delta version — so a stale delta deterministically
// beat the fresh base under
//
//	ORDER BY ver_ts DESC, source_tier_priority DESC, deleted_ts DESC, row_id ASC
//	(advanced_query_template_duckdb.go)
//
// and the federated path served superseded attributes until the row's next
// mutation. The sequence below is reachable straight through the documented
// onboarding steps (#176): backfill with cdc-init after some updates were
// already flushed, then clear change_log.
func TestInitVerTsLWW(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"fresh_base_beats_stale_delta", testFreshBaseBeatsStaleDelta},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), wide)
		})
	}
}

// testFreshBaseBeatsStaleDelta is #210's failure-mode-1 construction, built
// entirely from production verbs: create v1 (T1) -> update v2 (T2) -> flush
// (delta: v2 attrs @ ver_ts = T2) -> update v3 (T3) -> RunInit (base: v3
// attrs) -> onboarding change_log cleanup. The base copy holds the newest
// attribute state, so LWW must surface v3. With init stamping create-time the
// base carried ver_ts = T1 < T2 and the stale delta v2 won every read; with
// init stamping ltbase_updated_at the base carries T3 and wins by strict
// recency. The oracle folds the event log to v3, so AssertQueryMatches is the
// full assertion — there is no ver_ts tie here for its Seq tiebreak to paper
// over.
func testFreshBaseBeatsStaleDelta(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	target := CreateEvent(wide, map[string]any{"title": "v1", "count": float64(100)})
	bystander := CreateEvent(wide, map[string]any{"title": "bystander", "count": float64(999)})
	mustApplyEvents(ctx, t, env, "fm1 creates", target, bystander)

	waitClockPast(t, target)
	v2 := UpdateEvent(wide, target.RowID, map[string]any{"title": "stale-v2", "count": float64(400000)})
	mustApplyEvents(ctx, t, env, "fm1 update v2", v2)
	assertStrictlyNewer(t, []*Event{target}, []*Event{v2})
	mustFlush(ctx, t, env) // delta: v2 attrs @ ver_ts = T2

	waitClockPast(t, v2)
	v3 := UpdateEvent(wide, target.RowID, map[string]any{"title": "fresh-v3", "count": float64(500000)})
	mustApplyEvents(ctx, t, env, "fm1 update v3", v3)
	assertStrictlyNewer(t, []*Event{v2}, []*Event{v3})

	if _, err := env.RunInit(ctx, wide); err != nil { // base: v3 attrs @ init's ver_ts stamp
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log") // onboarding cleanup (#176) ⇒ cold-only, DuckDB routing

	q := Query{Schema: wide, Sorts: []Sort{{Attr: "count"}}, Limit: 10}
	res := env.AssertQueryMatches(ctx, q) // oracle expects v3 attributes
	if res == nil {
		return
	}
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("expected DuckDB routing, got OLTP: %+v", res.Plan.Routing)
	}
	seen := 0
	for _, rec := range res.Records {
		if rec.RowID == target.RowID {
			seen++
		}
	}
	if seen != 1 { // hard invariant: rn = 1 must yield exactly one surviving copy
		t.Fatalf("target row_id surfaced %d times, want exactly 1 (rn = 1 broke)", seen)
	}
}

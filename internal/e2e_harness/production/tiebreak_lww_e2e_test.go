//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestWarmColdTiebreak (#183, epic #172 Phase 3) probes the federated LWW
// tiebreak when a live BASE (cold init) copy and a live DELTA (cold flush)
// copy of the same row_id carry an EQUAL ver_ts — the tie #210 calls
// "undefined". The dedup ranks
//
//	ORDER BY ver_ts DESC, source_tier_priority DESC, deleted_ts DESC, row_id ASC
//	(advanced_query_template_duckdb.go:76-83)
//
// All parquet rows share tier priority 1; live BASE rows carry deleted_ts = 0
// (init COALESCEs ltbase_deleted_at, init_exporter.go:36) while live DELTA
// rows carry deleted_ts = NULL. Under DuckDB's default NULLS LAST, 0 sorts
// before NULL in a DESC key, so on an equal-ver_ts tie the BASE copy wins
// deterministically. cdc-init stamps base ver_ts from ltbase_created_at
// (init_exporter.go:35) and delta from changed_at, so create->flush->init with
// no intervening update yields identical ver_ts (scenario 1) while
// create->flush->update->init yields equal-ver_ts DIVERGENT values (scenario
// 2). Scenario 3 pins the same asymmetry from the deleted-row side.
func TestWarmColdTiebreak(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"equal_verts_identical_copies", testEqualVertsIdenticalCopies},
		{"equal_verts_divergent_values_probe", testEqualVertsDivergentProbe},
		{"equal_verts_tombstone_wins", testEqualVertsTombstoneWins},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), wide)
		})
	}
}

// testEqualVertsIdenticalCopies pins the hard invariant of the identical-copy
// tie: create -> flush (delta @ create-ts) -> RunInit (base @ ltbase_created_at
// = the same ts) leaves each row with two cold copies sharing ver_ts AND
// attribute values. Whichever copy wins rn = 1, the row set must be exactly the
// three v1 rows — no duplication (both copies surfacing) and no drop. The
// winner identity is irrelevant here (copies are identical), so this is a
// multiplicity invariant, not a scan-order pin.
func testEqualVertsIdenticalCopies(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := []*Event{
		CreateEvent(wide, map[string]any{"title": "copy-a", "count": float64(10)}),
		CreateEvent(wide, map[string]any{"title": "copy-b", "count": float64(20)}),
		CreateEvent(wide, map[string]any{"title": "copy-c", "count": float64(30)}),
	}
	mustApplyEvents(ctx, t, env, "identical-copy creates", creates...)
	mustFlush(ctx, t, env) // delta copies @ create-ts
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log") // cold-only ⇒ DuckDB routing

	q := Query{Schema: wide, Sorts: []Sort{{Attr: "count"}}, Limit: 10}
	res := env.AssertQueryMatches(ctx, q) // oracle also pins v1 values
	if res == nil {
		return
	}
	if len(res.Records) != 3 {
		t.Fatalf("identical-copy tie returned %d rows, want exactly 3 (a base/delta copy duplicated or dropped)", len(res.Records))
	}
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("expected DuckDB routing, got OLTP: %+v", res.Plan.Routing)
	}
	for i := 0; i < 10; i++ { // no flapping at the row-multiplicity level
		again, err := env.Query(ctx, q)
		if err != nil {
			t.Fatalf("repeat %d: %v", i, err)
		}
		if len(again.Records) != 3 || again.Total != 3 {
			t.Fatalf("repeat %d returned %d records / total %d, want 3/3 (equal-ver_ts tie flapped row multiplicity — engine finding)",
				i, len(again.Records), again.Total)
		}
	}
}

// testEqualVertsDivergentProbe is #210's failure-mode-2 construction, built
// entirely from production verbs: create stale-v1 + a bystander -> flush (delta
// v1 @ T1, deleted_ts NULL) -> update to fresh-v2 -> RunInit (base carries the
// v2 attrs but ver_ts = ltbase_created_at = T1, deleted_ts 0). After clearing
// change_log the row is cold-only with two live copies at an EQUAL ver_ts and
// DIVERGENT values. The probe settles empirically whether the tie is
// deterministic; it deliberately does NOT route through AssertQueryMatches
// because the oracle's Seq tiebreak would declare v2 the truth and beg the
// question. The only hard invariant is exactly-once (rn = 1).
func testEqualVertsDivergentProbe(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	target := CreateEvent(wide, map[string]any{"title": "stale-v1", "count": float64(111)})
	bystander := CreateEvent(wide, map[string]any{"title": "bystander", "count": float64(999)})
	mustApplyEvents(ctx, t, env, "divergent create + bystander", target, bystander)
	mustFlush(ctx, t, env) // delta v1 @ T1

	upd := UpdateEvent(wide, target.RowID, map[string]any{"title": "fresh-v2", "count": float64(222)})
	mustApplyEvents(ctx, t, env, "divergent update", upd)
	assertStrictlyNewer(t, []*Event{target}, []*Event{upd}) // v2 is a genuinely later write

	if _, err := env.RunInit(ctx, wide); err != nil { // base v2 attrs @ ver_ts = create T1
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log") // cold-only ⇒ DuckDB routing

	q := Query{Schema: wide, Sorts: []Sort{{Attr: "count"}}, Limit: 10}
	winners := make(map[string]int)
	for i := 0; i < 20; i++ {
		res, err := env.Query(ctx, q)
		if err != nil {
			t.Fatalf("probe run %d: %v", i, err)
		}
		if i == 0 && !res.Plan.Routing.UseDuckDB {
			t.Fatalf("expected DuckDB routing, got OLTP: %+v", res.Plan.Routing)
		}
		seen := 0
		title := ""
		for _, rec := range res.Records {
			if rec.RowID == target.RowID {
				seen++
				title = rec.TextItems["text_01"]
			}
		}
		if seen != 1 { // hard invariant: rn = 1 must yield exactly one surviving copy
			t.Fatalf("probe run %d: target row_id appeared %d times, want exactly 1 (rn = 1 broke)", i, seen)
		}
		winners[title]++
	}

	// Adjudication A3 — arm A (finalized from the 20-run probe: fresh-v2 won
	// 20/20). The base copy (fresh-v2, deleted_ts 0) beats the live delta
	// (stale-v1, deleted_ts NULL) on every run: 0 sorts before NULL under
	// deleted_ts DESC + DuckDB's default NULLS LAST, so on an equal ver_ts the
	// base wins deterministically. This is a CHARACTERIZATION of the
	// #174-pinned init/flush export asymmetry, NOT a designed contract — #210
	// still calls this tie undefined, so this assertion must be revisited when
	// #210 lands a deliberate tie rule.
	t.Logf("arm A: equal-ver_ts winner distribution %v", winners)
	if winners["fresh-v2"] != 20 {
		t.Fatalf("equal-ver_ts base/delta tie: fresh-v2 (base) won %d/20 runs, want 20 — the deterministic deleted_ts 0 < NULL tiebreak regressed (revisit against #210)", winners["fresh-v2"])
	}
}

// testEqualVertsTombstoneWins pins the deterministic side of the tie: on an
// equal ver_ts a tombstone (deleted_ts = T1) must beat a live copy (deleted_ts
// NULL/0) under deleted_ts DESC, hiding the row. Construction: create victim +
// bystander -> flush (delta live v1 @ T1) -> delete victim -> restamp the
// unflushed tombstone slot back to the create ts (forcing the equal-ver_ts tie
// production's monotonic clock would not) -> flush (tombstone parquet @ ver_ts
// T1, deleted_ts T1) -> clear change_log. This pins InjectRestore's documented
// same-millisecond assumption (restore.go: "ts must be > DeletedAt") from the
// read side.
func testEqualVertsTombstoneWins(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	victim := CreateEvent(wide, map[string]any{"title": "victim", "count": float64(10)})
	bystander := CreateEvent(wide, map[string]any{"title": "bystander", "count": float64(20)})
	mustApplyEvents(ctx, t, env, "tombstone creates", victim, bystander)
	mustFlush(ctx, t, env) // delta: live v1 @ T1

	del := DeleteEvent(wide, victim.RowID)
	mustApplyEvents(ctx, t, env, "tombstone delete", del)

	// Restamp the unflushed tombstone slot to the create ts so its ver_ts ties
	// the flushed live delta; change_log columns/schema-ID verified against
	// restore.go:49-54 (schema_id int16, changed_at, deleted_at, flushed_at).
	victimT := victim.ChangedAt // T1, read back at apply time
	env.ExecSQL(ctx,
		"UPDATE change_log SET changed_at = $1, deleted_at = $2 WHERE schema_id = $3 AND row_id = $4 AND flushed_at = 0",
		victimT, victimT, wide.ID, victim.RowID)
	del.ChangedAt, del.DeletedAt = victimT, victimT // mirror for the oracle (Seq breaks the tie)

	// Guard: prove the restamp hit the unflushed tombstone slot. Without this,
	// a silent no-op (e.g. after a flush-semantics change) would leave the
	// tombstone at its real, later ver_ts — the victim would still be hidden by
	// strict recency and the equal-ver_ts deleted_ts tie this scenario exists
	// to pin would never be exercised.
	var restamped int
	if err := env.Pool.QueryRow(ctx,
		"SELECT count(*) FROM change_log WHERE row_id = $1 AND schema_id = $2 AND flushed_at = 0 AND changed_at = $3 AND deleted_at = $3",
		victim.RowID, wide.ID, victimT).Scan(&restamped); err != nil {
		t.Fatalf("restamp verification query: %v", err)
	}
	if restamped != 1 {
		t.Fatalf("tombstone restamp matched %d rows, want 1 — the equal-ver_ts construction did not take", restamped)
	}

	mustFlush(ctx, t, env)                     // tombstone parquet @ ver_ts T1, deleted_ts T1
	env.ExecSQL(ctx, "DELETE FROM change_log") // cold-only ⇒ DuckDB routing

	// Positive control first: the bystander is visible on the DuckDB path, so a
	// zero-row victim probe cannot be a false green from a broken read path.
	bystanderQ := Query{Schema: wide, Filters: []Filter{{Attr: "title", Op: "equals", Value: "bystander"}}, Limit: 10}
	assertRowCount(ctx, t, env, "bystander control", bystanderQ, 1)
	if res, err := env.Query(ctx, bystanderQ); err != nil {
		t.Fatalf("routing probe: %v", err)
	} else if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("expected DuckDB routing, got OLTP: %+v", res.Plan.Routing)
	}

	// Probe: the tombstone (deleted_ts T1 > NULL/0) wins deleted_ts DESC on the
	// ver_ts tie, so the victim is invisible.
	victimQ := Query{Schema: wide, Filters: []Filter{{Attr: "title", Op: "equals", Value: "victim"}}, Limit: 10}
	assertZeroRows(ctx, t, env, "victim tombstone", victimQ)
	for i := 0; i < 5; i++ { // no flapping: the tombstone must never lose the tie
		res, err := env.Query(ctx, victimQ)
		if err != nil {
			t.Fatalf("repeat %d: %v", i, err)
		}
		if len(res.Records) != 0 {
			t.Fatalf("repeat %d: victim visible (%d rows) — tombstone lost the equal-ver_ts tie (engine finding)", i, len(res.Records))
		}
	}
}

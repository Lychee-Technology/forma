//go:build e2e

package production

import (
	"context"
	"testing"
	"time"
)

// TestMonotonicVersionsCloseSameMillisecondTies (#274 review P1) pins the
// write-side strictly-ordered version contract end-to-end: an update's (or
// delete's) effective version is strictly greater than the row's previous
// one even when the wall clock has NOT advanced past it — the repository
// computes GREATEST($now, ltbase_updated_at + 1) in PostgreSQL and stamps
// change_log from the RETURNING'd value. Without this, two serialized
// same-millisecond writes tie at the clock read, and the cold-tier LWW rank
// (all parquet at tier priority 1, live deleted_ts uniformly 0 post-#274)
// has no discriminator left: a stale flushed copy could be served over the
// current one.
//
// The wall clock cannot be frozen through the harness API, so each scenario
// forces the collision by restamping entity_main's ltbase_updated_at a few
// seconds AHEAD of the clock (the ExecSQL escape hatch, mirroring the
// tiebreak suite's restamps) before writing through the real API. One
// systemic consequence is exercised deliberately: a clock-ahead version is
// not flush-eligible until the wall clock passes it (the export snapshot
// filters changed_at <= snapshotTS), so both scenarios wait the drift out
// before flushing.
func TestMonotonicVersionsCloseSameMillisecondTies(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"update_advances_past_clock_ahead_version", testUpdateAdvancesPastClockAheadVersion},
		{"delete_outranks_clock_ahead_live_version", testDeleteOutranksClockAheadLiveVersion},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), wide)
		})
	}
}

// restampMainVersionAhead moves the row's stored version aheadMillis past the
// current wall clock and returns the restamped version. The guard count
// proves the restamp took — a silent no-op would leave the scenario probing
// ordinary strict recency instead of the GREATEST branch.
func restampMainVersionAhead(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, ev *Event, aheadMillis int64) int64 {
	t.Helper()
	ahead := time.Now().UnixMilli() + aheadMillis
	env.ExecSQL(ctx,
		"UPDATE entity_main SET ltbase_updated_at = $1 WHERE ltbase_schema_id = $2 AND ltbase_row_id = $3",
		ahead, schema.ID, ev.RowID)
	var n int
	if err := env.Pool.QueryRow(ctx,
		"SELECT count(*) FROM entity_main WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2 AND ltbase_updated_at = $3",
		schema.ID, ev.RowID, ahead).Scan(&n); err != nil {
		t.Fatalf("restamp verification query: %v", err)
	}
	if n != 1 {
		t.Fatalf("clock-ahead restamp matched %d rows, want 1 — the GREATEST construction did not take", n)
	}
	return ahead
}

// waitClockPastMillis blocks until the wall clock is strictly past the given
// millisecond, making a clock-ahead version flush-eligible.
func waitClockPastMillis(t *testing.T, millis int64) {
	t.Helper()
	for time.Now().UnixMilli() <= millis {
		time.Sleep(time.Millisecond)
	}
}

// testUpdateAdvancesPastClockAheadVersion drives the review's stale-copy
// sequence with the collision forced: a flushed stale delta, a row whose
// stored version sits ahead of the wall clock, then an ordinary API update.
// The update must land at exactly ahead+1 in BOTH stores (pre-fix it stamped
// the clock read, REGRESSING the row's version), and after flush + init +
// change_log truncation the cold read must serve the fresh value with no
// flapping.
func testUpdateAdvancesPastClockAheadVersion(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	target := CreateEvent(wide, map[string]any{"title": "stale-a", "count": float64(100)})
	bystander := CreateEvent(wide, map[string]any{"title": "mono-bystander", "count": float64(999)})
	mustApplyEvents(ctx, t, env, "monotonic creates", target, bystander)
	mustFlush(ctx, t, env) // delta A: live "stale-a" @ its create ts

	ahead := restampMainVersionAhead(ctx, t, env, wide, target, 2000)

	upd := UpdateEvent(wide, target.RowID, map[string]any{"title": "fresh-b", "count": float64(200)})
	mustApplyEvents(ctx, t, env, "monotonic update", upd)

	// The effective version must be ahead+1 — strictly past the clock-ahead
	// previous version, not the (smaller) wall-clock read.
	if upd.ChangedAt != ahead+1 {
		t.Fatalf("update stamped change_log at %d, want %d (GREATEST over the row's previous version)", upd.ChangedAt, ahead+1)
	}
	var mainVer int64
	if err := env.Pool.QueryRow(ctx,
		"SELECT ltbase_updated_at FROM entity_main WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		wide.ID, target.RowID).Scan(&mainVer); err != nil {
		t.Fatalf("read back ltbase_updated_at: %v", err)
	}
	if mainVer != upd.ChangedAt {
		t.Fatalf("entity_main version %d != change_log version %d — the #210 same-source contract broke", mainVer, upd.ChangedAt)
	}

	waitClockPastMillis(t, upd.ChangedAt) // clock-ahead versions flush only once the clock passes them
	mustFlush(ctx, t, env)                // delta B: live "fresh-b" @ ahead+1
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log") // cold-only ⇒ DuckDB routing

	// Positive control: the bystander is visible on the DuckDB path.
	bystanderQ := Query{Schema: wide, Filters: []Filter{{Attr: "title", Op: "equals", Value: "mono-bystander"}}, Limit: 10}
	assertRowCount(ctx, t, env, "monotonic bystander control", bystanderQ, 1)

	// The stale flushed copy must never be served: fresh-b strictly outranks
	// it, and base/delta copies of fresh-b are value-identical.
	staleQ := Query{Schema: wide, Filters: []Filter{{Attr: "title", Op: "equals", Value: "stale-a"}}, Limit: 10}
	freshQ := Query{Schema: wide, Filters: []Filter{{Attr: "title", Op: "equals", Value: "fresh-b"}}, Limit: 10}
	assertZeroRows(ctx, t, env, "stale copy", staleQ)
	assertRowCount(ctx, t, env, "fresh copy", freshQ, 1)
	for i := 0; i < 5; i++ { // no flapping: strict recency leaves no tie to lose
		res, err := env.Query(ctx, staleQ)
		if err != nil {
			t.Fatalf("repeat %d: %v", i, err)
		}
		if len(res.Records) != 0 {
			t.Fatalf("repeat %d: stale copy visible (%d rows) — a same-version tie leaked through the write path", i, len(res.Records))
		}
	}
}

// testDeleteOutranksClockAheadLiveVersion pins the lost-delete guard: a hard
// delete of a row whose version sits ahead of the wall clock must stamp its
// tombstone strictly past that version (pre-fix it stamped the clock read,
// which the live version outranked — the delete would lose the LWW fold and
// the row resurface).
func testDeleteOutranksClockAheadLiveVersion(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	victim := CreateEvent(wide, map[string]any{"title": "mono-victim", "count": float64(10)})
	control := CreateEvent(wide, map[string]any{"title": "mono-control", "count": float64(20)})
	mustApplyEvents(ctx, t, env, "delete-guard creates", victim, control)
	mustFlush(ctx, t, env) // delta: both live @ create ts

	ahead := restampMainVersionAhead(ctx, t, env, wide, victim, 2000)

	del := DeleteEvent(wide, victim.RowID)
	mustApplyEvents(ctx, t, env, "delete-guard delete", del)
	if del.ChangedAt != ahead+1 || del.DeletedAt != ahead+1 {
		t.Fatalf("tombstone stamped (changed_at=%d, deleted_at=%d), want both %d (strictly past the clock-ahead live version)",
			del.ChangedAt, del.DeletedAt, ahead+1)
	}

	waitClockPastMillis(t, del.ChangedAt)
	mustFlush(ctx, t, env)                     // tombstone parquet @ ahead+1
	env.ExecSQL(ctx, "DELETE FROM change_log") // cold-only ⇒ DuckDB routing

	controlQ := Query{Schema: wide, Filters: []Filter{{Attr: "title", Op: "equals", Value: "mono-control"}}, Limit: 10}
	assertRowCount(ctx, t, env, "delete-guard control", controlQ, 1)

	victimQ := Query{Schema: wide, Filters: []Filter{{Attr: "title", Op: "equals", Value: "mono-victim"}}, Limit: 10}
	assertZeroRows(ctx, t, env, "deleted victim", victimQ)
	for i := 0; i < 5; i++ { // no flapping: the tombstone strictly outranks the live version
		res, err := env.Query(ctx, victimQ)
		if err != nil {
			t.Fatalf("repeat %d: %v", i, err)
		}
		if len(res.Records) != 0 {
			t.Fatalf("repeat %d: deleted row visible (%d rows) — the tombstone lost to the clock-ahead live version", i, len(res.Records))
		}
	}
}

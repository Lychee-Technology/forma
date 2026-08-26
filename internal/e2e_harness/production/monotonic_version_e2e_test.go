//go:build e2e

package production

import (
	"context"
	"testing"
	"time"

	internal "github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/model"
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
// tiebreak suite's restamps) before writing through the real API. Both
// scenarios then flush IMMEDIATELY, while the version is still ahead of the
// wall clock, and assert the flush actually exported and marked the row:
// the export snapshot covers the batch's version watermark and marking is
// exact per listed (row_id, version), so a clock-ahead version must not
// starve CDC (review round 2 P1 — pre-fix the wall-clock snapshot excluded
// the row from both export and mark, shipping an empty delta and leaving
// the row dirty forever under sustained lead).
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

// clockAheadLeadMs is how far past the wall clock the restamps below move a
// row's version. The exact-version assertions (ahead+1 / ahead+2) hold only
// while the wall clock is still behind the restamp, so this is a budget for
// everything between the restamp and the asserted write — which for the
// recreate race includes a full flush list+export leg. Under -race that leg
// alone took ~2.1s (nightly production-race run, #410), blowing the original
// 2s lead; 20s keeps the GREATEST branch deterministically exercised on
// 2-core CI runners. Nothing waits for the clock to catch up, so the size
// costs no wall time.
const clockAheadLeadMs = 20_000

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

	ahead := restampMainVersionAhead(ctx, t, env, wide, target, clockAheadLeadMs)

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

	// Flush IMMEDIATELY, while the version is still ahead of the wall clock:
	// the row must be exported and marked in this run (round-2 P1 — pre-fix
	// this shipped an empty delta and left the row dirty).
	flush := mustFlush(ctx, t, env) // delta B: live "fresh-b" @ ahead+1
	if flush.UnflushedAfter != 0 {
		t.Fatalf("flush left %d rows dirty, want 0 — the clock-ahead version was not marked", flush.UnflushedAfter)
	}
	assertSoleLiveVersion(ctx, t, env, soleParquetKey(t, flush), upd)
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

	ahead := restampMainVersionAhead(ctx, t, env, wide, victim, clockAheadLeadMs)

	del := DeleteEvent(wide, victim.RowID)
	mustApplyEvents(ctx, t, env, "delete-guard delete", del)
	if del.ChangedAt != ahead+1 || del.DeletedAt != ahead+1 {
		t.Fatalf("tombstone stamped (changed_at=%d, deleted_at=%d), want both %d (strictly past the clock-ahead live version)",
			del.ChangedAt, del.DeletedAt, ahead+1)
	}

	// Flush IMMEDIATELY: the clock-ahead tombstone must be exported and
	// marked in this run (round-2 P1).
	flush := mustFlush(ctx, t, env) // tombstone parquet @ ahead+1
	if flush.UnflushedAfter != 0 {
		t.Fatalf("flush left %d rows dirty, want 0 — the clock-ahead tombstone was not marked", flush.UnflushedAfter)
	}
	assertTombstoneParquet(ctx, t, env, soleParquetKey(t, flush), del)
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

// TestRecreateBetweenExportAndMark (#274 review round 3 P1) drives the
// delete/recreate race straight through the export→mark window: a
// clock-ahead tombstone is selected and exported, the flush pauses at its
// CopyObject, the row is recreated while the flush hangs, and the mark then
// runs against a slot the recreate overwrote. The recreate goes through the
// repository contract (InsertPersistentRecord) directly: the EntityManager
// surface always mints a fresh UUIDv7, so row_id reuse is the repository's
// seam — the one restore/import-style callers use — and the harness mirrors
// the event into the oracle log like the tiebreak suite's restamps.
// Two write-side guarantees keep the recreate alive: nextRowVersion stamps
// it strictly above the retained tombstone (pre-fix it stamped wall time,
// REGRESSING below a clock-ahead tombstone), and MarkFlushedVersions is
// exact equality against the listed version (pre-fix `<=` would have
// accepted the regressed slot, clearing the dirty barrier for a payload no
// parquet holds — the cold tombstone would hide the recreate forever).
func TestRecreateBetweenExportAndMark(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	victim := CreateEvent(wide, map[string]any{"title": "doomed", "count": float64(10)})
	control := CreateEvent(wide, map[string]any{"title": "recreate-control", "count": float64(20)})
	mustApplyEvents(ctx, t, env, "recreate-race creates", victim, control)
	mustFlush(ctx, t, env) // delta #1: both live

	ahead := restampMainVersionAhead(ctx, t, env, wide, victim, clockAheadLeadMs)
	del := DeleteEvent(wide, victim.RowID)
	mustApplyEvents(ctx, t, env, "recreate-race delete", del)
	if del.ChangedAt != ahead+1 {
		t.Fatalf("tombstone stamped %d, want %d", del.ChangedAt, ahead+1)
	}

	// The paused flush lists and exports the tombstone @ ahead+1, then hangs
	// before mark; the recreate lands while it hangs. title/count are the
	// wide schema's text_01/integer_01 bindings (e2e_wide_attributes.json).
	repo := internal.NewDBPersistentRecordRepository(env.Pool, nil)
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_data", ChangeLog: "change_log"}
	rec := &model.PersistentRecord{
		SchemaID:   wide.ID,
		RowID:      victim.RowID,
		TextItems:  map[string]string{"text_01": "reborn"},
		Int32Items: map[string]int32{"integer_01": 30},
	}
	report, err := runPausedFlush(ctx, t, env, func() {
		if err := repo.InsertPersistentRecord(ctx, tables, rec); err != nil {
			t.Errorf("recreate during paused flush: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("paused flush: %v", err)
	}
	if rec.UpdatedAt != ahead+2 {
		t.Fatalf("recreate stamped %d, want %d (strictly above the retained tombstone)", rec.UpdatedAt, ahead+2)
	}
	// Mirror the recreate into the oracle event log (the restamp-mirroring
	// pattern) so oracle-checked probes stay meaningful.
	env.eventSeq++
	recreate := &Event{Kind: EventCreate, Schema: wide, RowID: victim.RowID,
		Attrs: map[string]any{"title": "reborn", "count": float64(30)},
		Seq:   env.eventSeq, ChangedAt: rec.UpdatedAt}
	env.events = append(env.events, recreate)
	// The recreate overwrote the listed slot, so the mark must leave it
	// dirty: its payload is in no parquet file yet.
	if report.UnflushedAfter != 1 {
		t.Fatalf("paused flush left %d rows dirty, want 1 (the recreated slot must not be marked)", report.UnflushedAfter)
	}

	// While dirty, the hot tier masks the exported tombstone: the recreate is
	// visible.
	rebornQ := Query{Schema: wide, Filters: []Filter{{Attr: "title", Op: "equals", Value: "reborn"}}, Limit: 10}
	assertRowCount(ctx, t, env, "recreate visible while dirty", rebornQ, 1)

	// A clean retry drains the recreate into its own delta.
	retry := runCleanRetry(ctx, t, env)
	retryFinals, _ := splitKeys(retry.NewObjects)
	if len(retryFinals) != 1 {
		t.Fatalf("retry must promote exactly one delta, got %v", retryFinals)
	}
	assertLiveParquetRow(ctx, t, env, retryFinals[0], recreate)

	// Cold-only: the recreate @ ahead+2 must outrank the flushed tombstone @
	// ahead+1 — no resurrection of the delete, no hidden recreate.
	env.ExecSQL(ctx, "DELETE FROM change_log")
	controlQ := Query{Schema: wide, Filters: []Filter{{Attr: "title", Op: "equals", Value: "recreate-control"}}, Limit: 10}
	assertRowCount(ctx, t, env, "recreate-race control", controlQ, 1)
	assertRowCount(ctx, t, env, "recreate wins cold", rebornQ, 1)
	doomedQ := Query{Schema: wide, Filters: []Filter{{Attr: "title", Op: "equals", Value: "doomed"}}, Limit: 10}
	assertZeroRows(ctx, t, env, "pre-delete payload", doomedQ)
	for i := 0; i < 5; i++ { // no flapping: strict ordering leaves no tie
		res, err := env.Query(ctx, rebornQ)
		if err != nil {
			t.Fatalf("repeat %d: %v", i, err)
		}
		if len(res.Records) != 1 {
			t.Fatalf("repeat %d: recreate returned %d rows, want 1 (lost to its own tombstone)", i, len(res.Records))
		}
	}
}

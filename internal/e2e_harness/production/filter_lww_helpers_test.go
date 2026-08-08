//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Shared fixture for the filter-LWW production suites (#178, #215).
// filter_lww_e2e_test.go, filter_lww_matrix_e2e_test.go, and
// filter_lww_tombstone_e2e_test.go build their row generations and controls
// from this single implementation so fixtures and timing guards cannot drift
// between scenarios. Scenario-specific assertions stay in their own files.

// buildOpenCreates builds n v1 rows in the canonical "open" shape shared by
// the stale-filter scenarios: title open-%02d and count 100000+i.
func buildOpenCreates(wide SchemaRef, n int) []*Event {
	creates := make([]*Event, 0, n)
	for i := 0; i < n; i++ {
		creates = append(creates, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("open-%02d", i),
			"count": float64(100000 + i),
		}))
	}
	return creates
}

// buildClosedUpdates flips each create to its non-matching v2 on the same
// row_id: title closed-%02d and count 200000+i.
func buildClosedUpdates(wide SchemaRef, creates []*Event) []*Event {
	v2 := make([]*Event, 0, len(creates))
	for i, c := range creates {
		v2 = append(v2, UpdateEvent(wide, c.RowID, map[string]any{
			"title": fmt.Sprintf("closed-%02d", i),
			"count": float64(200000 + i),
		}))
	}
	return v2
}

// mustApplyEvents applies events, failing the test with the given label.
func mustApplyEvents(ctx context.Context, t *testing.T, env *Env, label string, events ...*Event) {
	t.Helper()
	if err := env.ApplyEvents(ctx, events...); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
}

// assertRowCount runs an oracle-checked query and fails fast unless it
// returns exactly want rows. The load-bearing positive controls use this to
// guard the zero-row probes against a broken read path returning false
// greens.
func assertRowCount(ctx context.Context, t *testing.T, env *Env, name string, q Query, want int) {
	t.Helper()
	if res := env.AssertQueryMatches(ctx, q); res != nil && len(res.Records) != want {
		t.Fatalf("%s returned %d rows, want %d", name, len(res.Records), want)
	}
}

// waitClockPast blocks until the process wall clock is strictly past every
// given event's ChangedAt. The repository stamps changed_at from this same
// process clock (nowMillis, postgres_persistent_repository.go), so a write
// issued after this returns lands on a strictly later millisecond — making a
// subsequent assertStrictlyNewer deterministic instead of racing the clock.
func waitClockPast(t *testing.T, evs ...*Event) {
	t.Helper()
	for _, ev := range evs {
		waitClockPastMillis(ev.ChangedAt)
	}
}

// waitClockPastMillis is waitClockPast against a bare millisecond anchor: a
// flush snapshot (SelectBatchRowIDs' snapshot := time.Now().UnixMilli()) or any
// other timestamp not carried by an Event. It takes no *testing.T so CDC hooks
// can call it — a hook reports failure through the flush error, never through
// t.* (#276).
func waitClockPastMillis(ms int64) {
	for time.Now().UnixMilli() <= ms {
		time.Sleep(time.Millisecond)
	}
}

// assertStrictlyNewer fails fast when any new version's changed_at is not
// strictly after its predecessor's at millisecond resolution — otherwise an
// LWW probe degrades into an undefined equal-ver_ts tie (#210 fixed the init
// stamp; equal-timestamp DIVERGENT versions of a row remain unranked).
//
// Since #274 the write path guarantees this for pairs on the SAME row_id:
// ltbase_updated_at = GREATEST($now, ltbase_updated_at + 1) with RETURNING
// (postgres_persistent_repository_main_table.go), computeTombstoneStamp for
// deletes and nextRowVersion for recreates each raise the successor above the
// row's previous version even inside one millisecond, and change_log is stamped
// from that same effective value. For same-row pairs this guard is therefore a
// cheap redundant assertion, and no wait is needed before the later write.
//
// It stays load-bearing for pairs the write path cannot order, and those are the
// only sites that need waitClockPast / waitClockPastMillis first (#276): pairs
// on DIFFERENT row_ids (each write takes its own clock read, including two
// writes inside one ApplyEvents batch), and comparisons against an independent
// anchor such as a flush snapshot.
func assertStrictlyNewer(t *testing.T, olds, news []*Event) {
	t.Helper()
	for i := range olds {
		if news[i].ChangedAt <= olds[i].ChangedAt {
			t.Fatalf("row %d: newer changed_at %d not strictly after older %d",
				i, news[i].ChangedAt, olds[i].ChangedAt)
		}
	}
}

// assertZeroRows runs an oracle-checked query that must return no rows: the
// predicate matches only a superseded version, so any row in the result is a
// filter-before-LWW resurrection.
func assertZeroRows(ctx context.Context, t *testing.T, env *Env, name string, q Query) {
	t.Helper()
	if res := env.AssertQueryMatches(ctx, q); res != nil && len(res.Records) != 0 {
		t.Errorf("%s: stale probe returned %d rows, want 0", name, len(res.Records))
	}
}

//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"
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

// assertStrictlyNewer fails fast when any new version's changed_at is not
// strictly after its predecessor's at millisecond resolution — otherwise an
// LWW probe degrades into the undefined equal-ver_ts tie tracked by #210.
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

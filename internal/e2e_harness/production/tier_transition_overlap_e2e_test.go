//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/compaction"
)

// testHotOverDeltaOverBase continues TestTierTransitionParity from a
// rewritten base holding v2 (rows 0-3) and v1 (rows 4-7). Row 0 gets v3 in
// a delta and then an unflushed v4, so three versions overlap; row 4 gets
// v3 in a delta over its v1 base; row 5 gets an unflushed v4 over its v1
// base. Only the newest version of each row may be visible.
func testHotOverDeltaOverBase(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, rows []*transitionRow) {
	t.Helper()
	baseTitle0 := rows[0].latest.Attrs["title"].(string)
	updateTransitionRows(ctx, t, env, wide, []*transitionRow{rows[0], rows[4]}, 3)
	mustFlush(ctx, t, env)
	assertTierCounts(ctx, t, env, wide, "v3 flushed over rewritten base", 1, 1)
	deltaTitle0 := rows[0].latest.Attrs["title"].(string)

	beforeHot := len(env.events)
	updateTransitionRows(ctx, t, env, wide, []*transitionRow{rows[0], rows[5]}, 4)
	assertDirtyRows(ctx, t, env, wide, 2)

	label := "v4 hot over v3 delta over v2 base"
	assertTransitionQueries(ctx, t, env, wide, label)
	assertStaleOracleDisagrees(ctx, t, env, wide, beforeHot, rows[0].id, rows[5].id)

	// The live control first, so the zero-row probes below cannot pass on a
	// broken read path. Then every superseded title: the dirty-set
	// anti-join must drop the delta and base copies before the filter sees
	// them, and LWW must drop the base copy under the delta.
	assertTitleRows(ctx, t, env, wide, rows[0].latest.Attrs["title"].(string), 1)
	for _, stale := range []string{deltaTitle0, baseTitle0, "v1-5", "v1-4"} {
		assertTitleRows(ctx, t, env, wide, stale, 0)
	}

	mustFlush(ctx, t, env)
	assertTierCounts(ctx, t, env, wide, "v4 flushed", 1, 2)
	assertTransitionQueries(ctx, t, env, wide, "v4 flushed to delta")
}

// testTombstonesAcrossTiers deletes row 1, whose live version is only in
// the base, and row 0, whose newest version is in a delta. The rows must
// be invisible while the tombstones are hot, after they are flushed, and
// after a rewrite, which must drop them from the new base altogether.
func testTombstonesAcrossTiers(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, rows []*transitionRow) {
	t.Helper()
	deleted := []*transitionRow{rows[1], rows[0]}
	for _, row := range deleted {
		mustApplyEvents(ctx, t, env, fmt.Sprintf("delete %s", row.id), DeleteEvent(wide, row.id))
	}
	assertDirtyRows(ctx, t, env, wide, 2)
	assertTransitionQueries(ctx, t, env, wide, "hot tombstones")
	for _, row := range deleted {
		assertTitleRows(ctx, t, env, wide, row.latest.Attrs["title"].(string), 0)
	}

	mustFlush(ctx, t, env)
	assertTransitionQueries(ctx, t, env, wide, "flushed tombstones")
	for _, row := range deleted {
		assertTitleRows(ctx, t, env, wide, row.latest.Attrs["title"].(string), 0)
	}

	result, err := env.RunCompaction(ctx, wide)
	requireCompaction(t, "tombstone rewrite", result, err, compaction.RewriteApplied)
	assertTierCounts(ctx, t, env, wide, "after tombstone rewrite", 1, 0)
	winners := make(map[uuid.UUID]*Event, len(rows))
	for _, row := range rows[2:] {
		winners[row.id] = row.latest
	}
	assertRewrittenBase(ctx, t, env, result.NewBaseKey, winners, []uuid.UUID{rows[0].id, rows[1].id})
	assertBaseAmounts(ctx, t, env, result.NewBaseKey, rows[2:])
	assertTransitionQueries(ctx, t, env, wide, "tombstones compacted away")
}

// assertStaleOracleDisagrees is the end-to-end negative control: an oracle
// that never saw the hot writes (the event log cut at beforeHot) must
// disagree with the engine's real answer on exactly the rows those writes
// changed. If it agreed, the parity checks above would be blind to a merge
// that served the older version.
func assertStaleOracleDisagrees(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, beforeHot int, changed ...uuid.UUID) {
	t.Helper()
	q := Query{Schema: wide, Limit: 100}
	actual, err := env.Query(ctx, q)
	if err != nil {
		t.Fatalf("negative control query: %v", err)
	}
	cache, ok := env.Metadata.GetSchemaCacheByID(wide.ID)
	if !ok {
		t.Fatalf("no metadata cache for schema %d", wide.ID)
	}
	stale, err := ExpectedStateFromEvents(env.events[:beforeHot], wide, cache)
	if err != nil {
		t.Fatalf("build stale oracle: %v", err)
	}
	expected, err := stale.Run(q)
	if err != nil {
		t.Fatalf("run stale oracle: %v", err)
	}
	diff := computeDiff(q, expected, actual, cache)
	for _, id := range changed {
		if !hasAttrMismatch(diff, id, "title") || !hasAttrMismatch(diff, id, "count") {
			t.Fatalf("negative control: the pre-hot oracle does not flag row %s's title and count; diff %+v", id, diff)
		}
	}
	for _, m := range diff.Attrs {
		if !containsID(changed, m.RowID) {
			t.Fatalf("negative control: unexpected mismatch on unchanged row %s attr %s", m.RowID, m.Attr)
		}
	}
}

func containsID(ids []uuid.UUID, id string) bool {
	for _, candidate := range ids {
		if candidate.String() == id {
			return true
		}
	}
	return false
}

// assertTitleRows runs an oracle-checked title filter and also pins the row
// count, so a probe expecting zero rows cannot pass because both sides
// agree on a wrong non-zero answer.
func assertTitleRows(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, title string, want int) {
	t.Helper()
	result := env.AssertQueryMatches(ctx, Query{
		Schema: wide, Filters: []Filter{{Attr: "title", Value: title}}, Limit: 10,
	})
	if result != nil && len(result.Records) != want {
		t.Fatalf("title %q: %d rows, want %d", title, len(result.Records), want)
	}
}

func assertDirtyRows(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, want int64) {
	t.Helper()
	var dirty int64
	if err := env.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM change_log WHERE schema_id = $1 AND flushed_at = 0", wide.ID).Scan(&dirty); err != nil {
		t.Fatalf("count dirty rows: %v", err)
	}
	if dirty != want {
		t.Fatalf("dirty set holds %d rows, want %d", dirty, want)
	}
}

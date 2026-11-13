//go:build e2e

package production

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// TestHotKeysQueryConditionDSLParity is pgx-hot-leg-plan.md step 4's
// acceptance: the condition-DSL parity cases of
// TestConditionDSLOperatorParityAcrossTiers (in, not_in, is_null and
// is_not_null over every storage form of e2e_wide, and the list presence
// cases) run against the hot-keys query and the PostgreSQL route, and select
// the same rows.
//
// "all hot" writes the parity rows' final states without a flush, so every
// row is in the dirty window and the hot-keys query must answer exactly what
// the route answers. "tiers" reuses the parity test's cold/warm/hot layout:
// there the hot keys must be the route's rows that are in the window, at the
// widest cutoff and at the strict one, read in one snapshot.
func TestHotKeysQueryConditionDSLParity(t *testing.T) {
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	t.Run("all hot", func(t *testing.T) {
		env := NewEnv(t, SharedCluster(t))
		rows := seedParityHot(ctx, t, env, wide)
		forEachParityCase(rows, func(f Filter, want []uuid.UUID) {
			pg := pgRouteIDs(ctx, t, env, wide, f)
			assertSameIDs(t, f, "the PostgreSQL route", pg, want)
			hot, window := hotKeyIDsInWindow(ctx, t, env, wide, f, 0)
			assertSameIDs(t, f, "the dirty window", window, rows.ids())
			assertSameIDs(t, f, "the hot-keys query", hot, pg)
		})
	})

	t.Run("tiers", func(t *testing.T) {
		env := NewEnv(t, SharedCluster(t))
		rows := seedParityTiers(ctx, t, env, wide)
		for _, cutoff := range []int64{0, math.MaxInt64} {
			forEachParityCase(rows, func(f Filter, want []uuid.UUID) {
				pg := pgRouteIDs(ctx, t, env, wide, f)
				assertSameIDs(t, f, "the PostgreSQL route", pg, want)
				hot, window := hotKeyIDsInWindow(ctx, t, env, wide, f, cutoff)
				assertSameIDs(t, f, fmt.Sprintf("the hot-keys query at cutoff %d", cutoff), hot, intersectIDs(pg, window))
			})
		}
	})

	// Text ranges compare bytes on the hot-keys query as on the route (#728):
	// gt:B matches "a", which a linguistic collation would order first.
	t.Run("text ranges compare bytes", func(t *testing.T) {
		env := NewEnv(t, SharedCluster(t))
		texts := []string{"a", "B", "b", "Z", "é"}
		events := make([]*Event, len(texts))
		for i, v := range texts {
			events[i] = CreateEvent(wide, map[string]any{"title": v, "note": v})
		}
		mustApplyEvents(ctx, t, env, "seed byte-order rows", events...)
		stored := map[uuid.UUID]string{} // an event's RowID is set when it is applied
		for i, ev := range events {
			stored[ev.RowID] = texts[i]
		}
		for _, attr := range []string{"title", "note"} {
			for _, c := range []struct {
				op    string
				holds func(int) bool
			}{{"gt", func(c int) bool { return c > 0 }}, {"gte", func(c int) bool { return c >= 0 }}, {"lt", func(c int) bool { return c < 0 }}, {"lte", func(c int) bool { return c <= 0 }}} {
				for _, operand := range []string{"B", "a", "Z"} {
					f := Filter{Attr: attr, Op: c.op, Value: operand}
					var want []uuid.UUID
					for id, v := range stored {
						if c.holds(strings.Compare(v, operand)) {
							want = append(want, id)
						}
					}
					pg := pgRouteIDs(ctx, t, env, wide, f)
					assertSameIDs(t, f, "the PostgreSQL route", pg, sortedUUIDs(want))
					hot, _ := hotKeyIDsInWindow(ctx, t, env, wide, f, 0)
					assertSameIDs(t, f, "the hot-keys query", hot, pg)
				}
			}
		}
	})
}

// seedParityHot writes the final states seedParityTiers lays out across the
// tiers (c2 reaching label 1 through an update) without init or flush, so
// every row has an unflushed change_log entry.
func seedParityHot(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) parityRows {
	t.Helper()
	c1 := CreateEvent(wide, parityAttrsAt(0, []any{"p", "q"}))
	c2 := CreateEvent(wide, parityAttrsAt(2, []any{"r"}))
	w1 := CreateEvent(wide, parityAttrsAt(1, []any{}))
	w2 := CreateEvent(wide, map[string]any{"note": "z"})
	h1 := CreateEvent(wide, parityAttrsAt(0, nil))
	h2 := CreateEvent(wide, map[string]any{"count": float64(3)})
	mustApplyEvents(ctx, t, env, "seed parity rows", c1, c2, w1, w2, h1, h2)
	mustApplyEvents(ctx, t, env, "update parity row", UpdateEvent(wide, c2.RowID, parityAttrsAt(1, nil)))
	return parityRows{
		{id: c1.RowID, label: 0, tags: true},
		{id: c2.RowID, label: 1, tags: true},
		{id: w1.RowID, label: 1, tags: true},
		{id: w2.RowID, label: 2, only: "note"},
		{id: h1.RowID, label: 0},
		{id: h2.RowID, label: 2, only: "count"},
	}
}

func (rs parityRows) ids() []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(rs))
	for _, r := range rs {
		ids = append(ids, r.id)
	}
	return sortedUUIDs(ids)
}

// forEachParityCase calls check with every filter of the condition-DSL
// parity matrix and the rows it must select.
func forEachParityCase(rows parityRows, check func(Filter, []uuid.UUID)) {
	for _, a := range parityAttrs() {
		for _, c := range a.cases() {
			check(Filter{Attr: a.attr, Op: c.op, Value: c.value}, rows.matching(a.attr, c.match))
		}
	}
	check(Filter{Attr: "tags", Op: "is_null", Value: "true"}, rows.withoutTags())
	check(Filter{Attr: "tags", Op: "is_not_null", Value: "true"}, rows.withTags())
}

// pgRouteIDs answers the rows f selects on the PostgreSQL route.
func pgRouteIDs(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, f Filter) []uuid.UUID {
	t.Helper()
	res, err := env.Query(ctx, Query{Schema: wide, Filters: []Filter{f}, Limit: 100, PreferHot: true,
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold}})
	if err != nil {
		t.Fatalf("%s %s:%s on the PostgreSQL route: %v", f.Attr, f.Op, f.Value, err)
	}
	if res.Plan.Routing.UseDuckDB {
		t.Fatalf("%s %s:%s with prefer_hot routed to DuckDB: %+v", f.Attr, f.Op, f.Value, res.Plan.Routing)
	}
	ids := make([]uuid.UUID, 0, len(res.Records))
	for _, r := range res.Records {
		ids = append(ids, r.RowID)
	}
	return sortedUUIDs(ids)
}

// hotKeyIDsInWindow reads, in one REPEATABLE READ READ ONLY snapshot as a
// pgx pass does, the rows the hot-keys query selects for f at cutoff and the
// rows of the dirty window it reads.
func hotKeyIDsInWindow(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, f Filter, cutoff int64) (hot, window []uuid.UUID) {
	t.Helper()
	fq, err := env.buildFederatedQuery(Query{Schema: wide, Filters: []Filter{f}})
	if err != nil {
		t.Fatalf("build condition of %s %s:%s: %v", f.Attr, f.Op, f.Value, err)
	}
	tx, err := env.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatalf("begin snapshot: %v", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	repo := internal.NewDBPersistentRecordRepository(env.Pool, env.Metadata)
	keys, err := repo.QueryHotKeys(ctx, tx, model.HotKeysQuery{
		Tables: env.Tables, SchemaID: wide.ID, Condition: fq.Condition, GraceCutoffMs: cutoff,
	})
	if err != nil {
		t.Fatalf("%s %s:%s on the hot-keys query: %v", f.Attr, f.Op, f.Value, err)
	}
	for _, k := range keys {
		hot = append(hot, k.RowID)
	}
	rows, err := tx.Query(ctx, fmt.Sprintf("SELECT DISTINCT row_id FROM %s WHERE schema_id = $1 AND (flushed_at = 0 OR flushed_at >= $2)",
		sqlutil.SanitizeIdentifier(env.Tables.ChangeLog)), wide.ID, cutoff)
	if err != nil {
		t.Fatalf("read the dirty window: %v", err)
	}
	window, err = pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		t.Fatalf("read the dirty window: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("end snapshot: %v", err)
	}
	return sortedUUIDs(hot), sortedUUIDs(window)
}

func intersectIDs(a, b []uuid.UUID) []uuid.UUID {
	in := map[uuid.UUID]bool{}
	for _, id := range b {
		in[id] = true
	}
	out := []uuid.UUID{}
	for _, id := range a {
		if in[id] {
			out = append(out, id)
		}
	}
	return sortedUUIDs(out)
}

func assertSameIDs(t *testing.T, f Filter, what string, got, want []uuid.UUID) {
	t.Helper()
	if fmt.Sprint(sortedUUIDs(got)) != fmt.Sprint(sortedUUIDs(want)) {
		t.Errorf("%s %s:%s on %s = %v, want %v", f.Attr, f.Op, f.Value, what, got, want)
	}
}

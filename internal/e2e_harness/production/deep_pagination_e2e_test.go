//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestDeepPagination (#181): offset pagination must preserve totalRecords
// when the requested page is empty (offset >= matching total). Both engines
// read COUNT(*) OVER() off the returned data rows, so an empty page returns
// zero rows and the count is unreadable — without a fallback, deep pages
// report totalRecords=0 while matches exist. The oracle computes Total before
// slicing offset/limit, so AssertQueryMatches pins the contract on every
// probe. Red-first per #213: committed before the engine fix.
func TestDeepPagination(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide: count is unique per ordinal

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"offset_equals_total", testOffsetEqualsTotal},
		{"offset_beyond_total", testOffsetBeyondTotal},
		{"hot_only_oltp", testHotOnlyDeepOffset},
		{"multi_tier", testMultiTierDeepOffset},
		{"after_filter", testFilteredDeepOffset},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), wide)
		})
	}
}

// seedFlushed creates n rows and flushes them all to delta parquet, so the
// default-tier query routes through the DuckDB federated path (an empty
// parquet glob would error instead).
func seedFlushed(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, n int) {
	t.Helper()
	creates := env.GenerateScript(ScriptSpec{Schema: schema, Creates: n})
	mustApplyEvents(ctx, t, env, "apply creates", creates...)
	mustFlush(ctx, t, env)
}

// assertShallowPage is the load-bearing positive control: the first page must
// come back full with the true total, proving the read path works before any
// empty-page probe runs. Fail-fast so a broken seed cannot fake a green probe.
func assertShallowPage(ctx context.Context, t *testing.T, env *Env, name string, q Query, wantRows int, wantTotal int64) *QueryResult {
	t.Helper()
	res := env.AssertQueryMatches(ctx, q)
	if res == nil {
		return nil
	}
	if len(res.Records) != wantRows {
		t.Fatalf("%s: shallow page returned %d rows, want %d", name, len(res.Records), wantRows)
	}
	if res.Total != wantTotal {
		t.Fatalf("%s: shallow page reports total %d, want %d", name, res.Total, wantTotal)
	}
	return res
}

// assertEmptyPageTotal probes a page at or beyond the last matching row: the
// page must be empty while totalRecords still reports the full matching
// count. AssertQueryMatches already diffs the total against the oracle; the
// explicit checks keep the failure message on the issue #181 contract.
func assertEmptyPageTotal(ctx context.Context, t *testing.T, env *Env, name string, q Query, wantTotal int64) {
	t.Helper()
	res := env.AssertQueryMatches(ctx, q)
	if res == nil {
		return
	}
	if len(res.Records) != 0 {
		t.Errorf("%s: page beyond total returned %d rows, want 0", name, len(res.Records))
	}
	if res.Total != wantTotal {
		t.Errorf("%s: empty page reports total %d, want %d", name, res.Total, wantTotal)
	}
}

// testOffsetEqualsTotal is issue scenario 1: offset == total must return an
// empty page that still reports the full count, through the DuckDB path.
func testOffsetEqualsTotal(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	seedFlushed(ctx, t, env, wide, 25)

	sorts := []Sort{{Attr: "count"}}
	res := assertShallowPage(ctx, t, env, "offset_equals_total control",
		Query{Schema: wide, Sorts: sorts, Limit: 10}, 10, 25)
	if res != nil && !res.Plan.Routing.UseDuckDB {
		t.Errorf("control query did not route to duckdb: %+v", res.Plan.Routing)
	}

	assertEmptyPageTotal(ctx, t, env, "offset==total",
		Query{Schema: wide, Sorts: sorts, Limit: 10, Offset: 25}, 25)
}

// testOffsetBeyondTotal is issue scenario 2: offset past the last row behaves
// like scenario 1 — empty page, full count.
func testOffsetBeyondTotal(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	seedFlushed(ctx, t, env, wide, 25)

	sorts := []Sort{{Attr: "count"}}
	assertShallowPage(ctx, t, env, "offset_beyond_total control",
		Query{Schema: wide, Sorts: sorts, Limit: 10}, 10, 25)

	assertEmptyPageTotal(ctx, t, env, "offset>total",
		Query{Schema: wide, Sorts: sorts, Limit: 10, Offset: 35}, 25)
}

// testHotOnlyDeepOffset covers the Postgres OLTP path: unflushed rows with
// PreferHot route through queryPostgresOnly → runOptimizedQuery, which reads
// the window count off returned rows exactly like the DuckDB reader.
func testHotOnlyDeepOffset(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 25})
	mustApplyEvents(ctx, t, env, "apply hot creates", creates...)

	sorts := []Sort{{Attr: "count"}}
	res := assertShallowPage(ctx, t, env, "hot_only control",
		Query{Schema: wide, PreferHot: true, Sorts: sorts, Limit: 10}, 10, 25)
	if res != nil && res.Plan.Routing.UseDuckDB {
		t.Errorf("hot-only control unexpectedly routed to duckdb: %+v", res.Plan.Routing)
	}

	assertEmptyPageTotal(ctx, t, env, "hot offset==total",
		Query{Schema: wide, PreferHot: true, Sorts: sorts, Limit: 10, Offset: 25}, 25)
	assertEmptyPageTotal(ctx, t, env, "hot offset>total",
		Query{Schema: wide, PreferHot: true, Sorts: sorts, Limit: 10, Offset: 35}, 25)
}

// testMultiTierDeepOffset is issue scenario 3: rows spread across base
// (init), delta (flush), and hot (unflushed) — the post-dedup cross-tier
// count must survive an empty deep page. Seeding recipe mirrors
// testThreeTierUnion (#177).
func testMultiTierDeepOffset(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	old := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 10})
	mustApplyEvents(ctx, t, env, "apply base creates", old...)
	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 10 {
		t.Fatalf("init exported %d rows, want 10", report.RowsExported)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	mid := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 8})
	mustApplyEvents(ctx, t, env, "apply delta creates", mid...)
	mustFlush(ctx, t, env)

	fresh := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 7})
	mustApplyEvents(ctx, t, env, "apply hot creates", fresh...)

	// Tier precondition: exactly the 7 hot rows are dirty.
	var dirty int64
	if err := env.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM change_log WHERE flushed_at = 0 AND schema_id = $1", wide.ID).Scan(&dirty); err != nil {
		t.Fatalf("count dirty rows: %v", err)
	}
	if dirty != 7 {
		t.Fatalf("dirty set has %d rows, want 7 (hot batch only)", dirty)
	}

	sorts := []Sort{{Attr: "count"}}
	res := assertShallowPage(ctx, t, env, "multi_tier control",
		Query{Schema: wide, Sorts: sorts, Limit: 10}, 10, 25)
	if res != nil && !res.Plan.Routing.UseDuckDB {
		t.Errorf("multi-tier control did not route to duckdb: %+v", res.Plan.Routing)
	}

	assertEmptyPageTotal(ctx, t, env, "multi-tier offset==total",
		Query{Schema: wide, Sorts: sorts, Limit: 10, Offset: 25}, 25)
	assertEmptyPageTotal(ctx, t, env, "multi-tier offset>total",
		Query{Schema: wide, Sorts: sorts, Limit: 10, Offset: 35}, 25)
}

// testFilteredDeepOffset is issue scenario 4: a filter shrinks the visible
// set below the offset — the empty page must report the filtered count, not
// the raw table size. FullTypeProfile assigns count = ordinal*10 + [0,9], so
// with 25 creates (ordinals 0..24) the predicate count < 80 matches exactly
// ordinals 0..7 = 8 rows.
func testFilteredDeepOffset(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	seedFlushed(ctx, t, env, wide, 25)

	filters := []Filter{{Attr: "count", Op: "lt", Value: "80"}}
	sorts := []Sort{{Attr: "count"}}
	assertShallowPage(ctx, t, env, "after_filter control",
		Query{Schema: wide, Filters: filters, Sorts: sorts, Limit: 5}, 5, 8)

	assertEmptyPageTotal(ctx, t, env, "filtered offset==total",
		Query{Schema: wide, Filters: filters, Sorts: sorts, Limit: 5, Offset: 8}, 8)
	assertEmptyPageTotal(ctx, t, env, "filtered offset>total",
		Query{Schema: wide, Filters: filters, Sorts: sorts, Limit: 5, Offset: 20}, 8)
}

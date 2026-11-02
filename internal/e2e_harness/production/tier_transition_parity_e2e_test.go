//go:build e2e

package production

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/compaction"
)

// TestTierTransitionParity (#753) drives one data set through every tier
// transition the production pipeline makes and checks every query against
// the independent oracle at each step: a CDC flush to delta, compaction
// promotion to base, a flush that overlaps the base, a compaction rewrite
// (the DuckDB merge that writes a new base), a hot version over a delta
// version over the base, a flush of that, tombstones before and after
// their flush, and a final rewrite that must drop the deleted rows.
//
// The neighbouring tests prove each transition on its own; this one keeps
// one data set across all of them, so a merge that is right in one tier
// layout and wrong in the next fails here. The bound bigint "amount"
// straddles 2^53 and reaches ±MaxInt64, and rows are created in descending
// amount order, so a float64 hop anywhere (export staging, Parquet, the
// rewrite's union_by_name, the federated read) changes a value, a filter
// match or the sort order, and the exact oracle reports it.
func TestTierTransitionParity(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	rows := createTransitionRows(ctx, t, env, wide)
	mustFlush(ctx, t, env)
	assertTransitionQueries(ctx, t, env, wide, "v1 in delta")

	promoted, err := env.RunCompactionWith(ctx, wide, CompactionOverrides{TargetBaseSizeBytes: 1})
	requireCompaction(t, "promotion", promoted, err, compaction.PromotionApplied)
	assertTierCounts(ctx, t, env, wide, "after promotion", 1, 0)
	assertTransitionQueries(ctx, t, env, wide, "v1 in base")

	updateTransitionRows(ctx, t, env, wide, rows[:4], 2)
	mustFlush(ctx, t, env)
	assertTierCounts(ctx, t, env, wide, "v2 flushed over base", 1, 1)
	assertTransitionQueries(ctx, t, env, wide, "v2 delta over v1 base")

	rewritten, err := env.RunCompaction(ctx, wide)
	requireCompaction(t, "rewrite", rewritten, err, compaction.RewriteApplied)
	assertTierCounts(ctx, t, env, wide, "after rewrite", 1, 0)
	assertBaseAmounts(ctx, t, env, rewritten.NewBaseKey, rows)
	assertTransitionQueries(ctx, t, env, wide, "v2 in rewritten base")

	testHotOverDeltaOverBase(ctx, t, env, wide, rows)
	testTombstonesAcrossTiers(ctx, t, env, wide, rows)
}

// transitionAmounts are the bound bigint values, in creation order. 2^53+1,
// 2^53+2 past 2^53, and MaxInt64-1 have no float64 image; -(2^53+1) and
// -MaxInt64 cover the negative side.
var transitionAmounts = []int64{
	math.MaxInt64,
	math.MaxInt64 - 1,
	1<<53 + 2,
	1<<53 + 1,
	1 << 53,
	1<<53 - 1,
	-(1<<53 + 1),
	-math.MaxInt64,
}

// transitionRow tracks one entity and the event that last wrote it.
type transitionRow struct {
	id     uuid.UUID
	amount int64
	latest *Event
}

func createTransitionRows(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) []*transitionRow {
	t.Helper()
	rows := make([]*transitionRow, 0, len(transitionAmounts))
	for i, amount := range transitionAmounts {
		ev := CreateEvent(wide, transitionAttrs(i, amount))
		mustApplyEvents(ctx, t, env, fmt.Sprintf("create row %d", i), ev)
		rows = append(rows, &transitionRow{id: ev.RowID, amount: amount, latest: ev})
	}
	return rows
}

// transitionAttrs fills every non-list attribute of e2e_wide: the main
// columns and the EAV-only ones of each value type. rank repeats so a sort
// on it needs the next key; the EAV bigint "total" stays within ±2^53, the
// range the write accepts for it (#590), and row 0 holds the ceiling.
func transitionAttrs(i int, amount int64) map[string]any {
	total := int64(i)*1_000_003 + 1
	if i == 0 {
		total = 1 << 53
	}
	return map[string]any{
		"title":   fmt.Sprintf("v1-%d", i),
		"rank":    float64(i % 3),
		"count":   float64(100 + i),
		"amount":  amount,
		"score":   0.5 + float64(i),
		"ref":     fmt.Sprintf("00000000-0000-4000-8000-%012d", i),
		"note":    fmt.Sprintf("v1-note-%d", i),
		"active":  i%2 == 0,
		"born":    fmt.Sprintf("1990-01-%02d", i+1),
		"seen":    fmt.Sprintf("2026-07-10T01:02:%02dZ", i),
		"joined":  fmt.Sprintf("2001-02-%02d", i+1),
		"touched": fmt.Sprintf("2026-01-01T00:00:%02dZ", i),
		"level":   float64(i - 4),
		"qty":     float64(1000 * i),
		"total":   total,
		"ratio":   float64(i) / 4,
		"token":   fmt.Sprintf("00000000-0000-4000-9000-%012d", i),
	}
}

// updateTransitionRows writes a new version of each row: a new title and
// note, new main-column and EAV numerics, and the amount kept, so the
// version shows in every attribute family but the sort on amount stays a
// total order.
func updateTransitionRows(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, rows []*transitionRow, version int) {
	t.Helper()
	for i, row := range rows {
		ev := UpdateEvent(wide, row.id, map[string]any{
			"title": fmt.Sprintf("v%d-%s", version, row.id),
			"note":  fmt.Sprintf("v%d-note-%d", version, i),
			"count": float64(1000*version + i),
			"score": float64(version) + 0.25,
			"qty":   float64(7 * (i + 1) * version),
		})
		mustApplyEvents(ctx, t, env, fmt.Sprintf("v%d update of %s", version, row.id), ev)
		row.latest = ev
	}
}

// assertTransitionQueries runs the query set every step must answer like
// the oracle: the whole set (values and total), exact sorts on the bound
// bigint, a page window, a multi-key sort with ties on the first key, and
// filters on the bound and the EAV bigint at their exactness boundaries.
// Each query must take the DuckDB path, or the S3 tiers were never read.
func assertTransitionQueries(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, label string) {
	t.Helper()
	for _, q := range transitionQueries(wide) {
		result := env.AssertQueryMatches(ctx, q)
		if result == nil {
			return
		}
		if !result.Plan.Routing.UseDuckDB || result.Partial != nil {
			t.Fatalf("%s: query %+v did not read the S3 tiers through duckdb: routing %+v partial %+v",
				label, q, result.Plan.Routing, result.Partial)
		}
	}
}

func transitionQueries(wide SchemaRef) []Query {
	return []Query{
		{Schema: wide, Limit: 100},
		{Schema: wide, Sorts: []Sort{{Attr: "amount"}}, Limit: 100},
		{Schema: wide, Sorts: []Sort{{Attr: "amount", Desc: true}}, Limit: 3, Offset: 2},
		{Schema: wide, Sorts: []Sort{{Attr: "rank"}, {Attr: "count", Desc: true}}, Limit: 4, Offset: 1},
		{Schema: wide, Filters: []Filter{{Attr: "amount", Value: "9007199254740993"}}, Limit: 10},
		{Schema: wide, Filters: []Filter{{Attr: "amount", Op: "gt", Value: "9007199254740992"}},
			Sorts: []Sort{{Attr: "amount"}}, Limit: 10},
		{Schema: wide, Filters: []Filter{{Attr: "total", Value: "9007199254740992"}}, Limit: 10},
		{Schema: wide, Filters: []Filter{{Attr: "qty", Op: "gte", Value: "21"}},
			Sorts: []Sort{{Attr: "qty"}}, Limit: 100},
	}
}

func requireCompaction(t *testing.T, label string, result compaction.CompactionResult, err error, want compaction.CompactionOutcome) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: compaction: %v", label, err)
	}
	if result.Outcome != want {
		t.Fatalf("%s: outcome = %s (dirty ratio %.2f), want %s", label, result.Outcome, result.DirtyRatio, want)
	}
}

// assertTierCounts pins the manifest layout a step relies on, so a step
// cannot silently test a different layout than it claims.
func assertTierCounts(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, label string, base, delta int) {
	t.Helper()
	m := loadSchemaManifest(ctx, t, env, wide)
	if got := countTier(m, "base"); got != base {
		t.Fatalf("%s: manifest lists %d base files, want %d", label, got, base)
	}
	if got := countTier(m, "delta"); got != delta {
		t.Fatalf("%s: manifest lists %d delta files, want %d", label, got, delta)
	}
}

// assertBaseAmounts reads the rewritten base Parquet directly: every row's
// amount must be the exact int64 written, which separates the storage
// image from the query result the oracle checks.
func assertBaseAmounts(ctx context.Context, t *testing.T, env *Env, key string, rows []*transitionRow) {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, key)
	for _, row := range rows {
		var amount int64
		if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
			`SELECT "amount" FROM read_parquet('%s') WHERE CAST(row_id AS VARCHAR) = ?`, path),
			row.id.String()).Scan(&amount); err != nil {
			t.Fatalf("read amount of %s from %s: %v", row.id, key, err)
		}
		if amount != row.amount {
			t.Errorf("base %s stores amount %d for %s, want %d", key, amount, row.id, row.amount)
		}
	}
}

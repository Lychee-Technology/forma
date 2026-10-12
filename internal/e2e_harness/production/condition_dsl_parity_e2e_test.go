//go:build e2e

package production

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
)

// TestConditionDSLOperatorParityAcrossTiers proves #646's dialect parity:
// in, not_in, is_null and is_not_null answer identically on the DuckDB
// federated route (cold base parquet, warm delta parquet, hot Postgres rows
// merged by the dirty-set anti-join) and on the Postgres route, for every
// storage form of e2e_wide: main-column-bound text, integer, smallint,
// double, bigint, uuid, date and datetime, and their EAV-only counterparts
// plus bool. Expectations are derived from the seeded values, not from
// either route, and the test is not gated on -short.
func TestConditionDSLOperatorParityAcrossTiers(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	rows := seedParityTiers(ctx, t, env, wide)
	for _, a := range parityAttrs() {
		t.Run(a.attr, func(t *testing.T) {
			for _, c := range a.cases() {
				want := rows.matching(a.attr, c.match)
				assertBothRoutes(ctx, t, env, wide, Filter{Attr: a.attr, Op: c.op, Value: c.value}, want)
			}
		})
	}
	t.Run("tags list presence", func(t *testing.T) {
		assertBothRoutes(ctx, t, env, wide, Filter{Attr: "tags", Op: "is_null", Value: "true"}, rows.withoutTags())
		assertBothRoutes(ctx, t, env, wide, Filter{Attr: "tags", Op: "is_not_null", Value: "true"}, rows.withTags())
	})
}

// parityAttr is one e2e_wide attribute with three distinct event values;
// lit holds the list literals naming the first two.
type parityAttr struct {
	attr string
	vals [3]any
	lit  [2]string
}

func parityAttrs() []parityAttr {
	u := func(n int) string {
		return fmt.Sprintf("6f1c3c3e-%d%d%d%d-4%d%d%d-8%d%d%d-%012d", n, n, n, n, n, n, n, n, n, n, n)
	}
	return []parityAttr{
		{"title", [3]any{"alpha", "beta", "gamma"}, [2]string{`"alpha"`, `"beta"`}},
		{"note", [3]any{"x", "y", "z"}, [2]string{`"x"`, `"y"`}},
		{"count", [3]any{float64(1), float64(2), float64(3)}, [2]string{"1", "2.0"}},
		{"qty", [3]any{float64(1), float64(2), float64(3)}, [2]string{"1", "2"}},
		{"rank", [3]any{float64(1), float64(2), float64(3)}, [2]string{"1", "2"}},
		{"level", [3]any{float64(-1), float64(2), float64(3)}, [2]string{"-1", "2"}},
		{"score", [3]any{1.5, 2.5, 3.5}, [2]string{"1.5", "2.5"}},
		{"ratio", [3]any{1.25, 2.25, 3.25}, [2]string{"1.25", "2.25"}},
		{"amount", [3]any{float64(10), float64(20), float64(30)}, [2]string{"10", "20"}},
		{"total", [3]any{float64(10), float64(20), float64(30)}, [2]string{"10", "2e1"}},
		{"ref", [3]any{u(1), u(2), u(3)}, [2]string{`"` + strings.ToUpper(u(1)) + `"`, `"` + u(2) + `"`}},
		{"token", [3]any{u(1), u(2), u(3)}, [2]string{`"` + u(1) + `"`, `"` + strings.ToUpper(u(2)) + `"`}},
		{"active", [3]any{true, false, true}, [2]string{"true", "0"}},
		{"born", [3]any{"2024-01-02", "2024-01-03", "2024-01-04"}, [2]string{`"2024-01-02T00:00:00Z"`, "1704240000000"}},
		{"joined", [3]any{"2024-01-02", "2024-01-03", "2024-01-04"}, [2]string{"1704153600000", `"2024-01-03T00:00:00Z"`}},
		{"seen", [3]any{"2024-01-02T03:04:05Z", "2024-01-02T03:04:06Z", "2024-01-02T03:04:07Z"}, [2]string{`"2024-01-02T03:04:05Z"`, "1704164646000"}},
		{"touched", [3]any{"2024-01-02T03:04:05Z", "2024-01-02T03:04:06Z", "2024-01-02T03:04:07Z"}, [2]string{"1704164645000", `"2024-01-02T03:04:06Z"`}},
	}
}

type parityCase struct {
	op, value string
	match     func(label int, present bool) bool
}

// cases is the operator matrix over value labels 0/1/2 (vals index).
func (a parityAttr) cases() []parityCase {
	is := func(labels ...int) func(int, bool) bool {
		return func(l int, present bool) bool {
			for _, want := range labels {
				if present && l == want {
					return true
				}
			}
			return false
		}
	}
	return []parityCase{
		{"in", "[" + a.lit[0] + "]", is(0)},
		{"in", "[" + a.lit[0] + "," + a.lit[1] + "," + a.lit[0] + "]", is(0, 1)},
		{"not_in", "[" + a.lit[0] + "]", is(1, 2)},
		{"not_in", "[" + a.lit[0] + "," + a.lit[1] + "]", is(2)},
		{"is_null", "true", func(_ int, present bool) bool { return !present }},
		{"is_not_null", "true", func(_ int, present bool) bool { return present }},
	}
}

// parityRow is a seeded entity's final state: label is the vals index of
// every attribute it holds; sparse rows hold only `only`.
type parityRow struct {
	id    uuid.UUID
	label int
	only  string
	tags  bool
}

type parityRows []parityRow

func (rs parityRows) matching(attr string, match func(int, bool) bool) []uuid.UUID {
	var ids []uuid.UUID
	for _, r := range rs {
		if match(r.label, r.only == "" || r.only == attr) {
			ids = append(ids, r.id)
		}
	}
	return sortedUUIDs(ids)
}

func (rs parityRows) withTags() []uuid.UUID    { return rs.filterTags(true) }
func (rs parityRows) withoutTags() []uuid.UUID { return rs.filterTags(false) }

func (rs parityRows) filterTags(present bool) []uuid.UUID {
	var ids []uuid.UUID
	for _, r := range rs {
		if r.tags == present {
			ids = append(ids, r.id)
		}
	}
	return sortedUUIDs(ids)
}

func parityAttrsAt(label int, tags []any) map[string]any {
	attrs := map[string]any{}
	for _, a := range parityAttrs() {
		attrs[a.attr] = a.vals[label]
	}
	if tags != nil {
		attrs["tags"] = tags
	}
	return attrs
}

// seedParityTiers lays out: cold c1 (label 0) and c2 (label 2, shadowed by a
// hot update to label 1); warm w1 (label 1, empty tags list) and w2 (only
// note); hot h1 (label 0) and h2 (only count). Every attribute is absent on
// a sparse row, so is_null and not_in see absent values on all three tiers.
func seedParityTiers(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) parityRows {
	t.Helper()
	c1 := CreateEvent(wide, parityAttrsAt(0, []any{"p", "q"}))
	c2 := CreateEvent(wide, parityAttrsAt(2, []any{"r"}))
	mustApplyEvents(ctx, t, env, "seed cold", c1, c2)
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	w1 := CreateEvent(wide, parityAttrsAt(1, []any{}))
	w2 := CreateEvent(wide, map[string]any{"note": "z"})
	mustApplyEvents(ctx, t, env, "seed warm", w1, w2)
	mustFlush(ctx, t, env)

	h1 := CreateEvent(wide, parityAttrsAt(0, nil))
	h2 := CreateEvent(wide, map[string]any{"count": float64(3)})
	mustApplyEvents(ctx, t, env, "seed hot", h1, h2,
		UpdateEvent(wide, c2.RowID, parityAttrsAt(1, nil)))

	return parityRows{
		{id: c1.RowID, label: 0, tags: true},
		{id: c2.RowID, label: 1, tags: true},
		{id: w1.RowID, label: 1, tags: true},
		{id: w2.RowID, label: 2, only: "note"},
		{id: h1.RowID, label: 0},
		{id: h2.RowID, label: 2, only: "count"},
	}
}

// assertBothRoutes runs one filter through the DuckDB federated route and
// the Postgres route and requires both to return exactly want.
func assertBothRoutes(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, f Filter, want []uuid.UUID) {
	t.Helper()
	tiers := []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold}
	for _, preferHot := range []bool{false, true} {
		res, err := env.Query(ctx, Query{Schema: wide, Filters: []Filter{f}, Limit: 100, PreferredTiers: tiers, PreferHot: preferHot})
		if err != nil {
			t.Fatalf("%s:%s %s (prefer_hot=%v): %v", f.Op, f.Value, f.Attr, preferHot, err)
		}
		if res.Plan.Routing.UseDuckDB == preferHot {
			t.Fatalf("%s %s:%s routed use_duckdb=%v with prefer_hot=%v: %+v", f.Attr, f.Op, f.Value, res.Plan.Routing.UseDuckDB, preferHot, res.Plan.Routing)
		}
		got := make([]uuid.UUID, 0, len(res.Records))
		for _, r := range res.Records {
			got = append(got, r.RowID)
		}
		if g := sortedUUIDs(got); fmt.Sprint(g) != fmt.Sprint(want) {
			t.Errorf("%s %s:%s (use_duckdb=%v) = %v, want %v", f.Attr, f.Op, f.Value, res.Plan.Routing.UseDuckDB, g, want)
		}
	}
}

func sortedUUIDs(ids []uuid.UUID) []uuid.UUID {
	out := append([]uuid.UUID{}, ids...)
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

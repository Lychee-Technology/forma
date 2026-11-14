//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"

	forma "github.com/lychee-technology/forma"
)

// TestTierParitySuite is pgx-hot-leg-plan.md step 6's tier parity suite
// (design.md §6.1, Filtering splits by tier): each condition-DSL case over
// the same rows must match the same rows while they are hot, once they are
// flushed to delta parquet, and once compacted into base parquet.
// PostgreSQL filters the hot rows and DuckDB the parquet rows, so a
// predicate difference between the two would make a row's inclusion depend
// on its tier. The suite holds each federated answer, at every stage, to
// the PostgreSQL route's, which no flush changes.
//
// The rows cover every storage form of e2e_wide at three values each, rows
// holding one attribute only, the sort-key edges of step 2
// (lockstepEdgeRows), a row updated and a row deleted before the flush. The
// cases are the operators of docs/condition-dsl.md over each attribute: the
// comparisons at the middle value, in and not_in with mixed spellings, the
// null tests, text prefixes, substrings and byte-order ranges (#728),
// fractional operands on whole-number storage, and conjunctions across
// storage forms.
func TestTierParitySuite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	// The anchor gives the schema its first parquet object, so the hot stage
	// reads through a manifest like the later ones.
	mustApplyEvents(ctx, t, env, "anchor", CreateEvent(wide, map[string]any{"note": "anchor"}))
	mustFlush(ctx, t, env)
	seedTierParityRows(ctx, t, env, wide)

	cases := tierParityCases()
	want := make([]tierParityAnswer, len(cases))
	for i, c := range cases {
		want[i] = tierParityRoute(ctx, t, env, false, c)
	}
	requireTierParityNotVacuous(t, cases, want)

	check := func(stage string) {
		for i, c := range cases {
			got := tierParityRoute(ctx, t, env, true, c)
			if got.String() != want[i].String() {
				t.Errorf("%s: %s = %v, want the PostgreSQL route's %v", stage, c, got, want[i])
			}
		}
	}
	check("hot")
	mustFlush(ctx, t, env)
	check("warm")
	if _, err := env.RunCompaction(ctx, wide); err != nil {
		t.Fatalf("compaction: %v", err)
	}
	check("cold")
}

// seedTierParityRows writes the suite's rows without flushing them.
func seedTierParityRows(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	t.Helper()
	updated := CreateEvent(wide, parityAttrsAt(2, []any{"r"}))
	deleted := CreateEvent(wide, parityAttrsAt(1, []any{"s"}))
	events := []*Event{
		CreateEvent(wide, parityAttrsAt(0, []any{"p", "q"})),
		CreateEvent(wide, parityAttrsAt(1, []any{})),
		CreateEvent(wide, parityAttrsAt(2, nil)),
		CreateEvent(wide, map[string]any{"note": "z"}),
		CreateEvent(wide, map[string]any{"count": float64(3)}),
		updated, deleted,
	}
	events = append(events, lockstepEdgeRows(wide)...)
	mustApplyEvents(ctx, t, env, "seed tier parity rows", events...)
	mustApplyEvents(ctx, t, env, "update and delete before the flush",
		UpdateEvent(wide, updated.RowID, parityAttrsAt(1, nil)), DeleteEvent(wide, deleted.RowID))
}

// tierParityCase is one condition: its filters, all of which a row must
// pass.
type tierParityCase []Filter

func (c tierParityCase) String() string {
	parts := make([]string, len(c))
	for i, f := range c {
		parts[i] = f.Attr + " " + f.Op + ":" + f.Value
	}
	return strings.Join(parts, " and ")
}

// tierParityAnswer is a case's answer: the rows it selects, sorted, or the
// refusal of an operator or operand the attribute cannot take.
type tierParityAnswer struct {
	ids     []uuid.UUID
	refused bool
}

func (a tierParityAnswer) String() string {
	if a.refused {
		return "refused as invalid input"
	}
	return fmt.Sprint(a.ids)
}

// tierParityRoute answers c on the federated route over every tier when
// federated is set, and otherwise on the PostgreSQL route.
func tierParityRoute(ctx context.Context, t *testing.T, env *Env, federated bool, c tierParityCase) tierParityAnswer {
	t.Helper()
	q := Query{Schema: DefaultSchemaFixtures()[1], Filters: c, Limit: 100, PreferredTiers: allTiers, PreferHot: !federated}
	res, err := env.Query(ctx, q)
	if errors.Is(err, forma.ErrInvalidInput) {
		return tierParityAnswer{refused: true}
	}
	if err != nil {
		t.Fatalf("%s (federated %v): %v", c, federated, err)
	}
	if res.Plan.Routing.UseDuckDB != federated {
		t.Fatalf("%s routed use_duckdb=%v, want %v: %+v", c, res.Plan.Routing.UseDuckDB, federated, res.Plan.Routing)
	}
	ids := make([]uuid.UUID, 0, len(res.Records))
	for _, r := range res.Records {
		ids = append(ids, r.RowID)
	}
	return tierParityAnswer{ids: sortedUUIDs(ids)}
}

// requireTierParityNotVacuous holds the catalogue to cases that tell rows
// apart: for each attribute, some case must select some rows and not all.
func requireTierParityNotVacuous(t *testing.T, cases []tierParityCase, want []tierParityAnswer) {
	t.Helper()
	total := 0
	for _, a := range want {
		total = max(total, len(a.ids))
	}
	splits := map[string]bool{}
	for i, c := range cases {
		if n := len(want[i].ids); n > 0 && n < total && len(c) == 1 {
			splits[c[0].Attr] = true
		}
	}
	for _, a := range parityAttrs() {
		if !splits[a.attr] {
			t.Fatalf("no case on %s selects a proper subset of the rows: the suite would not see a tier split there", a.attr)
		}
	}
}

// tierParityCases is the suite's condition-DSL catalogue.
func tierParityCases() []tierParityCase {
	var cases []tierParityCase
	add := func(f Filter) { cases = append(cases, tierParityCase{f}) }
	for _, a := range parityAttrs() {
		for _, c := range a.cases() {
			add(Filter{Attr: a.attr, Op: c.op, Value: c.value})
		}
		mid := parityOperand(a.vals[1])
		ops := []string{"equals", "not_equals", "gt", "gte", "lt", "lte"}
		if a.attr == "active" {
			ops = ops[:2] // a bool takes no range
		}
		for _, op := range ops {
			add(Filter{Attr: a.attr, Op: op, Value: mid})
		}
		for _, f := range tierParityTypedCases(a) {
			add(f)
		}
	}
	add(Filter{Attr: "tags", Op: "is_null", Value: "true"})
	add(Filter{Attr: "tags", Op: "is_not_null", Value: "true"})
	return append(cases, tierParityConjunctions()...)
}

// tierParityTypedCases are the cases one storage form adds to the shared
// comparisons.
func tierParityTypedCases(a parityAttr) []Filter {
	f := func(op, value string) Filter { return Filter{Attr: a.attr, Op: op, Value: value} }
	switch a.attr {
	case "title", "note":
		// Byte order: "" < "B" < "Z" < "a" < "é".
		return []Filter{f("starts_with", "al"), f("starts_with", "x"), f("contains", "et"), f("contains", "a"),
			f("gt", "B"), f("lt", "a"), f("gte", "é"), f("lt", "B"), f("gt", "Z")}
	case "count", "qty", "rank", "level", "amount", "total":
		// A fractional operand on whole-number storage, and one past the
		// declared width.
		return []Filter{f("gt", "1.5"), f("lte", "2.5"), f("equals", "2.0"), f("not_equals", "2.5"), f("lt", "2147483648"), f("gt", "-2147483649")}
	case "score", "ratio":
		return []Filter{f("gt", "-1.5"), f("lte", "0.1"), f("gte", "1e300")}
	case "ref", "token":
		upper := strings.ToUpper(a.vals[1].(string))
		return []Filter{f("equals", upper), f("gt", upper), f("lte", "7fffffff-0000-4000-8000-000000000000")}
	case "active":
		return []Filter{f("equals", "1"), f("equals", "false"), f("not_equals", "0")}
	case "born", "joined":
		return []Filter{f("gt", "1704240000000"), f("lte", "-86400000"), f("gte", "1969-12-31T00:00:00Z")}
	case "seen", "touched":
		return []Filter{f("gt", "1704164645000"), f("gte", "2024-01-02T03:04:05.678Z"), f("lt", "2024-01-02T03:04:06.000Z")}
	}
	return nil
}

// tierParityConjunctions combine attributes stored in entity_main and in
// eav_data, so one row's inclusion depends on both.
func tierParityConjunctions() []tierParityCase {
	return []tierParityCase{
		{{Attr: "title", Op: "gte", Value: "b"}, {Attr: "count", Op: "lt", Value: "3"}},
		{{Attr: "note", Op: "is_not_null", Value: "true"}, {Attr: "amount", Op: "gt", Value: "10"}},
		{{Attr: "active", Op: "equals", Value: "false"}, {Attr: "seen", Op: "is_null", Value: "true"}},
	}
}

// parityOperand spells a parityAttrs value as a condition operand. A date
// operand names an instant, so a date value reads as its midnight UTC.
func parityOperand(v any) string {
	switch x := v.(type) {
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	case string:
		if len(x) == len("2006-01-02") && strings.Count(x, "-") == 2 {
			return x + "T00:00:00Z"
		}
		return x
	default:
		return fmt.Sprint(x)
	}
}

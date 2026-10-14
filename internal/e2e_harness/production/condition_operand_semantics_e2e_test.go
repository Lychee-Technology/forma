//go:build e2e

package production

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma/internal/model"
)

// operandRow is one seeded entity: num is the value of every numeric
// attribute, text the value of title and note.
type operandRow struct {
	ev   *Event
	num  int64
	text string
}

// TestConditionOperandSemanticsAcrossRoutes runs numeric operands that are
// fractions or beyond a column's width, and starts_with / contains operands
// holding LIKE metacharacters, on the Postgres route, the DuckDB route over
// hot rows (the entity_main pushdown DuckDB evaluates over postgres_scan),
// and the DuckDB route over warm delta parquet. Expected rows come from exact
// arithmetic and Go string matching over the seeded values, never from
// another route. The numeric attributes cover smallint, integer and bigint,
// both column-bound and EAV-only.
func TestConditionOperandSemanticsAcrossRoutes(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	// The DuckDB route reads parquet, so a tier must exist before the probe
	// rows are hot. The filler holds only ratio, which no probe names.
	filler := CreateEvent(wide, map[string]any{"ratio": 0.5})
	mustApplyEvents(ctx, t, env, "seed cold filler", filler)
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	numericAttrs := []string{"rank", "count", "amount", "level", "qty", "total"}
	var rows []operandRow
	for _, seed := range []struct {
		num  int64
		text string
	}{{1, "abc"}, {7, "a_c"}, {8, "a%c"}, {-1, `a\c`}} {
		attrs := map[string]any{"title": seed.text, "note": seed.text}
		for _, a := range numericAttrs {
			attrs[a] = seed.num
		}
		rows = append(rows, operandRow{ev: CreateEvent(wide, attrs), num: seed.num, text: seed.text})
	}
	events := make([]*Event, 0, len(rows))
	for _, r := range rows {
		events = append(events, r.ev)
	}
	mustApplyEvents(ctx, t, env, "seed operand rows", events...)

	all := []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold}
	runOperandProbes(ctx, t, env, "hot-pg", Query{Schema: wide, PreferHot: true, PreferredTiers: all, Limit: 100}, false, rows, numericAttrs)
	runOperandProbes(ctx, t, env, "hot-duck", Query{Schema: wide, PreferredTiers: all, Limit: 100}, true, rows, numericAttrs)

	// Flush the probe rows to warm delta parquet; they leave the dirty set.
	mustFlush(ctx, t, env)
	runOperandProbes(ctx, t, env, "warm-duck", Query{Schema: wide, PreferredTiers: all, Limit: 100}, true, rows, numericAttrs)
}

func runOperandProbes(ctx context.Context, t *testing.T, env *Env, label string, base Query, wantDuck bool,
	rows []operandRow, numericAttrs []string) {
	t.Helper()
	ops := map[string]string{"equals": "=", "not_equals": "!=", "lt": "<", "lte": "<=", "gt": ">", "gte": ">=", "in": "=", "not_in": "!="}
	for _, attr := range numericAttrs {
		for _, literal := range []string{"7.5", "7.9", "1.5", "-0.5", "2147483648", "-2147483649"} {
			operand, _, err := big.ParseFloat(literal, 10, 128, big.ToNearestEven)
			if err != nil {
				t.Fatalf("parse %s: %v", literal, err)
			}
			for op, sqlOp := range ops {
				value := literal
				if op == "in" || op == "not_in" {
					value = "[" + literal + "]"
				}
				var want []uuid.UUID
				for _, r := range rows {
					if operandHolds(sqlOp, new(big.Float).SetInt64(r.num).Cmp(operand)) {
						want = append(want, r.ev.RowID)
					}
				}
				assertOperandProbe(ctx, t, env, label, base, wantDuck, Filter{Attr: attr, Op: op, Value: value}, want)
			}
		}
	}
	for _, attr := range []string{"title", "note"} {
		for _, operand := range []string{"a_c", "_", "%", `a\`, `\c`, "a"} {
			for _, op := range []string{"starts_with", "contains"} {
				var want []uuid.UUID
				for _, r := range rows {
					if (op == "starts_with" && strings.HasPrefix(r.text, operand)) || (op == "contains" && strings.Contains(r.text, operand)) {
						want = append(want, r.ev.RowID)
					}
				}
				assertOperandProbe(ctx, t, env, label, base, wantDuck, Filter{Attr: attr, Op: op, Value: operand}, want)
			}
		}
	}
}

func operandHolds(sqlOp string, c int) bool {
	switch sqlOp {
	case "=":
		return c == 0
	case "!=":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	}
	return c >= 0
}

func assertOperandProbe(ctx context.Context, t *testing.T, env *Env, label string, base Query, wantDuck bool, f Filter, want []uuid.UUID) {
	t.Helper()
	q := base
	q.Filters = []Filter{f}
	res, err := env.Query(ctx, q)
	if err != nil {
		t.Fatalf("%s: %s %s:%s: %v", label, f.Attr, f.Op, f.Value, err)
	}
	if res.Plan.Routing.UseDuckDB != wantDuck {
		t.Fatalf("%s: %s %s:%s routed use_duckdb=%v, want %v (%+v)", label, f.Attr, f.Op, f.Value,
			res.Plan.Routing.UseDuckDB, wantDuck, res.Plan.Routing)
	}
	got := make([]uuid.UUID, 0, len(res.Records))
	for _, r := range res.Records {
		got = append(got, r.RowID)
	}
	if g, w := sortedUUIDs(got), sortedUUIDs(want); !equalUUIDs(g, w) {
		t.Errorf("%s: %s %s:%s returned %v, want %v", label, f.Attr, f.Op, f.Value, g, w)
	}
}

func equalUUIDs(a, b []uuid.UUID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

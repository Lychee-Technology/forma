//go:build e2e

package production

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/lychee-technology/forma"
)

// This file is the #502 acceptance suite: a bigint predicate operand that
// does not fit int64 must get the same answer on the hot Postgres route and on
// every DuckDB tier. The answer is a rejection — shared normalization refuses
// the operand as invalid input on both routes — because BIGINT columns must
// keep exact comparison above 2^53 (#281/#357) and so cannot widen to DOUBLE
// the way #384 widened the narrow classes. Pre-#502 the Postgres route
// compared the operand against NUMERIC and answered, while the DuckDB route
// rendered CAST('1e+30' AS BIGINT) and raised a Conversion Error.

// rejectionProbe is one filter that must fail with forma.ErrInvalidInput on
// every route.
type rejectionProbe struct {
	name   string
	filter Filter
}

// runRejectionProbes asserts every probe fails with a user-facing invalid
// input error on the given route. A nil error is the pre-#502 Postgres
// behaviour (answering); a non-sentinel error is the pre-#502 DuckDB
// behaviour (Conversion Error surfacing as an opaque failure).
func runRejectionProbes(ctx context.Context, t *testing.T, env *Env, label string,
	base Query, probes []rejectionProbe) {
	t.Helper()
	for _, p := range probes {
		q := base
		q.Filters = []Filter{p.filter}
		_, err := env.Query(ctx, q)
		if err == nil {
			t.Errorf("%s/%s: out-of-int64 bigint operand must be rejected, got an answer", label, p.name)
			continue
		}
		if !errors.Is(err, forma.ErrInvalidInput) {
			t.Errorf("%s/%s: rejection must be user-facing invalid input, got %v", label, p.name, err)
		}
	}
}

// TestBigIntOperandRangeParityBothDialects seeds a bound bigint (`amount`,
// bigint_01) at MaxInt64 and an EAV-only bigint (`total`, attr 15), then
// drives the same probes through hot Postgres, warm delta (real flush) and
// cold base (real compaction): the out-of-int64 operands are rejected
// identically everywhere, and the boundary literals keep answering the same
// row set — the rejection must not clip MaxInt64 itself.
func TestBigIntOperandRangeParityBothDialects(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	rowMax := CreateEvent(wide, map[string]any{"title": "r-max", "amount": int64(math.MaxInt64), "total": int64(7)})
	rowSmall := CreateEvent(wide, map[string]any{"title": "r-small", "amount": int64(5), "total": int64(9)})
	mustApplyEvents(ctx, t, env, "bigint operand range creates", rowMax, rowSmall)

	rejected := []rejectionProbe{
		// The issue's headline: an EAV-only bigint compared against 1e30.
		{"total_gt_1e30", Filter{Attr: "total", Op: "gt", Value: "1e30"}},
		// The bound bigint takes the pg-main binder on Postgres and the same
		// DuckDB cast on the other route.
		{"amount_gt_1e30", Filter{Attr: "amount", Op: "gt", Value: "1e30"}},
		{"amount_lt_negative_1e19", Filter{Attr: "amount", Op: "lt", Value: "-1e19"}},
		// The first integer past MaxInt64, in bare digits.
		{"total_equals_2p63", Filter{Attr: "total", Op: "equals", Value: "9223372036854775808"}},
	}
	controls := []widthProbe{
		// MaxInt64 stays addressable in bare and exponent spelling (#357).
		{"amount_gte_maxint64", Filter{Attr: "amount", Op: "gte", Value: "9223372036854775807"}, []*Event{rowMax}},
		{"amount_gte_maxint64_exponent", Filter{Attr: "amount", Op: "gte", Value: "9.223372036854775807e18"}, []*Event{rowMax}},
		// MinInt64 as a bound answers (everything is above it).
		{"amount_gt_minint64", Filter{Attr: "amount", Op: "gt", Value: "-9223372036854775808"}, []*Event{rowMax, rowSmall}},
		// The exact in-range equivalent of the rejected gt:1e30 answers empty.
		{"total_gt_maxint64_empty", Filter{Attr: "total", Op: "gt", Value: "9223372036854775807"}, nil},
		{"total_lte_maxint64", Filter{Attr: "total", Op: "lte", Value: "9223372036854775807"}, []*Event{rowMax, rowSmall}},
	}

	hotBase := Query{Schema: wide, PreferHot: true, Limit: 100}
	runRejectionProbes(ctx, t, env, "hot-pg", hotBase, rejected)
	runWidthProbes(ctx, t, env, "hot-pg", hotBase, false, controls)

	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	duckBase := Query{Schema: wide, Limit: 100}
	runRejectionProbes(ctx, t, env, "warm-duck", duckBase, rejected)
	runWidthProbes(ctx, t, env, "warm-duck", duckBase, true, controls)

	if _, err := env.RunCompaction(ctx, wide); err != nil {
		t.Fatalf("compaction: %v", err)
	}
	runRejectionProbes(ctx, t, env, "cold-duck", duckBase, rejected)
	runWidthProbes(ctx, t, env, "cold-duck", duckBase, true, controls)
}

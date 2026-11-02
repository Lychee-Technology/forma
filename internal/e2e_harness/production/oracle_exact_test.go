package production

import (
	"math"
	"testing"

	"github.com/google/uuid"
	forma "github.com/lychee-technology/forma"
)

// exactCache is wideCache plus the two bigint shapes whose contracts differ:
// "amount" is column-bound (bigint_01, the full int64 range, #205) and
// "total" is EAV-only (value_numeric, capped at ±2^53 by the write, #590).
func exactCache() forma.SchemaAttributeCache {
	cache := wideCache()
	cache["amount"] = forma.AttributeMetadata{AttributeID: 4, ValueType: forma.ValueTypeBigInt,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnBigint01}}
	cache["total"] = forma.AttributeMetadata{AttributeID: 15, ValueType: forma.ValueTypeBigInt}
	return cache
}

func mustExactState(t *testing.T, events []*Event) *ExpectedState {
	t.Helper()
	state, err := ExpectedStateFromEvents(events, testSchema(), exactCache())
	if err != nil {
		t.Fatalf("build expected state: %v", err)
	}
	return state
}

// pow53Events creates the bound-bigint rows in DESCENDING value order, so
// ascending row_id (UUIDv7 creation order in the real harness, newRowID
// order here) runs against ascending amount. An oracle that folds bigint to
// float64 sees 2^53 and 2^53+1 tie and lets the row_id tiebreak put 2^53+1
// first, which is the wrong order.
func pow53Events() []*Event {
	amounts := []int64{math.MaxInt64, math.MaxInt64 - 1, 1<<53 + 1, 1 << 53, 1<<53 - 1}
	events := make([]*Event, 0, len(amounts))
	for i, amount := range amounts {
		events = append(events, &Event{
			Kind: EventCreate, Schema: testSchema(), RowID: newRowID(byte(i + 1)),
			Seq: i + 1, ChangedAt: int64(i + 1),
			Attrs: map[string]any{"title": "r", "amount": amount},
		})
	}
	return events
}

func TestOracle_BoundBigintKeepsExactInt64(t *testing.T) {
	state := mustExactState(t, pow53Events())
	got := state.Rows[newRowID(3)].Attrs["amount"]
	if got != int64(1<<53+1) {
		t.Fatalf("amount = %v (%T), want int64 9007199254740993", got, got)
	}
	if compareValues(state.Rows[newRowID(3)].Attrs["amount"], state.Rows[newRowID(4)].Attrs["amount"]) == 0 {
		t.Fatal("2^53+1 and 2^53 compare equal: the oracle lost bigint precision")
	}
}

func TestOracle_BoundBigintFiltersAreExact(t *testing.T) {
	state := mustExactState(t, pow53Events())
	cases := []struct {
		name   string
		filter Filter
		want   []uuid.UUID
	}{
		{"equals 2^53+1", Filter{Attr: "amount", Value: "9007199254740993"}, []uuid.UUID{newRowID(3)}},
		{"equals 2^53 excludes neighbour", Filter{Attr: "amount", Value: "9007199254740992"}, []uuid.UUID{newRowID(4)}},
		{"decimal spelling", Filter{Attr: "amount", Value: "9007199254740993.0"}, []uuid.UUID{newRowID(3)}},
		{"exponent spelling", Filter{Attr: "amount", Value: "9.007199254740993e15"}, []uuid.UUID{newRowID(3)}},
		{"equals MaxInt64-1", Filter{Attr: "amount", Value: "9223372036854775806"}, []uuid.UUID{newRowID(2)}},
		{"gt 2^53", Filter{Attr: "amount", Op: "gt", Value: "9007199254740992"},
			[]uuid.UUID{newRowID(1), newRowID(2), newRowID(3)}},
		// A fractional operand compares as its float64 image, as the engine
		// binds it (sqlgen.numericOperand): 2^53+0.5 rounds to 2^53 and
		// 2^53+1.5 to 2^53+2, so these differ from the exact rational.
		{"lt fraction compares its image", Filter{Attr: "amount", Op: "lt", Value: "9007199254740992.5"},
			[]uuid.UUID{newRowID(5)}},
		{"gte fraction compares its image", Filter{Attr: "amount", Op: "gte", Value: "9007199254740992.5"},
			[]uuid.UUID{newRowID(1), newRowID(2), newRowID(3), newRowID(4)}},
		{"equals fraction matches its image", Filter{Attr: "amount", Value: "9007199254740992.5"},
			[]uuid.UUID{newRowID(4)}},
		{"gt fraction rounding up", Filter{Attr: "amount", Op: "gt", Value: "9007199254740993.5"},
			[]uuid.UUID{newRowID(1), newRowID(2)}},
		// 2^53-0.5 lies halfway between two doubles and rounds to the even
		// one, 2^53, so gt excludes 2^53 although the rational is below it.
		{"gt halfway fraction rounds to even", Filter{Attr: "amount", Op: "gt", Value: "9007199254740991.5"},
			[]uuid.UUID{newRowID(1), newRowID(2), newRowID(3)}},
		// MaxInt64's decimal and exponent spellings round to the double 2^63,
		// outside int64, yet still denote MaxInt64 exactly, as the engine binds
		// them (numutil.TryParseNumber).
		{"decimal spelling of MaxInt64", Filter{Attr: "amount", Value: "9223372036854775807.0"},
			[]uuid.UUID{newRowID(1)}},
		{"lt exponent spelling of MaxInt64", Filter{Attr: "amount", Op: "lt", Value: "9.223372036854775807e18"},
			[]uuid.UUID{newRowID(2), newRowID(3), newRowID(4), newRowID(5)}},
		{"lt 2^63 keeps MaxInt64", Filter{Attr: "amount", Op: "lt", Value: "9223372036854775808.0"},
			[]uuid.UUID{newRowID(1), newRowID(2), newRowID(3), newRowID(4), newRowID(5)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := mustRun(t, state, Query{Schema: testSchema(), Filters: []Filter{tc.filter}})
			assertOracleRows(t, result, tc.want)
		})
	}
}

// The oracle accepts the operands the engine accepts: ParseFloat's grammar,
// finite values only. A rational spelling, NaN, an infinity and a literal
// past float64 are refused, so no oracle result exists for a query the
// engine rejects.
func TestParseExactInteger_RefusesWhatTheEngineRefuses(t *testing.T) {
	for _, raw := range []string{"1/3", "NaN", "Inf", "-Infinity", "1e400", "abc"} {
		if got, err := parseExactInteger(raw); err == nil {
			t.Errorf("parseExactInteger(%q) = %v (%T), want an error", raw, got, got)
		}
	}
}

// At the int64 edges the float64 image of an integral literal can be ±2^63
// whether or not the literal itself is in range: the literal decides.
func TestParseExactInteger_Int64Edges(t *testing.T) {
	cases := []struct {
		raw  string
		want any
	}{
		{"9223372036854775807.0", int64(math.MaxInt64)},
		{"9.223372036854775807e18", int64(math.MaxInt64)},
		{"-9223372036854775808.0", int64(math.MinInt64)},
		{"-9.223372036854775808e18", int64(math.MinInt64)},
		{"9223372036854775808.0", math.Ldexp(1, 63)},
		{"-9223372036854775809.0", -math.Ldexp(1, 63)},
		{"9223372036854775806.5", math.Ldexp(1, 63)},
	}
	for _, tc := range cases {
		got, err := parseExactInteger(tc.raw)
		if err != nil {
			t.Errorf("parseExactInteger(%q): %v", tc.raw, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseExactInteger(%q) = %v (%T), want %v (%T)", tc.raw, got, got, tc.want, tc.want)
		}
	}
}

func TestOracle_BoundBigintSortIsExact(t *testing.T) {
	state := mustExactState(t, pow53Events())
	asc := mustRun(t, state, Query{Schema: testSchema(), Sorts: []Sort{{Attr: "amount"}}})
	assertOracleRows(t, asc, []uuid.UUID{newRowID(5), newRowID(4), newRowID(3), newRowID(2), newRowID(1)})
	desc := mustRun(t, state, Query{Schema: testSchema(), Sorts: []Sort{{Attr: "amount", Desc: true}}})
	assertOracleRows(t, desc, []uuid.UUID{newRowID(1), newRowID(2), newRowID(3), newRowID(4), newRowID(5)})
}

// The EAV-only bigint keeps its float64 storage image (#590 bounds it to
// ±2^53, where that image is exact): the oracle must still equate the
// event's int64 with the value_numeric double the engine returns.
func TestOracle_EAVBigintMatchesItsDoubleImage(t *testing.T) {
	state := mustExactState(t, []*Event{{
		Kind: EventCreate, Schema: testSchema(), RowID: newRowID(1), Seq: 1, ChangedAt: 1,
		Attrs: map[string]any{"title": "r", "total": int64(1 << 53)},
	}})
	stored := normalizeEAVNumeric(float64(1<<53), forma.ValueTypeBigInt)
	if !valuesEqual(state.Rows[newRowID(1)].Attrs["total"], stored) {
		t.Fatalf("expected %v does not equal stored image %v", state.Rows[newRowID(1)].Attrs["total"], stored)
	}
	if valuesEqual(int64(1<<53+1), stored) {
		t.Fatal("int64 2^53+1 equals the double 2^53: mixed comparison is lossy")
	}
}

func TestCompareValues_MixedNumericForms(t *testing.T) {
	cases := []struct {
		name string
		a, b any
		want int
	}{
		{"int64 vs equal double", int64(7), float64(7), 0},
		{"int64 above 2^53 vs its rounded double", int64(1<<53 + 1), float64(1 << 53), 1},
		{"double vs larger int64", float64(1 << 53), int64(1<<53 + 1), -1},
		{"MaxInt64 vs 2^63 double", int64(math.MaxInt64), math.Ldexp(1, 63), -1},
		{"int64 vs fraction", int64(2), float64(2.5), -1},
		{"int64 vs +Inf", int64(math.MaxInt64), math.Inf(1), -1},
		{"doubles", float64(1.5), float64(2.5), -1},
		{"strings", "a", "b", -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := compareValues(tc.a, tc.b); got != tc.want {
				t.Fatalf("compareValues(%v, %v) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func assertOracleRows(t *testing.T, result *ExpectedResult, want []uuid.UUID) {
	t.Helper()
	got := make([]uuid.UUID, 0, len(result.Rows))
	for _, row := range result.Rows {
		got = append(got, row.RowID)
	}
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("rows = %v, want %v", got, want)
		}
	}
	if result.Total != int64(len(want)) {
		t.Fatalf("total = %d, want %d", result.Total, len(want))
	}
}

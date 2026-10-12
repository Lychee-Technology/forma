package sqlgen

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// extendedCache extends the characterization cache with the value types the
// #646 operators treat specially: a list attribute, an unbound date and an
// EAV-only integer (DOUBLE storage width, #384).
func extendedCache() forma.SchemaAttributeCache {
	cache := characterizationCache()
	cache["labels"] = forma.AttributeMetadata{AttributeID: 20, ValueType: forma.ValueTypeList}
	cache["due"] = forma.AttributeMetadata{AttributeID: 21, ValueType: forma.ValueTypeDate}
	cache["qty"] = forma.AttributeMetadata{AttributeID: 22, ValueType: forma.ValueTypeInteger}
	return cache
}

func charEavHead(attrPh string) string {
	return "SELECT 1 FROM eav_table x WHERE x.schema_id = e.schema_id AND x.row_id = e.row_id AND x.attr_id = " + attrPh
}

func buildExtendedSetCases() []charCase {
	return []charCase{
		{
			name: "in text bound: pg-main skips, one array bind per target",
			cond: charKv("username", `in:["Alice","Bob"]`),
			want: DualClauses{
				PgClause:   "EXISTS (" + charEavHead("$1") + " AND x.value_text = ANY($2))",
				PgArgs:     []any{int16(1), []string{"Alice", "Bob"}},
				DuckClause: "username = ANY(CAST(? AS VARCHAR[]))", DuckArgs: []any{[]string{"Alice", "Bob"}},
			},
			span: 2,
		},
		{
			name: "not_in integer bound: DOUBLE width, NULL excluded",
			cond: charKv("age", "not_in:[1, 2.5, 3]"),
			want: DualClauses{
				PgClause:   "EXISTS (" + charEavHead("$1") + " AND (x.value_numeric IS NOT NULL AND x.value_numeric <> ALL($2)))",
				PgArgs:     []any{int16(2), []float64{1, 2.5, 3}},
				DuckClause: "(age IS NOT NULL AND age <> ALL(CAST(? AS DOUBLE[])))",
				DuckArgs:   []any{[]float64{1, 2.5, 3}},
			},
			span: 2,
		},
		{
			name: "in bigint keeps exact int64 and drops fractions",
			cond: charKv("total", "in:[1, 1.5, 9007199254740993]"),
			want: DualClauses{
				PgClause:   "EXISTS (" + charEavHead("$1") + " AND x.value_numeric = ANY($2))",
				PgArgs:     []any{int16(13), []int64{1, 9007199254740993}},
				DuckClause: "total = ANY(CAST(? AS BIGINT[]))", DuckArgs: []any{[]int64{1, 9007199254740993}},
			},
			span: 2,
		},
		{
			// The storage is DOUBLE, so 1.5 can match fractional history, as
			// equals:1.5 does; only whole-number storage drops it.
			name: "in EAV-only integer keeps fractions at DOUBLE storage width",
			cond: charKv("qty", "in:[1.5, 2]"),
			want: DualClauses{
				PgClause:   "EXISTS (" + charEavHead("$1") + " AND x.value_numeric = ANY($2))",
				PgArgs:     []any{int16(22), []float64{1.5, 2}},
				DuckClause: "qty = ANY(CAST(? AS DOUBLE[]))", DuckArgs: []any{[]float64{1.5, 2}},
			},
			span: 2,
		},
		{
			name: "in uuid canonicalizes and compares the VARCHAR image on DuckDB",
			cond: charKv("ref", `in:["6F1C3C3E-1111-4111-8111-111111111111"]`),
			want: DualClauses{
				PgClause:   "EXISTS (" + charEavHead("$1") + " AND x.value_text = ANY($2))",
				PgArgs:     []any{int16(11), []string{"6f1c3c3e-1111-4111-8111-111111111111"}},
				DuckClause: "CAST(ref AS VARCHAR) = ANY(CAST(? AS VARCHAR[]))",
				DuckArgs:   []any{[]string{"6f1c3c3e-1111-4111-8111-111111111111"}},
			},
			span: 2,
		},
		{
			name: "in bool compares the truthiness image",
			cond: charKv("flag", `in:[true, 0]`),
			want: DualClauses{
				PgClause:   "EXISTS (" + charEavHead("$1") + " AND (x.value_numeric > 0.5) = ANY($2))",
				PgArgs:     []any{int16(7), []bool{true, false}},
				DuckClause: "flag = ANY(CAST(? AS BOOLEAN[]))", DuckArgs: []any{[]bool{true, false}},
			},
			span: 2,
		},
		{
			name: "in date takes epoch millis under the equals rules",
			cond: charKv("born", `in:["2024-01-02T00:00:00Z", 1704153600000]`),
			want: DualClauses{
				PgClause:   "EXISTS (" + charEavHead("$1") + " AND x.value_numeric = ANY($2))",
				PgArgs:     []any{int16(8), []int64{1704153600000, 1704153600000}},
				DuckClause: "born = ANY(CAST(? AS BIGINT[]))", DuckArgs: []any{[]int64{1704153600000, 1704153600000}},
			},
			span: 2,
		},
	}
}

func buildExtendedNullCases() []charCase {
	return []charCase{
		{
			name: "is_null bound pushes down without a bind",
			cond: charKv("username", "is_null:true"),
			want: DualClauses{
				PgMainClause: "m.text_01 IS NULL",
				PgClause:     "NOT EXISTS (" + charEavHead("$1") + " AND x.value_text IS NOT NULL)",
				PgArgs:       []any{int16(1)},
				DuckClause:   "username IS NULL",
			},
			span: 1,
		},
		{
			name: "is_not_null unbound",
			cond: charKv("tag", "is_not_null:true"),
			want: DualClauses{
				PgClause:   "EXISTS (" + charEavHead("$1") + " AND x.value_text IS NOT NULL)",
				PgArgs:     []any{int16(4)},
				DuckClause: "tag IS NOT NULL",
			},
			span: 1,
		},
		{
			name: "is_null on a list: any row, the empty-list marker included, is presence",
			cond: charKv("labels", "is_null:true"),
			want: DualClauses{
				PgClause:   "NOT EXISTS (" + charEavHead("$1") + ")",
				PgArgs:     []any{int16(20)},
				DuckClause: "labels IS NULL",
			},
			span: 1,
		},
		{
			name: "is_not_null bool bound",
			cond: charKv("active", "is_not_null:true"),
			want: DualClauses{
				PgMainClause: "m.bool_01 IS NOT NULL",
				PgClause:     "EXISTS (" + charEavHead("$1") + " AND x.value_numeric IS NOT NULL)",
				PgArgs:       []any{int16(5)},
				DuckClause:   "active IS NOT NULL",
			},
			span: 1,
		},
		{
			name: "AND of null test and equals pushes both",
			cond: charAnd(charKv("username", "is_null:true"), charKv("age", "equals:3")),
			want: DualClauses{
				PgMainClause: "((m.text_01 IS NULL) AND (m.integer_01 = ?))",
				PgMainArgs:   []any{int64(3)},
				PgClause: "((NOT EXISTS (" + charEavHead("$2") + " AND x.value_text IS NOT NULL)) AND (" +
					charEavClause("$3", "value_numeric", "=", "$4") + "))",
				PgArgs:     []any{int16(1), int16(2), float64(3)},
				DuckClause: "(username IS NULL) AND (age = CAST(? AS DOUBLE))",
				DuckArgs:   []any{int64(3)},
			},
			span: 4,
		},
		{
			name: "OR with a set leaf is not pushed down",
			cond: charOr(charKv("username", "is_null:true"), charKv("age", "in:[3]")),
			want: DualClauses{
				PgClause: "((NOT EXISTS (" + charEavHead("$1") + " AND x.value_text IS NOT NULL)) OR (EXISTS (" +
					charEavHead("$2") + " AND x.value_numeric = ANY($3))))",
				PgArgs:     []any{int16(1), int16(2), []float64{3}},
				DuckClause: "(username IS NULL) OR (age = ANY(CAST(? AS DOUBLE[])))",
				DuckArgs:   []any{[]float64{3}},
			},
			span: 3,
		},
		{
			name: "$now on a text attribute is a literal",
			cond: charKv("username", "equals:$now"),
			want: DualClauses{
				PgMainClause: "m.text_01 = ?", PgMainArgs: []any{"$now"},
				PgClause: charEavClause("$2", "value_text", "=", "$3"), PgArgs: []any{int16(1), "$now"},
				DuckClause: "username = ?", DuckArgs: []any{"$now"},
			},
			span: 3,
		},
	}
}

func TestToDualClauses_ExtendedOperators(t *testing.T) {
	cache := extendedCache()
	cases := append(buildExtendedSetCases(), buildExtendedNullCases()...)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			paramIndex := 0
			dc, err := ToDualClauses(tc.cond, "eav_table", 7, cache, &paramIndex)
			require.NoError(t, err)
			require.Equal(t, tc.want, dc)
			require.Equal(t, tc.span, paramIndex, "paramIndex advancement")

			plan, err := PlanDualClauses(tc.cond, "eav_table", 7, cache, 0)
			require.NoError(t, err)
			bound := 0
			got, err := plan.Bind(tc.cond, cache, &bound)
			require.NoError(t, err)
			require.Equal(t, dc, got)
			require.Equal(t, paramIndex, bound)
		})
	}
}

func TestToDualClauses_ExtendedOperatorErrors(t *testing.T) {
	cache := extendedCache()
	oversized := "in:[" + strings.Repeat("1,", 1000) + "1]"
	cases := []struct {
		name    string
		cond    forma.Condition
		wantErr string
	}{
		{"$now on a federated datetime", charKv("seen", "lte:$now-30s"),
			"the $now operand on datetime attribute 'seen' is not supported on a federated query"},
		{"$now on a federated bound date", charKv("born", "equals:$now"),
			"the $now operand on date attribute 'born' is not supported on a federated query"},
		{"empty list", charKv("tag", "in:[]"),
			"operator 'in' on attribute 'tag' takes a JSON array of 1 to 1000 values; the array is empty"},
		{"oversized list", charKv("tag", oversized),
			"operator 'in' on attribute 'tag' takes a JSON array of 1 to 1000 values; the array has more than 1000 values"},
		{"non-array operand", charKv("tag", `not_in:"x"`),
			"operator 'not_in' on attribute 'tag' takes a JSON array of 1 to 1000 values; the value is not a JSON array"},
		{"null element", charKv("age", "in:[1,null]"),
			"element at index 1 of the 'in' list for attribute 'age' is null"},
		{"unparseable number", charKv("age", `in:[1,"x"]`),
			`element at index 1 of the 'in' list for integer attribute 'age': "x" is not a number`},
		{"invalid uuid", charKv("ref", `in:["nope"]`),
			`element at index 0 of the 'in' list for uuid attribute 'ref': "nope" is not a UUID`},
		{"invalid boolean", charKv("flag", `not_in:["maybe"]`),
			`element at index 0 of the 'not_in' list for bool attribute 'flag': "maybe" is not a boolean`},
		{"invalid date", charKv("seen", `in:["yesterday"]`),
			"element at index 0 of the 'in' list for datetime attribute 'seen': invalid date value"},
		{"fractional second on iso8601", charKv("joined", `in:["2024-01-02T03:04:05.5Z"]`),
			"element at index 0 of the 'in' list for datetime attribute 'joined': datetime filter value 2024-01-02T03:04:05.5Z"},
		{"$now inside a list", charKv("seen", `in:["$now"]`),
			"element at index 0 of the 'in' list for datetime attribute 'seen': the $now operand is not accepted inside a list"},
		{"bigint out of range", charKv("total", "in:[2, 1e30]"),
			"element at index 1 of the 'in' list for bigint attribute 'total': value 1e30 out of range for bigint attribute 'total'"},
		{"list attribute", charKv("labels", `in:["a"]`),
			"operator 'in' is not supported on list attribute 'labels'"},
		{"is_null false", charKv("tag", "is_null:false"),
			`operator 'is_null' on attribute 'tag' takes only the value true, got "false"`},
		{"is_not_null yes", charKv("username", "is_not_null:yes"),
			`operator 'is_not_null' on attribute 'username' takes only the value true, got "yes"`},
		{"set on an unknown attribute", charKv("ghost", `in:["a"]`), "attribute not found in cache: ghost"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			paramIndex := 0
			_, err := ToDualClauses(tc.cond, "eav_table", 7, cache, &paramIndex)
			require.Error(t, err)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, ok := forma.ResolvePublicMessage(err)
			require.True(t, ok, "the refusal must publish its message: %v", err)
			require.Contains(t, msg, tc.wantErr)
		})
	}
}

// The federated plan cache keys on (attribute, operator) alone and reuses the
// cached clause text, so a plan compiled for one list must bind any other
// list of any length.
func TestPlanDualClauses_SetOperandIsOneBindAtAnyLength(t *testing.T) {
	cache := extendedCache()
	short := charKv("username", `in:["a","b"]`)
	long := charKv("username", `in:["a","b","c","d","e","f","g"]`)

	shortPlan, err := PlanDualClauses(short, "eav_table", 7, cache, 0)
	require.NoError(t, err)
	longPlan, err := PlanDualClauses(long, "eav_table", 7, cache, 0)
	require.NoError(t, err)
	require.Equal(t, shortPlan, longPlan)

	idx := 0
	bound, err := shortPlan.Bind(long, cache, &idx)
	require.NoError(t, err)
	want := []string{"a", "b", "c", "d", "e", "f", "g"}
	require.Equal(t, []any{int16(1), want}, bound.PgArgs)
	require.Equal(t, []any{want}, bound.DuckArgs)
	require.Equal(t, shortPlan.ParamSpan, idx)
}

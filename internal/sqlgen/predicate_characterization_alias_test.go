package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestToDualClauses_OperatorAliasesRenderAsCanonical pins #469: an operator
// alias (eq, neq) or a different letter case is the same operator as its
// canonical spelling, as docs/condition-dsl.md states. Each pair must render
// byte-identical clauses and args on all three emitters, or be refused by
// both, whether the attribute is column-bound or stored in eav_data. Before
// #469 the pg-main pushability check read the raw spelling, so eq/neq on a
// bound column answered 400 while EAV and DuckDB accepted them.
func TestToDualClauses_OperatorAliasesRenderAsCanonical(t *testing.T) {
	cache := characterizationCache()
	cases := []struct {
		name             string
		canonical, alias forma.Condition
	}{
		{"bound text eq", charKv("username", "equals:Alice"), charKv("username", "eq:Alice")},
		{"bound integer eq", charKv("age", "equals:2"), charKv("age", "eq:2")},
		{"bound integer neq", charKv("age", "not_equals:7"), charKv("age", "neq:7")},
		{"bound bigint neq", charKv("amount", "not_equals:7"), charKv("amount", "neq:7")},
		{"bound bool int eq", charKv("active", "equals:true"), charKv("active", "eq:true")},
		{"bound bool text neq", charKv("verified", "not_equals:true"), charKv("verified", "neq:true")},
		{"bound unix-ms date neq", charKv("born", "not_equals:1704164645000"), charKv("born", "neq:1704164645000")},
		{"bound iso8601 datetime eq", charKv("joined", "equals:2024-01-02T03:04:05Z"), charKv("joined", "eq:2024-01-02T03:04:05Z")},
		{"bound uuid eq", charKv("owner", "equals:0b210f52-1f4d-4f47-9799-1e2f2c0efc07"), charKv("owner", "eq:0b210f52-1f4d-4f47-9799-1e2f2c0efc07")},
		{"eav text eq", charKv("tag", "equals:x"), charKv("tag", "eq:x")},
		{"eav text neq", charKv("tag", "not_equals:x"), charKv("tag", "neq:x")},
		{"eav numeric eq", charKv("score", "equals:1.5"), charKv("score", "eq:1.5")},
		{"eav numeric neq", charKv("score", "not_equals:1.5"), charKv("score", "neq:1.5")},
		{"eav bool neq", charKv("flag", "not_equals:true"), charKv("flag", "neq:true")},
		// not_equals on a bound text column is refused today (#676); neq
		// must share that answer rather than diverge from it.
		{"bound text neq refused like not_equals", charKv("username", "not_equals:x"), charKv("username", "neq:x")},
		{"upper-case alias", charKv("age", "equals:2"), charKv("age", "EQ:2")},
		{"mixed-case canonical", charKv("age", "not_equals:7"), charKv("age", "Not_Equals:7")},
		{"upper-case range operator", charKv("age", "gt:7"), charKv("age", "GT:7")},
		{"upper-case LIKE operator", charKv("username", "starts_with:Al"), charKv("username", "STARTS_WITH:Al")},
		// An OR is pushed only when every branch is pushable, so an alias
		// leaf must not veto the group.
		{"alias inside OR",
			charOr(charKv("age", "equals:2"), charKv("username", "equals:A")),
			charOr(charKv("age", "eq:2"), charKv("username", "eq:A"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantIdx := 0
			want, wantErr := ToDualClauses(tc.canonical, "eav_table", 7, cache, &wantIdx)
			gotIdx := 0
			got, gotErr := ToDualClauses(tc.alias, "eav_table", 7, cache, &gotIdx)
			if wantErr != nil {
				require.Error(t, gotErr, "the canonical spelling is refused (%v); the alias must be too", wantErr)
				require.ErrorIs(t, gotErr, forma.ErrInvalidInput)
				return
			}
			require.NoError(t, gotErr)
			require.Equal(t, want, got)
			require.Equal(t, wantIdx, gotIdx, "paramIndex advancement")
		})
	}
}

// TestToDualClauses_DesignExampleFilter pins the worked example of
// docs/federated-query/design.md §4.1 (symbol eq:AAPL AND trade_type eq:2)
// with both attributes column-bound: both leaves push to entity_main.
func TestToDualClauses_DesignExampleFilter(t *testing.T) {
	cond := charAnd(charKv("username", "eq:AAPL"), charKv("age", "eq:2"))
	paramIndex := 0
	dc, err := ToDualClauses(cond, "eav_table", 7, characterizationCache(), &paramIndex)
	require.NoError(t, err)
	require.Equal(t, DualClauses{
		PgMainClause: "((m.text_01 = ?) AND (m.integer_01 = ?))",
		PgMainArgs:   []any{"AAPL", int64(2)},
		PgClause: "((" + charEavClause("$3", "value_text", "=", "$4") + ") AND (" +
			charEavClause("$5", "value_numeric", "=", "$6") + "))",
		PgArgs:     []any{int16(1), "AAPL", int16(2), float64(2)},
		DuckClause: "(username = ?) AND (age = CAST(? AS DOUBLE))",
		DuckArgs:   []any{"AAPL", int64(2)},
	}, dc)
	require.Equal(t, 6, paramIndex)
}

package sqlgen

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hybridLeafClause renders a leaf through the Postgres route's two spellings:
// RenderMain for a main-branch leaf and Render for an EAV one.
func hybridLeafClause(t *testing.T, attr, value string, cache forma.SchemaAttributeCache) string {
	t.Helper()
	leaf := normalizePredicates(&forma.KvCondition{Attr: attr, Value: value}, cache, targetHybrid).(*PredicateLeaf)
	p := leaf.Hybrid
	require.NoError(t, p.Err)
	require.NoError(t, p.Eav.Err)
	next := func() string { return "$n" }
	if p.IsMain {
		clause, _ := p.RenderMain("m."+p.MainColumn, next)
		return clause
	}
	return p.Eav.Render("eav", "m.s", "m.r", next)
}

// TestPostgresTextRangeRendersInByteOrder pins #728's rendering on the Postgres
// route: a range over a text column or over eav_data.value_text names the C
// collation on the column, and nothing else does. A column-bound uuid keeps its
// native comparison, since uuid takes no collation; an rfc3339 date stays on
// the column's own collation, so a declared index keeps seeking it.
func TestPostgresTextRangeRendersInByteOrder(t *testing.T) {
	const operandUUID = "6f1c3c3e-1111-4111-8111-111111111111"
	cases := []struct {
		name, attr, value, want string
	}{
		{"bound text gt", "username", "gt:M", `m.text_01 COLLATE "C" > $n`},
		{"bound text gte", "username", "gte:M", `m.text_01 COLLATE "C" >= $n`},
		{"bound text lt", "username", "lt:M", `m.text_01 COLLATE "C" < $n`},
		{"bound text lte", "username", "lte:M", `m.text_01 COLLATE "C" <= $n`},
		{"raw text column", "text_09", "gt:M", `m.text_09 COLLATE "C" > $n`},
		{"eav text gt", "tag", "gt:M", `x.value_text COLLATE "C" > $n`},
		{"eav text gte", "tag", "gte:M", `x.value_text COLLATE "C" >= $n`},
		{"eav text lt", "tag", "lt:M", `x.value_text COLLATE "C" < $n`},
		{"eav text lte", "tag", "lte:M", `x.value_text COLLATE "C" <= $n`},
		{"eav uuid range", "ref", "gt:" + operandUUID, `x.value_text COLLATE "C" > $n`},

		{"bound uuid range is native", "owner", "gt:" + operandUUID, `m.uuid_01 > $n`},
		{"raw uuid column is native", "uuid_02", "gt:" + operandUUID, `m.uuid_02 > $n`},
		{"rfc3339 date range", "joined", "gt:2024-01-02T03:04:05Z", `m.text_03 > $n`},
		{"bound numeric range", "age", "gt:7", `m.integer_01 > $n::bigint`},
		{"bound text equals", "username", "equals:M", `m.text_01 = $n`},
		{"bound text not_equals", "username", "not_equals:M", `m.text_01 != $n`},
		{"bound text starts_with", "username", "starts_with:M", `m.text_01 LIKE $n`},
		{"eav text equals", "tag", "equals:M", `x.value_text = $n`},
		{"eav text contains", "tag", "contains:M", `x.value_text LIKE $n`},
		{"eav text in", "tag", `in:["M"]`, `x.value_text = ANY($n)`},
		{"eav uuid equals", "ref", "equals:" + operandUUID, `x.value_text = $n`},
		{"eav numeric range", "score", "gt:7", `x.value_numeric > $n`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clause := hybridLeafClause(t, tc.attr, tc.value, characterizationCache())
			assert.Contains(t, clause, tc.want)
			assert.Equal(t, strings.Contains(tc.want, "COLLATE"), strings.Contains(clause, "COLLATE"), clause)
		})
	}
}

// TestDuckDBRangeClausesNameNoCollation pins the other half of #728's rule:
// the DuckDB route, and the pushdown it evaluates itself, already compare in
// byte order. A collation there would be a second definition of that order.
func TestDuckDBRangeClausesNameNoCollation(t *testing.T) {
	for _, attr := range []string{"username", "tag"} {
		paramIndex := 0
		dc, err := ToDualClauses(charKv(attr, "gt:M"), "eav_table", 7, characterizationCache(), &paramIndex)
		require.NoError(t, err)
		assert.NotContains(t, dc.DuckClause, "COLLATE", attr)
		assert.NotContains(t, dc.PgMainClause, "COLLATE", attr)
	}
}

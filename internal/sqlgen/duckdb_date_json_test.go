package sqlgen

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// dateJSONCache mixes date/datetime attributes with numeric neighbours, the
// shape under which DuckDB unifies a struct field across list elements.
func dateJSONCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"seenAt": {AttributeID: 9, ValueType: forma.ValueTypeDateTime},
		"amount": {AttributeID: 11, ValueType: forma.ValueTypeNumeric},
		"days":   {AttributeID: 12, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeDate},
		"scores": {AttributeID: 13, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeNumeric},
		"flag":   {AttributeID: 14, ValueType: forma.ValueTypeBool},
	}
}

// attributesJSONExpr cuts the attributes_json expression out of the outer
// select so it can run against a literal row.
func attributesJSONExpr(t *testing.T, sp *SchemaProjection) string {
	t.Helper()
	idx := strings.Index(sp.OuterSelect, "to_json(list_filter(")
	require.GreaterOrEqual(t, idx, 0)
	end := strings.Index(sp.OuterSelect[idx:], " AS attributes_json")
	require.GreaterOrEqual(t, end, 0)
	return sp.OuterSelect[idx : idx+end]
}

// #592: a date/datetime value_numeric is the unified epoch-ms BIGINT as
// JSON, scalar and list; every other type keeps its expression.
func TestAttributesJSON_DateValueNumericIsTheExactBigint(t *testing.T) {
	sp, err := BuildSchemaProjection(3, dateJSONCache())
	require.NoError(t, err)

	require.Contains(t, sp.OuterSelect, "'attr_id': 9, 'array_indices': '', 'value_text': NULL, 'value_numeric': to_json(seenAt)}")
	require.Contains(t, sp.OuterSelect, "'attr_id': 12, 'array_indices': CAST(i - 1 AS VARCHAR), 'value_text': NULL, 'value_numeric': to_json(x)})")
	require.Contains(t, sp.OuterSelect, "'value_numeric': CAST(amount AS DOUBLE)}")
	require.Contains(t, sp.OuterSelect, "'attr_id': 13, 'array_indices': CAST(i - 1 AS VARCHAR), 'value_text': NULL, 'value_numeric': CAST(x AS DOUBLE)})")
	require.Contains(t, sp.OuterSelect, "'value_numeric': CAST(CAST(flag AS INTEGER) AS DOUBLE)}")
	require.NotContains(t, sp.OuterSelect, "CAST(seenAt AS DOUBLE)")

	scalarOnly, err := BuildSchemaProjection(3, forma.SchemaAttributeCache{
		"seenAt": {AttributeID: 9, ValueType: forma.ValueTypeDateTime},
		"amount": {AttributeID: 11, ValueType: forma.ValueTypeNumeric},
	})
	require.NoError(t, err)
	require.Contains(t, scalarOnly.OuterSelect, "'value_numeric': to_json(seenAt)}")
	require.NotContains(t, scalarOnly.OuterSelect, "flatten(")
}

// The generated expression, run on a real DuckDB over BIGINT and
// LIST(BIGINT) columns next to DOUBLE ones, emits a date's digits exactly:
// 2^53+1 reaches model.ParseAttributesJSON as 9007199254740993, not the
// 9007199254740992.0 a CAST to DOUBLE (or a BIGINT field unified with a
// DOUBLE one) rendered. The DOUBLE neighbours render as they always did.
func TestAttributesJSON_DateTokensSurviveDuckDB(t *testing.T) {
	sp, err := BuildSchemaProjection(3, dateJSONCache())
	require.NoError(t, err)
	expr := attributesJSONExpr(t, sp)

	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	query := "SELECT " + expr + ` FROM (SELECT '018f4a3c-0000-7000-8000-000000000001' AS row_id,
		9007199254740993::BIGINT AS seenAt, 1.5::DOUBLE AS amount,
		[-9007199254740993, 1704067200123, 0]::BIGINT[] AS days, [2.5, 1704067200123]::DOUBLE[] AS scores,
		true AS flag)`
	var attrsJSON string
	require.NoError(t, db.QueryRow(query).Scan(&attrsJSON), "query: %s", query)

	var record model.PersistentRecord
	require.NoError(t, model.ParseAttributesJSON([]byte(attrsJSON), &record), attrsJSON)
	got := map[string]string{}
	for _, attr := range record.OtherAttributes {
		require.NotNil(t, attr.ValueNumeric, attrsJSON)
		got[fmt.Sprintf("%d:%s", attr.AttrID, attr.ArrayIndices)] = attr.ValueNumericRaw
	}
	require.Equal(t, map[string]string{
		"9:":   "9007199254740993",
		"11:":  "1.5",
		"12:0": "-9007199254740993",
		"12:1": "1704067200123",
		"12:2": "0",
		"13:0": "2.5",
		"13:1": "1704067200123.0",
		"14:":  "1.0",
	}, got, attrsJSON)
}

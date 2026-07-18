package sqlgen

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// TestBuildOuterSelectScalarOnlyShapeUnchanged: schemas without list attrs
// must render the exact pre-#204 attributes_json expression — no flatten
// wrapper — so existing rendered-SQL contracts stay byte-identical.
func TestBuildOuterSelectScalarOnlyShapeUnchanged(t *testing.T) {
	sp, err := BuildSchemaProjection(2, forma.SchemaAttributeCache{
		"note": {AttributeID: 7, ValueType: forma.ValueTypeText},
	})
	require.NoError(t, err)

	require.Contains(t, sp.OuterSelect,
		"to_json(list_filter([CASE WHEN note IS NOT NULL THEN {'schema_id': 2, 'row_id': CAST(row_id AS VARCHAR), 'attr_id': 7, 'array_indices': '', 'value_text': CAST(note AS VARCHAR), 'value_numeric': NULL} END], x -> x IS NOT NULL))::TEXT AS attributes_json")
	require.NotContains(t, sp.OuterSelect, "flatten(")
}

// TestBuildOuterSelectListAttrEmitsPerElementObjects pins the #204 read-side
// reconstruction: a list attr expands into one JSON object per element with
// positional array_indices; scalar attrs keep their single-object shape.
func TestBuildOuterSelectListAttrEmitsPerElementObjects(t *testing.T) {
	sp, err := BuildSchemaProjection(2, forma.SchemaAttributeCache{
		"tags": {AttributeID: 18, ValueType: forma.ValueTypeList},
		"nums": {AttributeID: 19, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger},
		"note": {AttributeID: 7, ValueType: forma.ValueTypeText},
	})
	require.NoError(t, err)

	require.Contains(t, sp.OuterSelect,
		"CASE WHEN tags IS NOT NULL AND len(tags) = 0 THEN [{'schema_id': 2, 'row_id': CAST(row_id AS VARCHAR), 'attr_id': 18, 'array_indices': '', 'value_text': NULL, 'value_numeric': NULL}] WHEN tags IS NOT NULL THEN list_transform(tags, (x, i) -> {'schema_id': 2, 'row_id': CAST(row_id AS VARCHAR), 'attr_id': 18, 'array_indices': CAST(i - 1 AS VARCHAR), 'value_text': CAST(x AS VARCHAR), 'value_numeric': NULL}) ELSE [] END")
	require.Contains(t, sp.OuterSelect,
		"CASE WHEN nums IS NOT NULL AND len(nums) = 0 THEN [{'schema_id': 2, 'row_id': CAST(row_id AS VARCHAR), 'attr_id': 19, 'array_indices': '', 'value_text': NULL, 'value_numeric': NULL}] WHEN nums IS NOT NULL THEN list_transform(nums, (x, i) -> {'schema_id': 2, 'row_id': CAST(row_id AS VARCHAR), 'attr_id': 19, 'array_indices': CAST(i - 1 AS VARCHAR), 'value_text': NULL, 'value_numeric': CAST(x AS DOUBLE)}) ELSE [] END")
	// Scalar attr keeps its single object, wrapped for the flatten form.
	require.Contains(t, sp.OuterSelect,
		"[CASE WHEN note IS NOT NULL THEN {'schema_id': 2, 'row_id': CAST(row_id AS VARCHAR), 'attr_id': 7, 'array_indices': '', 'value_text': CAST(note AS VARCHAR), 'value_numeric': NULL} END]")
	require.Contains(t, sp.OuterSelect, "to_json(list_filter(flatten([")
}

// TestListAttributesJSONRoundTripsThroughParser executes the generated
// attributes_json expression against a real DuckDB engine and feeds the
// result to model.ParseAttributesJSON — the same parser the federated read
// path uses — asserting per-element records with real array_indices.
func TestListAttributesJSONRoundTripsThroughParser(t *testing.T) {
	sp, err := BuildSchemaProjection(2, forma.SchemaAttributeCache{
		"tags": {AttributeID: 18, ValueType: forma.ValueTypeList},
		"note": {AttributeID: 7, ValueType: forma.ValueTypeText},
	})
	require.NoError(t, err)

	// Extract the attributes_json expression from the outer select.
	idx := strings.Index(sp.OuterSelect, "to_json(")
	require.GreaterOrEqual(t, idx, 0)
	expr := strings.TrimSuffix(sp.OuterSelect[idx:], " AS attributes_json")
	expr = strings.TrimSuffix(expr, ",")
	expr = strings.TrimSuffix(strings.TrimSpace(expr), " AS attributes_json")

	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	const rowID = "018f4a3c-0000-7000-8000-000000000001"
	var attrsJSON string
	query := "SELECT " + expr + " FROM (SELECT '" + rowID + "' AS row_id, ['alpha','beta','gamma'] AS tags, 'hello' AS note)"
	require.NoError(t, db.QueryRow(query).Scan(&attrsJSON), "query: %s", query)

	var record model.PersistentRecord
	require.NoError(t, model.ParseAttributesJSON([]byte(attrsJSON), &record))
	require.Len(t, record.OtherAttributes, 4, "3 tags elements + 1 note, got: %s", attrsJSON)

	var gotTags []string
	var gotIndices []string
	for _, attr := range record.OtherAttributes {
		if attr.AttrID != 18 {
			continue
		}
		require.NotNil(t, attr.ValueText)
		gotTags = append(gotTags, *attr.ValueText)
		gotIndices = append(gotIndices, attr.ArrayIndices)
	}
	require.Equal(t, []string{"alpha", "beta", "gamma"}, gotTags)
	require.Equal(t, []string{"0", "1", "2"}, gotIndices)

	// An explicit empty list ([] column, non-NULL) reconstructs as the marker
	// object — array_indices '' with both value columns NULL — which the
	// transform layer materializes back into an empty array (#204).
	query = "SELECT " + expr + " FROM (SELECT '" + rowID + "' AS row_id, []::VARCHAR[] AS tags, 'hello' AS note)"
	require.NoError(t, db.QueryRow(query).Scan(&attrsJSON), "query: %s", query)
	record = model.PersistentRecord{}
	require.NoError(t, model.ParseAttributesJSON([]byte(attrsJSON), &record))
	require.Len(t, record.OtherAttributes, 2, "1 tags marker + 1 note, got: %s", attrsJSON)
	var marker *model.EAVRecord
	for i := range record.OtherAttributes {
		if record.OtherAttributes[i].AttrID == 18 {
			marker = &record.OtherAttributes[i]
		}
	}
	require.NotNil(t, marker, "no marker object for empty list: %s", attrsJSON)
	require.Equal(t, "", marker.ArrayIndices)
	require.Nil(t, marker.ValueText)
	require.Nil(t, marker.ValueNumeric)

	// An absent attribute (NULL column) emits nothing.
	query = "SELECT " + expr + " FROM (SELECT '" + rowID + "' AS row_id, CAST(NULL AS VARCHAR[]) AS tags, 'hello' AS note)"
	require.NoError(t, db.QueryRow(query).Scan(&attrsJSON), "query: %s", query)
	record = model.PersistentRecord{}
	require.NoError(t, model.ParseAttributesJSON([]byte(attrsJSON), &record))
	require.Len(t, record.OtherAttributes, 1, "only note expected, got: %s", attrsJSON)
}

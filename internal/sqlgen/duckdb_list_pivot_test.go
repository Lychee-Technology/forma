package sqlgen

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestBuildEAVPivotListAttrAggregatesElements pins the #204 hot-tier shape:
// a list attribute pivots into a DuckDB LIST ordered by the numeric element
// index (element cast per items_type), mirroring cdc.castEAVValue on the
// export side so the pg_source leg type-unifies with the parquet legs.
func TestBuildEAVPivotListAttrAggregatesElements(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"tags": {AttributeID: 18, ValueType: forma.ValueTypeList},
		"nums": {AttributeID: 19, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger},
		"note": {AttributeID: 7, ValueType: forma.ValueTypeText},
	}
	sp, err := BuildSchemaProjection(2, cache)
	require.NoError(t, err)

	require.Contains(t, sp.EAVPivotSelect,
		"CASE WHEN count(*) FILTER (WHERE attr_id = 18) > 0 THEN coalesce(list(value_text ORDER BY TRY_CAST(array_indices AS BIGINT)) FILTER (WHERE attr_id = 18 AND array_indices <> ''), []) END AS tags")
	require.Contains(t, sp.EAVPivotSelect,
		"CASE WHEN count(*) FILTER (WHERE attr_id = 19) > 0 THEN coalesce(list(TRY_CAST(value_numeric AS BIGINT) ORDER BY TRY_CAST(array_indices AS BIGINT)) FILTER (WHERE attr_id = 19 AND array_indices <> ''), []) END AS nums")
	// Scalar attrs keep the MAX(CASE ...) pivot untouched.
	require.Contains(t, sp.EAVPivotSelect,
		"MAX(CASE WHEN attr_id = 7 THEN value_text END) AS note")

	// The hot leg folds the pivot through ANY_VALUE like every EAV attr.
	require.Contains(t, sp.PGSourceSelect, "ANY_VALUE(hot_vals.tags) AS tags")
}

// TestDuckDBAttrCastListPassesThrough: CAST(list AS VARCHAR) would stringify
// the column; the list branch must return the expression unchanged.
func TestDuckDBAttrCastListPassesThrough(t *testing.T) {
	require.Equal(t, "tags", duckDBAttrCast("tags", forma.ValueTypeList))
	// Scalars keep their casts.
	require.Equal(t, "CAST(note AS VARCHAR)", duckDBAttrCast("note", forma.ValueTypeText))
}

// TestDuckDBListLegUnification pins the engine facts the list read path rests
// on: ANY_VALUE over a LIST, COALESCE of two VARCHAR[] legs, and UNION ALL
// type-unification of VARCHAR[] with NULL-typed legs (absent column under
// union_by_name).
func TestDuckDBListLegUnification(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	var s string
	require.NoError(t, db.QueryRow(
		`SELECT to_json(ANY_VALUE(l))::VARCHAR FROM (SELECT ['a','b'] AS l)`).Scan(&s))
	require.Equal(t, `["a","b"]`, s)

	require.NoError(t, db.QueryRow(
		`SELECT to_json(COALESCE(CAST(NULL AS VARCHAR[]), ['a','b']))::VARCHAR`).Scan(&s))
	require.Equal(t, `["a","b"]`, s)

	var typ string
	require.NoError(t, db.QueryRow(
		`SELECT typeof(x) FROM (SELECT ['a'] AS x UNION ALL SELECT NULL) LIMIT 1`).Scan(&typ))
	require.Equal(t, "VARCHAR[]", typ)
}

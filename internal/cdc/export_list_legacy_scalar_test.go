package cdc

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestExportListAggregateShapes executes the CDC export's list aggregate
// against the four eav_data shapes a list attribute can carry and pins each
// (#372), in lockstep with sqlgen.TestBuildEAVPivotExprListShapes: absent →
// NULL, empty-list marker → [], per-element rows → the ordered list, and a
// legacy scalar row (array_indices ” with a value, written before the schema
// declared the attribute a list) → a one-element list instead of [].
func TestExportListAggregateShapes(t *testing.T) {
	projection, err := buildSchemaDrivenProjection(forma.SchemaAttributeCache{
		"tags": {AttributeName: "tags", AttributeID: 18, ValueType: forma.ValueTypeList},
		"nums": {AttributeName: "nums", AttributeID: 19, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger},
	})
	require.NoError(t, err)
	require.Len(t, projection.eavAgg, 2)

	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	rows, err := db.Query(fmt.Sprintf(`SELECT row_id,
			coalesce(to_json(nums)::VARCHAR, 'NULL'),
			coalesce(to_json(tags)::VARCHAR, 'NULL')
		FROM (SELECT row_id, %s, %s FROM (VALUES
			('absent',  7,  '',  'note',  CAST(NULL AS DOUBLE)),
			('marker',  18, '',  NULL,    NULL),
			('marker',  19, '',  NULL,    NULL),
			('elems',   18, '1', 'beta',  NULL),
			('elems',   18, '0', 'alpha', NULL),
			('elems',   19, '1', NULL,    2),
			('elems',   19, '0', NULL,    1),
			('legacy',  18, '',  'alpha', NULL),
			('legacy',  19, '',  NULL,    42)
		) t(row_id, attr_id, array_indices, value_text, value_numeric)
		GROUP BY row_id) ORDER BY row_id`, projection.eavAgg[0], projection.eavAgg[1]))
	require.NoError(t, err)
	defer rows.Close()

	got := map[string][2]string{}
	for rows.Next() {
		var id, numsJSON, tagsJSON string
		require.NoError(t, rows.Scan(&id, &numsJSON, &tagsJSON))
		got[id] = [2]string{tagsJSON, numsJSON}
	}
	require.NoError(t, rows.Err())

	require.Equal(t, [2]string{"NULL", "NULL"}, got["absent"])
	require.Equal(t, [2]string{"[]", "[]"}, got["marker"])
	require.Equal(t, [2]string{`["alpha","beta"]`, "[1.0,2.0]"}, got["elems"])
	require.Equal(t, [2]string{`["alpha"]`, "[42.0]"}, got["legacy"])
}

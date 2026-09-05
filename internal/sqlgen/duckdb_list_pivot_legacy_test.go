package sqlgen

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestBuildEAVPivotExprListShapes executes the hot-tier list pivot against
// the four eav_data shapes a list attribute can carry and pins the result of
// each (#372): absent → NULL, empty-list marker → [], per-element rows → the
// ordered list, and a legacy scalar row (array_indices ” with a value,
// written before the schema declared the attribute a list) → a one-element
// list instead of the [] that silently dropped the stored value.
func TestBuildEAVPivotExprListShapes(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	tags := buildEAVPivotExpr(attrProjectionInfo{name: "tags", attrID: 18,
		meta: forma.AttributeMetadata{AttributeID: 18, ValueType: forma.ValueTypeList}})
	nums := buildEAVPivotExpr(attrProjectionInfo{name: "nums", attrID: 19,
		meta: forma.AttributeMetadata{AttributeID: 19, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger}})

	rows, err := db.Query(fmt.Sprintf(`SELECT row_id,
			coalesce(to_json(%s)::VARCHAR, 'NULL'),
			coalesce(to_json(%s)::VARCHAR, 'NULL')
		FROM (VALUES
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
		GROUP BY row_id ORDER BY row_id`, tags, nums))
	require.NoError(t, err)
	defer rows.Close()

	got := map[string][2]string{}
	for rows.Next() {
		var id, tagsJSON, numsJSON string
		require.NoError(t, rows.Scan(&id, &tagsJSON, &numsJSON))
		got[id] = [2]string{tagsJSON, numsJSON}
	}
	require.NoError(t, rows.Err())

	require.Equal(t, [2]string{"NULL", "NULL"}, got["absent"])
	require.Equal(t, [2]string{"[]", "[]"}, got["marker"])
	require.Equal(t, [2]string{`["alpha","beta"]`, "[1.0,2.0]"}, got["elems"])
	require.Equal(t, [2]string{`["alpha"]`, "[42.0]"}, got["legacy"])
}

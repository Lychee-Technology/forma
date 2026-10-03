package sqlgen

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/model"
)

// TestBenchmarkEAVJSONArrayScalarShapeUnchanged: benchmark schemas carry no
// list attrs today, so the scalar json_array form stays. A numeric value is
// the JSON number the production projection renders (#592); it was a
// VARCHAR, which the parser dropped as an absent attribute.
func TestBenchmarkEAVJSONArrayScalarShapeUnchanged(t *testing.T) {
	got := benchmarkEAVJSONArray(102, 102, "",
		eavJSONAttr{id: 8, name: "exchange", type_: "text"},
		eavJSONAttr{id: 9, name: "commission", type_: "numeric"},
		eavJSONAttr{id: 10, name: "isCash", type_: "numeric"},
	)
	require.Equal(t,
		`json_array(json_object('schema_id', 102, 'row_id', CAST(row_id AS VARCHAR), 'attr_id', 8, 'array_indices', '', 'value_text', CAST(exchange AS VARCHAR), 'value_numeric', NULL), `+
			`json_object('schema_id', 102, 'row_id', CAST(row_id AS VARCHAR), 'attr_id', 9, 'array_indices', '', 'value_numeric', CAST(commission AS DOUBLE), 'value_text', NULL), `+
			`json_object('schema_id', 102, 'row_id', CAST(row_id AS VARCHAR), 'attr_id', 10, 'array_indices', '', 'value_numeric', CAST(CAST(CAST(isCash AS BOOLEAN) AS INTEGER) AS DOUBLE), 'value_text', NULL))`,
		got)
}

// The trade schema's attributes_json, run on a real DuckDB, parses through
// model.ParseAttributesJSON with every EAV attribute present: the numeric
// and boolean ones as numbers, the text ones as text (#592). The unified
// columns are typed on the Parquet leg alone and VARCHAR once the hot leg's
// value_text pivot joins; both read the same.
func TestBenchmarkAttributesJSONParsesOnDuckDB(t *testing.T) {
	outer := BuildBenchmarkOuterSelect(102)
	start := strings.Index(outer, "json_array(")
	require.GreaterOrEqual(t, start, 0)
	end := strings.Index(outer, "::TEXT AS attributes_json")
	require.Greater(t, end, start)

	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	for name, unified := range map[string]string{
		"typed":   "26.31::DOUBLE AS commission, true AS isCash",
		"varchar": "'26.31' AS commission, 'true' AS isCash",
	} {
		t.Run(name, func(t *testing.T) {
			query := "SELECT " + outer[start:end] + `::TEXT FROM (SELECT '018f4a3c-0000-7000-8000-000000000001' AS row_id,
				'NYSE' AS exchange, ` + unified + `, 'b-7' AS brokerId, 'web' AS orderChannel)`
			var attrsJSON string
			require.NoError(t, db.QueryRow(query).Scan(&attrsJSON), "query: %s", query)

			var record model.PersistentRecord
			require.NoError(t, model.ParseAttributesJSON([]byte(attrsJSON), &record), attrsJSON)
			got := map[int16]string{}
			for _, attr := range record.OtherAttributes {
				if attr.ValueText != nil {
					got[attr.AttrID] = *attr.ValueText
					continue
				}
				require.NotNil(t, attr.ValueNumeric, attrsJSON)
				got[attr.AttrID] = attr.ValueNumericRaw
			}
			require.Equal(t, map[int16]string{8: "NYSE", 9: "26.31", 10: "1.0", 11: "b-7", 12: "web"}, got, attrsJSON)
		})
	}
}

// TestBenchmarkEAVJSONArrayListAttrPositionalIndices: if a benchmark shape
// ever declares a list attribute, it must reconstruct positional
// array_indices exactly like the production projection (#204) instead of the
// hardcoded empty string.
func TestBenchmarkEAVJSONArrayListAttrPositionalIndices(t *testing.T) {
	got := benchmarkEAVJSONArray(102, 102, "",
		eavJSONAttr{id: 8, name: "exchange", type_: "text"},
		eavJSONAttr{id: 13, name: "tags", type_: "list"},
	)
	require.Contains(t, got,
		`list_transform(tags, (x, i) -> json_object('schema_id', 102, 'row_id', CAST(row_id AS VARCHAR), 'attr_id', 13, 'array_indices', CAST(i - 1 AS VARCHAR), 'value_text', CAST(x AS VARCHAR), 'value_numeric', NULL))`)
	require.Contains(t, got, "to_json(list_filter(flatten([")
	// The scalar attr keeps its object, wrapped for the flatten form.
	require.Contains(t, got,
		`[json_object('schema_id', 102, 'row_id', CAST(row_id AS VARCHAR), 'attr_id', 8, 'array_indices', '', 'value_text', CAST(exchange AS VARCHAR), 'value_numeric', NULL)]`)
}

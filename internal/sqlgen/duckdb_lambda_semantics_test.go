package sqlgen

import (
	"database/sql"
	"sort"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// strictLambdaDSN opens an in-memory DuckDB that refuses the deprecated
// single-arrow lambda (`x -> ...`), as DuckDB 2.0 does by default (#751).
// Tests that execute generated SQL open it so that a reintroduced arrow
// lambda fails here on the pinned 1.5.x runtime, before the driver bump.
const strictLambdaDSN = "?lambda_syntax=DISABLE_SINGLE_ARROW"

// openStrictLambdaDuckDB opens a strictLambdaDSN database and proves the
// setting took: a single-arrow lambda must be refused, or every test that
// relies on this helper would pass vacuously.
func openStrictLambdaDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	var n int
	err = db.QueryRow(`SELECT len(list_filter([1, 2], x -> x > 1))`).Scan(&n)
	require.Error(t, err, "DuckDB accepted a single-arrow lambda under %s", strictLambdaDSN)
	return db
}

// TestListPredicatesExecuteUnderStrictLambdaSyntax runs every LIST predicate
// BuildListPredicate renders against real lists: populated, empty, NULL,
// and holding a NULL element. The lambda forms must parse without the
// single-arrow syntax and keep their filtering meaning.
func TestListPredicatesExecuteUnderStrictLambdaSyntax(t *testing.T) {
	db := openStrictLambdaDuckDB(t)
	_, err := db.Exec(`CREATE TABLE lists AS SELECT * FROM (VALUES
		(1, ['alpha', 'beta'], [1.5, 10.0]::DOUBLE[], [3, 7]::BIGINT[]),
		(2, []::VARCHAR[], []::DOUBLE[], []::BIGINT[]),
		(3, CAST(NULL AS VARCHAR[]), CAST(NULL AS DOUBLE[]), CAST(NULL AS BIGINT[])),
		(4, [NULL, 'gamma'], [NULL, 2.0]::DOUBLE[], [NULL, 5]::BIGINT[])
	) t(id, names, vals, stamps)`)
	require.NoError(t, err)

	cases := []struct {
		column, operator, value string
		elem                    forma.ValueType
		want                    []int
	}{
		{"names", "contains", "ph", forma.ValueTypeText, []int{1}},
		{"names", "contains", "a", forma.ValueTypeText, []int{1, 4}},
		{"names", "starts_with", "ga", forma.ValueTypeText, []int{4}},
		{"names", "equals", "beta", forma.ValueTypeText, []int{1}},
		{"names", "not_equals", "beta", forma.ValueTypeText, []int{2, 4}},
		{"vals", "gt", "5", forma.ValueTypeNumeric, []int{1}},
		{"vals", "gte", "2", forma.ValueTypeNumeric, []int{1, 4}},
		{"vals", "lt", "2", forma.ValueTypeNumeric, []int{1}},
		{"vals", "lte", "2", forma.ValueTypeNumeric, []int{1, 4}},
		{"stamps", "lt", "5", forma.ValueTypeDate, []int{1}},
		{"stamps", "lte", "5", forma.ValueTypeDate, []int{1, 4}},
		{"stamps", "gt", "6", forma.ValueTypeDate, []int{1}},
	}
	for _, tc := range cases {
		t.Run(tc.column+"_"+tc.operator+"_"+tc.value, func(t *testing.T) {
			pred, arg, err := BuildListPredicate(tc.column, tc.operator, tc.value, tc.elem)
			require.NoError(t, err)
			rows, err := db.Query("SELECT id FROM lists WHERE "+pred+" ORDER BY id", arg)
			require.NoError(t, err, "predicate: %s", pred)
			got := []int{}
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				got = append(got, id)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.Equal(t, tc.want, got, "predicate: %s", pred)
		})
	}
}

// TestAttributesJSONLambdaSemantics pins the exact attributes_json the
// list_transform/list_filter lambdas produce for typed lists (text, integer,
// bool, date), next to scalar parts: positional zero-based array_indices in
// element order, NULL elements kept at their position, the empty-list
// marker, nothing for a NULL list, and the struct unification across
// scalar and list parts (value_numeric is JSON wherever a date list or
// scalar joins). The expected JSON is what the single-arrow form rendered
// on DuckDB 1.5.6; the lambda keyword form must render it byte for byte.
func TestAttributesJSONLambdaSemantics(t *testing.T) {
	sp, err := BuildSchemaProjection(2, forma.SchemaAttributeCache{
		"tags":  {AttributeID: 18, ValueType: forma.ValueTypeList},
		"nums":  {AttributeID: 19, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger},
		"flags": {AttributeID: 20, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeBool},
		"days":  {AttributeID: 21, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeDate},
		"note":  {AttributeID: 7, ValueType: forma.ValueTypeText},
		"score": {AttributeID: 8, ValueType: forma.ValueTypeNumeric},
	})
	require.NoError(t, err)
	expr := attributesJSONExpr(t, sp)
	db := openStrictLambdaDuckDB(t)

	const rowID = "018f4a3c-0000-7000-8000-000000000001"
	cases := []struct {
		name, columns, want string
	}{
		{
			name: "populated typed lists in element order",
			columns: `['c', 'a', 'b'] AS tags, [30, 10]::DOUBLE[] AS nums, [true, false] AS flags,
				[9007199254740993, 0]::BIGINT[] AS days, 'hi' AS note, 1.25::DOUBLE AS score`,
			want: `[` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":21,"array_indices":"0","value_text":null,"value_numeric":9007199254740993},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":21,"array_indices":"1","value_text":null,"value_numeric":0},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":20,"array_indices":"0","value_text":null,"value_numeric":1.0},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":20,"array_indices":"1","value_text":null,"value_numeric":0.0},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":7,"array_indices":"","value_text":"hi","value_numeric":null},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":19,"array_indices":"0","value_text":null,"value_numeric":30.0},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":19,"array_indices":"1","value_text":null,"value_numeric":10.0},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":8,"array_indices":"","value_text":null,"value_numeric":1.25},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":18,"array_indices":"0","value_text":"c","value_numeric":null},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":18,"array_indices":"1","value_text":"a","value_numeric":null},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":18,"array_indices":"2","value_text":"b","value_numeric":null}` +
				`]`,
		},
		{
			name: "NULL elements keep their position",
			columns: `['a', NULL, 'c'] AS tags, [NULL, 2]::DOUBLE[] AS nums, CAST(NULL AS BOOLEAN[]) AS flags,
				CAST(NULL AS BIGINT[]) AS days, CAST(NULL AS VARCHAR) AS note, CAST(NULL AS DOUBLE) AS score`,
			want: `[` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":19,"array_indices":"0","value_text":null,"value_numeric":null},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":19,"array_indices":"1","value_text":null,"value_numeric":2.0},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":18,"array_indices":"0","value_text":"a","value_numeric":null},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":18,"array_indices":"1","value_text":null,"value_numeric":null},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":18,"array_indices":"2","value_text":"c","value_numeric":null}` +
				`]`,
		},
		{
			name: "empty lists emit the marker, NULL lists nothing",
			columns: `[]::VARCHAR[] AS tags, CAST(NULL AS DOUBLE[]) AS nums, []::BOOLEAN[] AS flags,
				CAST(NULL AS BIGINT[]) AS days, 'hi' AS note, CAST(NULL AS DOUBLE) AS score`,
			want: `[` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":20,"array_indices":"","value_text":null,"value_numeric":null},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":7,"array_indices":"","value_text":"hi","value_numeric":null},` +
				`{"schema_id":2,"row_id":"018f4a3c-0000-7000-8000-000000000001","attr_id":18,"array_indices":"","value_text":null,"value_numeric":null}` +
				`]`,
		},
		{
			name: "every attribute absent",
			columns: `CAST(NULL AS VARCHAR[]) AS tags, CAST(NULL AS DOUBLE[]) AS nums, CAST(NULL AS BOOLEAN[]) AS flags,
				CAST(NULL AS BIGINT[]) AS days, CAST(NULL AS VARCHAR) AS note, CAST(NULL AS DOUBLE) AS score`,
			want: `[]`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			query := "SELECT " + expr + " FROM (SELECT '" + rowID + "' AS row_id, " + tc.columns + ")"
			var got string
			require.NoError(t, db.QueryRow(query).Scan(&got), "query: %s", query)
			require.Equal(t, tc.want, got)
			var record model.PersistentRecord
			require.NoError(t, model.ParseAttributesJSON([]byte(got), &record), got)
		})
	}
}

// TestAttributesJSONPositionalIndicesRoundTrip feeds a long list through
// the generated expression and the federated parser: every element comes
// back once, under the zero-based index of its parquet LIST position.
func TestAttributesJSONPositionalIndicesRoundTrip(t *testing.T) {
	sp, err := BuildSchemaProjection(2, forma.SchemaAttributeCache{
		"nums": {AttributeID: 19, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger},
	})
	require.NoError(t, err)
	db := openStrictLambdaDuckDB(t)

	query := "SELECT " + attributesJSONExpr(t, sp) +
		" FROM (SELECT 'r' AS row_id, list_transform(range(12), lambda v: CAST(100 - v AS DOUBLE)) AS nums)"
	var got string
	require.NoError(t, db.QueryRow(query).Scan(&got), "query: %s", query)
	var record model.PersistentRecord
	require.NoError(t, model.ParseAttributesJSON([]byte(got), &record), got)
	require.Len(t, record.OtherAttributes, 12, got)

	byIndex := map[string]string{}
	for _, attr := range record.OtherAttributes {
		require.NotNil(t, attr.ValueNumeric, got)
		byIndex[attr.ArrayIndices] = attr.ValueNumericRaw
	}
	indices := make([]string, 0, len(byIndex))
	for idx := range byIndex {
		indices = append(indices, idx)
	}
	sort.Strings(indices)
	require.Equal(t, []string{"0", "1", "10", "11", "2", "3", "4", "5", "6", "7", "8", "9"}, indices)
	require.Equal(t, "100.0", byIndex["0"])
	require.Equal(t, "95.0", byIndex["5"])
	require.Equal(t, "89.0", byIndex["11"])
}

// TestBenchmarkListJSONLambdaSemantics executes the benchmark projection's
// list form: json_object per element with zero-based array_indices, the
// marker for an empty list, and the scalar part beside it.
func TestBenchmarkListJSONLambdaSemantics(t *testing.T) {
	expr := benchmarkEAVJSONArray(102, 102, "",
		eavJSONAttr{id: 8, name: "exchange", type_: "text"},
		eavJSONAttr{id: 13, name: "tags", type_: "list"},
	)
	db := openStrictLambdaDuckDB(t)

	for name, tc := range map[string]struct{ tags, want string }{
		"populated": {`['x', 'y']`, `[{"schema_id":102,"row_id":"r","attr_id":8,"array_indices":"","value_text":"NYSE","value_numeric":null},{"schema_id":102,"row_id":"r","attr_id":13,"array_indices":"0","value_text":"x","value_numeric":null},{"schema_id":102,"row_id":"r","attr_id":13,"array_indices":"1","value_text":"y","value_numeric":null}]`},
		"empty":     {`[]::VARCHAR[]`, `[{"schema_id":102,"row_id":"r","attr_id":8,"array_indices":"","value_text":"NYSE","value_numeric":null},{"schema_id":102,"row_id":"r","attr_id":13,"array_indices":"","value_text":null,"value_numeric":null}]`},
	} {
		t.Run(name, func(t *testing.T) {
			query := "SELECT " + expr + "::TEXT FROM (SELECT 'r' AS row_id, 'NYSE' AS exchange, " + tc.tags + " AS tags)"
			var got string
			require.NoError(t, db.QueryRow(query).Scan(&got), "query: %s", query)
			require.Equal(t, tc.want, got)
		})
	}
}

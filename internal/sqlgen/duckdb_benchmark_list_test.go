package sqlgen

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBenchmarkEAVJSONArrayScalarShapeUnchanged: benchmark schemas carry no
// list attrs today; their json_array output must stay byte-identical so the
// synthetic parquet shape (and any recorded baseline SQL) does not churn.
func TestBenchmarkEAVJSONArrayScalarShapeUnchanged(t *testing.T) {
	got := benchmarkEAVJSONArray(102, 102, "",
		eavJSONAttr{id: 8, name: "exchange", type_: "text"},
		eavJSONAttr{id: 9, name: "commission", type_: "numeric"},
	)
	require.Equal(t,
		`json_array(json_object('schema_id', 102, 'row_id', CAST(row_id AS VARCHAR), 'attr_id', 8, 'array_indices', '', 'value_text', CAST(exchange AS VARCHAR), 'value_numeric', NULL), `+
			`json_object('schema_id', 102, 'row_id', CAST(row_id AS VARCHAR), 'attr_id', 9, 'array_indices', '', 'value_numeric', CAST(commission AS VARCHAR), 'value_text', NULL))`,
		got)
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

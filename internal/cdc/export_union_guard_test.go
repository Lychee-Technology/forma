package cdc

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// unionSchemaCache loads, through the directory registry the server loads,
// a schema whose office is declared as an object or an array of objects,
// with a scalar and a list member, beside a top-level note. With declared
// set, it also declares lines, an array of objects.
func unionSchemaCache(t *testing.T, declared bool) (int64, forma.SchemaAttributeCache) {
	t.Helper()
	dir := t.TempDir()
	lines, linesAttr := "", ""
	if declared {
		lines = `,"lines":{"type":"array","items":{"type":"object","properties":{"sku":{"type":"string"}}}}`
		linesAttr = `,"lines.sku":{"attributeID":5,"valueType":"text"}`
	}
	members := `{"city":{"type":"string"},"tags":{"type":"array","items":{"type":"string"}}}`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "offices.json"), []byte(`{"type":"object","properties":{
		"note":{"type":"string"},
		"office":{"properties":`+members+`,"anyOf":[{"type":"object"},{"type":"array","items":{"type":"object","properties":`+members+`}}]}`+lines+`}}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "offices_attributes.json"), []byte(`{
		"note":{"attributeID":1,"valueType":"text"},
		"office.city":{"attributeID":2,"valueType":"text"},
		"office.tags":{"attributeID":3,"valueType":"list","items_type":"text"}`+linesAttr+`}`), 0o600))
	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)
	id, cache, err := registry.GetSchemaAttributeCacheByName("offices")
	require.NoError(t, err)
	require.Equal(t, []string{"office"}, cache["office.city"].ArrayAncestors, "precondition: the read places office's array form")
	return int64(id), cache
}

// TestCDCExportsAnObjectOrArrayUnionSchema pins #696 review R4-F1: a schema
// whose only arrays of objects are object-or-array unions stays exportable,
// as before #696 derived the union's array form, and its object-form rows
// export as they did: the scalar member's value, the list member's ordered
// elements and its empty-list marker. A schema that also declares an array
// of objects is still skipped (#704).
func TestCDCExportsAnObjectOrArrayUnionSchema(t *testing.T) {
	id, cache := unionSchemaCache(t, false)
	exportable, skipped := exportableSchemaIDs(zap.NewNop(), []int64{id}, map[int16]forma.SchemaAttributeCache{int16(id): cache})
	require.Equal(t, []int64{id}, exportable)
	require.Empty(t, skipped)

	declaredID, declared := unionSchemaCache(t, true)
	exportable, skipped = exportableSchemaIDs(zap.NewNop(), []int64{declaredID}, map[int16]forma.SchemaAttributeCache{int16(declaredID): declared})
	require.Empty(t, exportable)
	require.Len(t, skipped, 1)
	require.Contains(t, skipped[0].Reason, `attribute "lines.sku" lies inside the array of objects "lines"`)

	got, err := runUnionExportAggregate(t, cache, `
		('object', 1, '',  'n'),
		('object', 2, '',  'o1'),
		('object', 3, '1', 'b'),
		('object', 3, '0', 'a'),
		('marker', 2, '',  'o2'),
		('marker', 3, '',  NULL)`)
	require.NoError(t, err)
	require.Equal(t, []string{
		`{"row_id":"marker","note":null,"office_city":"o2","office_tags":[]}`,
		`{"row_id":"object","note":"n","office_city":"o1","office_tags":["a","b"]}`,
	}, got)
}

// TestCDCExportRefusesARowHoldingAUnionsArrayForm pins the other half of
// R4-F1: a row writing office in its array form holds one value of each
// member per element, which the parquet tiers cannot hold (#704), so the
// export fails naming the attribute and the row instead of exporting the
// row collapsed and marking it flushed. That holds for a scalar member, a
// list member's elements and its empty-list marker in an element.
func TestCDCExportRefusesARowHoldingAUnionsArrayForm(t *testing.T) {
	_, cache := unionSchemaCache(t, false)
	for _, tc := range []struct {
		records string
		attr    string
	}{
		{`('array', 2, '0', 'o1'), ('array', 2, '1', 'o2')`, "office.city"},
		{`('array', 2, '0', 'o1')`, "office.city"},
		{`('array', 3, '0,0', 'a')`, "office.tags"},
		{`('array', 3, '1', NULL)`, "office.tags"},
	} {
		_, err := runUnionExportAggregate(t, cache, `('object', 2, '', 'o1'), `+tc.records)
		require.Error(t, err, tc.records)
		require.Contains(t, err.Error(), fmt.Sprintf(`cdc export refused: a row holds attribute %q`, tc.attr), tc.records)
		require.Contains(t, err.Error(), `inside the array form of the object-or-array union "office"`, tc.records)
		require.Contains(t, err.Error(), "row_id=array", tc.records)
	}
}

// runUnionExportAggregate runs the export's EAV aggregate for cache over
// records (row_id, attr_id, array_indices, value_text) in DuckDB and
// answers each row as JSON, in row order.
func runUnionExportAggregate(t *testing.T, cache forma.SchemaAttributeCache, records string) ([]string, error) {
	t.Helper()
	projection, err := buildSchemaDrivenProjection(cache)
	require.NoError(t, err)
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	rows, err := db.Query(fmt.Sprintf(`SELECT to_json(x)::VARCHAR FROM (SELECT row_id, %s FROM (
			SELECT row_id, attr_id, array_indices, value_text, CAST(NULL AS DOUBLE) AS value_numeric
			FROM (VALUES %s) v(row_id, attr_id, array_indices, value_text))
		GROUP BY row_id) x ORDER BY row_id`, strings.Join(projection.eavAgg, ", "), records))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var row string
		require.NoError(t, rows.Scan(&row))
		out = append(out, row)
	}
	return out, rows.Err()
}

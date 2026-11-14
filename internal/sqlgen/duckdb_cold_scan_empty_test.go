package sqlgen

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// emptyScanAttrs are the attribute columns both scan sources below carry: one
// per DuckDB type an attribute column can have.
var emptyScanAttrs = []ScanColumn{
	{Name: "title", DuckDBType: "VARCHAR"},
	{Name: "score", DuckDBType: "DOUBLE"},
	{Name: "rank", DuckDBType: "INTEGER"},
	{Name: "tags", DuckDBType: "VARCHAR[]"},
}

// TestEmptyParquetScanSourceMatchesTheGuardedScan pins #720's scan source to
// the columns the guarded scan over a production-shaped object exposes: same
// names, same types. A column the empty source lacked would fail to bind in
// the s3_source projection or the semijoin, and a different type would change
// the type the outer select and the sort keys read.
func TestEmptyParquetScanSourceMatchesTheGuardedScan(t *testing.T) {
	db, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	defer db.Close()

	path := filepath.Join(t.TempDir(), "delta.parquet")
	_, err = db.Exec(fmt.Sprintf(`COPY (SELECT
		CAST('018f05c0-0000-7000-8000-00000000000a' AS UUID) AS row_id,
		CAST(100 AS BIGINT) AS changed_at, CAST(0 AS BIGINT) AS deleted_at,
		CAST(50 AS BIGINT) AS ltbase_created_at,
		'alive' AS title, CAST(1.5 AS DOUBLE) AS score, CAST(3 AS INTEGER) AS rank,
		['a', 'b'] AS tags) TO '%s' (FORMAT PARQUET)`, path))
	require.NoError(t, err)

	guarded := describeScanSource(t, db, BuildParquetScanSource("'"+path+"'", nil, nil))
	empty := describeScanSource(t, db, BuildEmptyParquetScanSource(emptyScanAttrs))
	require.Equal(t, guarded, empty)
}

// TestEmptyParquetScanSourceServesTheHotLegAlone runs the parquet-only
// template, both scan sites included, over the empty source on DuckDB. The
// query must bind and answer no parquet row, so the Go merge serves the hot
// leg alone.
func TestEmptyParquetScanSourceServesTheHotLegAlone(t *testing.T) {
	db, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(CreateStagedDirtyIDsSQL)
	require.NoError(t, err)

	cache := forma.SchemaAttributeCache{
		"title": {AttributeID: 1, ValueType: forma.ValueTypeText},
		"score": {AttributeID: 2, ValueType: forma.ValueTypeNumeric},
		"tags":  {AttributeID: 3, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeText},
	}
	params := applyProjectionParams(t, map[string]any{
		"EmptyParquetScan": true,
		"ColdMissingColumns": []ScanColumn{
			{Name: "score", DuckDBType: "DOUBLE"},
			{Name: "tags", DuckDBType: "VARCHAR[]"},
			{Name: "title", DuckDBType: "VARCHAR"},
		},
	}, 7, cache)
	q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{
		SchemaID: 7,
		Limit:    10,
		Condition: &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "title", Value: "equals:hot"},
			&forma.KvCondition{Attr: "score", Value: "gt:1"},
		}},
	}}
	dual := parityDual(t, q.Condition, cache)
	query, args, err := BuildParquetOnlyQuery(params, q, dual, ParquetWindow{Limit: 10})
	require.NoError(t, err)
	require.Equal(t, 2, strings.Count(query, BuildEmptyParquetScanSource(params["ColdMissingColumns"].([]ScanColumn))),
		"both scan sites read the empty source")

	rows, err := db.Query(query, args...)
	require.NoError(t, err, "the parquet leg must bind over the empty source")
	defer rows.Close()
	require.False(t, rows.Next(), "the empty source answers no parquet row")
	require.NoError(t, rows.Err())
}

// describeScanSource returns column name → DuckDB type for the columns of a
// scan source that the empty source also renders.
func describeScanSource(t *testing.T, db *sql.DB, source string) map[string]string {
	t.Helper()
	rows, err := db.Query("DESCRIBE SELECT * FROM " + source)
	require.NoError(t, err)
	defer rows.Close()
	want := map[string]bool{"row_id": true, "changed_at": true, "deleted_at": true, "ltbase_created_at": true}
	for _, c := range emptyScanAttrs {
		want[c.Name] = true
	}
	cols := map[string]string{}
	for rows.Next() {
		var name, typ string
		var null, key, def, extra sql.NullString
		require.NoError(t, rows.Scan(&name, &typ, &null, &key, &def, &extra))
		if want[name] {
			cols[name] = typ
		}
	}
	require.NoError(t, rows.Err())
	require.Len(t, cols, len(want))
	return cols
}

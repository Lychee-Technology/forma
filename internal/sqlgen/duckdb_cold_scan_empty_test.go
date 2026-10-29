package sqlgen

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

// emptyScanAttrs are the attribute columns both scan sources below carry: one
// per DuckDB type a hot-leg UNION ALL has to unify with.
var emptyScanAttrs = []ScanColumn{
	{Name: "title", DuckDBType: "VARCHAR"},
	{Name: "score", DuckDBType: "DOUBLE"},
	{Name: "rank", DuckDBType: "INTEGER"},
	{Name: "tags", DuckDBType: "VARCHAR[]"},
}

// TestEmptyParquetScanSourceMatchesTheGuardedScan pins #720's scan source to
// the columns the guarded scan over a production-shaped object exposes: same
// names, same types. A column the empty source lacked would fail to bind in
// the s3_source projection or the semijoin, and a different type would widen
// or break the UNION ALL with pg_source.
func TestEmptyParquetScanSourceMatchesTheGuardedScan(t *testing.T) {
	db, err := sql.Open("duckdb", "")
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

// TestEmptyParquetScanSourceServesTheHotLegAlone runs the advanced
// template's parquet leg, both scan sites included, over the empty source
// and UNIONs it with a hot row the way pg_source does. The query must bind,
// and the answer is the hot row alone.
func TestEmptyParquetScanSourceServesTheHotLegAlone(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	scan := BuildEmptyParquetScanSource(emptyScanAttrs)
	query := strings.NewReplacer("{{SCAN}}", scan).Replace(`
WITH dirty_ids AS (SELECT CAST('018f05c0-0000-7000-8000-00000000000b' AS UUID) AS row_id),
s3_source AS (
  SELECT row_id, ltbase_created_at AS created_at, changed_at AS ver_ts, deleted_at AS deleted_ts,
    title, score, rank, tags, 1 AS source_tier_priority
  FROM {{SCAN}}
  WHERE CAST(row_id AS UUID) NOT IN (SELECT row_id FROM dirty_ids)
    AND row_id IN (SELECT row_id FROM {{SCAN}} WHERE (title = ? AND score > ?))
),
pg_source AS (
  SELECT CAST('018f05c0-0000-7000-8000-00000000000b' AS UUID) AS row_id,
    CAST(10 AS BIGINT) AS created_at, CAST(20 AS BIGINT) AS ver_ts, CAST(0 AS BIGINT) AS deleted_ts,
    'hot' AS title, CAST(2.5 AS DOUBLE) AS score, CAST(7 AS INTEGER) AS rank, ['x'] AS tags,
    3 AS source_tier_priority
)
SELECT row_id::VARCHAR, title FROM (SELECT * FROM s3_source UNION ALL SELECT * FROM pg_source)`)

	rows, err := db.Query(query, "hot", 1.0)
	require.NoError(t, err)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var rowID, title string
		require.NoError(t, rows.Scan(&rowID, &title))
		got = append(got, rowID+" "+title)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"018f05c0-0000-7000-8000-00000000000b hot"}, got)
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

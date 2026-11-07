package sqlgen

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The tests below prove #409 against the pinned DuckDB: a scan source whose
// missing set names a column that some scanned file DOES carry. The #255
// scope key makes this unreachable today, because a skeleton compiled while
// the column was cold-absent is re-keyed when the first flush lands it. It
// remains reachable on a glob path set whose listing changes between the
// validator's footer probe and the scan. The scan source used to project the
// typed NULL beside the star, so the column was projected twice and the
// answer depended on DuckDB binding the first of two identically-named
// columns. These tests pin that the column is now projected once, carries the
// real values, and stays at the schema type.

var staleScoreMissing = []ScanColumn{{Name: "score", DuckDBType: "INTEGER"}}

// scanColumnNames returns the column names a scan source exposes, in order.
func scanColumnNames(t *testing.T, db *sql.DB, source string) []string {
	t.Helper()
	rows, err := db.Query("DESCRIBE SELECT * FROM " + source)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name, typ string
		var null, key, def, extra sql.NullString
		require.NoError(t, rows.Scan(&name, &typ, &null, &key, &def, &extra))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

// writeScorelessParquet writes a production-shaped object that carries no
// score column, under a row_id no attrPinFixtures object uses.
func writeScorelessParquet(t *testing.T, db *sql.DB) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scoreless.parquet")
	_, err := db.Exec(fmt.Sprintf("COPY (SELECT "+
		"CAST('018f05c0-0000-7000-8000-0000000000ff' AS UUID) AS row_id, CAST(300 AS BIGINT) AS changed_at, "+
		"CAST(0 AS BIGINT) AS deleted_at, CAST(50 AS BIGINT) AS ltbase_created_at, 'alive' AS title"+
		") TO '%s' (FORMAT PARQUET)", path))
	require.NoError(t, err, "write parquet fixture %s", path)
	return path
}

// A stale missing set projects the column exactly once, and both template
// scan sites (the s3_source projection and the semijoin's logical clause)
// read the real value, not the stale NULL.
func TestParquetScanStaleMissingColumnIsProjectedOnce(t *testing.T) {
	db := guardDuckDB(t)
	fx := attrPinFixtures(t, db)
	// current carries score = 100; the scoreless object carries no score.
	scan := BuildParquetScanSource(formatPathList(fx.current, writeScorelessParquet(t, db)), staleScoreMissing, nil)

	require.Equal(t,
		[]string{"row_id", "changed_at", "deleted_at", "ltbase_created_at", "score", "title"},
		scanColumnNames(t, db, scan),
		"each column is projected once; no duplicate score and no engine-renamed score_1")

	var scoreType string
	var matched int
	require.NoError(t, db.QueryRow(
		"SELECT any_value(typeof(score)), count(*) FROM "+scan+" WHERE score > 50").Scan(&scoreType, &matched))
	require.Equal(t, "INTEGER", scoreType)
	require.Equal(t, 1, matched, "the s3_source site reads the real score, not the stale NULL")

	ids, err := scanRowIDs(db,
		"SELECT row_id FROM "+scan+" WHERE row_id IN (SELECT row_id FROM "+scan+" WHERE score > 50)")
	require.NoError(t, err)
	require.Len(t, ids, 1, "the semijoin site reads the real score too")
}

// A stale missing column that landed under a different type is re-pinned to
// the schema type, as a #371 pin would be: union_by_name cannot widen it to
// VARCHAR and turn ORDER BY lexicographic.
func TestParquetScanStaleMissingColumnKeepsSchemaType(t *testing.T) {
	db := guardDuckDB(t)
	fx := attrPinFixtures(t, db)
	scan := BuildParquetScanSource(formatPathList(fx.current, fx.stale), staleScoreMissing, nil)

	var topType string
	var top int64
	require.NoError(t, db.QueryRow(
		"SELECT typeof(score), score FROM "+scan+" ORDER BY score DESC LIMIT 1").Scan(&topType, &top))
	require.Equal(t, "INTEGER", topType)
	require.Equal(t, int64(100), top, "numeric order: 100 above the stale '9'")
}

// The healthy #255 case is unchanged: a column absent from every file reads
// as a typed NULL at the schema type, scalar and list alike.
func TestParquetScanMissingColumnsReadAsTypedNulls(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)
	scan := BuildParquetScanSource(formatPathList(fx.healthy), []ScanColumn{
		{Name: "score", DuckDBType: "INTEGER"},
		{Name: "tags", DuckDBType: "BIGINT[]"},
	}, nil)

	var scoreType, tagsType string
	var nulls int
	require.NoError(t, db.QueryRow(
		"SELECT any_value(typeof(score)), any_value(typeof(tags)), count(*) FROM "+scan+
			" WHERE score IS NULL AND tags IS NULL").Scan(&scoreType, &tagsType, &nulls))
	require.Equal(t, "INTEGER", scoreType)
	require.Equal(t, "BIGINT[]", tagsType)
	require.Equal(t, 1, nulls)
}

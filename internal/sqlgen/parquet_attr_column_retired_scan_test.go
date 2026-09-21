package sqlgen

import (
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// The engine half of #549: the two halves of ValidateParquetAttrColumns
// treat a retired entry differently because the two hazards live in
// different places, and both claims are pinned against the pinned DuckDB
// rather than against reasoning about it. Shares guardDuckDB and
// formatPathList with duckdb_cold_scan_guard_test.go.

// TestParquetScan_RetiredReservedColumnIsInert is why a retired entry is
// exempt from the reserved-column half. The fixture is what the exporter
// wrote while Created_At (a case variant of the unified-CTE column) and
// Row_Id (a case variant of a physical export column) were active. The file
// holds Created_At verbatim and Row_Id as Row_Id_1 — DuckDB dedups the
// duplicate at COPY time, so no flushed file ever carries two spellings of a
// system column — and the exact s3_source projection built from the active
// cache, over the exact cold-scan source, binds every system column to the
// system value. Nothing references the retired columns; they are inert.
func TestParquetScan_RetiredReservedColumnIsInert(t *testing.T) {
	db := guardDuckDB(t)
	path := filepath.Join(t.TempDir(), "flushed_while_active.parquet")
	const rowID = "018f05c0-0000-7000-8000-00000000000a"
	_, err := db.Exec(fmt.Sprintf(`COPY (SELECT
  CAST(1 AS SMALLINT) AS schema_id, CAST('%s' AS UUID) AS row_id,
  CAST(100 AS BIGINT) AS changed_at, CAST(0 AS BIGINT) AS deleted_at,
  CAST(50 AS BIGINT) AS ltbase_created_at, CAST(50 AS BIGINT) AS ltbase_updated_at,
  CAST(NULL AS BIGINT) AS ltbase_deleted_at,
  'live' AS title, 'stale' AS Created_At, 'stale' AS Row_Id
) TO '%s' (FORMAT PARQUET)`, rowID, path))
	require.NoError(t, err, "write the fixture the exporter produced while both attributes were active")

	names, err := scanRowIDs(db, fmt.Sprintf("SELECT name FROM parquet_schema('%s') ORDER BY name", path))
	require.NoError(t, err)
	require.Contains(t, names, "Created_At", "the unified-CTE case variant is written verbatim")
	require.Contains(t, names, "Row_Id_1", "the physical-column case variant is deduplicated at COPY time")
	require.NotContains(t, names, "Row_Id", "so no file holds two spellings of row_id")

	// The registry has since retired both; the active cache the projection
	// is built from holds only title.
	sp, err := BuildSchemaProjection(1, forma.SchemaAttributeCache{
		"title": {AttributeName: "title", AttributeID: 1, ValueType: forma.ValueTypeText},
	})
	require.NoError(t, err)

	var gotRowID, gotTitle string
	var gotCreated, gotVer, gotDeleted int64
	err = db.QueryRow(fmt.Sprintf("SELECT CAST(row_id AS VARCHAR), created_at, ver_ts, deleted_ts, title FROM (SELECT %s FROM %s)",
		sp.S3SourceSelect, BuildParquetScanSource(formatPathList(path), nil, nil))).
		Scan(&gotRowID, &gotCreated, &gotVer, &gotDeleted, &gotTitle)
	require.NoError(t, err, "the s3_source projection over the cold scan must bind with the retired columns present")
	require.Equal(t, rowID, gotRowID, "row_id binds to the system column, not the retired Row_Id")
	require.Equal(t, int64(50), gotCreated, "created_at binds to ltbase_created_at, not the retired Created_At")
	require.Equal(t, int64(100), gotVer)
	require.Equal(t, int64(0), gotDeleted)
	require.Equal(t, "live", gotTitle)
}

// TestParquetScan_FoldCollisionMergesAcrossFiles is why a retired entry
// stays in scope for the collision half. Foo was flushed while active and is
// now retired; foo is its active same-folding successor. union_by_name
// resolves the two spellings onto one column, so the active projection of
// foo silently reads the retired Foo's values from the older file. The
// hazard is in the files, not in any projection, which is why the guard
// refuses the pair and names the retired side as the ledger.
func TestParquetScan_FoldCollisionMergesAcrossFiles(t *testing.T) {
	db := guardDuckDB(t)
	dir := t.TempDir()
	older := filepath.Join(dir, "flushed_while_Foo_active.parquet")
	newer := filepath.Join(dir, "flushed_with_foo.parquet")
	_, err := db.Exec(fmt.Sprintf(
		"COPY (SELECT CAST('018f05c0-0000-7000-8000-00000000000a' AS UUID) AS row_id, 'from-retired-Foo' AS Foo) TO '%s' (FORMAT PARQUET)", older))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(
		"COPY (SELECT CAST('018f05c0-0000-7000-8000-00000000000b' AS UUID) AS row_id, 'from-active-foo' AS foo) TO '%s' (FORMAT PARQUET)", newer))
	require.NoError(t, err)

	got, err := scanRowIDs(db, fmt.Sprintf(
		"SELECT foo FROM read_parquet(%s, union_by_name=true) ORDER BY row_id", formatPathList(older, newer)))
	require.NoError(t, err)
	require.Equal(t, []any{"from-retired-Foo", "from-active-foo"}, got,
		"the active attribute's column reads the retired attribute's flushed values")
}

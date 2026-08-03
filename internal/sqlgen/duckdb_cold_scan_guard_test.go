package sqlgen

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

// The tests below are the real-DuckDB proof of the #256 scan-level row_id
// guard. A manifest stamp that satisfies the parquetcheck invariant spares its
// path the footer probe, so a rogue overwrite (or a tampered manifest) can put
// an object whose real bytes lack row_id into a scan the validator waved
// through. union_by_name then NULL-fills row_id, those rows fall out of the
// dirty anti-join, and the query SUCCEEDS while ignoring the file — the silent
// data loss the pre-stamp probe path failed loudly on.
//
// They run against a real engine because every load-bearing property here is
// the engine's, not the string's: that error() is not folded away at bind time
// on a healthy scan, that it type-unifies with row_id instead of coercing it,
// and that it survives into the plan at both template scan sites.

// guardFixtures writes the three parquet shapes the guard has to handle:
// a production-shaped healthy file (row_id UUID), a rogue file with no row_id
// column at all, and a benchmark-shaped file (row_id VARCHAR — schemas 100-102
// carry the legacy CSV-sniffed harness shape and render through this same
// scan source).
func guardFixtures(t *testing.T, db *sql.DB) (healthy, rogue, varcharRowID string) {
	t.Helper()
	dir := t.TempDir()
	healthy = filepath.Join(dir, "healthy.parquet")
	rogue = filepath.Join(dir, "rogue.parquet")
	varcharRowID = filepath.Join(dir, "benchmark.parquet")

	writes := [][2]string{
		{healthy, "SELECT CAST('018f05c0-0000-7000-8000-00000000000a' AS UUID) AS row_id, " +
			"CAST(1 AS BIGINT) AS changed_at, 'alive' AS title"},
		{rogue, "SELECT CAST(2 AS BIGINT) AS changed_at, 'rogue' AS title"},
		{varcharRowID, "SELECT CAST('rid-1' AS VARCHAR) AS row_id, " +
			"CAST(3 AS BIGINT) AS changed_at, 'bench' AS title"},
	}
	for _, w := range writes {
		_, err := db.Exec(fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", w[1], w[0]))
		require.NoError(t, err, "write parquet fixture %s", w[0])
	}
	return healthy, rogue, varcharRowID
}

func guardDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func pathList(paths ...string) string {
	quoted := make([]string, 0, len(paths))
	for _, p := range paths {
		quoted = append(quoted, "'"+p+"'")
	}
	out := "["
	for i, q := range quoted {
		if i > 0 {
			out += ", "
		}
		out += q
	}
	return out + "]"
}

// scanRowIDs runs a query and returns the row_id values it produced, or the
// error the engine raised.
func scanRowIDs(db *sql.DB, query string) ([]any, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []any
	for rows.Next() {
		var v any
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// TestParquetScanGuardFailsLoudlyOnRogueObject is the silent-loss closer. The
// scan set mixes a healthy object with one whose bytes carry no row_id — the
// rogue-overwrite shape a valid-looking manifest stamp lets past the pre-read
// validator. The query mirrors the template's s3_source site (the dirty
// anti-join reads row_id), and it must FAIL rather than quietly return the
// rogue rows with a NULL row_id that the anti-join then discards.
func TestParquetScanGuardFailsLoudlyOnRogueObject(t *testing.T) {
	db := guardDuckDB(t)
	healthy, rogue, _ := guardFixtures(t, db)

	scan := BuildParquetScanSource(pathList(healthy, rogue), nil)
	got, err := scanRowIDs(db, "SELECT row_id FROM "+scan+
		" WHERE CAST(row_id AS UUID) NOT IN (SELECT CAST('018f05c0-0000-7000-8000-0000000000ff' AS UUID))")

	require.Error(t, err, "a scanned object without row_id must fail the query, not vanish from the anti-join; got rows %v", got)
	require.Contains(t, err.Error(), "NULL row_id",
		"the failure must name the violated invariant so an operator can act on it")
}

// TestParquetScanGuardFiresAtSemijoinSite pins the SECOND template scan site
// (the pushdown semijoin's inner SELECT row_id). Both sites render the same
// scan source, so both must carry the guard — a semijoin that silently loses
// the rogue rows would under-qualify the outer scan.
func TestParquetScanGuardFiresAtSemijoinSite(t *testing.T) {
	db := guardDuckDB(t)
	healthy, rogue, _ := guardFixtures(t, db)

	scan := BuildParquetScanSource(pathList(healthy, rogue), nil)
	got, err := scanRowIDs(db, "SELECT row_id FROM "+scan+" WHERE (1=1)")

	require.Error(t, err, "semijoin site must fail on the rogue object too; got rows %v", got)
	require.Contains(t, err.Error(), "NULL row_id")
}

// TestParquetScanGuardPassesHealthyScan is the constant-folding pin. DuckDB
// evaluates some scalar functions at bind time; if error() were folded, EVERY
// guarded scan would fail — the guard has to be inert on healthy bytes.
func TestParquetScanGuardPassesHealthyScan(t *testing.T) {
	db := guardDuckDB(t)
	healthy, _, _ := guardFixtures(t, db)

	scan := BuildParquetScanSource(pathList(healthy), nil)
	got, err := scanRowIDs(db, "SELECT row_id FROM "+scan)

	require.NoError(t, err, "the guard must not fire on an object that satisfies the invariant")
	require.Len(t, got, 1)
}

// TestParquetScanGuardPreservesRowIDType is the #147 no-coercion pin. row_id is
// UUID in production exports and VARCHAR in the benchmark shape, and the scan
// source is schema-blind — so the guard must adopt the column's own type at
// both, never impose one. A CAST(error(...) AS UUID) form passes the first case
// and fails to bind the second; this test is what separates them.
func TestParquetScanGuardPreservesRowIDType(t *testing.T) {
	db := guardDuckDB(t)
	healthy, _, varcharRowID := guardFixtures(t, db)

	for _, tc := range []struct {
		name, path, want string
	}{
		{"production_uuid", healthy, "UUID"},
		{"benchmark_varchar", varcharRowID, "VARCHAR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scan := BuildParquetScanSource(pathList(tc.path), nil)

			var guarded string
			require.NoError(t, db.QueryRow("SELECT typeof(row_id) FROM "+scan).Scan(&guarded),
				"guarded scan of %s must bind", tc.path)

			var bare string
			require.NoError(t, db.QueryRow(fmt.Sprintf(
				"SELECT typeof(row_id) FROM read_parquet('%s', union_by_name=true)", tc.path)).Scan(&bare))

			require.Equal(t, bare, guarded,
				"the guard must leave row_id's physical type exactly as the unguarded scan reports it")
			require.Equal(t, tc.want, guarded)
		})
	}
}

// TestParquetScanGuardComposesWithNullAugmentation pins that the guard and the
// #255 typed-NULL augmentation share one SELECT without either disabling the
// other: REPLACE rewrites row_id in place while the appended items add the
// cold-absent columns.
func TestParquetScanGuardComposesWithNullAugmentation(t *testing.T) {
	db := guardDuckDB(t)
	healthy, rogue, _ := guardFixtures(t, db)

	scan := BuildParquetScanSource(pathList(healthy), []NullScanColumn{{Name: "score", DuckDBType: "INTEGER"}})
	var rowID any
	var score sql.NullInt64
	var scoreType string
	require.NoError(t, db.QueryRow("SELECT row_id, score, typeof(score) FROM "+scan).Scan(&rowID, &score, &scoreType))
	require.False(t, score.Valid, "the augmented column stays NULL")
	require.Equal(t, "INTEGER", scoreType)

	// And the guard still fires when the augmented scan hits a rogue object.
	rogueScan := BuildParquetScanSource(pathList(healthy, rogue), []NullScanColumn{{Name: "score", DuckDBType: "INTEGER"}})
	_, err := scanRowIDs(db, "SELECT row_id FROM "+rogueScan)
	require.Error(t, err)
	require.Contains(t, err.Error(), "NULL row_id")
}

// TestParquetScanGuardFailsWhenRowIDAbsentEverywhere covers the degenerate set
// where NO scanned object carries row_id: REPLACE cannot bind the column, so
// the engine refuses the query. Different message, same contract — loud, never
// silent.
func TestParquetScanGuardFailsWhenRowIDAbsentEverywhere(t *testing.T) {
	db := guardDuckDB(t)
	_, rogue, _ := guardFixtures(t, db)

	scan := BuildParquetScanSource(pathList(rogue), nil)
	got, err := scanRowIDs(db, "SELECT changed_at FROM "+scan)

	require.Error(t, err, "a scan set with no row_id column anywhere must not bind; got rows %v", got)
	require.Contains(t, err.Error(), "row_id")
}

// TestUnguardedScanLosesRogueRowsSilently characterizes what the guard exists
// to prevent, straight from the engine: the bare union_by_name scan the
// template used before #256 accepts the rogue object and hands its rows a NULL
// row_id. Downstream that is indistinguishable from "not in this tier".
func TestUnguardedScanLosesRogueRowsSilently(t *testing.T) {
	db := guardDuckDB(t)
	healthy, rogue, _ := guardFixtures(t, db)

	got, err := scanRowIDs(db, fmt.Sprintf(
		"SELECT row_id FROM read_parquet(%s, union_by_name=true)", pathList(healthy, rogue)))

	require.NoError(t, err, "the unguarded scan is exactly the silent path")
	require.Len(t, got, 2)
	require.Contains(t, got, nil, "the rogue object's row arrives with a NULL row_id and drops out of the anti-join")
}

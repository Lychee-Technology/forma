package sqlgen

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The tests below are the #371 attribute-column type pin: the rendered form
// and the real-DuckDB proof of the QUIET failure the issue names. A delta
// generation written while an attribute was still `text` survives a re-init
// that only replaces the base tier; union_by_name then widens the column to
// VARCHAR across the whole scan, every value coerces, no error fires, and an
// ORDER BY on the attribute is lexicographic — '9' above '100'. The loud
// sibling (a text→list flip, #315) fails on its own; this pin exists for the
// case that does not.

// The pin renders INSIDE the system REPLACE list, after the system items, as
// CAST(col AS type) AS col — never as a projection alias, which would
// duplicate the column instead of replacing it. Missing columns keep their
// typed-NULL projection after the REPLACE.
func TestBuildParquetScanSourcePinsAttributeColumnsInsideReplace(t *testing.T) {
	got := BuildParquetScanSource("'s3://b/a.parquet'", nil, []ScanColumn{
		{Name: "amount", DuckDBType: "DOUBLE"},
		{Name: "score", DuckDBType: "INTEGER"},
	})
	wantReplace := wantGuardREPLACE[:len(wantGuardREPLACE)-1] +
		", CAST(amount AS DOUBLE) AS amount, CAST(score AS INTEGER) AS score)"
	require.Equal(t,
		"(SELECT "+wantReplace+" FROM read_parquet('s3://b/a.parquet', union_by_name=true)) AS cold_scan",
		got)

	both := BuildParquetScanSource("'s3://b/a.parquet'",
		[]ScanColumn{{Name: "tags", DuckDBType: "BIGINT[]"}},
		[]ScanColumn{{Name: "score", DuckDBType: "INTEGER"}})
	require.Equal(t,
		"(SELECT "+wantGuardREPLACE[:len(wantGuardREPLACE)-1]+", CAST(score AS INTEGER) AS score), "+
			"NULL::BIGINT[] AS tags FROM read_parquet('s3://b/a.parquet', union_by_name=true)) AS cold_scan",
		both)
}

// A healthy scan set pins nothing, so the rendered SQL is byte-identical to
// the pre-#371 form: the pin never taxes the common case.
func TestBuildParquetScanSourceNoPinIsByteIdentical(t *testing.T) {
	require.Equal(t,
		BuildParquetScanSource("'s3://b/a.parquet'", nil, nil),
		BuildParquetScanSource("'s3://b/a.parquet'", nil, []ScanColumn{}))
}

func TestScanTypesCompatible(t *testing.T) {
	require.True(t, ScanTypesCompatible("INTEGER", "INTEGER"))
	require.True(t, ScanTypesCompatible("integer", "INTEGER"), "case-insensitive")
	require.True(t, ScanTypesCompatible("BIGINT[]", "BIGINT[]"))
	require.True(t, ScanTypesCompatible("UUID", "VARCHAR"), "column-bound uuid exports as parquet UUID (#147)")
	require.True(t, ScanTypesCompatible("VARCHAR", "UUID"))
	require.False(t, ScanTypesCompatible("VARCHAR", "INTEGER"), "the #371 drift")
	require.False(t, ScanTypesCompatible("INTEGER", "DOUBLE"), "storage-width mismatch is a pin, not a pass (#384)")
	require.False(t, ScanTypesCompatible("VARCHAR", "BIGINT[]"), "the #315 flip")
	require.False(t, ScanTypesCompatible("UUID", "INTEGER"))
}

// attrPinFixtures writes three generations of one schema whose `score`
// attribute is declared integer: a current one (INTEGER 100), a stale one
// written while the attribute was still text (VARCHAR '9'), and a garbage
// one (VARCHAR 'n/a'). All carry the production system columns.
type attrPinFixtureSet struct {
	current, stale, garbage string
}

func attrPinFixtures(t *testing.T, db *sql.DB) attrPinFixtureSet {
	t.Helper()
	dir := t.TempDir()
	const sys = "CAST(NULL AS BIGINT) AS deleted_at, CAST(50 AS BIGINT) AS ltbase_created_at"
	set := attrPinFixtureSet{}
	for _, w := range []struct {
		dst       *string
		name, sel string
	}{
		{&set.current, "current.parquet", "CAST('018f05c0-0000-7000-8000-00000000000a' AS UUID) AS row_id, CAST(200 AS BIGINT) AS changed_at, " + sys + ", CAST(100 AS INTEGER) AS score"},
		{&set.stale, "stale.parquet", "CAST('018f05c0-0000-7000-8000-00000000000b' AS UUID) AS row_id, CAST(100 AS BIGINT) AS changed_at, " + sys + ", CAST('9' AS VARCHAR) AS score"},
		{&set.garbage, "garbage.parquet", "CAST('018f05c0-0000-7000-8000-00000000000c' AS UUID) AS row_id, CAST(100 AS BIGINT) AS changed_at, " + sys + ", CAST('n/a' AS VARCHAR) AS score"},
	} {
		*w.dst = filepath.Join(dir, w.name)
		_, err := db.Exec(fmt.Sprintf("COPY (SELECT %s) TO '%s' (FORMAT PARQUET)", w.sel, *w.dst))
		require.NoError(t, err, "write parquet fixture %s", *w.dst)
	}
	return set
}

var scorePin = []ScanColumn{{Name: "score", DuckDBType: "INTEGER"}}

// TestParquetScanPinRestoresNumericAttributeOrdering is acceptance criterion
// (b) of #371: the quiet INTEGER→VARCHAR case. Unpinned, the widened column
// orders lexicographically and the query succeeds; pinned, the type is
// INTEGER again, '9' coerces to 9, and the order is numeric. The unpinned
// leg is asserted alongside so the test fails if DuckDB ever stops widening.
func TestParquetScanPinRestoresNumericAttributeOrdering(t *testing.T) {
	db := guardDuckDB(t)
	fx := attrPinFixtures(t, db)
	paths := formatPathList(fx.current, fx.stale)

	var bareType, bareTop string
	require.NoError(t, db.QueryRow(fmt.Sprintf(
		"SELECT typeof(score), score FROM read_parquet(%s, union_by_name=true) ORDER BY score DESC LIMIT 1",
		paths)).Scan(&bareType, &bareTop))
	require.Equal(t, "VARCHAR", bareType, "union_by_name widens to the stale generation's type")
	require.Equal(t, "9", bareTop, "and the order is silently lexicographic: '9' sorts above '100'")

	var pinnedType string
	var pinnedTop int64
	require.NoError(t, db.QueryRow(
		"SELECT typeof(score), score FROM "+BuildParquetScanSource(paths, nil, scorePin)+" ORDER BY score DESC LIMIT 1",
	).Scan(&pinnedType, &pinnedTop), "a numeric-string attribute must coerce, not fail")
	require.Equal(t, "INTEGER", pinnedType, "the pin re-pins the schema type")
	require.Equal(t, int64(100), pinnedTop, "and the numeric order is restored")

	// A filter on the pinned column compares numerically too: '9' < 50.
	ids, err := scanRowIDs(db, "SELECT row_id FROM "+BuildParquetScanSource(paths, nil, scorePin)+" WHERE score > 50")
	require.NoError(t, err)
	require.Len(t, ids, 1, "only the INTEGER 100 row passes a numeric filter once the column is pinned")
}

// A pinned column whose stale value cannot convert fails loudly with the
// engine's own conversion error — the same contract as the changed_at guard.
func TestParquetScanPinFailsLoudlyOnUnconvertibleValue(t *testing.T) {
	db := guardDuckDB(t)
	fx := attrPinFixtures(t, db)
	paths := formatPathList(fx.current, fx.garbage)

	_, bareErr := scanRowIDs(db, fmt.Sprintf("SELECT row_id FROM read_parquet(%s, union_by_name=true) ORDER BY score", paths))
	require.NoError(t, bareErr, "unpinned, garbage under a widened VARCHAR column reads without complaint")

	// A query that never reads the column is pruned past the CAST and still
	// succeeds — the pin fails the read that would have consumed the value,
	// which is the same scope as the changed_at guard.
	_, err := scanRowIDs(db, "SELECT row_id FROM "+BuildParquetScanSource(paths, nil, scorePin)+" ORDER BY score")
	require.Error(t, err, "pinned, the garbage generation must fail a read that consumes the column")
	require.Contains(t, err.Error(), "Conversion Error")
}

// On a healthy set the pin is a no-op the engine agrees with: the column is
// already INTEGER and a redundant CAST changes neither type nor values.
func TestParquetScanPinIsIdentityOnHealthySet(t *testing.T) {
	db := guardDuckDB(t)
	fx := attrPinFixtures(t, db)
	paths := formatPathList(fx.current)

	var unpinnedType, pinnedType string
	var unpinned, pinned int64
	require.NoError(t, db.QueryRow("SELECT typeof(score), score FROM "+BuildParquetScanSource(paths, nil, nil)).Scan(&unpinnedType, &unpinned))
	require.NoError(t, db.QueryRow("SELECT typeof(score), score FROM "+BuildParquetScanSource(paths, nil, scorePin)).Scan(&pinnedType, &pinned))
	require.Equal(t, "INTEGER", unpinnedType)
	require.Equal(t, unpinnedType, pinnedType)
	require.Equal(t, unpinned, pinned)
}

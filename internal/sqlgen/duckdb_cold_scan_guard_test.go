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

// The tests below are the real-DuckDB proof of the #256 scan-level
// system-column guard. A manifest stamp that satisfies the parquetcheck
// invariant spares its path the footer probe, so a rogue overwrite (or a
// tampered manifest) can put an object whose real bytes lack a system column
// into a scan the validator waved through. union_by_name then NULL-fills it:
// rows with a NULL row_id fall out of the dirty anti-join, rows with a NULL
// changed_at flow straight into LWW version ordering, and the query SUCCEEDS
// either way — the silent data loss the pre-stamp probe path failed loudly on.
//
// They run against a real engine because every load-bearing property here is
// the engine's, not the string's: that error() is not folded away at bind time
// on a healthy scan, that it type-unifies with row_id instead of coercing it,
// that the CAST re-pins a union-widened changed_at, and that it all survives
// into the plan at both template scan sites.

// guardFixtureSet is the parquet corpus the guard has to handle. Every file
// carries the production system-column trio unless the field name says
// otherwise.
type guardFixtureSet struct {
	// healthy is production-shaped: row_id UUID, changed_at BIGINT,
	// deleted_at BIGINT NULL (the pre-#274 legacy live-row delta encoding,
	// still readable until compaction retires it — #365).
	healthy string
	// noRowID / noChangedAt / noDeletedAt each drop exactly one system column,
	// the rogue-overwrite shape a stale-but-valid stamp lets past the probe.
	noRowID, noChangedAt, noDeletedAt string
	// varcharRowID is the benchmark shape — schemas 100-102 carry the legacy
	// CSV-sniffed harness shape and render through this same scan source.
	varcharRowID string
	// varcharChangedAt / garbageChangedAt exercise the type channel: a rogue
	// file whose changed_at is VARCHAR widens the union and would make LWW
	// ordering lexicographic.
	varcharChangedAt, garbageChangedAt string
	// noCreatedAt / varcharCreatedAt / nullCreatedAt exercise the #460
	// creation-stamp column: absent entirely (caught before the read, by the
	// parquetcheck invariant — the scan's REPLACE list simply fails to bind),
	// carried as VARCHAR (the type channel the CAST re-pins), and present but
	// NULL (the hard-delete tombstone shape the guard must tolerate).
	noCreatedAt, varcharCreatedAt, nullCreatedAt string
}

func guardFixtures(t *testing.T, db *sql.DB) guardFixtureSet {
	t.Helper()
	dir := t.TempDir()
	const rowIDA = "CAST('018f05c0-0000-7000-8000-00000000000a' AS UUID) AS row_id"
	const rowIDB = "CAST('018f05c0-0000-7000-8000-00000000000b' AS UUID) AS row_id"
	const liveDeleted = "CAST(NULL AS BIGINT) AS deleted_at"
	// The creation stamp every production generation carries (#460). Held
	// well below the changed_at values so a projection that confused the two
	// is visible by value.
	const created = "CAST(50 AS BIGINT) AS ltbase_created_at"

	set := guardFixtureSet{}
	for _, w := range []struct {
		dst       *string
		name, sel string
	}{
		// changed_at=100 against varcharChangedAt's '9' is deliberate: the two
		// order one way numerically and the other way lexicographically, which
		// is what makes the union-widening failure visible.
		{&set.healthy, "healthy.parquet", rowIDA + ", CAST(100 AS BIGINT) AS changed_at, " + liveDeleted + ", " + created + ", 'alive' AS title"},
		{&set.noRowID, "no_row_id.parquet", "CAST(2 AS BIGINT) AS changed_at, " + liveDeleted + ", " + created + ", 'rogue' AS title"},
		{&set.noChangedAt, "no_changed_at.parquet", rowIDB + ", " + liveDeleted + ", " + created + ", 'rogue' AS title"},
		{&set.noDeletedAt, "no_deleted_at.parquet", rowIDB + ", CAST(4 AS BIGINT) AS changed_at, " + created + ", 'rogue' AS title"},
		{&set.varcharRowID, "benchmark.parquet", "CAST('rid-1' AS VARCHAR) AS row_id, " +
			"CAST(3 AS BIGINT) AS changed_at, CAST(0 AS BIGINT) AS deleted_at, " + created + ", 'bench' AS title"},
		{&set.varcharChangedAt, "varchar_changed_at.parquet", rowIDB +
			", CAST('9' AS VARCHAR) AS changed_at, " + liveDeleted + ", " + created + ", 'numeric-string' AS title"},
		{&set.garbageChangedAt, "garbage_changed_at.parquet", rowIDB +
			", CAST('not-a-number' AS VARCHAR) AS changed_at, " + liveDeleted + ", " + created + ", 'garbage' AS title"},
		{&set.noCreatedAt, "no_created_at.parquet", rowIDB +
			", CAST(4 AS BIGINT) AS changed_at, " + liveDeleted + ", 'rogue' AS title"},
		{&set.varcharCreatedAt, "varchar_created_at.parquet", rowIDB +
			", CAST(4 AS BIGINT) AS changed_at, " + liveDeleted +
			", CAST('9' AS VARCHAR) AS ltbase_created_at, 'numeric-string' AS title"},
		{&set.nullCreatedAt, "null_created_at.parquet", rowIDB +
			", CAST(4 AS BIGINT) AS changed_at, CAST(4 AS BIGINT) AS deleted_at" +
			", CAST(NULL AS BIGINT) AS ltbase_created_at, 'tombstone' AS title"},
	} {
		*w.dst = filepath.Join(dir, w.name)
		_, err := db.Exec(fmt.Sprintf("COPY (SELECT %s) TO '%s' (FORMAT PARQUET)", w.sel, *w.dst))
		require.NoError(t, err, "write parquet fixture %s", *w.dst)
	}
	return set
}

func guardDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

// formatPathList renders paths as the quoted DuckDB list literal
// BuildParquetScanSource expects.
func formatPathList(paths ...string) string {
	quoted := make([]string, 0, len(paths))
	for _, p := range paths {
		quoted = append(quoted, "'"+p+"'")
	}
	return "[" + strings.Join(quoted, ", ") + "]"
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
	fx := guardFixtures(t, db)

	scan := BuildParquetScanSource(formatPathList(fx.healthy, fx.noRowID), nil)
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
	fx := guardFixtures(t, db)

	scan := BuildParquetScanSource(formatPathList(fx.healthy, fx.noRowID), nil)
	got, err := scanRowIDs(db, "SELECT row_id FROM "+scan+" WHERE (1=1)")

	require.Error(t, err, "semijoin site must fail on the rogue object too; got rows %v", got)
	require.Contains(t, err.Error(), "NULL row_id")
}

// TestParquetScanGuardPassesHealthyScan is the constant-folding pin. DuckDB
// evaluates some scalar functions at bind time; if error() were folded, EVERY
// guarded scan would fail — the guard has to be inert on healthy bytes.
func TestParquetScanGuardPassesHealthyScan(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)

	scan := BuildParquetScanSource(formatPathList(fx.healthy), nil)
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
	fx := guardFixtures(t, db)

	for _, tc := range []struct {
		name, path, want string
	}{
		{"production_uuid", fx.healthy, "UUID"},
		{"benchmark_varchar", fx.varcharRowID, "VARCHAR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scan := BuildParquetScanSource(formatPathList(tc.path), nil)

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
	fx := guardFixtures(t, db)

	scan := BuildParquetScanSource(formatPathList(fx.healthy), []NullScanColumn{{Name: "score", DuckDBType: "INTEGER"}})
	var rowID any
	var score sql.NullInt64
	var scoreType string
	require.NoError(t, db.QueryRow("SELECT row_id, score, typeof(score) FROM "+scan).Scan(&rowID, &score, &scoreType))
	require.False(t, score.Valid, "the augmented column stays NULL")
	require.Equal(t, "INTEGER", scoreType)

	// And the guard still fires when the augmented scan hits a rogue object.
	rogueScan := BuildParquetScanSource(formatPathList(fx.healthy, fx.noRowID), []NullScanColumn{{Name: "score", DuckDBType: "INTEGER"}})
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
	fx := guardFixtures(t, db)

	scan := BuildParquetScanSource(formatPathList(fx.noRowID), nil)
	got, err := scanRowIDs(db, "SELECT changed_at FROM "+scan)

	require.Error(t, err, "a scan set with no row_id column anywhere must not bind; got rows %v", got)
	require.Contains(t, err.Error(), "row_id")
}

// TestParquetScanGuardFailsLoudlyOnMissingChangedAt is the second silent-loss
// closer. row_id is not the only column a stale-but-valid stamp can let a
// rogue object omit: changed_at is the LWW version, so a NULL-filled one does
// not drop the rows — it feeds NULL into version ordering and lets the merge
// pick a winner on garbage. The scan set keeps a healthy sibling so changed_at
// still binds and the guard, not the binder, is what fires.
func TestParquetScanGuardFailsLoudlyOnMissingChangedAt(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)

	scan := BuildParquetScanSource(formatPathList(fx.healthy, fx.noChangedAt), nil)
	got, err := scanRowIDs(db, "SELECT changed_at FROM "+scan)

	require.Error(t, err, "a scanned object without changed_at must fail the query, not enter LWW ordering as NULL; got rows %v", got)
	require.Contains(t, err.Error(), "NULL changed_at",
		"the failure must name the violated column so an operator can act on it")
}

// TestUnguardedScanAdmitsNullChangedAtSilently is the RED half of the test
// above, straight from the engine: without the guard the rogue object's rows
// arrive with a NULL changed_at and no error at all.
func TestUnguardedScanAdmitsNullChangedAtSilently(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)

	got, err := scanRowIDs(db, fmt.Sprintf(
		"SELECT changed_at FROM read_parquet(%s, union_by_name=true)",
		formatPathList(fx.healthy, fx.noChangedAt)))

	require.NoError(t, err, "the unguarded scan is exactly the silent path")
	require.Len(t, got, 2)
	require.Contains(t, got, nil, "the rogue object's row reaches LWW ordering with a NULL version")
}

// TestParquetScanGuardPinsSystemColumnTypes is the type-channel pin. A rogue
// file carrying changed_at as VARCHAR widens the whole union_by_name result to
// VARCHAR, at which point LWW version ordering is lexicographic — '9' sorts
// above '100' — and the merge silently picks the wrong winner. The CAST in the
// guard re-pins BIGINT: numeric strings coerce value-preservingly, ordering
// stays numeric. The unguarded leg is asserted alongside so the test fails if
// DuckDB ever stops widening (which would make the CAST look load-bearing when
// it no longer is).
func TestParquetScanGuardPinsSystemColumnTypes(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)
	paths := formatPathList(fx.healthy, fx.varcharChangedAt)

	var bareType, bareMax string
	require.NoError(t, db.QueryRow(fmt.Sprintf(
		"SELECT typeof(changed_at), max(changed_at) OVER () FROM read_parquet(%s, union_by_name=true) LIMIT 1",
		paths)).Scan(&bareType, &bareMax))
	require.Equal(t, "VARCHAR", bareType, "union_by_name widens to the rogue file's type")
	require.Equal(t, "9", bareMax,
		"and the LWW winner silently flips: lexicographically '9' beats '100'")

	var guardedType string
	var guardedMax, guardedMin int64
	require.NoError(t, db.QueryRow("SELECT typeof(changed_at), max(changed_at) OVER (), min(changed_at) OVER () FROM "+
		BuildParquetScanSource(paths, nil)+" LIMIT 1").Scan(&guardedType, &guardedMax, &guardedMin),
		"a numeric-string changed_at must coerce, not fail")
	require.Equal(t, "BIGINT", guardedType, "the guard re-pins the LWW version type")
	require.Equal(t, int64(100), guardedMax, "and the numeric winner is restored")
	require.Equal(t, int64(9), guardedMin, "the VARCHAR '9' coerced value-preservingly to 9")

	// deleted_at is pinned the same way, on the healthy file where it is the
	// live-row NULL — the CAST must not turn that into a failure.
	var deletedType string
	require.NoError(t, db.QueryRow("SELECT typeof(deleted_at) FROM "+
		BuildParquetScanSource(formatPathList(fx.healthy), nil)).Scan(&deletedType))
	require.Equal(t, "BIGINT", deletedType)

	// Garbage in the version column is loud, not coerced to some default.
	_, err := scanRowIDs(db, "SELECT changed_at FROM "+
		BuildParquetScanSource(formatPathList(fx.healthy, fx.garbageChangedAt), nil))
	require.Error(t, err, "a non-numeric changed_at must fail loudly")
	require.Contains(t, err.Error(), "not-a-number")
}

// TestParquetScanGuardTolerateNullDeletedAt is the deleted_at RESIDUAL
// characterization (#365). #274 normalized the delta export to COALESCE live
// rows to 0, but delta objects written BEFORE #274 still encode live rows as
// a literal NULL and remain readable until compaction retires them, so the
// scan must keep tolerating the NULL. This pins BOTH halves of that residual:
//
//   - a legacy live delta row (deleted_at NULL) must pass — a presence guard
//     today would fail every healthy scan touching a pre-#274 object, which
//     is why there isn't one;
//   - a rogue object MISSING deleted_at entirely still flows through with a
//     NULL, undetected by the scan. That gap is covered only by the pre-read
//     footer probe and the manifest stamp. Closing it is gated on the legacy
//     objects being retired — tracked in #365; when THAT lands, this test is
//     the one that should go red.
func TestParquetScanGuardTolerateNullDeletedAt(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)

	var deleted sql.NullInt64
	require.NoError(t, db.QueryRow("SELECT deleted_at FROM "+
		BuildParquetScanSource(formatPathList(fx.healthy), nil)).Scan(&deleted),
		"a legacy live delta row's NULL deleted_at is legitimate and must not fire the guard (#365)")
	require.False(t, deleted.Valid)

	got, err := scanRowIDs(db, "SELECT deleted_at FROM "+
		BuildParquetScanSource(formatPathList(fx.healthy, fx.noDeletedAt), nil))
	require.NoError(t, err,
		"RESIDUAL (#365): an object missing deleted_at is NOT caught by the scan guard today")
	require.Len(t, got, 2)
	require.Equal(t, []any{nil, nil}, got,
		"both the legacy live-row NULL and the rogue absence look identical here — "+
			"exactly why deleted_at presence cannot be value-guarded until #365")
}

// TestUnguardedScanLosesRogueRowsSilently characterizes what the guard exists
// to prevent, straight from the engine: the bare union_by_name scan the
// template used before #256 accepts the rogue object and hands its rows a NULL
// row_id. Downstream that is indistinguishable from "not in this tier".
func TestUnguardedScanLosesRogueRowsSilently(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)

	got, err := scanRowIDs(db, fmt.Sprintf(
		"SELECT row_id FROM read_parquet(%s, union_by_name=true)", formatPathList(fx.healthy, fx.noRowID)))

	require.NoError(t, err, "the unguarded scan is exactly the silent path")
	require.Len(t, got, 2)
	require.Contains(t, got, nil, "the rogue object's row arrives with a NULL row_id and drops out of the anti-join")
}

// TestParquetScanGuardPinsCreatedAtType is the #460 type channel. Since the
// reader projects ltbase_created_at as created_at, a rogue file carrying it as
// VARCHAR widens the union_by_name result and the DEFAULT page order
// (created_at DESC) silently goes lexicographic — '9' above '100' — exactly
// the failure the changed_at CAST prevents for LWW. The bare leg is asserted
// alongside so this test fails if DuckDB ever stops widening, which would make
// the CAST look load-bearing when it no longer is.
func TestParquetScanGuardPinsCreatedAtType(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)
	paths := formatPathList(fx.healthy, fx.varcharCreatedAt)

	var bareType, bareMax string
	require.NoError(t, db.QueryRow(fmt.Sprintf(
		"SELECT typeof(ltbase_created_at), max(ltbase_created_at) OVER () FROM read_parquet(%s, union_by_name=true) LIMIT 1",
		paths)).Scan(&bareType, &bareMax))
	require.Equal(t, "VARCHAR", bareType, "union_by_name widens to the rogue file's type")
	require.Equal(t, "9", bareMax,
		"and the default page order silently flips: lexicographically '9' beats '50'")

	var guardedType string
	var guardedMax, guardedMin int64
	require.NoError(t, db.QueryRow("SELECT typeof(ltbase_created_at), max(ltbase_created_at) OVER (), min(ltbase_created_at) OVER () FROM "+
		BuildParquetScanSource(paths, nil)+" LIMIT 1").Scan(&guardedType, &guardedMax, &guardedMin),
		"a numeric-string creation stamp must coerce, not fail")
	require.Equal(t, "BIGINT", guardedType, "the guard re-pins the creation-stamp type")
	require.Equal(t, int64(50), guardedMax, "and the numeric order is restored")
	require.Equal(t, int64(9), guardedMin, "the VARCHAR '9' coerced value-preservingly to 9")
}

// TestParquetScanGuardToleratesNullCreatedAt pins the deliberate asymmetry in
// the #460 guard: ltbase_created_at gets a TYPE pin but NO value-presence
// guard. Hard-delete tombstones legitimately carry a NULL creation stamp — the
// delta export LEFT JOINs entity_main so a hard-deleted row's change_log entry
// still exports (#173) — so a presence guard would fail every healthy scan
// touching one. Those rows are dropped by the deleted_ts filter long before
// any ORDER BY, so the NULL never reaches a caller.
func TestParquetScanGuardToleratesNullCreatedAt(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)

	got, err := scanRowIDs(db, "SELECT ltbase_created_at FROM "+
		BuildParquetScanSource(formatPathList(fx.healthy, fx.nullCreatedAt), nil))
	require.NoError(t, err,
		"a tombstone's NULL creation stamp must pass the scan: the column is type-pinned, its value is not required")
	require.Len(t, got, 2)
	require.Contains(t, got, nil, "the tombstone row flows through with its NULL, to be dropped by the deleted_ts filter")
}

// TestParquetScanGuardFailsWhenCreatedAtAbsentEverywhere pins the binder
// channel: a scan set where NO object carries ltbase_created_at cannot bind
// the REPLACE list. Loud, never silent — the same contract row_id has. The
// dangerous case is the MIXED set (some objects carrying it), which binds
// fine and NULL-pads the rest; that one is caught a layer up by the
// parquetcheck invariant, not here.
func TestParquetScanGuardFailsWhenCreatedAtAbsentEverywhere(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)

	_, err := scanRowIDs(db, "SELECT row_id FROM "+
		BuildParquetScanSource(formatPathList(fx.noCreatedAt), nil))
	require.Error(t, err, "a scan set with no creation stamp anywhere must fail to bind, not scan on")
	require.Contains(t, err.Error(), "ltbase_created_at")
}

// TestUnguardedMixedGenerationNullPadsCreatedAt is the RED half of the
// invariant: with the column absent from ONE object of a mixed set, the scan
// BINDS and those rows arrive with a NULL created_at. The caller gets a wrong
// created_at, and the row is ordered by that NULL (DuckDB's default null order
// is NULLS LAST) rather than by its real creation time — so its page position
// is wrong now and MOVES once compaction folds the object into a file that
// carries the column. Nothing at scan level can catch this: the REPLACE list
// binds fine because a sibling object supplies the column. That is precisely
// why ltbase_created_at joined parquetcheck.SystemColumns, so the pre-read
// validator rejects the object instead (#460).
func TestUnguardedMixedGenerationNullPadsCreatedAt(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)

	rows, err := db.Query("SELECT ltbase_created_at FROM " +
		BuildParquetScanSource(formatPathList(fx.healthy, fx.noCreatedAt), nil) +
		" ORDER BY ltbase_created_at DESC")
	require.NoError(t, err, "the mixed set binds — the scan cannot see the missing column")
	defer func() { require.NoError(t, rows.Close()) }()

	var order []any
	for rows.Next() {
		var v sql.NullInt64
		require.NoError(t, rows.Scan(&v))
		if v.Valid {
			order = append(order, v.Int64)
			continue
		}
		order = append(order, nil)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []any{int64(50), nil}, order,
		"the NULL-padded row is ordered by its NULL, not by its real creation time — "+
			"the silent wrong value and unstable position the pre-read invariant now prevents")
}

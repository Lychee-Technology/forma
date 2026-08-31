package sqlgen

import (
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

// The #460 creation-stamp half of the cold-scan guard contract, split from
// duckdb_cold_scan_guard_test.go along the 500-line source limit. It shares
// that file's fixtures (guardFixtures, guardDuckDB, formatPathList,
// scanRowIDs); what lives here is everything specific to ltbase_created_at:
// the type pin, the tolerated tombstone NULL, the absent-everywhere binder
// failure, and the conditional presence guard that closes the #256
// trusted-stamp channel.

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

// TestParquetScanGuardToleratesNullCreatedAt pins the permissive half of the
// #460 guard: ltbase_created_at gets a TYPE pin plus a CONDITIONAL presence
// guard that fires only on LIVE rows, so a hard-delete tombstone's NULL
// creation stamp passes. Tombstones legitimately have none — the delta export
// LEFT JOINs entity_main so a hard-deleted row's change_log entry still
// exports (#173) — and they are dropped by the deleted_ts filter long before
// any ORDER BY, so the NULL never reaches a caller. The rejecting half lives
// in TestParquetScanGuardCreatedAtPresenceIsConditionalOnDeleted.
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

// TestParquetScanGuardFailsMixedGenerationLiveRow is the trusted-stamp
// channel (#460). A mixed set — one object carrying ltbase_created_at, one
// not — BINDS: the REPLACE list is satisfied by the sibling that has the
// column, so nothing structural stops the scan. union_by_name NULL-pads the
// rest, and before the conditional guard a LIVE row reached the caller with a
// NULL created_at.
//
// The pre-read validator cannot cover this on its own: a manifest stamp that
// satisfies the invariant spares its path the footer probe (#256), so a rogue
// overwrite behind a valid-looking stamp puts exactly this object into the
// scan. The guard is the second layer, and it is why the presence check had
// to be conditional rather than absent.
func TestParquetScanGuardFailsMixedGenerationLiveRow(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)
	paths := formatPathList(fx.healthy, fx.noCreatedAt)

	// RED half, straight from the engine: unguarded, the mixed set is silent.
	bare, err := scanRowIDs(db, fmt.Sprintf(
		"SELECT ltbase_created_at FROM read_parquet(%s, union_by_name=true)", paths))
	require.NoError(t, err, "the unguarded mixed set is exactly the silent path")
	require.Len(t, bare, 2)
	require.Contains(t, bare, nil,
		"the NULL-padded live row reaches the caller with no creation stamp at all")

	// GREEN half: the guard turns it into a loud failure.
	_, err = scanRowIDs(db, "SELECT ltbase_created_at FROM "+BuildParquetScanSource(paths, nil))
	require.Error(t, err,
		"a live row with no creation stamp must fail the query, not be served with a NULL created_at")
	require.Contains(t, err.Error(), ParquetNullCreatedAtMessage,
		"the conditional presence channel must raise our authored message — the text callers quote in triage")
}

// TestParquetScanGuardCreatedAtPresenceIsConditionalOnDeleted pins the exact
// seam of the conditional guard, so a later "simplification" to an
// unconditional COALESCE cannot pass: the SAME NULL creation stamp must fail
// on a live row and pass on a tombstone.
func TestParquetScanGuardCreatedAtPresenceIsConditionalOnDeleted(t *testing.T) {
	db := guardDuckDB(t)
	dir := t.TempDir()
	const rid = "CAST('018f05c0-0000-7000-8000-00000000000c' AS UUID) AS row_id"

	write := func(name, deletedAt string) string {
		path := filepath.Join(dir, name)
		_, err := db.Exec(fmt.Sprintf(
			"COPY (SELECT %s, CAST(7 AS BIGINT) AS changed_at, %s AS deleted_at, "+
				"CAST(NULL AS BIGINT) AS ltbase_created_at, 'x' AS title) TO '%s' (FORMAT PARQUET)",
			rid, deletedAt, path))
		require.NoError(t, err)
		return path
	}

	for _, tc := range []struct {
		name, deletedAt string
		wantErr         bool
	}{
		// Both live encodings must fail: 0 since #274, NULL on pre-#274
		// legacy delta objects.
		{"live_zero", "CAST(0 AS BIGINT)", true},
		{"live_legacy_null", "CAST(NULL AS BIGINT)", true},
		// A tombstone's NULL creation stamp is the healthy hard-delete shape.
		{"tombstone", "CAST(9 AS BIGINT)", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := write(tc.name+".parquet", tc.deletedAt)
			_, err := scanRowIDs(db, "SELECT ltbase_created_at FROM "+
				BuildParquetScanSource(formatPathList(path), nil))
			if tc.wantErr {
				require.Error(t, err, "a live row with a NULL creation stamp must fail loudly")
				require.Contains(t, err.Error(), ParquetNullCreatedAtMessage)
				return
			}
			require.NoError(t, err,
				"a hard-delete tombstone legitimately carries a NULL creation stamp and must pass")
		})
	}
}

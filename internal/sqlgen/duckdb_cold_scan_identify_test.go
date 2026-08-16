package sqlgen

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Single-file differential-drain proof for #351: a schema-wrong object reads
// clean under a bare SELECT * (which is why the #251 verify pass can never
// attribute it) but fails the guarded single-file scan — for every guard
// channel: the two error() presence guards, the changed_at/deleted_at CAST
// type guard, and the binder failure when a guarded column is absent from the
// single-file scan set. federated.identifyGuardViolations relies on exactly
// this split; if one of these tests goes red on a DuckDB upgrade, the
// identification pass loses that channel.
//
// The SQL shapes below are the ones the identification pass renders: the bare
// leg mirrors parquet_verify.go's `SELECT * FROM read_parquet('<path>')`, and
// the guarded leg feeds BuildParquetScanSource the single-path rendering
// formatDuckDBPathList produces for a one-element set (a bare quoted string,
// not a list literal).
//
// Deliberate breadth note: deleted_at is part of the parquetcheck invariant,
// so a file missing it fails the guarded SINGLE-file scan (binder: REPLACE
// target absent) even though a MULTI-file set scan tolerates it when a
// sibling carries the column (characterized in
// TestParquetScanGuardTolerateNullDeletedAt). Identification may therefore
// name an object whose only deviation is a missing deleted_at — a real
// invariant violation (the footer probe would reject it too), just not
// necessarily the one that fired this particular query's guard. The error
// wording in ParquetGuardViolationError says "fails the guarded single-file
// scan", not "caused this failure", for exactly this reason.

// singleFileScanSource renders the guarded scan over exactly one path, in the
// shape formatDuckDBPathList yields for a single-element set.
func singleFileScanSource(path string) string {
	return BuildParquetScanSource(fmt.Sprintf("'%s'", path), nil)
}

// drainQuery runs one query and iterates it to exhaustion, returning whichever
// leg raised first: a guard can fire at open or mid-read, and the
// identification pass treats both alike.
func drainQuery(db *sql.DB, query string) error {
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("query differential-drain SQL: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate differential-drain rows: %w", err)
	}
	return nil
}

// bareDrain is the differential's bare leg (parquet_verify.go, #251);
// guardedDrain is its guarded leg (parquet_guard_identify.go, #351).
func bareDrain(db *sql.DB, path string) error {
	return drainQuery(db, fmt.Sprintf("SELECT * FROM read_parquet('%s')", path))
}

func guardedDrain(db *sql.DB, path string) error {
	return drainQuery(db, "SELECT * FROM "+singleFileScanSource(path))
}

func TestSingleFileDifferentialDrainSplitsSchemaWrongFromHealthy(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)

	cases := []struct {
		name, path string
		violates   bool
	}{
		{"healthy", fx.healthy, false},
		{"varchar_row_id_benchmark_shape", fx.varcharRowID, false}, // untyped COALESCE adopts VARCHAR (#147)
		{"no_row_id", fx.noRowID, true},                            // binder: REPLACE target absent
		{"no_changed_at", fx.noChangedAt, true},                    // binder: REPLACE target absent
		{"no_deleted_at", fx.noDeletedAt, true},                    // binder — see breadth note above
		{"garbage_changed_at", fx.garbageChangedAt, true},          // CAST conversion failure
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, bareDrain(db, tc.path),
				"bare SELECT * must read the object clean — that is why #251 verify cannot attribute it")
			gerr := guardedDrain(db, tc.path)
			if tc.violates {
				require.Error(t, gerr, "guarded single-file scan must fail a schema-wrong object")
			} else {
				require.NoError(t, gerr, "guarded single-file scan must pass a healthy object")
			}
		})
	}
}

// TestSingleFileGuardedDrainRaisesNullPresenceMessages covers BOTH error()
// presence channels rather than the binder channel: a file that HAS the column
// but with NULL values in it. Written inline because rows with a
// present-but-NULL row_id or changed_at cannot come from guardFixtures (its
// rogue shapes drop columns outright). The NULL literal must be typed so
// parquet keeps the column. Each case also asserts the bare leg reads clean,
// so the differential identification relies on is characterized per channel,
// not just the guarded half.
func TestSingleFileGuardedDrainRaisesNullPresenceMessages(t *testing.T) {
	db := guardDuckDB(t)
	dir := t.TempDir()
	const liveRowID = `CAST('018f05c0-0000-7000-8000-00000000000a' AS UUID) AS row_id`

	cases := []struct{ name, sel, message string }{
		{
			name: "null_row_id",
			sel: `CAST(NULL AS UUID) AS row_id, CAST(1 AS BIGINT) AS changed_at, ` +
				`CAST(0 AS BIGINT) AS deleted_at`,
			message: ParquetNullRowIDMessage,
		},
		{
			name: "null_changed_at",
			sel: liveRowID + `, CAST(NULL AS BIGINT) AS changed_at, ` +
				`CAST(0 AS BIGINT) AS deleted_at`,
			message: ParquetNullChangedAtMessage,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, tc.name+".parquet")
			_, err := db.Exec(fmt.Sprintf(
				"COPY (SELECT %s) TO '%s' (FORMAT PARQUET)", tc.sel, path))
			require.NoError(t, err)

			require.NoError(t, bareDrain(db, path),
				"bare SELECT * must read the object clean — that is why #251 verify cannot attribute it")
			gerr := guardedDrain(db, path)
			require.Error(t, gerr)
			require.Contains(t, gerr.Error(), tc.message,
				"the presence channel must raise our authored message — the one text callers may quote in triage")
		})
	}
}

package sqlgen

import (
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

func TestSingleFileDifferentialDrainSplitsSchemaWrongFromHealthy(t *testing.T) {
	db := guardDuckDB(t)
	fx := guardFixtures(t, db)

	drain := func(query string) error {
		rows, err := db.Query(query)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
		}
		return rows.Err()
	}
	bare := func(path string) error {
		return drain(fmt.Sprintf("SELECT * FROM read_parquet('%s')", path))
	}
	guarded := func(path string) error {
		return drain("SELECT * FROM " + singleFileScanSource(path))
	}

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
			require.NoError(t, bare(tc.path),
				"bare SELECT * must read the object clean — that is why #251 verify cannot attribute it")
			gerr := guarded(tc.path)
			if tc.violates {
				require.Error(t, gerr, "guarded single-file scan must fail a schema-wrong object")
			} else {
				require.NoError(t, gerr, "guarded single-file scan must pass a healthy object")
			}
		})
	}
}

// TestSingleFileGuardedDrainRaisesNullPresenceMessage covers the error()
// channel rather than the binder channel: a file that HAS the column but with
// NULL values in it. Written inline because rows with row_id present-but-NULL
// cannot come from guardFixtures (its rogue shapes drop columns outright). The
// NULL literal must be typed so parquet keeps the column.
func TestSingleFileGuardedDrainRaisesNullPresenceMessage(t *testing.T) {
	db := guardDuckDB(t)
	path := filepath.Join(t.TempDir(), "null_row_id.parquet")
	_, err := db.Exec(fmt.Sprintf(
		`COPY (SELECT CAST(NULL AS UUID) AS row_id, CAST(1 AS BIGINT) AS changed_at, `+
			`CAST(0 AS BIGINT) AS deleted_at) TO '%s' (FORMAT PARQUET)`, path))
	require.NoError(t, err)

	rows, qerr := db.Query("SELECT * FROM " + singleFileScanSource(path))
	gerr := qerr
	if qerr == nil {
		defer func() { _ = rows.Close() }()
		for rows.Next() {
		}
		gerr = rows.Err()
	}

	require.Error(t, gerr)
	require.Contains(t, gerr.Error(), ParquetNullRowIDMessage,
		"the presence channel must raise our authored message — the one text callers may quote in triage")
}

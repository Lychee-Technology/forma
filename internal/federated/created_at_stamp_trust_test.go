package federated

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// #460 / #256 trust boundary: a manifest stamp that satisfies the parquet
// invariant spares its path the footer probe. A stamp describes the schema an
// object was written with — it does not prove the bytes behind that key still
// have it. So a rogue overwrite (or a tampered manifest) can put an object
// whose real bytes lack ltbase_created_at into a scan the pre-read validator
// waved through, union_by_name NULL-pads the column, and before the
// conditional scan guard a LIVE row was served with a NULL created_at.
//
// This test walks both layers over the same scenario: the validator accepts
// (by design), and the scan guard is what actually catches it.

// writeStampTrustFixtures writes one healthy object and one whose bytes lack
// ltbase_created_at entirely, and returns their paths.
func writeStampTrustFixtures(t *testing.T, db *sql.DB) (healthy, rogue string) {
	t.Helper()
	dir := t.TempDir()
	const rowA = "CAST('018f05c0-0000-7000-8000-0000000000aa' AS UUID) AS row_id"
	const rowB = "CAST('018f05c0-0000-7000-8000-0000000000bb' AS UUID) AS row_id"

	healthy = filepath.Join(dir, "healthy.parquet")
	_, err := db.Exec(fmt.Sprintf(
		"COPY (SELECT %s, CAST(100 AS BIGINT) AS changed_at, CAST(0 AS BIGINT) AS deleted_at, "+
			"CAST(50 AS BIGINT) AS ltbase_created_at, 'alive' AS title) TO '%s' (FORMAT PARQUET)",
		rowA, healthy))
	require.NoError(t, err)

	// The rogue object is LIVE (deleted_at = 0) and simply has no creation
	// stamp column at all — the shape a pre-#460 exporter, a restore from an
	// older backup, or a hand-written object would have.
	rogue = filepath.Join(dir, "rogue.parquet")
	_, err = db.Exec(fmt.Sprintf(
		"COPY (SELECT %s, CAST(200 AS BIGINT) AS changed_at, CAST(0 AS BIGINT) AS deleted_at, "+
			"'rogue' AS title) TO '%s' (FORMAT PARQUET)",
		rowB, rogue))
	require.NoError(t, err)
	return healthy, rogue
}

// TestTrustedStampAdmitsObjectMissingCreatedAt characterizes the boundary
// itself: a stamp satisfying the invariant short-circuits validation, so the
// validator never learns that the object's real bytes lack the column. This
// is deliberate (#256 — a stamp may only short-circuit SUCCESS, and probing
// every object every query is the cost that design bought down), which is
// exactly why the scan guard has to exist.
func TestTrustedStampAdmitsObjectMissingCreatedAt(t *testing.T) {
	const path = "s3://b/7/delta/rogue.parquet"
	exec := &scriptedDescribeExecutor{cols: map[string][][2]string{}}
	v := newParquetSchemaValidator()

	// A stamp that satisfies the invariant — including ltbase_created_at —
	// while the object behind the key does not.
	stamps := map[string]map[string]string{path: stampWith()}

	union, complete, err := v.Validate(context.Background(), exec, []string{path}, stamps)
	require.NoError(t, err, "a stamp satisfying the invariant is trusted by design (#256)")
	require.Empty(t, exec.probes,
		"the trusted stamp spares the footer probe — the validator never sees the real bytes")
	require.True(t, complete)
	require.Contains(t, union, "ltbase_created_at",
		"the union is built from the STAMP, which claims the column the bytes lack")
}

// TestScanGuardCatchesWhatTheTrustedStampAdmitted is the regression proper:
// the object the validator waved through must not be able to serve a live row
// with a NULL created_at. The scan guard fires instead, loudly, naming the
// authored message callers quote in triage.
func TestScanGuardCatchesWhatTheTrustedStampAdmitted(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	healthy, rogue := writeStampTrustFixtures(t, db)
	paths := fmt.Sprintf("['%s', '%s']", healthy, rogue)

	// RED half: the scan the validator would have allowed, unguarded, is
	// silent — the rogue row arrives with no creation stamp at all.
	rows, err := db.Query(fmt.Sprintf(
		"SELECT title, ltbase_created_at FROM read_parquet(%s, union_by_name=true)", paths))
	require.NoError(t, err, "the mixed set binds: the healthy sibling supplies the column")
	var sawNullLiveRow bool
	for rows.Next() {
		var title string
		var created sql.NullInt64
		require.NoError(t, rows.Scan(&title, &created))
		if title == "rogue" && !created.Valid {
			sawNullLiveRow = true
		}
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.True(t, sawNullLiveRow,
		"unguarded, the live rogue row is served with a NULL created_at — the silent path under test")

	// GREEN half: the production scan source refuses it.
	guarded, err := db.Query("SELECT ltbase_created_at FROM " +
		sqlgen.BuildParquetScanSource(paths, nil))
	if err == nil {
		for guarded.Next() {
		}
		err = guarded.Err()
		require.NoError(t, guarded.Close())
	}
	require.Error(t, err,
		"an object admitted by a trusted stamp must still fail the scan when a LIVE row has no creation stamp")
	require.Contains(t, err.Error(), sqlgen.ParquetNullCreatedAtMessage,
		"the guard must raise the authored #460 message, not a bare binder error")
}

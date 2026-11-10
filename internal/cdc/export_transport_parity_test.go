package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// The acceptance gate of #785: the pgx staging must write the Parquet the
// postgres_query transport wrote, for the delta export, the base export and
// the zero-row base export (#519). Each case exports one batch through both
// transports and requires equal DESCRIBE column types and an empty EXCEPT
// ALL in both directions. The fixture also asserts what it covers, so a
// fixture that stopped producing NaN or a tombstone fails instead of passing
// vacuously.
func TestExportTransportParity_PgxStagingMatchesPostgresQuery(t *testing.T) {
	ctx := context.Background()
	fx := newParityFixture(t, ctx)
	cache := parityAttrCache()
	exp, err := NewDuckExporter(ctx, fx.cfg, "", "", "", zaptest.NewLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = exp.DB.Close() })
	legacy := openLegacyPostgresQueryEngine(t, fx.dsn)
	dir := t.TempDir()

	baseIDs := fx.ids[:7] // ids[6] is soft-deleted, which the base export skips
	cases := []struct {
		name     string
		wantRows int
		export   func(out string) error
		plan     func(out string) (exportSQLPlan, error)
	}{
		{
			name:     "delta",
			wantRows: 8,
			export: func(out string) error {
				return exp.ExportSnapshotToTmp(ctx, fx.cfg, fx.db, out, paritySchemaID, paritySnapshot, fx.ids, cache)
			},
			plan: func(out string) (exportSQLPlan, error) {
				return buildDeltaExportPlan(legacyPostgresQueryTransport(fx.ids), out, fx.cfg, paritySchemaID, paritySnapshot, fx.ids, cache)
			},
		},
		{
			name:     "base",
			wantRows: 6,
			export: func(out string) error {
				return exp.ExportBaseFileToTmp(ctx, fx.cfg, fx.db, out, paritySchemaID, baseIDs, cache)
			},
			plan: func(out string) (exportSQLPlan, error) {
				return buildBaseExportPlan(legacyPostgresQueryTransport(baseIDs), out, fx.cfg, paritySchemaID, baseIDs, cache)
			},
		},
		{
			name:     "empty base",
			wantRows: 0,
			export: func(out string) error {
				return exp.ExportBaseFileToTmp(ctx, fx.cfg, fx.db, out, paritySchemaID, nil, cache)
			},
			plan: func(out string) (exportSQLPlan, error) {
				return buildBaseExportPlan(legacyPostgresQueryTransport(nil), out, fx.cfg, paritySchemaID, nil, cache)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := filepath.Join(dir, tc.name+"-pgx.parquet")
			want := filepath.Join(dir, tc.name+"-postgres_query.parquet")
			require.NoError(t, tc.export(got))
			plan, err := tc.plan(want)
			require.NoError(t, err)
			_, err = legacy.ExecContext(ctx, plan.copySQL)
			require.NoError(t, err, "legacy export")

			requireSameParquet(t, exp.DB, got, want, tc.wantRows)
		})
	}

	requireParityCoverage(t, exp.DB, filepath.Join(dir, "delta-pgx.parquet"), fx.ids)
}

// openLegacyPostgresQueryEngine opens a DuckDB with postgres_scanner and the
// fixture's PostgreSQL attached as pg_db, the way the exporter did before
// #785. Without the scanner the gate cannot run: that skips it locally and
// fails it in CI, where the extension cache provides the scanner.
func openLegacyPostgresQueryEngine(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("INSTALL postgres_scanner; LOAD postgres_scanner;"); err != nil {
		if testdb.InCI() {
			t.Fatalf("postgres_scanner unavailable for the #785 transport parity gate: %v", err)
		}
		t.Skipf("postgres_scanner unavailable, the #785 transport parity gate cannot run: %v", err)
	}
	_, err = db.Exec(fmt.Sprintf("ATTACH '%s' AS pg_db (TYPE postgres, READ_ONLY)", sqlutil.EscapeLiteral(dsn)))
	require.NoError(t, err, "attach the fixture PostgreSQL")
	return db
}

// requireSameParquet requires got and want to hold the same columns, in the
// same order and with the same types, and the same multiset of rows.
func requireSameParquet(t *testing.T, db *sql.DB, got, want string, wantRows int) {
	t.Helper()
	require.Equal(t, describeParquet(t, db, want), describeParquet(t, db, got), "column names and types")
	for _, dir := range [][2]string{{got, want}, {want, got}} {
		var extra int
		require.NoError(t, db.QueryRow(fmt.Sprintf(
			"SELECT count(*) FROM (SELECT * FROM read_parquet('%s') EXCEPT ALL SELECT * FROM read_parquet('%s'))",
			sqlutil.EscapeLiteral(dir[0]), sqlutil.EscapeLiteral(dir[1]))).Scan(&extra))
		require.Zero(t, extra, "rows of %s missing from %s", dir[0], dir[1])
	}
	var rows int
	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", sqlutil.EscapeLiteral(got))).Scan(&rows))
	require.Equal(t, wantRows, rows)
}

func describeParquet(t *testing.T, db *sql.DB, path string) []string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('%s'))", sqlutil.EscapeLiteral(path)))
	require.NoError(t, err)
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var name, typ string
		require.NoError(t, rows.Scan(&name, &typ))
		cols = append(cols, name+" "+typ)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, cols)
	return cols
}

// requireParityCoverage pins what the delta fixture exercises, read back from
// the pgx export.
func requireParityCoverage(t *testing.T, db *sql.DB, path string, ids []uuid.UUID) {
	t.Helper()
	src := fmt.Sprintf("read_parquet('%s')", sqlutil.EscapeLiteral(path))
	var nan, posInf, negInf, zeroRatio int
	require.NoError(t, db.QueryRow(`SELECT count(*) FILTER (WHERE isnan(ratio)), count(*) FILTER (WHERE ratio = 'Infinity'::DOUBLE),
		count(*) FILTER (WHERE ratio = '-Infinity'::DOUBLE), count(*) FILTER (WHERE ratio = 0) FROM `+src).Scan(&nan, &posInf, &negInf, &zeroRatio))
	require.Equal(t, []int{1, 2, 1, 1}, []int{nan, posInf, negInf, zeroRatio}, "NaN, +Infinity (one past the double range), -Infinity, an underflow to 0")

	var emptyList, nullList, legacyScalar int
	var ordered string
	require.NoError(t, db.QueryRow(`SELECT count(*) FILTER (WHERE tags = []), count(*) FILTER (WHERE tags IS NULL),
		count(*) FILTER (WHERE nums = [5]), coalesce(max(CAST(nums AS VARCHAR)) FILTER (WHERE row_id = ?), '') FROM `+src, ids[0]).
		Scan(&emptyList, &nullList, &legacyScalar, &ordered))
	require.Equal(t, 1, emptyList, "the empty-list marker exports []")
	require.Positive(t, nullList, "an absent list stays NULL")
	require.Equal(t, 1, legacyScalar, "a legacy scalar row exports as a one-element list")
	require.Equal(t, "[3.0, 1.0, 2.0]", ordered, "list elements follow their array index")

	var tombstones, softDeleted int
	require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT count(*) FILTER (WHERE row_id = '%s' AND deleted_at = 4500 AND ltbase_created_at IS NULL),
		count(*) FILTER (WHERE row_id = '%s' AND ltbase_deleted_at = 4000) FROM %s`, ids[7], ids[6], src)).Scan(&tombstones, &softDeleted))
	require.Equal(t, []int{1, 1}, []int{tombstones, softDeleted}, "the hard-delete tombstone and the soft-deleted row")

	var boolInt, boolText, boolDefault, isoDates int
	require.NoError(t, db.QueryRow(`SELECT count(active_i), count(active_t), count(active_d), count(born) FROM `+src).
		Scan(&boolInt, &boolText, &boolDefault, &isoDates))
	require.Equal(t, []int{3, 3, 3, 2}, []int{boolInt, boolText, boolDefault, isoDates}, "non-NULL bool encodings and parsed rfc3339 dates")
}

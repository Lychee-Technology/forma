package cdc

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// exportPGFixture is one isolated set of change_log/entity_main/eav_data
// tables in a fresh PostgreSQL schema, with one changed row.
type exportPGFixture struct {
	cfg    CDCConfig
	cc     *pgx.ConnConfig
	pgConn string
	rowID  uuid.UUID
}

func newExportPGFixture(t *testing.T, ctx context.Context) exportPGFixture {
	t.Helper()
	pool := testdb.Connect(t, ctx)
	schema := "cdc_export_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	qs := pgx.Identifier{schema}.Sanitize()
	rowID := uuid.New()
	stmts := []string{
		"CREATE SCHEMA " + qs,
		"CREATE TABLE " + qs + ".change_log (schema_id smallint, row_id uuid, flushed_at bigint NOT NULL DEFAULT 0, changed_at bigint, deleted_at bigint)",
		"CREATE TABLE " + qs + ".entity_main (ltbase_schema_id smallint, ltbase_row_id uuid, ltbase_created_at bigint, ltbase_updated_at bigint, ltbase_deleted_at bigint, text_01 text)",
		"CREATE TABLE " + qs + ".eav_data (schema_id smallint, row_id uuid, attr_id smallint, array_indices text NOT NULL DEFAULT '', value_text text, value_numeric numeric)",
		// Every number differs, so a value landing in the wrong column shows.
		fmt.Sprintf("INSERT INTO %s.change_log (schema_id, row_id, changed_at) VALUES (7, '%s', 1111)", qs, rowID),
		fmt.Sprintf("INSERT INTO %s.entity_main VALUES (7, '%s', 2222, 3333, NULL, 'alpha')", qs, rowID),
		fmt.Sprintf("INSERT INTO %s.eav_data (schema_id, row_id, attr_id, value_numeric) VALUES (7, '%s', 11, 1)", qs, rowID),
	}
	for _, stmt := range stmts {
		_, err := pool.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+qs+" CASCADE")
	})

	cc := pool.Config().ConnConfig
	return exportPGFixture{
		cfg: CDCConfig{
			ChangeLogTable:  schema + ".change_log",
			EntityMainTable: schema + ".entity_main",
			EAVDataTable:    schema + ".eav_data",
		},
		cc:     cc,
		pgConn: BuildPGDSN(PGDSNParams{Host: cc.Host, Port: int(cc.Port), User: cc.User, Password: cc.Password, DB: cc.Database, SSLMode: "disable"}),
		rowID:  rowID,
	}
}

// DuckDB 2.0's postgres_query decodes PostgreSQL's columns in its query's
// order even when the planner asks for another one: the change_log join
// reorders row_id first, so the export failed with "Postgres binary reader
// - expected 16 bytes for a value of type UUID, got 2", and same-typed
// columns would swap silently. The export must read every column back
// under its own name (#752).
func TestExportSnapshotToTmp_RealPostgresKeepsEveryColumnInPlace(t *testing.T) {
	ctx := context.Background()
	fx := newExportPGFixture(t, ctx)
	exp, err := NewDuckExporter(ctx, fx.cfg, "", "", "", zaptest.NewLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = exp.DB.Close() })

	out := filepath.Join(t.TempDir(), "delta.parquet")
	require.NoError(t, exp.ExportSnapshotToTmp(ctx, fx.cfg, fx.pgConn, out, 7, 9999, []uuid.UUID{fx.rowID}, testAttrCache()))

	var (
		schemaID             int16
		rowID                uuid.UUID
		changedAt, deletedAt int64
		createdAt, updatedAt int64
		name                 string
		flag                 bool
	)
	require.NoError(t, exp.DB.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT schema_id, row_id, changed_at, deleted_at, ltbase_created_at, ltbase_updated_at, name, flag FROM read_parquet('%s')", out)).
		Scan(&schemaID, &rowID, &changedAt, &deletedAt, &createdAt, &updatedAt, &name, &flag))
	require.Equal(t, int16(7), schemaID)
	require.Equal(t, fx.rowID, rowID)
	require.Equal(t, int64(1111), changedAt)
	require.Equal(t, int64(0), deletedAt)
	require.Equal(t, int64(2222), createdAt)
	require.Equal(t, int64(3333), updatedAt)
	require.Equal(t, "alpha", name)
	require.True(t, flag)
}

// A bad PostgreSQL password must fail the export without the password in
// the error, although DuckDB's attach error quotes the connection string
// (#290, #752).
func TestExportSnapshotToTmp_RealPostgresAttachFailureHidesPassword(t *testing.T) {
	ctx := context.Background()
	fx := newExportPGFixture(t, ctx)
	exp, err := NewDuckExporter(ctx, fx.cfg, "", "", "", zaptest.NewLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = exp.DB.Close() })

	const secret = "not-the-password-752"
	badConn := BuildPGDSN(PGDSNParams{Host: fx.cc.Host, Port: int(fx.cc.Port), User: fx.cc.User, Password: secret, DB: fx.cc.Database, SSLMode: "disable"})
	out := filepath.Join(t.TempDir(), "delta.parquet")
	err = exp.ExportSnapshotToTmp(ctx, fx.cfg, badConn, out, 7, 9999, []uuid.UUID{fx.rowID}, testAttrCache())
	if err == nil {
		t.Skip("the integration PostgreSQL accepts any password, so no attach fails")
	}
	require.NotContains(t, err.Error(), secret)
}

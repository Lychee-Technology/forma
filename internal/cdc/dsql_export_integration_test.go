package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/lychee-technology/forma/internal/testdb"
)

// The CDC tools' PostgreSQL reads on the aurora-dsql test target (#691): the
// batch selection of cdc-flush and cdc-init, and the export's per-batch
// REPEATABLE READ READ ONLY transaction with its `= ANY($1::uuid[])` row-id
// parameter (#785). FORMA_DSQL_DSN names a cluster, and with
// FORMA_DSQL_TARGET=ministack MiniStack's strict proxy
// (docs/dsql-ministack.md). Without either they run on the integration
// PostgreSQL. The tables use DSQL's DDL either way.

const dsqlExportSchemaID int16 = 7

// dsqlExportFixture is one test's tables on the target, through the
// database/sql pool the CDC tools read with.
type dsqlExportFixture struct {
	cfg CDCConfig
	db  *sql.DB
	// live are two rows written and not flushed, gone a row deleted and not
	// flushed, clean a row whose only entry is flushed, and other an
	// unflushed row of another schema.
	live        [2]uuid.UUID
	gone, clean uuid.UUID
	other       uuid.UUID
}

func newDSQLExportFixture(t *testing.T, ctx context.Context) dsqlExportFixture {
	t.Helper()
	pool, _ := testdb.OpenDSQLPool(t, ctx)
	tables := testdb.CreateDSQLStorageTables(t, ctx, pool, "cdc_export_dsql")
	f := dsqlExportFixture{
		cfg: CDCConfig{ChangeLogTable: tables.ChangeLog, EntityMainTable: tables.EntityMain, EAVDataTable: tables.EAVData},
		db:  stdlib.OpenDBFromPool(pool),
	}
	t.Cleanup(func() { _ = f.db.Close() })
	f.live = [2]uuid.UUID{uuid.New(), uuid.New()}
	f.gone, f.clean, f.other = uuid.New(), uuid.New(), uuid.New()

	cl, main, eav := sanitizeIdentifier(tables.ChangeLog), sanitizeIdentifier(tables.EntityMain), sanitizeIdentifier(tables.EAVData)
	type seed struct {
		stmt string
		args []any
	}
	logEntry := "INSERT INTO " + cl + " (schema_id, row_id, flushed_at, changed_at, deleted_at) VALUES ($1, $2, $3, $4, $5)"
	mainRow := "INSERT INTO " + main + " (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, text_01) VALUES ($1, $2, $3, $4, $5)"
	flagRow := "INSERT INTO " + eav + " (schema_id, row_id, attr_id, value_numeric) VALUES ($1, $2, 11, $3)"
	// Every number differs, so a value landing in the wrong column shows.
	seeds := []seed{
		{logEntry, []any{dsqlExportSchemaID, f.live[0], 0, 1111, nil}},
		{mainRow, []any{dsqlExportSchemaID, f.live[0], 2221, 3331, "alpha"}},
		{flagRow, []any{dsqlExportSchemaID, f.live[0], 1}},
		{logEntry, []any{dsqlExportSchemaID, f.live[1], 0, 1112, nil}},
		{mainRow, []any{dsqlExportSchemaID, f.live[1], 2222, 3332, "beta"}},
		{flagRow, []any{dsqlExportSchemaID, f.live[1], 0}},
		{logEntry, []any{dsqlExportSchemaID, f.gone, 0, 1113, 1113}},
		{logEntry, []any{dsqlExportSchemaID, f.clean, 500, 900, nil}},
		{mainRow, []any{dsqlExportSchemaID, f.clean, 2223, 900, "gamma"}},
		{logEntry, []any{dsqlExportSchemaID + 1, f.other, 0, 1114, nil}},
	}
	for _, s := range seeds {
		_, err := pool.Exec(ctx, s.stmt, s.args...)
		require.NoError(t, err, s.stmt)
	}
	return f
}

func (f dsqlExportFixture) exporter(t *testing.T, ctx context.Context) *DuckExporter {
	t.Helper()
	exp, err := NewDuckExporter(ctx, f.cfg, "", "", "", zaptest.NewLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = exp.DB.Close() })
	return exp
}

// exportedRow is one row of an export's parquet file.
type exportedRow struct {
	rowID                uuid.UUID
	changedAt, deletedAt int64
	name                 sql.NullString
	flag                 sql.NullBool
}

func readExport(t *testing.T, exp *DuckExporter, path string) []exportedRow {
	t.Helper()
	rows, err := exp.DB.Query(fmt.Sprintf(
		"SELECT row_id, changed_at, deleted_at, name, flag FROM read_parquet('%s') ORDER BY changed_at, row_id", path))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var out []exportedRow
	for rows.Next() {
		var r exportedRow
		require.NoError(t, rows.Scan(&r.rowID, &r.changedAt, &r.deletedAt, &r.name, &r.flag))
		out = append(out, r)
	}
	require.NoError(t, rows.Err())
	return out
}

func liveName(v string) sql.NullString { return sql.NullString{String: v, Valid: true} }
func liveFlag(v bool) sql.NullBool     { return sql.NullBool{Bool: v, Valid: true} }

// cdc-flush's reads of one batch: the schemas to flush, a schema's backlog,
// the batch and its versions, then the export of exactly the listed rows in
// one snapshot. The deleted row is exported as a tombstone, and the flushed
// row and the other schema's row stay out.
func TestDSQLExportFlushReadsOneBatch(t *testing.T) {
	ctx := context.Background()
	f := newDSQLExportFixture(t, ctx)

	schemas, err := getUnflushedSchemaIDs(ctx, f.db, f.cfg.ChangeLogTable)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{int64(dsqlExportSchemaID), int64(dsqlExportSchemaID + 1)}, schemas)

	count, oldest, err := GetChangeLogStats(ctx, f.db, f.cfg.ChangeLogTable, dsqlExportSchemaID)
	require.NoError(t, err)
	require.Equal(t, int64(3), count, "the unflushed entries of the schema")
	require.Equal(t, int64(1111), oldest)

	ids, versions, snapshot, err := SelectBatchRowIDs(ctx, f.db, f.cfg.ChangeLogTable, dsqlExportSchemaID, 10)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{f.live[0], f.live[1], f.gone}, ids, "listed oldest first")
	require.Equal(t, map[uuid.UUID]int64{f.live[0]: 1111, f.live[1]: 1112, f.gone: 1113}, versions)

	exp := f.exporter(t, ctx)
	out := filepath.Join(t.TempDir(), "delta.parquet")
	require.NoError(t, exp.ExportSnapshotToTmp(ctx, f.cfg, f.db, out, dsqlExportSchemaID, snapshot, ids, testAttrCache()))
	require.Equal(t, []exportedRow{
		{rowID: f.live[0], changedAt: 1111, name: liveName("alpha"), flag: liveFlag(true)},
		{rowID: f.live[1], changedAt: 1112, name: liveName("beta"), flag: liveFlag(false)},
		{rowID: f.gone, changedAt: 1113, deletedAt: 1113},
	}, readExport(t, exp, out))
	requireNoPostgresInDuckDB(t, exp.DB)

	// The row-id parameter selects: a batch of one row exports that row.
	one := filepath.Join(t.TempDir(), "one.parquet")
	require.NoError(t, exp.ExportSnapshotToTmp(ctx, f.cfg, f.db, one, dsqlExportSchemaID, snapshot, []uuid.UUID{f.live[1]}, testAttrCache()))
	require.Equal(t, []exportedRow{{rowID: f.live[1], changedAt: 1112, name: liveName("beta"), flag: liveFlag(false)}}, readExport(t, exp, one))
}

// cdc-init's reads: the schema's live row count, its keyset batches by row
// id, and the base export of a batch, which reads entity_main without
// change_log.
func TestDSQLExportInitReadsItsBatches(t *testing.T) {
	ctx := context.Background()
	f := newDSQLExportFixture(t, ctx)

	count, err := getEntityMainCount(ctx, f.db, f.cfg.EntityMainTable, dsqlExportSchemaID)
	require.NoError(t, err)
	require.Equal(t, int64(3), count)

	want := sortedUUIDs(f.live[0], f.live[1], f.clean)
	first, err := selectEntityMainBatch(ctx, f.db, f.cfg.EntityMainTable, dsqlExportSchemaID, nil, 2)
	require.NoError(t, err)
	require.Equal(t, want[:2], first)
	rest, err := selectEntityMainBatch(ctx, f.db, f.cfg.EntityMainTable, dsqlExportSchemaID, &first[1], 2)
	require.NoError(t, err)
	require.Equal(t, want[2:], rest)

	exp := f.exporter(t, ctx)
	out := filepath.Join(t.TempDir(), "base.parquet")
	require.NoError(t, exp.ExportBaseFileToTmp(ctx, f.cfg, f.db, out, dsqlExportSchemaID, want, testAttrCache()))
	// A base row's changed_at is its ltbase_updated_at (#210).
	require.Equal(t, []exportedRow{
		{rowID: f.clean, changedAt: 900, name: liveName("gamma")},
		{rowID: f.live[0], changedAt: 3331, name: liveName("alpha"), flag: liveFlag(true)},
		{rowID: f.live[1], changedAt: 3332, name: liveName("beta"), flag: liveFlag(false)},
	}, readExport(t, exp, out))
	requireNoPostgresInDuckDB(t, exp.DB)
}

func sortedUUIDs(ids ...uuid.UUID) []uuid.UUID {
	out := append([]uuid.UUID(nil), ids...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].String() < out[j-1].String(); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

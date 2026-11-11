package federated

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/stretchr/testify/require"
)

// snapshotTablesFor creates a change_log and an entity_main of the init-db
// shape for one test and answers their names.
func snapshotTablesFor(t *testing.T, ctx context.Context, pool *pgxpool.Pool) model.StorageTables {
	t.Helper()
	prefix := "snap_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	tables := model.StorageTables{ChangeLog: prefix + "_change_log", EntityMain: prefix + "_entity_main"}
	cl, main := pgx.Identifier{tables.ChangeLog}.Sanitize(), pgx.Identifier{tables.EntityMain}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (schema_id SMALLINT NOT NULL, row_id UUID NOT NULL, flushed_at BIGINT NOT NULL DEFAULT 0,
			changed_at BIGINT NOT NULL, deleted_at BIGINT, PRIMARY KEY (schema_id, row_id, flushed_at));
		CREATE TABLE %s (ltbase_schema_id SMALLINT NOT NULL, ltbase_row_id UUID NOT NULL,
			PRIMARY KEY (ltbase_schema_id, ltbase_row_id))`, cl, main))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+cl+", "+main) })
	return tables
}

// The pgx hot leg's reads see one snapshot (design.md §6.1.1): an entry
// committed, or marked flushed, after the snapshot opened reaches neither
// the dirty set nor the parquet-history answer read in it, while a new
// snapshot sees both. The transaction refuses writes.
func TestChangeLogSnapshotReadsOneSnapshot(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Connect(t, ctx)
	tables := snapshotTablesFor(t, ctx, pool)
	cl := pgx.Identifier{tables.ChangeLog}.Sanitize()
	first, second := uuid.New(), uuid.New()
	_, err := pool.Exec(ctx, "INSERT INTO "+cl+" (schema_id, row_id, changed_at) VALUES (100, $1, 1)", first)
	require.NoError(t, err)

	req := ChangeLogSnapshotRequest{Tables: tables, SchemaID: 100, Form: DirtySetHot, GraceCutoffMs: 1_000, ParquetHistory: true}
	tx, err := beginReadSnapshot(ctx, pool)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	before, err := readChangeLogSnapshot(ctx, tx, req)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{first}, before.DirtyIDs)
	require.False(t, before.HasParquetHistory, "every entry is unflushed and entity_main is empty")

	// A writer and a flush commit while the snapshot is open: a new row, and
	// the first row's entry marked below the cutoff, so it leaves the hot set.
	_, err = pool.Exec(ctx, "INSERT INTO "+cl+" (schema_id, row_id, changed_at) VALUES (100, $1, 2)", second)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "UPDATE "+cl+" SET flushed_at = 500 WHERE row_id = $1", first)
	require.NoError(t, err)

	during, err := readChangeLogSnapshot(ctx, tx, req)
	require.NoError(t, err)
	require.Equal(t, before, during, "a read in the open snapshot must not see later commits")

	_, err = tx.Exec(ctx, "INSERT INTO "+cl+" (schema_id, row_id, changed_at) VALUES (100, $1, 3)", uuid.New())
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "a write in the snapshot must fail, got %v", err)
	require.Equal(t, "25006", pgErr.Code, "read_only_sql_transaction")

	after, err := NewPostgresDirtyIDFetcher(pool).ReadChangeLogSnapshot(ctx, req)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{second}, after.DirtyIDs)
	require.True(t, after.HasParquetHistory, "the mark is a flushed entry")
}

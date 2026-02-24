//go:build integration

package cdc

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/e2e_harness"
	"github.com/stretchr/testify/require"
)

func TestMarkFlushedIDsAtSnapshot_SkipsUpdatedRows(t *testing.T) {
	ctx := context.Background()
	h := &e2e_harness.TestHarness{}
	_, err := h.StartPostgres(ctx)
	require.NoError(t, err)
	defer func() { _ = h.StopPostgres(ctx) }()

	db := h.PGDB
	require.NoError(t, createChangeLogTable(ctx, db))

	rowID := uuid.New()
	snapshot1 := dbNowMs(t, db)

	_, err = db.ExecContext(ctx, `
		INSERT INTO change_log (schema_id, row_id, flushed_at, changed_at, deleted_at)
		VALUES ($1, $2, 0, $3, NULL)
	`, int16(1), rowID, snapshot1)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		UPDATE change_log SET changed_at = $1
		WHERE schema_id = $2 AND row_id = $3 AND flushed_at = 0
	`, snapshot1+1000, int16(1), rowID)
	require.NoError(t, err)

	updated, err := MarkFlushedIDsAtSnapshot(ctx, db, "change_log", 1, []uuid.UUID{rowID}, snapshot1, snapshot1+2000)
	require.NoError(t, err)
	require.Len(t, updated, 0)

	var changedAt, flushedAt int64
	err = db.QueryRowContext(ctx, `
		SELECT changed_at, flushed_at
		FROM change_log
		WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0
	`, int16(1), rowID).Scan(&changedAt, &flushedAt)
	require.NoError(t, err)
	require.Greater(t, changedAt, snapshot1)
	require.Equal(t, int64(0), flushedAt)

	snapshot2 := snapshot1 + 2000
	updated, err = MarkFlushedIDsAtSnapshot(ctx, db, "change_log", 1, []uuid.UUID{rowID}, snapshot2, snapshot2+1000)
	require.NoError(t, err)
	require.Len(t, updated, 1)
	require.Equal(t, rowID, updated[0])

	err = db.QueryRowContext(ctx, `
		SELECT flushed_at
		FROM change_log
		WHERE schema_id = $1 AND row_id = $2
	`, int16(1), rowID).Scan(&flushedAt)
	require.NoError(t, err)
	require.Greater(t, flushedAt, int64(0))
}

func createChangeLogTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS change_log (
			schema_id  SMALLINT NOT NULL,
			row_id     UUID     NOT NULL,
			flushed_at BIGINT   NOT NULL DEFAULT 0,
			changed_at BIGINT   NOT NULL,
			deleted_at BIGINT,
			PRIMARY KEY (schema_id, row_id, flushed_at)
		)
	`)
	return err
}

func dbNowMs(t *testing.T, db *sql.DB) int64 {
	var ts int64
	err := db.QueryRow("SELECT (extract(epoch from clock_timestamp())*1000)::bigint").Scan(&ts)
	require.NoError(t, err)
	return ts
}

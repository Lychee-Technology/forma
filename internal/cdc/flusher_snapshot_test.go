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

// TestMarkFlushedVersions_SkipsAdvancedRows pins the per-listed-version mark
// contract: a row whose slot-0 changed_at advanced past its listed version
// stays dirty (its export copy, if any, is superseded), and re-listing at
// the advanced version marks it.
func TestMarkFlushedVersions_SkipsAdvancedRows(t *testing.T) {
	ctx := context.Background()
	h := &e2e_harness.TestHarness{}
	_, err := h.StartPostgres(ctx)
	require.NoError(t, err)
	defer func() { _ = h.StopPostgres(ctx) }()

	db := h.PGDB
	require.NoError(t, createChangeLogTable(ctx, db))

	rowID := uuid.New()
	listed := dbNowMs(t, db)

	_, err = db.ExecContext(ctx, `
		INSERT INTO change_log (schema_id, row_id, flushed_at, changed_at, deleted_at)
		VALUES ($1, $2, 0, $3, NULL)
	`, int16(1), rowID, listed)
	require.NoError(t, err)

	// The row is concurrently rewritten after listing: versions only advance
	// (#274 monotonic ordering), so the listed pair no longer matches.
	_, err = db.ExecContext(ctx, `
		UPDATE change_log SET changed_at = $1
		WHERE schema_id = $2 AND row_id = $3 AND flushed_at = 0
	`, listed+1000, int16(1), rowID)
	require.NoError(t, err)

	updated, err := MarkFlushedVersions(ctx, db, "change_log", 1, []uuid.UUID{rowID}, map[uuid.UUID]int64{rowID: listed}, listed+2000)
	require.NoError(t, err)
	require.Len(t, updated, 0)

	var changedAt, flushedAt int64
	err = db.QueryRowContext(ctx, `
		SELECT changed_at, flushed_at
		FROM change_log
		WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0
	`, int16(1), rowID).Scan(&changedAt, &flushedAt)
	require.NoError(t, err)
	require.Greater(t, changedAt, listed)
	require.Equal(t, int64(0), flushedAt)

	// Re-listed at the advanced version, the mark takes.
	updated, err = MarkFlushedVersions(ctx, db, "change_log", 1, []uuid.UUID{rowID}, map[uuid.UUID]int64{rowID: listed + 1000}, listed+3000)
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

// TestMarkFlushedVersions_MarksClockAheadVersions pins the round-2 P1 fix:
// a listed version AHEAD of the wall clock (#274 monotonic versions can
// outrun it) marks normally — eligibility follows the listed version, never
// a wall-clock cutoff, so a clock-ahead row cannot starve CDC.
func TestMarkFlushedVersions_MarksClockAheadVersions(t *testing.T) {
	ctx := context.Background()
	h := &e2e_harness.TestHarness{}
	_, err := h.StartPostgres(ctx)
	require.NoError(t, err)
	defer func() { _ = h.StopPostgres(ctx) }()

	db := h.PGDB
	require.NoError(t, createChangeLogTable(ctx, db))

	rowID := uuid.New()
	future := dbNowMs(t, db) + 3_600_000 // one hour ahead of the wall clock

	_, err = db.ExecContext(ctx, `
		INSERT INTO change_log (schema_id, row_id, flushed_at, changed_at, deleted_at)
		VALUES ($1, $2, 0, $3, NULL)
	`, int16(1), rowID, future)
	require.NoError(t, err)

	updated, err := MarkFlushedVersions(ctx, db, "change_log", 1, []uuid.UUID{rowID}, map[uuid.UUID]int64{rowID: future}, dbNowMs(t, db))
	require.NoError(t, err)
	require.Len(t, updated, 1)
	require.Equal(t, rowID, updated[0])

	var flushedAt int64
	err = db.QueryRowContext(ctx, `
		SELECT flushed_at FROM change_log WHERE schema_id = $1 AND row_id = $2
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

// TestMarkFlushedVersions_SkipsRegressedRows codifies the round-3 review
// probe: marking is EQUALITY against the listed version, so a slot whose
// changed_at moved in EITHER direction stays dirty. A recreate overwrites
// slot-0 and (before the #274 create ordering) could land BELOW a
// clock-ahead tombstone's listing — a <= predicate would have marked it
// flushed with its payload in no parquet file.
func TestMarkFlushedVersions_SkipsRegressedRows(t *testing.T) {
	ctx := context.Background()
	h := &e2e_harness.TestHarness{}
	_, err := h.StartPostgres(ctx)
	require.NoError(t, err)
	defer func() { _ = h.StopPostgres(ctx) }()

	db := h.PGDB
	require.NoError(t, createChangeLogTable(ctx, db))

	rowID := uuid.New()
	listed := dbNowMs(t, db)

	_, err = db.ExecContext(ctx, `
		INSERT INTO change_log (schema_id, row_id, flushed_at, changed_at, deleted_at)
		VALUES ($1, $2, 0, $3, NULL)
	`, int16(1), rowID, listed)
	require.NoError(t, err)

	// The slot regresses below its listing (the recreate-overwrites-slot
	// shape the round-3 probe demonstrated).
	_, err = db.ExecContext(ctx, `
		UPDATE change_log SET changed_at = $1
		WHERE schema_id = $2 AND row_id = $3 AND flushed_at = 0
	`, listed-1000, int16(1), rowID)
	require.NoError(t, err)

	updated, err := MarkFlushedVersions(ctx, db, "change_log", 1, []uuid.UUID{rowID}, map[uuid.UUID]int64{rowID: listed}, listed+2000)
	require.NoError(t, err)
	require.Len(t, updated, 0, "a regressed slot must stay dirty — its payload is in no exported parquet")

	var flushedAt int64
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT flushed_at FROM change_log WHERE schema_id = $1 AND row_id = $2
	`, int16(1), rowID).Scan(&flushedAt))
	require.Equal(t, int64(0), flushedAt)
}

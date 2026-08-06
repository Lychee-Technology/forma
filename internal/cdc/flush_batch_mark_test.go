package cdc

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Mark-flushed and before-export-hook coverage for the flush batch
// executor, split from flush_batch_test.go to keep both files under the
// 500-line source limit; the #274 listed-version mark seam pins live here.

func TestExecuteBatch_MarkStampSampledAfterManifestAppend(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE change_log (schema_id SMALLINT, row_id UUID, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)
	rowID := uuid.MustParse("018f05c0-0000-7000-8000-000000000001")
	snapshot := time.Now().UnixMilli()
	_, err = db.ExecContext(ctx, "INSERT INTO change_log VALUES (7, ?, ?, 0)", rowID, snapshot-1000)
	require.NoError(t, err)

	store := newInMemoryManifestStore()
	store.saveDelay = 5 * time.Millisecond
	resolver := manifest.PathResolver{Prefix: "cdc", PathTemplate: "manifest/{{.SchemaID}}.json"}

	executor := &flushBatchExecutor{
		db:               db,
		duck:             &DuckExporter{Logger: zap.NewNop()},
		s3Client:         &objectOnlyS3Client{},
		cfg:              CDCConfig{S3Bucket: "test-bucket", S3Prefix: "cdc"},
		tableName:        "change_log",
		schemaID:         7,
		snapshot:         snapshot,
		versions:         map[uuid.UUID]int64{rowID: snapshot - 1000},
		pgConnForDuck:    "host=pg port=5432 user=pguser password=secret dbname=forma sslmode=disable",
		logger:           zap.NewNop(),
		manifestStore:    store,
		manifestResolver: resolver,
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			return nil
		},
	}

	require.NoError(t, executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "single"))

	var flushedAt int64
	err = db.QueryRowContext(ctx, "SELECT flushed_at FROM change_log WHERE schema_id = 7 AND row_id = ?", rowID).Scan(&flushedAt)
	require.NoError(t, err)
	require.GreaterOrEqual(t, flushedAt, store.lastSaveDoneMs,
		"flushed_at must be sampled after the manifest append completes")
}

// TestExecuteBatch_MarkFailsAfterManifestAppend pins the #252 failure
// contract's second leg: mark-flushed failing after a successful append
// leaves a LISTED delta with dirty rows. The retry then produces a second
// listed delta whose duplicate (row_id, ver_ts) copies are LWW-deduped at
// read time — the write path must only propagate the error, not attempt to
// roll the manifest entry back.
func TestExecuteBatch_MarkFailsAfterManifestAppend(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE change_log (schema_id SMALLINT, row_id UUID, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)
	rowID := uuid.MustParse("018f05c0-0000-7000-8000-000000000001")
	snapshot := time.Now().UnixMilli()
	_, err = db.ExecContext(ctx, "INSERT INTO change_log VALUES (7, ?, ?, 0)", rowID, snapshot-1000)
	require.NoError(t, err)

	store := newInMemoryManifestStore()
	resolver := manifest.PathResolver{Prefix: "cdc", PathTemplate: "manifest/{{.SchemaID}}.json"}
	markErr := errors.New("pg connection reset")

	executor := &flushBatchExecutor{
		db:               db,
		duck:             &DuckExporter{Logger: zap.NewNop()},
		s3Client:         &objectOnlyS3Client{},
		cfg:              CDCConfig{S3Bucket: "test-bucket", S3Prefix: "cdc"},
		tableName:        "change_log",
		schemaID:         7,
		snapshot:         snapshot,
		versions:         map[uuid.UUID]int64{rowID: snapshot - 1000},
		pgConnForDuck:    "host=pg port=5432 user=pguser password=secret dbname=forma sslmode=disable",
		logger:           zap.NewNop(),
		manifestStore:    store,
		manifestResolver: resolver,
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			return nil
		},
		markFlushed: func(context.Context, *sql.DB, string, int16, []uuid.UUID, map[uuid.UUID]int64, int64) ([]uuid.UUID, error) {
			return nil, markErr
		},
	}

	err = executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "single")
	require.Error(t, err)
	require.ErrorIs(t, err, markErr)
	require.Contains(t, err.Error(), "mark flushed at listed versions")

	// The manifest entry stays: the delta is listed while the rows stay dirty.
	require.Equal(t, 1, store.saved)
	var flushedAt int64
	err = db.QueryRowContext(ctx, "SELECT flushed_at FROM change_log WHERE schema_id = 7 AND row_id = ?", rowID).Scan(&flushedAt)
	require.NoError(t, err)
	require.Zero(t, flushedAt)
}

// TestExecuteBatch_BeforeExportHookRunsBeforeExport pins the #182 seam: the
// hook fires before the DuckDB export (the selection snapshot is already
// fixed on the executor) and receives the batch's schema, row IDs, and
// snapshot, so a test can mutate rows inside the selection->export window.
func TestExecuteBatch_BeforeExportHookRunsBeforeExport(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE change_log (schema_id SMALLINT, row_id UUID, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)
	rowID := uuid.MustParse("018f05c0-0000-7000-8000-000000000001")
	snapshot := time.Now().UnixMilli()
	_, err = db.ExecContext(ctx, "INSERT INTO change_log VALUES (7, ?, ?, 0)", rowID, snapshot-1000)
	require.NoError(t, err)

	var calls []string
	var hookSchemaID int16
	var hookIDs []uuid.UUID
	var hookSnapshot int64
	executor := &flushBatchExecutor{
		db:       db,
		duck:     &DuckExporter{Logger: zap.NewNop()},
		s3Client: &objectOnlyS3Client{},
		cfg: CDCConfig{S3Bucket: "test-bucket", S3Prefix: "cdc",
			BeforeExportHook: func(_ context.Context, schemaID int16, ids []uuid.UUID, snap int64) error {
				calls = append(calls, "hook")
				hookSchemaID, hookIDs, hookSnapshot = schemaID, ids, snap
				return nil
			}},
		tableName:        "change_log",
		schemaID:         7,
		snapshot:         snapshot,
		versions:         map[uuid.UUID]int64{rowID: snapshot - 1000},
		pgConnForDuck:    "host=pg port=5432 user=pguser password=secret dbname=forma sslmode=disable",
		logger:           zap.NewNop(),
		manifestStore:    newInMemoryManifestStore(),
		manifestResolver: manifest.PathResolver{Prefix: "cdc", PathTemplate: "manifest/{{.SchemaID}}.json"},
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			calls = append(calls, "export")
			return nil
		},
	}

	err = executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "single")
	require.NoError(t, err)
	require.Equal(t, []string{"hook", "export"}, calls)
	require.Equal(t, int16(7), hookSchemaID)
	require.Equal(t, []uuid.UUID{rowID}, hookIDs)
	require.Equal(t, snapshot, hookSnapshot)
}

// TestExecuteBatch_BeforeExportHookErrorAbortsBeforeSideEffects pins the
// hook's failure contract: an error aborts the batch before any side effect
// (no export, no flushed_at change, no manifest save).
func TestExecuteBatch_BeforeExportHookErrorAbortsBeforeSideEffects(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE change_log (schema_id SMALLINT, row_id UUID, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)
	rowID := uuid.MustParse("018f05c0-0000-7000-8000-000000000001")
	snapshot := time.Now().UnixMilli()
	_, err = db.ExecContext(ctx, "INSERT INTO change_log VALUES (7, ?, ?, 0)", rowID, snapshot-1000)
	require.NoError(t, err)

	store := newInMemoryManifestStore()
	hookErr := errors.New("hook failed")
	exportCalled := false
	executor := &flushBatchExecutor{
		db:       db,
		duck:     &DuckExporter{Logger: zap.NewNop()},
		s3Client: &objectOnlyS3Client{},
		cfg: CDCConfig{S3Bucket: "test-bucket", S3Prefix: "cdc",
			BeforeExportHook: func(context.Context, int16, []uuid.UUID, int64) error {
				return hookErr
			}},
		tableName:     "change_log",
		schemaID:      7,
		snapshot:      snapshot,
		versions:      map[uuid.UUID]int64{rowID: snapshot - 1000},
		pgConnForDuck: "host=pg port=5432 user=pguser password=secret dbname=forma sslmode=disable",
		logger:        zap.NewNop(),
		manifestStore: store,
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			exportCalled = true
			return nil
		},
	}

	err = executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "single")
	require.Error(t, err)
	require.ErrorIs(t, err, hookErr)
	require.Contains(t, err.Error(), "before-export hook")
	require.False(t, exportCalled, "hook failure must abort before the export")

	var flushedAt int64
	err = db.QueryRowContext(ctx, "SELECT flushed_at FROM change_log WHERE schema_id = 7 AND row_id = ?", rowID).Scan(&flushedAt)
	require.NoError(t, err)
	require.Zero(t, flushedAt, "hook failure must not mark rows flushed")
	require.Zero(t, store.saved, "hook failure must not save the manifest")
}

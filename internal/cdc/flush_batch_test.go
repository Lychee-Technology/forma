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

func TestExecuteFlush_SplitsBatchWhenByteTargetIsExceeded(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE change_log (schema_id SMALLINT, row_id UUID, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)
	rows := []struct {
		id        uuid.UUID
		changedAt int64
	}{
		{uuid.MustParse("018f05c0-0000-7000-8000-000000000001"), time.Now().Add(-3 * time.Minute).UnixMilli()},
		{uuid.MustParse("018f05c0-0001-7000-8000-000000000001"), time.Now().Add(-2 * time.Minute).UnixMilli()},
		{uuid.MustParse("018f05c0-0002-7000-8000-000000000001"), time.Now().Add(-1 * time.Minute).UnixMilli()},
	}
	for _, row := range rows {
		_, err = db.ExecContext(ctx, "INSERT INTO change_log VALUES (7, ?, ?, 0)", row.id, row.changedAt)
		require.NoError(t, err)
	}

	chunkCalled := false
	flushCtx := &schemaFlushContext{
		db:         db,
		cfg:        CDCConfig{BatchSize: 10, MaxBatchBytes: 10, EstimatedRowBytes: 10, PGHost: "localhost", PGPort: 5432, PGUser: "pguser", PGDB: "forma"},
		tableName:  "change_log",
		pgPassword: "secret",
		logger:     zap.NewNop(),
		executeSingle: func(*flushBatchExecutor, []uuid.UUID) error {
			return errors.New("unexpected single execution")
		},
		executeInChunks: func(executor *flushBatchExecutor, batchIDs []uuid.UUID, maxRows int) error {
			chunkCalled = true
			require.Equal(t, 1, maxRows)
			require.Len(t, batchIDs, 3)
			require.Equal(t, int16(7), executor.schemaID)
			return nil
		},
	}

	err = flushCtx.executeFlush(ctx, 7)
	require.NoError(t, err)
	require.True(t, chunkCalled)
}

// executeBatch must not swallow manifest failures: a delta file absent from
// the manifest is invisible to manifest consumers (e.g. compaction) while
// the run would otherwise report success. Under the #252 ordering the
// manifest append runs BEFORE mark-flushed, so an append failure leaves the
// rows dirty: the retry re-selects them and self-heals with a fresh key,
// and the old copied final is an unlisted orphan for manifest-reconcile
// --gc. The error still carries the final key for observability.
func TestExecuteBatch_ReturnsErrorWhenManifestLoadFails(t *testing.T) {
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
	store.loadErr = errors.New("boom")
	resolver := manifest.PathResolver{Prefix: "cdc", PathTemplate: "manifest/{{.SchemaID}}.json"}

	executor := &flushBatchExecutor{
		db:               db,
		duck:             &DuckExporter{Logger: zap.NewNop()},
		s3Client:         &objectOnlyS3Client{},
		cfg:              CDCConfig{S3Bucket: "test-bucket", S3Prefix: "cdc"},
		tableName:        "change_log",
		schemaID:         7,
		snapshot:         snapshot,
		pgConnForDuck:    "host=pg port=5432 user=pguser password=secret dbname=forma sslmode=disable",
		logger:           zap.NewNop(),
		manifestStore:    store,
		manifestResolver: resolver,
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			return nil
		},
	}

	err = executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "single")
	require.Error(t, err)
	require.Contains(t, err.Error(), "manifest update")
	require.Contains(t, err.Error(), "cdc/7/delta-file.parquet")

	// #252: mark-flushed runs after the append, so the failed append leaves
	// the rows dirty — the retry re-selects and re-exports them.
	var flushedAt int64
	err = db.QueryRowContext(ctx, "SELECT flushed_at FROM change_log WHERE schema_id = 7 AND row_id = ?", rowID).Scan(&flushedAt)
	require.NoError(t, err)
	require.Zero(t, flushedAt)
	require.Zero(t, store.saved)
}

// Save-failure variant, mirroring init_test.go's failingSaveStore coverage:
// Load reports NoSuchKey (create path), Save fails inside AppendFile.
func TestExecuteBatch_ReturnsErrorWhenManifestSaveFails(t *testing.T) {
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

	saveErr := errors.New("s3 write denied")
	store := newInMemoryManifestStore()
	store.saveErr = saveErr
	resolver := manifest.PathResolver{Prefix: "cdc", PathTemplate: "manifest/{{.SchemaID}}.json"}

	executor := &flushBatchExecutor{
		db:               db,
		duck:             &DuckExporter{Logger: zap.NewNop()},
		s3Client:         &objectOnlyS3Client{},
		cfg:              CDCConfig{S3Bucket: "test-bucket", S3Prefix: "cdc"},
		tableName:        "change_log",
		schemaID:         7,
		snapshot:         snapshot,
		pgConnForDuck:    "host=pg port=5432 user=pguser password=secret dbname=forma sslmode=disable",
		logger:           zap.NewNop(),
		manifestStore:    store,
		manifestResolver: resolver,
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			return nil
		},
	}

	err = executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "single")
	require.Error(t, err)
	require.ErrorIs(t, err, saveErr)
	require.Contains(t, err.Error(), "cdc/7/delta-file.parquet")

	// #252: the failed save precedes mark-flushed, so the rows stay dirty.
	var flushedAt int64
	err = db.QueryRowContext(ctx, "SELECT flushed_at FROM change_log WHERE schema_id = 7 AND row_id = ?", rowID).Scan(&flushedAt)
	require.NoError(t, err)
	require.Zero(t, flushedAt)
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
		pgConnForDuck:    "host=pg port=5432 user=pguser password=secret dbname=forma sslmode=disable",
		logger:           zap.NewNop(),
		manifestStore:    store,
		manifestResolver: resolver,
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			return nil
		},
		markFlushed: func(context.Context, *sql.DB, string, int16, []uuid.UUID, int64, int64) ([]uuid.UUID, error) {
			return nil, markErr
		},
	}

	err = executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "single")
	require.Error(t, err)
	require.ErrorIs(t, err, markErr)
	require.Contains(t, err.Error(), "mark flushed at snapshot")

	// The manifest entry stays: the delta is listed while the rows stay dirty.
	require.Equal(t, 1, store.saved)
	var flushedAt int64
	err = db.QueryRowContext(ctx, "SELECT flushed_at FROM change_log WHERE schema_id = 7 AND row_id = ?", rowID).Scan(&flushedAt)
	require.NoError(t, err)
	require.Zero(t, flushedAt)
}

func TestExecuteBatch_ReturnsErrorWhenExportFailsAndDoesNotAdvanceState(t *testing.T) {
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
	s3Client := &recordingS3Client{}
	executor := &flushBatchExecutor{
		db:            db,
		duck:          &DuckExporter{Logger: zap.NewNop()},
		s3Client:      s3Client,
		cfg:           CDCConfig{S3Bucket: "test-bucket", S3Prefix: "cdc"},
		tableName:     "change_log",
		schemaID:      7,
		snapshot:      snapshot,
		pgConnForDuck: "host=pg port=5432 user=pguser password=secret dbname=forma sslmode=disable",
		logger:        zap.NewNop(),
		manifestStore: store,
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			return errors.New("export failed")
		},
	}

	err = executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "single")
	require.Error(t, err)
	require.Contains(t, err.Error(), "duck export snapshot")

	// #226: DuckDB may have written the tmp object before failing and the
	// retry uses a fresh UUID, so the batch must best-effort delete its tmp.
	require.Equal(t, []string{"cdc/7/_tmp/file.parquet"}, s3Client.deletedKeys)

	var flushedAt int64
	err = db.QueryRowContext(ctx, "SELECT flushed_at FROM change_log WHERE schema_id = 7 AND row_id = ?", rowID).Scan(&flushedAt)
	require.NoError(t, err)
	require.Zero(t, flushedAt)
	require.Zero(t, store.saved)
}

func TestExecuteBatch_ReturnsErrorWhenCopyFailsAndDoesNotAdvanceState(t *testing.T) {
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
	executor := &flushBatchExecutor{
		db:            db,
		duck:          &DuckExporter{Logger: zap.NewNop()},
		s3Client:      &copyFailingS3Client{},
		cfg:           CDCConfig{S3Bucket: "test-bucket", S3Prefix: "cdc"},
		tableName:     "change_log",
		schemaID:      7,
		snapshot:      snapshot,
		pgConnForDuck: "host=pg port=5432 user=pguser password=secret dbname=forma sslmode=disable",
		logger:        zap.NewNop(),
		manifestStore: store,
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			return nil
		},
	}

	err = executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "single")
	require.Error(t, err)
	require.Contains(t, err.Error(), "copy tmp to final")

	var flushedAt int64
	err = db.QueryRowContext(ctx, "SELECT flushed_at FROM change_log WHERE schema_id = 7 AND row_id = ?", rowID).Scan(&flushedAt)
	require.NoError(t, err)
	require.Zero(t, flushedAt)
	require.Zero(t, store.saved)
}

// Dry-run must be decided before the first side effect: no DuckDB export
// (which itself writes a _tmp S3 object via httpfs), no tmp->final copy, no
// mark-flushed, and no manifest access (#180).
func TestExecuteBatch_DryRunPerformsNoSideEffects(t *testing.T) {
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
	exportCalled := false
	executor := &flushBatchExecutor{
		db:               db,
		duck:             &DuckExporter{Logger: zap.NewNop()},
		s3Client:         &copyFailingS3Client{}, // any S3 call during dry-run fails the batch
		cfg:              CDCConfig{S3Bucket: "test-bucket", S3Prefix: "cdc"},
		tableName:        "change_log",
		schemaID:         7,
		snapshot:         snapshot,
		pgConnForDuck:    "host=pg port=5432 user=pguser password=secret dbname=forma sslmode=disable",
		dryRun:           true,
		logger:           zap.NewNop(),
		manifestStore:    store,
		manifestResolver: manifest.PathResolver{Prefix: "cdc", PathTemplate: "manifest/{{.SchemaID}}.json"},
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			exportCalled = true
			return nil
		},
	}

	err = executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "single")
	require.NoError(t, err)
	require.False(t, exportCalled, "dry-run must not run the DuckDB export")

	var flushedAt int64
	err = db.QueryRowContext(ctx, "SELECT flushed_at FROM change_log WHERE schema_id = 7 AND row_id = ?", rowID).Scan(&flushedAt)
	require.NoError(t, err)
	require.Zero(t, flushedAt, "dry-run must not mark rows flushed")
	require.Zero(t, store.saved, "dry-run must not save the manifest")
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

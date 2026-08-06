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
		versions:         map[uuid.UUID]int64{rowID: snapshot - 1000},
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
		versions:         map[uuid.UUID]int64{rowID: snapshot - 1000},
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

// TestExecuteBatch_MarkStampSampledAfterManifestAppend pins the #252 review-P1
// fix: the flushed_at stamp must be sampled AFTER the manifest append
// completes. A reader that anchored its cutoff and resolved its path set
// while the append was in flight has cutoff >= append-start; sampling the
// stamp before the append could write flushed_at < cutoff and hide the rows
// from the widened barrier. The store's Save is slowed so a pre-append stamp
// is strictly older than the append completion at millisecond resolution.

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
		versions:      map[uuid.UUID]int64{rowID: snapshot - 1000},
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
		versions:      map[uuid.UUID]int64{rowID: snapshot - 1000},
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
		versions:         map[uuid.UUID]int64{rowID: snapshot - 1000},
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

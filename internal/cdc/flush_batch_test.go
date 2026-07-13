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
// the run would otherwise report success. Rows are already marked flushed
// by this point, so the flush state persists even though the pass fails —
// the returned error carries the final key for manual reconciliation.
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

	// The flush itself is durable: rows stay marked flushed even though the
	// pass reports failure. A re-run will not re-export them.
	var flushedAt int64
	err = db.QueryRowContext(ctx, "SELECT flushed_at FROM change_log WHERE schema_id = 7 AND row_id = ?", rowID).Scan(&flushedAt)
	require.NoError(t, err)
	require.NotZero(t, flushedAt)
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
	executor := &flushBatchExecutor{
		db:            db,
		duck:          &DuckExporter{Logger: zap.NewNop()},
		s3Client:      &objectOnlyS3Client{},
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

func TestUpdateManifest_NilStore(t *testing.T) {
	rowID := uuid.MustParse("018f05c0-0000-7000-8000-000000000001")
	err := updateManifest(
		context.Background(),
		nil,
		manifest.PathResolver{},
		1,
		"cdc/1/file.parquet",
		"delta",
		[]uuid.UUID{rowID},
		1700000000000,
		zap.NewNop(),
	)
	require.NoError(t, err)
}

func TestUpdateManifest_AppendsEntryWithRowBounds(t *testing.T) {
	ctx := context.Background()
	store := newInMemoryManifestStore()
	resolver := manifest.PathResolver{
		Prefix:       "cdc",
		PathTemplate: "manifest/{{.SchemaID}}.json",
	}

	idOldest := uuid.MustParse("018f05c0-0000-7000-8000-000000000001")
	idMiddle := uuid.MustParse("018f05c0-0001-7000-8000-000000000001")
	idNewest := uuid.MustParse("018f05c0-0002-7000-8000-000000000001")
	rowIDs := []uuid.UUID{idMiddle, idNewest, idOldest}
	createdAt := int64(1700000000000)

	err := updateManifest(
		ctx,
		store,
		resolver,
		7,
		"cdc/7/delta-file.parquet",
		"delta",
		rowIDs,
		createdAt,
		zap.NewNop(),
	)
	require.NoError(t, err)

	manifestPath, err := resolver.Resolve(7)
	require.NoError(t, err)

	payload := store.data[manifestPath]
	require.NotEmpty(t, payload)

	m, err := manifest.Parse(payload)
	require.NoError(t, err)
	require.Equal(t, int16(7), m.SchemaID)
	require.Len(t, m.Files, 1)

	entry := m.Files[0]
	expectedMin, expectedMax := minMaxRowID(rowIDs)
	require.Equal(t, "delta", entry.Tier)
	require.Equal(t, "cdc/7/delta-file.parquet", entry.Path)
	require.Equal(t, int64(3), entry.RowCount)
	require.Equal(t, createdAt, entry.CreatedMin)
	require.Equal(t, createdAt, entry.CreatedMax)
	require.Equal(t, expectedMin.String(), entry.RowIDMin)
	require.Equal(t, expectedMax.String(), entry.RowIDMax)
}

func TestUpdateManifest_ResolverError(t *testing.T) {
	err := updateManifest(
		context.Background(),
		newInMemoryManifestStore(),
		manifest.PathResolver{PathTemplate: "{{"},
		1,
		"cdc/1/file.parquet",
		"delta",
		nil,
		1700000000000,
		zap.NewNop(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolve manifest path")
}

func TestUpdateManifest_AppendError(t *testing.T) {
	store := newInMemoryManifestStore()
	store.loadErr = errors.New("boom")

	err := updateManifest(
		context.Background(),
		store,
		manifest.PathResolver{},
		1,
		"cdc/1/file.parquet",
		"delta",
		nil,
		1700000000000,
		zap.NewNop(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "append to manifest")
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

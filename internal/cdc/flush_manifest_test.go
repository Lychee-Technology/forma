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
		0,
		nil,
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
		4096,
		nil,
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
	require.Equal(t, int64(4096), entry.SizeBytes)
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
		0,
		nil,
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
		0,
		nil,
		zap.NewNop(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "append to manifest")
}

// The final delta's footer columns are stamped into the manifest entry so
// federated reads can validate the #189 invariant without a probe (#256).
func TestExecuteBatchStampsManifestColumns(t *testing.T) {
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

	// Track the URI passed to the seam to verify it receives the final key, not tmp
	var seenURI string
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
		describeColumns: func(ctx context.Context, uri string) (map[string]string, error) {
			seenURI = uri
			return map[string]string{
				"row_id":     "UUID",
				"changed_at": "BIGINT",
				"deleted_at": "BIGINT",
			}, nil
		},
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			return nil
		},
	}

	err = executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "batch")
	require.NoError(t, err)

	// Verify seam received the final key URI, not tmp
	require.Equal(t, "s3://test-bucket/cdc/7/delta-file.parquet", seenURI)

	// Verify columns are stamped in the manifest
	entry := store.readLastEntry(t)
	require.Equal(t, "UUID", entry.Columns["row_id"])
	require.Equal(t, "BIGINT", entry.Columns["changed_at"])
	require.Equal(t, "BIGINT", entry.Columns["deleted_at"])
	require.Len(t, entry.Columns, 3)
}

// Manifest-disabled deployments must not pay a footer DESCRIBE per flush
// batch: the stamp has nowhere to go (the manifest update is skipped), so the
// probe would be a discarded S3 read. Flush twin of
// TestInitStampColumnsShortCircuitWhenNoManifestStore.
func TestExecuteBatchSkipsStampWhenNoManifestStore(t *testing.T) {
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

	executor := &flushBatchExecutor{
		db:            db,
		duck:          &DuckExporter{Logger: zap.NewNop()},
		s3Client:      &objectOnlyS3Client{},
		cfg:           CDCConfig{S3Bucket: "test-bucket", S3Prefix: "cdc"},
		tableName:     "change_log",
		schemaID:      7,
		snapshot:      snapshot,
		versions:      map[uuid.UUID]int64{rowID: snapshot - 1000},
		pgConnForDuck: "host=pg port=5432 user=pguser password=secret dbname=forma sslmode=disable",
		logger:        zap.NewNop(),
		manifestStore: nil,
		describeColumns: func(ctx context.Context, uri string) (map[string]string, error) {
			t.Fatalf("describe must not be called when manifestStore is nil (uri=%s)", uri)
			return nil, nil
		},
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			return nil
		},
	}

	err = executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "batch")
	require.NoError(t, err)
}

// A failed describe must not fail the flush and must leave the entry
// unstamped — the read path falls back to footer probing (SizeBytes
// precedent).
func TestExecuteBatchDescribeFailureLeavesEntryUnstamped(t *testing.T) {
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
		describeColumns: func(ctx context.Context, uri string) (map[string]string, error) {
			return nil, errors.New("footer read failed")
		},
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			return nil
		},
	}

	// Failure to describe must not fail executeBatch
	err = executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "batch")
	require.NoError(t, err)

	// Entry must be saved but unstamped
	entry := store.readLastEntry(t)
	require.Nil(t, entry.Columns)
}

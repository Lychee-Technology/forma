package cdc

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// trackingRegistry is a gated registry that started on generation 1 of
// schema 7; apply moves the active generation, as apply-schema would.
type trackingRegistry struct {
	forma.SchemaRegistry
	active int64
}

func (r *trackingRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "orders", forma.SchemaAttributeCache{"qty": {AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeSmallInt}}, nil
}

func (r *trackingRegistry) LoadedGeneration(schemaID int16) (int64, bool) { return 1, schemaID == 7 }

func (r *trackingRegistry) ActiveGeneration(context.Context, int16) (int64, bool, error) {
	return r.active, true, nil
}

func (r *trackingRegistry) apply() { r.active++ }

var _ schemaledger.Tracker = (*trackingRegistry)(nil)

// copyCountingS3Client counts tmp->final copies: a batch that must not be
// published never reaches one.
type copyCountingS3Client struct {
	objectOnlyS3Client
	copies int
}

func (c *copyCountingS3Client) CopyObject(ctx context.Context, in *s3.CopyObjectInput, opts ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	c.copies++
	return c.objectOnlyS3Client.CopyObject(ctx, in, opts...)
}

// TestFlushBatchStaysInTmpWhenAGenerationIsRecordedDuringTheExport: the
// export wrote stored values through the run's declared types, so a
// generation recorded meanwhile fails the batch before the copy, the
// manifest append and the mark: the rows stay dirty for the next run.
func TestFlushBatchStaysInTmpWhenAGenerationIsRecordedDuringTheExport(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE change_log (schema_id SMALLINT, row_id UUID, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)
	rowID := uuid.MustParse("018f05c0-0000-7000-8000-000000000001")
	snapshot := time.Now().UnixMilli()
	_, err = db.ExecContext(ctx, "INSERT INTO change_log VALUES (7, ?, ?, 0)", rowID, snapshot-1000)
	require.NoError(t, err)

	registry := &trackingRegistry{active: 1}
	s3Client := &copyCountingS3Client{}
	store := newInMemoryManifestStore()
	executor := &flushBatchExecutor{
		db:               db,
		duck:             &DuckExporter{Logger: zap.NewNop()},
		s3Client:         s3Client,
		cfg:              CDCConfig{S3Bucket: "test-bucket", S3Prefix: "cdc"},
		tableName:        "change_log",
		schemaID:         7,
		snapshot:         snapshot,
		versions:         map[uuid.UUID]int64{rowID: snapshot - 1000},
		registry:         registry,
		logger:           zap.NewNop(),
		manifestStore:    store,
		manifestResolver: manifest.PathResolver{Prefix: "cdc", PathTemplate: "manifest/{{.SchemaID}}.json"},
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			registry.apply()
			return nil
		},
	}

	err = executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "single")
	require.ErrorIs(t, err, schemaledger.ErrGenerationChanged)
	require.Zero(t, s3Client.copies, "the batch must not leave _tmp")
	require.Zero(t, store.saved, "the manifest must not list the batch")
	var flushedAt int64
	require.NoError(t, db.QueryRowContext(ctx, "SELECT flushed_at FROM change_log WHERE row_id = ?", rowID).Scan(&flushedAt))
	require.Zero(t, flushedAt, "the rows must stay dirty")

	// Unchanged, the same batch publishes.
	registry.active = 1
	executor.exportSnapshot = func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
		return nil
	}
	require.NoError(t, executor.executeBatch(ctx, []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "single"))
	require.Equal(t, 1, s3Client.copies)
}

func TestInitBatchStaysInTmpWhenAGenerationIsRecordedDuringTheExport(t *testing.T) {
	runCtx, state := newChecksumInitContext(nil)
	registry := &trackingRegistry{active: 1}
	s3Client := &copyCountingS3Client{}
	runCtx.s3Client = s3Client
	runCtx.schemaRegistry = registry
	runCtx.exportBaseFile = func(context.Context, *schemaInitState, schemaBatchExport) error {
		registry.apply()
		return nil
	}

	err := exportSchemaBatch(context.Background(), runCtx, state, initBatchRowIDs())
	require.ErrorIs(t, err, schemaledger.ErrGenerationChanged)
	require.Zero(t, s3Client.copies, "the base batch must not leave _tmp")
	require.Empty(t, state.fileEntries, "the manifest must not list the batch")
}

// TestRunPreflightRefusesASupersededGeneration: a long-lived process whose
// registry started on generation 1 does not start a run once another
// generation is active; a registry with no ledger is not checked.
func TestRunPreflightRefusesASupersededGeneration(t *testing.T) {
	ctx := context.Background()
	registry := &trackingRegistry{active: 1}
	_, err := resolveRunAttrCache(ctx, registry, 7)
	require.NoError(t, err)

	registry.apply()
	_, err = resolveRunAttrCache(ctx, registry, 7)
	require.ErrorIs(t, err, schemaledger.ErrGenerationChanged)

	runCtx := &initRunContext{schemaRegistry: registry, logger: zap.NewNop()}
	_, err = processInitSchemas(ctx, runCtx, []int64{7})
	require.ErrorIs(t, err, schemaledger.ErrGenerationChanged)
	flushCtx := &schemaFlushContext{schemaRegistry: registry, logger: zap.NewNop(),
		processSchemaFn: func(context.Context, int16) error { return errors.New("must not run") }}
	_, err = flushCtx.processSchemas(ctx, []int64{7})
	require.ErrorIs(t, err, schemaledger.ErrGenerationChanged)

	untracked := struct{ forma.SchemaRegistry }{registry}
	_, err = resolveRunAttrCache(ctx, untracked, 7)
	require.NoError(t, err, "a registry that is not a Tracker has no ledger to check")
}

package cdc

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// newChecksumStampExecutor builds the executeBatch fixture the checksum tests
// share: a DuckDB stand-in for change_log holding one unflushed row, stubbed
// export and describe seams so the batch reaches the manifest append without
// touching Postgres or S3, and the caller's checksum seam.
func newChecksumStampExecutor(
	t *testing.T,
	store manifest.Store,
	checksumObject func(ctx context.Context, key string) (string, error),
) (*flushBatchExecutor, uuid.UUID) {
	t.Helper()

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
		manifestResolver: manifest.PathResolver{Prefix: "cdc", PathTemplate: "manifest/{{.SchemaID}}.json"},
		checksumObject:   checksumObject,
		describeColumns: func(ctx context.Context, uri string) (map[string]string, error) {
			return map[string]string{"row_id": "UUID"}, nil
		},
		exportSnapshot: func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			return nil
		},
	}
	return executor, rowID
}

// The published final object's content hash is stamped into the manifest entry
// so a later verification pass can detect mutation without re-reading the whole
// tier (#347).
func TestExecuteBatchStampsManifestChecksum(t *testing.T) {
	store := newInMemoryManifestStore()

	// Track the key handed to the seam: the hash must bless the final object,
	// not the tmp one the export wrote.
	var seenKey string
	executor, rowID := newChecksumStampExecutor(t, store, func(ctx context.Context, key string) (string, error) {
		seenKey = key
		return "sha256:deadbeef", nil
	})

	err := executor.executeBatch(context.Background(), []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "batch")
	require.NoError(t, err)

	require.Equal(t, "cdc/7/delta-file.parquet", seenKey)
	require.Equal(t, "sha256:deadbeef", store.readLastEntry(t).Checksum)
}

// A failed hash must not fail the flush and must leave the entry unstamped —
// verification passes skip an empty Checksum (stampColumns/SizeBytes
// precedent).
func TestExecuteBatchChecksumFailureLeavesEntryUnstamped(t *testing.T) {
	store := newInMemoryManifestStore()
	// The seam returns a value alongside the error — a partial hash over a
	// truncated read is exactly what must never reach the manifest, so the
	// error, not the emptiness of the value, has to be what discards it.
	executor, rowID := newChecksumStampExecutor(t, store, func(ctx context.Context, key string) (string, error) {
		return "sha256:partial", errors.New("get object failed")
	})

	err := executor.executeBatch(context.Background(), []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "batch")
	require.NoError(t, err)

	entry := store.readLastEntry(t)
	require.Empty(t, entry.Checksum)
	// The rest of the entry is unaffected: only the checksum is best-effort.
	require.Equal(t, "cdc/7/delta-file.parquet", entry.Path)
}

// Deployments without the seam wired (no full S3 client) keep the pre-#347
// behavior: the entry is appended, just unstamped.
func TestExecuteBatchWithoutChecksumSeamLeavesEntryUnstamped(t *testing.T) {
	store := newInMemoryManifestStore()
	executor, rowID := newChecksumStampExecutor(t, store, nil)

	err := executor.executeBatch(context.Background(), []uuid.UUID{rowID}, "cdc/7/_tmp/file.parquet", "cdc/7/delta-file.parquet", "batch")
	require.NoError(t, err)

	require.Empty(t, store.readLastEntry(t).Checksum)
}

// The run-level seam has to reach the executor, or every entry silently goes
// unstamped in production while the executor's own tests stay green (#318).
func TestExecuteFlushCarriesChecksumSeamToExecutor(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE change_log (schema_id SMALLINT, row_id UUID, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO change_log VALUES (7, ?, ?, 0)",
		uuid.MustParse("018f05c0-0000-7000-8000-000000000001"), time.Now().Add(-time.Minute).UnixMilli())
	require.NoError(t, err)

	var seen string
	flushCtx := &schemaFlushContext{
		db:         db,
		cfg:        CDCConfig{BatchSize: 10, PGHost: "localhost", PGPort: 5432, PGUser: "pguser", PGDB: "forma"},
		tableName:  "change_log",
		pgPassword: "secret",
		logger:     zap.NewNop(),
		checksumObject: func(ctx context.Context, key string) (string, error) {
			return "sha256:wired", nil
		},
		executeSingle: func(executor *flushBatchExecutor, _ []uuid.UUID) error {
			require.NotNil(t, executor.checksumObject, "executeFlush dropped the run's checksum seam")
			seen, err = executor.checksumObject(ctx, "cdc/7/delta-file.parquet")
			return err
		},
	}

	require.NoError(t, flushCtx.executeFlush(ctx, 7))
	require.Equal(t, "sha256:wired", seen)
}

// hashableFullS3Client is an S3FullClient whose GetObject returns real bytes,
// so the wired seam produces a checksum rather than the nil-body error the
// shared fullS3ClientMock yields.
type hashableFullS3Client struct {
	fullS3ClientMock
	body []byte
}

func (c *hashableFullS3Client) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(c.body))}, nil
}

func TestNewChecksumSeamHashesThroughFullClient(t *testing.T) {
	seam := newChecksumSeam(&hashableFullS3Client{body: []byte("hello parquet")}, "test-bucket")
	require.NotNil(t, seam)

	got, err := seam(context.Background(), "cdc/7/delta-file.parquet")
	require.NoError(t, err)
	// sha256("hello parquet")
	require.Equal(t, "sha256:950423965d5b936670f1549c58ce0594b58e1027c2b5e1e2a4f1515b1bc2f1b0", got)
}

func TestNewChecksumSeamIsNilWithoutFullClient(t *testing.T) {
	require.Nil(t, newChecksumSeam(nil, "test-bucket"))
}

// An interface holding a typed-nil client passes `!= nil` and would panic on
// the first GetObject, so the seam must judge the concrete value (#302).
func TestNewChecksumSeamIsNilForTypedNilFullClient(t *testing.T) {
	var typedNil *s3.Client
	require.Nil(t, newChecksumSeam(typedNil, "test-bucket"))
}

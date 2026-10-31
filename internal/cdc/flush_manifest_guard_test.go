package cdc

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// #474: a flush with no manifest store must not mark a batch flushed while the
// schema's manifest lists files, since the batch's delta would then be listed
// nowhere and its rows gone from the hot tier.

const listingManifestJSON = `{"schema_id":7,"version":3,"files":[{"tier":"base","path":"cdc/7/base.parquet"}]}`

// manifestBucketClient is a full S3 client over an in-memory key set: GetObject
// and HeadObject answer from objects (absent keys get the SDK's typed 404s),
// and CopyObject is counted so a test can prove no delta was published.
type manifestBucketClient struct {
	objects map[string][]byte
	getErr  error
	copies  int
}

func (c *manifestBucketClient) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if c.getErr != nil {
		return nil, c.getErr
	}
	body, ok := c.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body)), ETag: aws.String("etag-1")}, nil
}

func (c *manifestBucketClient) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, errors.New("unexpected PutObject")
}

func (c *manifestBucketClient) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	body, ok := c.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &types.NotFound{}
	}
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(body)))}, nil
}

func (c *manifestBucketClient) CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	c.copies++
	return &s3.CopyObjectOutput{}, nil
}

func (c *manifestBucketClient) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

// objectOnlyBucketClient hides the Get/Put half, as a caller's object-only
// client does.
type objectOnlyBucketClient struct{ inner *manifestBucketClient }

func (c objectOnlyBucketClient) CopyObject(ctx context.Context, in *s3.CopyObjectInput, o ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return c.inner.CopyObject(ctx, in, o...)
}

func (c objectOnlyBucketClient) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, o ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return c.inner.DeleteObject(ctx, in, o...)
}

func (c objectOnlyBucketClient) HeadObject(ctx context.Context, in *s3.HeadObjectInput, o ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return c.inner.HeadObject(ctx, in, o...)
}

// unwiredFlushFixture is a change_log holding one dirty row of schema 7 and a
// flush context built by the real constructor with no manifest template.
type unwiredFlushFixture struct {
	db       *sql.DB
	flushCtx *schemaFlushContext
	rowID    uuid.UUID
	exported bool
}

func newUnwiredFlushFixture(t *testing.T, clients resolvedS3Clients) *unwiredFlushFixture {
	t.Helper()
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE change_log (schema_id SMALLINT, row_id UUID, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)
	f := &unwiredFlushFixture{db: db, rowID: uuid.MustParse("018f05c0-0000-7000-8000-000000000001")}
	_, err = db.ExecContext(ctx, "INSERT INTO change_log VALUES (7, ?, ?, 0)", f.rowID, time.Now().UnixMilli()-1000)
	require.NoError(t, err)

	flushCtx, err := newSchemaFlushContext(flushContextParams{
		cfg:     CDCConfig{S3Bucket: "test-bucket", S3Prefix: "cdc", ManifestPrefix: "cdc", BatchSize: 10},
		db:      db,
		duck:    &DuckExporter{Logger: zap.NewNop()},
		clients: clients,
		logger:  zap.NewNop(),
	})
	require.NoError(t, err)
	require.Nil(t, flushCtx.manifestStore, "an empty template must leave the store unwired")
	// The real batch path, with only the DuckDB export stubbed.
	flushCtx.executeSingle = func(e *flushBatchExecutor, ids []uuid.UUID) error {
		e.exportSnapshot = func(*DuckExporter, context.Context, CDCConfig, string, string, int16, int64, []uuid.UUID, forma.SchemaAttributeCache) error {
			f.exported = true
			return nil
		}
		return executeFlushSingle(ctx, e, ids)
	}
	f.flushCtx = flushCtx
	return f
}

func (f *unwiredFlushFixture) flushedAt(t *testing.T) int64 {
	t.Helper()
	var flushedAt int64
	err := f.db.QueryRowContext(context.Background(),
		"SELECT flushed_at FROM change_log WHERE schema_id = 7 AND row_id = ?", f.rowID).Scan(&flushedAt)
	require.NoError(t, err)
	return flushedAt
}

// The issue's regression: no store, a manifest that lists files → an error,
// not a silent mark. The refusal comes before the export, so no delta object
// is written and the row stays hot.
func TestExecuteFlush_UnwiredStoreRefusesWhenManifestListsFiles(t *testing.T) {
	bucket := &manifestBucketClient{objects: map[string][]byte{"cdc/manifest/7.json": []byte(listingManifestJSON)}}
	f := newUnwiredFlushFixture(t, resolvedS3Clients{object: bucket, full: bucket})

	err := f.flushCtx.executeFlush(context.Background(), 7)
	require.ErrorIs(t, err, ErrManifestStoreUnwired)
	require.Contains(t, err.Error(), "cdc/manifest/7.json")
	require.False(t, f.exported, "a refused batch must not export")
	require.Zero(t, bucket.copies, "a refused batch must not publish a delta")
	require.Zero(t, f.flushedAt(t), "a refused batch must leave its rows hot")
}

// Readers glob a schema whose manifest is absent or empty, so marking without
// an append keeps the batch visible there: those flushes proceed.
func TestExecuteFlush_UnwiredStoreFlushesWhenManifestAbsentOrEmpty(t *testing.T) {
	cases := map[string]map[string][]byte{
		"absent": {},
		"empty":  {"cdc/manifest/7.json": []byte(`{"schema_id":7,"version":1,"files":[]}`)},
	}
	for name, objects := range cases {
		t.Run(name, func(t *testing.T) {
			bucket := &manifestBucketClient{objects: objects}
			f := newUnwiredFlushFixture(t, resolvedS3Clients{object: bucket, full: bucket})

			require.NoError(t, f.flushCtx.executeFlush(context.Background(), 7))
			require.True(t, f.exported)
			require.Equal(t, 1, bucket.copies)
			require.NotZero(t, f.flushedAt(t))
		})
	}
}

// A probe that cannot tell whether the manifest lists files refuses: a read
// failure does not prove the manifest absent (#464).
func TestExecuteFlush_UnwiredStoreRefusesWhenManifestUnreadable(t *testing.T) {
	cases := map[string]*manifestBucketClient{
		"load fails":  {objects: map[string][]byte{}, getErr: errors.New("access denied")},
		"not a JSON":  {objects: map[string][]byte{"cdc/manifest/7.json": []byte("{")}},
		"wrong shape": {objects: map[string][]byte{"cdc/manifest/7.json": []byte(`{"files":"x"}`)}},
	}
	for name, bucket := range cases {
		t.Run(name, func(t *testing.T) {
			f := newUnwiredFlushFixture(t, resolvedS3Clients{object: bucket, full: bucket})

			err := f.flushCtx.executeFlush(context.Background(), 7)
			require.Error(t, err)
			require.Contains(t, err.Error(), "cdc/manifest/7.json")
			require.False(t, f.exported)
			require.Zero(t, f.flushedAt(t))
		})
	}
}

// An object-only client cannot read the manifest, so an existing manifest
// object refuses the batch whatever it lists; a confirmed 404 lets it flush.
func TestExecuteFlush_UnwiredStoreObjectOnlyClientFailsClosed(t *testing.T) {
	present := &manifestBucketClient{objects: map[string][]byte{"cdc/manifest/7.json": []byte(`{"files":[]}`)}}
	f := newUnwiredFlushFixture(t, resolvedS3Clients{object: objectOnlyBucketClient{inner: present}})
	err := f.flushCtx.executeFlush(context.Background(), 7)
	require.ErrorIs(t, err, ErrManifestStoreUnwired)
	require.False(t, f.exported)
	require.Zero(t, f.flushedAt(t))

	absent := &manifestBucketClient{objects: map[string][]byte{}}
	f = newUnwiredFlushFixture(t, resolvedS3Clients{object: objectOnlyBucketClient{inner: absent}})
	require.NoError(t, f.flushCtx.executeFlush(context.Background(), 7))
	require.NotZero(t, f.flushedAt(t))
}

// A run with a template has a store and appends; it carries no probe.
func TestNewSchemaFlushContextProbesOnlyWithoutTemplate(t *testing.T) {
	bucket := &manifestBucketClient{objects: map[string][]byte{}}
	flushCtx, err := newSchemaFlushContext(flushContextParams{
		cfg:     CDCConfig{S3Bucket: "test-bucket", ManifestTemplate: "manifest/{{.SchemaID}}.json"},
		clients: resolvedS3Clients{object: bucket, full: bucket},
		logger:  zap.NewNop(),
	})
	require.NoError(t, err)
	require.NotNil(t, flushCtx.manifestStore)
	require.Nil(t, flushCtx.probeUnwiredManifest)
}

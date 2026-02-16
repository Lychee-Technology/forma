package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type inMemoryManifestStore struct {
	data    map[string][]byte
	etags   map[string]string
	loadErr error
	saveErr error
	saved   int
}

type objectOnlyS3Client struct{}

func (c *objectOnlyS3Client) CopyObject(_ context.Context, _ *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return &s3.CopyObjectOutput{}, nil
}

func (c *objectOnlyS3Client) DeleteObject(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

type fullS3ClientMock struct {
	objectOnlyS3Client
}

func (c *fullS3ClientMock) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{}, nil
}

func (c *fullS3ClientMock) PutObject(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}

func newInMemoryManifestStore() *inMemoryManifestStore {
	return &inMemoryManifestStore{
		data:  make(map[string][]byte),
		etags: make(map[string]string),
	}
}

func (s *inMemoryManifestStore) Load(_ context.Context, path string) ([]byte, string, error) {
	if s.loadErr != nil {
		return nil, "", s.loadErr
	}
	b, ok := s.data[path]
	if !ok {
		return nil, "", fmt.Errorf("not found")
	}
	return append([]byte(nil), b...), s.etags[path], nil
}

func (s *inMemoryManifestStore) Save(_ context.Context, path string, data []byte, _ string) (string, error) {
	if s.saveErr != nil {
		return "", s.saveErr
	}
	s.saved++
	etag := fmt.Sprintf("etag-%d", s.saved)
	s.data[path] = append([]byte(nil), data...)
	s.etags[path] = etag
	return etag, nil
}

func TestRunOnce_RequiresSchemaRegistry(t *testing.T) {
	err := RunOnce(context.Background(), CDCConfig{}, nil, false, zap.NewNop(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema registry is required")
}

func TestResolveS3Clients_UsesFallbackWhenCustomClientNotProvided(t *testing.T) {
	fallback := &fullS3ClientMock{}

	objectClient, fullClient, err := resolveS3Clients(nil, fallback, false)
	require.NoError(t, err)
	require.Same(t, fallback, objectClient)
	require.Same(t, fallback, fullClient)
}

func TestResolveS3Clients_AcceptsObjectOnlyClientWhenFullNotRequired(t *testing.T) {
	customObjectClient := &objectOnlyS3Client{}
	fallback := &fullS3ClientMock{}

	objectClient, fullClient, err := resolveS3Clients(customObjectClient, fallback, false)
	require.NoError(t, err)
	require.Same(t, customObjectClient, objectClient)
	require.Nil(t, fullClient)
}

func TestResolveS3Clients_RejectsObjectOnlyClientWhenFullRequired(t *testing.T) {
	customObjectClient := &objectOnlyS3Client{}
	fallback := &fullS3ClientMock{}

	_, _, err := resolveS3Clients(customObjectClient, fallback, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "manifest requires S3FullClient")
}

func TestResolveS3Clients_UsesCustomFullClientWhenProvided(t *testing.T) {
	customFullClient := &fullS3ClientMock{}
	fallback := &fullS3ClientMock{}

	objectClient, fullClient, err := resolveS3Clients(customFullClient, fallback, true)
	require.NoError(t, err)
	require.Same(t, customFullClient, objectClient)
	require.Same(t, customFullClient, fullClient)
}

func TestSetupPostgresConnection_UsesIAMToken(t *testing.T) {
	orig := generateIAMTokenFn
	defer func() { generateIAMTokenFn = orig }()

	const token = "iam-token"
	generateIAMTokenFn = func(ctx context.Context, endpoint, region string, creds interface{}) (string, error) {
		return token, nil
	}

	cfg := CDCConfig{
		PGHost:     "localhost",
		PGPort:     5432,
		PGUser:     "user",
		PGPassword: "envpass",
		PGDB:       "db",
		PGUseIAM:   true,
	}

	db, pgPassword, err := setupPostgresConnection(context.Background(), cfg, "us-east-1", nil, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, db)
	require.Equal(t, token, pgPassword)
	require.NoError(t, db.Close())
}

func TestSetupPostgresConnection_FallsBackToPGPassword(t *testing.T) {
	orig := generateIAMTokenFn
	defer func() { generateIAMTokenFn = orig }()

	generateIAMTokenFn = func(ctx context.Context, endpoint, region string, creds interface{}) (string, error) {
		return "", errors.New("token unavailable")
	}

	cfg := CDCConfig{
		PGHost:     "localhost",
		PGPort:     5432,
		PGUser:     "user",
		PGPassword: "envpass",
		PGDB:       "db",
		PGUseIAM:   true,
	}

	db, pgPassword, err := setupPostgresConnection(context.Background(), cfg, "us-east-1", nil, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, db)
	require.Equal(t, "envpass", pgPassword)
	require.NoError(t, db.Close())
}

func TestShouldFlush(t *testing.T) {
	now := time.Now().UnixMilli()
	tests := []struct {
		name   string
		cfg    CDCConfig
		cnt    int64
		oldest int64
		want   bool
	}{
		{
			name:   "flushes when min records threshold is reached",
			cfg:    CDCConfig{MinRecords: 10, MaxAgeMs: 60000},
			cnt:    10,
			oldest: now - 1000,
			want:   true,
		},
		{
			name:   "flushes when max age threshold is reached",
			cfg:    CDCConfig{MinRecords: 100, MaxAgeMs: 1000},
			cnt:    1,
			oldest: now - 5000,
			want:   true,
		},
		{
			name:   "does not flush when neither threshold is reached",
			cfg:    CDCConfig{MinRecords: 100, MaxAgeMs: 60000},
			cnt:    99,
			oldest: now - 1000,
			want:   false,
		},
		{
			name:   "does not flush without oldest timestamp and insufficient rows",
			cfg:    CDCConfig{MinRecords: 2, MaxAgeMs: 1000},
			cnt:    1,
			oldest: 0,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldFlush(tt.cfg, tt.cnt, tt.oldest))
		})
	}
}

func TestMinMaxRowID(t *testing.T) {
	idOldest := uuid.MustParse("018f05c0-0000-7000-8000-000000000001")
	idMiddle := uuid.MustParse("018f05c0-0001-7000-8000-000000000001")
	idNewest := uuid.MustParse("018f05c0-0002-7000-8000-000000000001")

	minID, maxID := minMaxRowID(nil)
	require.Equal(t, uuid.Nil, minID)
	require.Equal(t, uuid.Nil, maxID)

	minID, maxID = minMaxRowID([]uuid.UUID{idMiddle})
	require.Equal(t, idMiddle, minID)
	require.Equal(t, idMiddle, maxID)

	minID, maxID = minMaxRowID([]uuid.UUID{idMiddle, idNewest, idOldest})
	require.Equal(t, idOldest, minID)
	require.Equal(t, idNewest, maxID)
}

func TestGetUnflushedSchemaIDs(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE change_log (schema_id BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO change_log VALUES (1, 0), (1, 0), (2, 0), (3, 100)")
	require.NoError(t, err)

	schemaIDs, err := getUnflushedSchemaIDs(ctx, db, "change_log")
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{1, 2}, schemaIDs)
}

func TestGetUnflushedSchemaIDs_QueryError(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	schemaIDs, err := getUnflushedSchemaIDs(context.Background(), db, "missing_table")
	require.Error(t, err)
	require.Nil(t, schemaIDs)
	require.Contains(t, err.Error(), "query distinct schema ids")
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

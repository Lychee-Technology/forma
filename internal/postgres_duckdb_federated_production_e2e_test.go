//go:build e2e

package internal

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type productionFederatedE2EEnv struct {
	ctx         context.Context
	pgContainer testcontainers.Container
	s3Container testcontainers.Container
	pool        *pgxpool.Pool
	duck        *DuckDBClient
	s3Client    *s3.Client
	bucket      string
	prefix      string
	tmpDir      string
	repo        *DBPersistentRecordRepository
	tables      StorageTables
	pgConn      string
}

type productionTestRecord struct {
	RowID     uuid.UUID
	SchemaID  int16
	Name      string
	Age       int32
	Tag       string
	CreatedAt int64
	ChangedAt int64
	DeletedAt int64
}

func setupProductionFederatedE2EEnv(t *testing.T) *productionFederatedE2EEnv {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	pgContainer, pool, host, port := startProductionFederatedPostgres(t, ctx)
	s3Container, endpoint := startProductionFederatedS3(t, ctx)
	s3Client := newProductionFederatedS3Client(t, ctx, endpoint)
	bucket := "test-bucket"
	_, _ = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})

	tmpDir, err := os.MkdirTemp("", "prod-fed-e2e-*")
	require.NoError(t, err)

	duckCfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MemoryLimitMB:  512,
		EnableS3:       true,
		EnableParquet:  true,
		S3Endpoint:     endpoint,
		S3AccessKey:    "minio",
		S3SecretKey:    "minio",
		S3Region:       "us-east-1",
		MaxConnections: 1,
		QueryTimeout:   60 * time.Second,
		MaxParallelism: 1,
	}
	duck, err := NewDuckDBClient(duckCfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		if duck != nil {
			require.NoError(t, duck.Close())
		}
		if pool != nil {
			pool.Close()
		}
		if s3Container != nil {
			require.NoError(t, s3Container.Terminate(context.Background()))
		}
		if pgContainer != nil {
			require.NoError(t, pgContainer.Terminate(context.Background()))
		}
		require.NoError(t, os.RemoveAll(tmpDir))
	})

	require.NoError(t, createProductionFederatedSchema(ctx, pool))
	metadata := loadProductionFederatedMetadata(t, ctx, pool)
	repo := NewDBPersistentRecordRepository(pool, metadata, duck, duckCfg)

	return &productionFederatedE2EEnv{
		ctx:         ctx,
		pgContainer: pgContainer,
		s3Container: s3Container,
		pool:        pool,
		duck:        duck,
		s3Client:    s3Client,
		bucket:      bucket,
		prefix:      "prod-fed-tests",
		tmpDir:      tmpDir,
		repo:        repo,
		tables:      StorageTables{EntityMain: "entity_main_dev", EAVData: "eav_data_dev", ChangeLog: "change_log_dev"},
		pgConn:      fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s", host, port, "postgres", "password", "postgres"),
	}
}

func startProductionFederatedPostgres(t *testing.T, ctx context.Context) (testcontainers.Container, *pgxpool.Pool, string, string) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_PASSWORD": "password",
			"POSTGRES_USER":     "postgres",
			"POSTGRES_DB":       "postgres",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(30 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)
	mapped, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err)
	dsn := fmt.Sprintf("postgres://postgres:password@%s:%s/postgres?sslmode=disable", host, mapped.Port())
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return pool.Ping(ctx) == nil }, 20*time.Second, 200*time.Millisecond)
	return container, pool, host, mapped.Port()
}

func startProductionFederatedS3(t *testing.T, ctx context.Context) (testcontainers.Container, string) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "rustfs/rustfs:latest",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"RUSTFS_ACCESS_KEY": "minio",
			"RUSTFS_SECRET_KEY": "minio",
		},
		WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(30 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)
	mapped, err := container.MappedPort(ctx, "9000")
	require.NoError(t, err)
	return container, fmt.Sprintf("http://%s:%s", host, mapped.Port())
}

func newProductionFederatedS3Client(t *testing.T, ctx context.Context, endpoint string) *s3.Client {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minio", "minio", "")),
	)
	require.NoError(t, err)
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

func createProductionFederatedSchema(ctx context.Context, pool *pgxpool.Pool) error {
	stmts := []string{
		`DROP TABLE IF EXISTS schema_registry`,
		`DROP TABLE IF EXISTS change_log_dev`,
		`DROP TABLE IF EXISTS eav_data_dev`,
		`DROP TABLE IF EXISTS entity_main_dev`,
		`CREATE TABLE schema_registry (schema_name TEXT PRIMARY KEY, schema_id SMALLINT NOT NULL)`,
		`CREATE TABLE entity_main_dev (
			ltbase_schema_id SMALLINT NOT NULL,
			ltbase_row_id UUID NOT NULL,
			ltbase_created_at BIGINT NOT NULL,
			ltbase_updated_at BIGINT NOT NULL,
			ltbase_deleted_at BIGINT,
			text_01 TEXT,
			integer_01 INTEGER,
			PRIMARY KEY (ltbase_schema_id, ltbase_row_id)
		)`,
		`CREATE TABLE eav_data_dev (
			schema_id SMALLINT NOT NULL,
			row_id UUID NOT NULL,
			attr_id SMALLINT NOT NULL,
			array_indices TEXT NOT NULL DEFAULT '',
			value_text TEXT,
			value_numeric DOUBLE PRECISION,
			PRIMARY KEY (schema_id, row_id, attr_id, array_indices)
		)`,
		`CREATE TABLE change_log_dev (
			schema_id SMALLINT NOT NULL,
			row_id UUID NOT NULL,
			flushed_at BIGINT NOT NULL DEFAULT 0,
			changed_at BIGINT NOT NULL,
			deleted_at BIGINT,
			PRIMARY KEY (schema_id, row_id, flushed_at)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	for name, id := range map[string]int16{"activity": 100, "lead": 101, "visit": 102, "communication": 103, "log": 104} {
		if _, err := pool.Exec(ctx, `INSERT INTO schema_registry (schema_name, schema_id) VALUES ($1, $2)`, name, id); err != nil {
			return err
		}
	}
	return nil
}

func loadProductionFederatedMetadata(t *testing.T, ctx context.Context, pool *pgxpool.Pool) *MetadataCache {
	t.Helper()
	loader := NewMetadataLoader(pool, "schema_registry", "../cmd/server/schemas")
	metadata, err := loader.LoadMetadata(ctx)
	require.NoError(t, err)
	return metadata
}

func withProductionDuckDBTemplateDescriptors(t *testing.T) {
	t.Helper()
	orig := entityMainColumnDescriptors
	entityMainColumnDescriptors = []columnDescriptor{
		{name: "ltbase_schema_id", kind: columnKindSmallint},
		{name: "ltbase_row_id", kind: columnKindText},
		{name: "ltbase_created_at", kind: columnKindBigint},
		{name: "ltbase_updated_at", kind: columnKindBigint},
		{name: "ltbase_deleted_at", kind: columnKindBigint},
		{name: "text_01", kind: columnKindText},
		{name: "integer_01", kind: columnKindInteger},
	}
	t.Cleanup(func() {
		entityMainColumnDescriptors = orig
	})
}

func clearProductionFederatedData(t *testing.T, env *productionFederatedE2EEnv, schemaID int16) {
	t.Helper()
	_, err := env.pool.Exec(env.ctx, `DELETE FROM change_log_dev WHERE schema_id = $1`, schemaID)
	require.NoError(t, err)
	_, err = env.pool.Exec(env.ctx, `DELETE FROM eav_data_dev WHERE schema_id = $1`, schemaID)
	require.NoError(t, err)
	_, err = env.pool.Exec(env.ctx, `DELETE FROM entity_main_dev WHERE ltbase_schema_id = $1`, schemaID)
	require.NoError(t, err)
	resp, err := env.s3Client.ListObjectsV2(env.ctx, &s3.ListObjectsV2Input{Bucket: aws.String(env.bucket), Prefix: aws.String(env.prefix)})
	require.NoError(t, err)
	for _, obj := range resp.Contents {
		_, err = env.s3Client.DeleteObject(env.ctx, &s3.DeleteObjectInput{Bucket: aws.String(env.bucket), Key: obj.Key})
		require.NoError(t, err)
	}
}

func writeProductionParquet(t *testing.T, env *productionFederatedE2EEnv, tier, filename string, records []productionTestRecord) {
	t.Helper()
	csvPath := filepath.Join(env.tmpDir, filename+".csv")
	parquetPath := filepath.Join(env.tmpDir, filename)
	var csv bytes.Buffer
	csv.WriteString("row_id,ltbase_created_at,ltbase_updated_at,ltbase_deleted_at,name,age,tag\n")
	for _, r := range records {
		createdAt := r.CreatedAt
		if createdAt == 0 {
			createdAt = r.ChangedAt
		}
		csv.WriteString(fmt.Sprintf("%s,%d,%d,%d,%s,%d,%s\n", r.RowID.String(), createdAt, r.ChangedAt, r.DeletedAt, r.Name, r.Age, r.Tag))
	}
	require.NoError(t, os.WriteFile(csvPath, csv.Bytes(), 0o644))
	_, err := env.duck.DB.ExecContext(env.ctx, fmt.Sprintf(`CREATE OR REPLACE TABLE temp_export AS SELECT * FROM read_csv_auto('%s')`, csvPath))
	require.NoError(t, err)
	_, err = env.duck.DB.ExecContext(env.ctx, fmt.Sprintf(`COPY temp_export TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`, parquetPath))
	require.NoError(t, err)
	data, err := os.ReadFile(parquetPath)
	require.NoError(t, err)
	key := fmt.Sprintf("%s/%d/%s/%s", env.prefix, records[0].SchemaID, tier, filename)
	_, err = env.s3Client.PutObject(env.ctx, &s3.PutObjectInput{Bucket: aws.String(env.bucket), Key: aws.String(key), Body: bytes.NewReader(data)})
	require.NoError(t, err)
}

func insertProductionHotRecord(t *testing.T, env *productionFederatedE2EEnv, schemaID int16, rowID uuid.UUID, changedAt int64, name string, age int32) {
	t.Helper()
	_, err := env.pool.Exec(env.ctx, `
		INSERT INTO entity_main_dev (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, text_01, integer_01)
		VALUES ($1, $2, $3, $4, NULL, $5, $6)
		ON CONFLICT (ltbase_schema_id, ltbase_row_id) DO UPDATE SET
			ltbase_updated_at = EXCLUDED.ltbase_updated_at,
			text_01 = EXCLUDED.text_01,
			integer_01 = EXCLUDED.integer_01
	`, schemaID, rowID, changedAt, changedAt, name, age)
	require.NoError(t, err)
	_, err = env.pool.Exec(env.ctx, `
		INSERT INTO change_log_dev (schema_id, row_id, flushed_at, changed_at, deleted_at)
		VALUES ($1, $2, 0, $3, NULL)
		ON CONFLICT (schema_id, row_id, flushed_at) DO UPDATE SET changed_at = EXCLUDED.changed_at
	`, schemaID, rowID, changedAt)
	require.NoError(t, err)
}

func insertProductionDeletedHotRecord(t *testing.T, env *productionFederatedE2EEnv, schemaID int16, rowID uuid.UUID, changedAt int64, deletedAt int64, name string, age int32) {
	t.Helper()
	_, err := env.pool.Exec(env.ctx, `
		INSERT INTO entity_main_dev (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, text_01, integer_01)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (ltbase_schema_id, ltbase_row_id) DO UPDATE SET
			ltbase_updated_at = EXCLUDED.ltbase_updated_at,
			ltbase_deleted_at = EXCLUDED.ltbase_deleted_at,
			text_01 = EXCLUDED.text_01,
			integer_01 = EXCLUDED.integer_01
	`, schemaID, rowID, changedAt, changedAt, deletedAt, name, age)
	require.NoError(t, err)
	_, err = env.pool.Exec(env.ctx, `
		INSERT INTO change_log_dev (schema_id, row_id, flushed_at, changed_at, deleted_at)
		VALUES ($1, $2, 0, $3, $4)
		ON CONFLICT (schema_id, row_id, flushed_at) DO UPDATE SET changed_at = EXCLUDED.changed_at, deleted_at = EXCLUDED.deleted_at
	`, schemaID, rowID, changedAt, deletedAt)
	require.NoError(t, err)
}

func requireExecutionPlanHasSource(t *testing.T, sources []DataSourcePlan, engine string, reasonSubstring string, predicatePushdown *bool) {
	t.Helper()
	for _, source := range sources {
		if source.Engine != engine {
			continue
		}
		if reasonSubstring != "" && !strings.Contains(source.Reason, reasonSubstring) {
			continue
		}
		if predicatePushdown != nil && source.PredicatePushdown != *predicatePushdown {
			continue
		}
		return
	}
	t.Fatalf("expected execution plan source engine=%q reason~=%q predicatePushdown=%v, got %#v", engine, reasonSubstring, predicatePushdown, sources)
}

func TestStreamDuckDBFederatedQuery_GivenBaseAndHotVersions_WhenQueried_ThenLatestHotVersionWins(t *testing.T) {
	withProductionDuckDBTemplateDescriptors(t)
	env := setupProductionFederatedE2EEnv(t)
	clearProductionFederatedData(t, env, 100)

	rowID := uuid.Must(uuid.NewV7())
	baseTime := time.Now().Add(-24 * time.Hour).UnixMilli()
	hotTime := time.Now().UnixMilli()

	writeProductionParquet(t, env, "base", "base_hot.parquet", []productionTestRecord{{
		RowID:     rowID,
		SchemaID:  100,
		Name:      "base-version",
		Age:       21,
		Tag:       "base-tag",
		ChangedAt: baseTime,
	}})
	writeProductionParquet(t, env, "delta", "empty_delta.parquet", []productionTestRecord{{
		RowID:     uuid.Must(uuid.NewV7()),
		SchemaID:  100,
		Name:      "placeholder",
		Age:       0,
		Tag:       "placeholder",
		ChangedAt: 0,
		DeletedAt: time.Now().UnixMilli(),
	}})
	insertProductionHotRecord(t, env, 100, rowID, hotTime, "hot-version", 42)

	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{SchemaID: 100, Limit: 10, Offset: 0},
		DuckDBHints: &DuckDBRenderHints{
			S3ParquetPathTemplate: fmt.Sprintf("s3://%s/%s/{{.SchemaID}}/base/*.parquet, s3://%s/%s/{{.SchemaID}}/delta/*.parquet", env.bucket, env.prefix, env.bucket, env.prefix),
		},
	}
	var got []*PersistentRecord
	total, err := env.repo.StreamDuckDBFederatedQuery(env.ctx, env.tables, q, 10, 0, nil, nil, func(_ context.Context, record *PersistentRecord) error {
		got = append(got, record)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, int64(1), total)
	require.Len(t, got, 1)
	require.Equal(t, hotTime, got[0].UpdatedAt)
	require.Equal(t, "hot-version", got[0].TextItems["text_01"])
	require.Equal(t, int32(42), got[0].Int32Items["integer_01"])
}

func TestStreamDuckDBFederatedQuery_GivenConflictingCreatedAndUpdatedAt_WhenQueried_ThenLatestUpdatedAtWins(t *testing.T) {
	withProductionDuckDBTemplateDescriptors(t)
	env := setupProductionFederatedE2EEnv(t)
	clearProductionFederatedData(t, env, 100)

	rowID := uuid.Must(uuid.NewV7())
	now := time.Now()
	olderCreatedAt := now.Add(-72 * time.Hour).UnixMilli()
	olderUpdatedAt := now.Add(-2 * time.Hour).UnixMilli()
	newerCreatedAt := now.Add(-24 * time.Hour).UnixMilli()
	newerUpdatedAt := now.Add(-1 * time.Hour).UnixMilli()

	writeProductionParquet(t, env, "base", "updated_at_wins.parquet", []productionTestRecord{
		{
			RowID:     rowID,
			SchemaID:  100,
			Name:      "older-updated",
			Age:       21,
			Tag:       "older-tag",
			CreatedAt: newerCreatedAt,
			ChangedAt: olderUpdatedAt,
		},
		{
			RowID:     rowID,
			SchemaID:  100,
			Name:      "newer-updated",
			Age:       42,
			Tag:       "newer-tag",
			CreatedAt: olderCreatedAt,
			ChangedAt: newerUpdatedAt,
		},
	})

	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{SchemaID: 100, Limit: 10, Offset: 0},
		DuckDBHints: &DuckDBRenderHints{
			S3ParquetPathTemplate: fmt.Sprintf("s3://%s/%s/{{.SchemaID}}/base/*.parquet", env.bucket, env.prefix),
		},
	}

	var got []*PersistentRecord
	total, err := env.repo.StreamDuckDBFederatedQuery(env.ctx, env.tables, q, 10, 0, nil, nil, func(_ context.Context, record *PersistentRecord) error {
		got = append(got, record)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, int64(1), total)
	require.Len(t, got, 1)
	require.Equal(t, newerUpdatedAt, got[0].UpdatedAt)
	require.Equal(t, olderCreatedAt, got[0].CreatedAt)
	require.Equal(t, "newer-updated", got[0].TextItems["text_01"])
	require.Equal(t, int32(42), got[0].Int32Items["integer_01"])
}

func TestStreamDuckDBFederatedQuery_GivenDirtyHotRow_WhenColdVersionExists_ThenColdVersionIsExcluded(t *testing.T) {
	withProductionDuckDBTemplateDescriptors(t)
	env := setupProductionFederatedE2EEnv(t)
	clearProductionFederatedData(t, env, 100)

	rowID := uuid.Must(uuid.NewV7())
	baseTime := time.Now().Add(-48 * time.Hour).UnixMilli()
	hotTime := time.Now().Add(-1 * time.Hour).UnixMilli()

	writeProductionParquet(t, env, "base", "dirty_exclusion.parquet", []productionTestRecord{{
		RowID:     rowID,
		SchemaID:  100,
		Name:      "cold-version",
		Age:       18,
		Tag:       "cold-tag",
		ChangedAt: baseTime,
	}})
	writeProductionParquet(t, env, "delta", "empty_delta.parquet", []productionTestRecord{{
		RowID:     uuid.Must(uuid.NewV7()),
		SchemaID:  100,
		Name:      "placeholder",
		Age:       0,
		Tag:       "placeholder",
		ChangedAt: 0,
		DeletedAt: time.Now().UnixMilli(),
	}})
	insertProductionHotRecord(t, env, 100, rowID, hotTime, "dirty-hot-version", 99)

	plan := &ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
	opts := &FederatedQueryOptions{IncludeExecutionPlan: true, ExecutionPlan: plan}
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{SchemaID: 100, Limit: 10, Offset: 0},
		DuckDBHints: &DuckDBRenderHints{
			S3ParquetPathTemplate: fmt.Sprintf("s3://%s/%s/{{.SchemaID}}/base/*.parquet, s3://%s/%s/{{.SchemaID}}/delta/*.parquet", env.bucket, env.prefix, env.bucket, env.prefix),
		},
	}

	var got []*PersistentRecord
	total, err := env.repo.StreamDuckDBFederatedQuery(env.ctx, env.tables, q, 10, 0, nil, opts, func(_ context.Context, record *PersistentRecord) error {
		got = append(got, record)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, int64(1), total)
	require.Len(t, got, 1)
	require.Equal(t, hotTime, got[0].UpdatedAt)
	require.Equal(t, "dirty-hot-version", got[0].TextItems["text_01"])
	require.NotEmpty(t, opts.ExecutionPlan.Sources)
	require.Len(t, opts.ExecutionPlan.Sources, 3)
	requireExecutionPlanHasSource(t, opts.ExecutionPlan.Sources, "postgres", "dirty", nil)
	requireExecutionPlanHasSource(t, opts.ExecutionPlan.Sources, "postgres", "pushdown", nil)
	requireExecutionPlanHasSource(t, opts.ExecutionPlan.Sources, "duckdb", "template rendered", nil)
	require.Contains(t, opts.ExecutionPlan.Timings, "duckdb_fetch")
	require.Contains(t, opts.ExecutionPlan.Timings, "total")
	require.Contains(t, fmt.Sprint(opts.ExecutionPlan.Notes), "StreamDuckDBFederatedQuery started")
	require.Contains(t, fmt.Sprint(opts.ExecutionPlan.Notes), "pushdown_efficiency=")
}

func TestStreamDuckDBFederatedQuery_GivenBaseDeltaAndHotRows_WhenQueried_ThenAllNonOverlappingRowsAreReturned(t *testing.T) {
	withProductionDuckDBTemplateDescriptors(t)
	env := setupProductionFederatedE2EEnv(t)
	clearProductionFederatedData(t, env, 100)

	baseRowID := uuid.Must(uuid.NewV7())
	deltaRowID := uuid.Must(uuid.NewV7())
	hotRowID := uuid.Must(uuid.NewV7())
	now := time.Now()

	writeProductionParquet(t, env, "base", "three_tier_base.parquet", []productionTestRecord{{
		RowID:     baseRowID,
		SchemaID:  100,
		Name:      "base-record",
		Age:       11,
		Tag:       "base-tag",
		ChangedAt: now.Add(-72 * time.Hour).UnixMilli(),
	}})
	writeProductionParquet(t, env, "delta", "three_tier_delta.parquet", []productionTestRecord{{
		RowID:     deltaRowID,
		SchemaID:  100,
		Name:      "delta-record",
		Age:       22,
		Tag:       "delta-tag",
		ChangedAt: now.Add(-12 * time.Hour).UnixMilli(),
	}})
	insertProductionHotRecord(t, env, 100, hotRowID, now.UnixMilli(), "hot-record", 33)

	plan := &ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
	opts := &FederatedQueryOptions{IncludeExecutionPlan: true, ExecutionPlan: plan}
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{SchemaID: 100, Limit: 10, Offset: 0},
		DuckDBHints: &DuckDBRenderHints{
			S3ParquetPathTemplate: fmt.Sprintf("s3://%s/%s/{{.SchemaID}}/base/*.parquet, s3://%s/%s/{{.SchemaID}}/delta/*.parquet", env.bucket, env.prefix, env.bucket, env.prefix),
		},
	}

	var got []*PersistentRecord
	total, err := env.repo.StreamDuckDBFederatedQuery(env.ctx, env.tables, q, 10, 0, nil, opts, func(_ context.Context, record *PersistentRecord) error {
		got = append(got, record)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, int64(3), total)
	require.Len(t, got, 3)

	names := map[string]bool{}
	ages := map[int32]bool{}
	for _, record := range got {
		names[record.TextItems["text_01"]] = true
		ages[record.Int32Items["integer_01"]] = true
	}

	require.Equal(t, map[string]bool{
		"base-record":  true,
		"delta-record": true,
		"hot-record":   true,
	}, names)
	require.Equal(t, map[int32]bool{11: true, 22: true, 33: true}, ages)
	require.NotEmpty(t, opts.ExecutionPlan.Sources)
	require.Len(t, opts.ExecutionPlan.Sources, 3)
	requireExecutionPlanHasSource(t, opts.ExecutionPlan.Sources, "postgres", "dirty", nil)
	requireExecutionPlanHasSource(t, opts.ExecutionPlan.Sources, "postgres", "pushdown", nil)
	requireExecutionPlanHasSource(t, opts.ExecutionPlan.Sources, "duckdb", "template rendered", nil)
	require.Contains(t, opts.ExecutionPlan.Timings, "duckdb_fetch")
	require.Contains(t, opts.ExecutionPlan.Timings, "total")
	require.Contains(t, fmt.Sprint(opts.ExecutionPlan.Notes), "StreamDuckDBFederatedQuery started")
	require.Contains(t, fmt.Sprint(opts.ExecutionPlan.Notes), "pushdown_efficiency=")
}

func TestStreamDuckDBFederatedQuery_GivenBaseDeltaAndHotOverlap_WhenQueried_ThenNewestHotVersionWins(t *testing.T) {
	withProductionDuckDBTemplateDescriptors(t)
	env := setupProductionFederatedE2EEnv(t)
	clearProductionFederatedData(t, env, 100)

	rowID := uuid.Must(uuid.NewV7())
	now := time.Now()
	baseTime := now.Add(-72 * time.Hour).UnixMilli()
	deltaTime := now.Add(-24 * time.Hour).UnixMilli()
	hotTime := now.UnixMilli()

	writeProductionParquet(t, env, "base", "overlap_base.parquet", []productionTestRecord{{
		RowID:     rowID,
		SchemaID:  100,
		Name:      "base-version",
		Age:       10,
		Tag:       "base-tag",
		ChangedAt: baseTime,
	}})
	writeProductionParquet(t, env, "delta", "overlap_delta.parquet", []productionTestRecord{{
		RowID:     rowID,
		SchemaID:  100,
		Name:      "delta-version",
		Age:       20,
		Tag:       "delta-tag",
		ChangedAt: deltaTime,
	}})
	insertProductionHotRecord(t, env, 100, rowID, hotTime, "hot-version", 30)

	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{SchemaID: 100, Limit: 10, Offset: 0},
		DuckDBHints: &DuckDBRenderHints{
			S3ParquetPathTemplate: fmt.Sprintf("s3://%s/%s/{{.SchemaID}}/base/*.parquet, s3://%s/%s/{{.SchemaID}}/delta/*.parquet", env.bucket, env.prefix, env.bucket, env.prefix),
		},
	}

	var got []*PersistentRecord
	total, err := env.repo.StreamDuckDBFederatedQuery(env.ctx, env.tables, q, 10, 0, nil, nil, func(_ context.Context, record *PersistentRecord) error {
		got = append(got, record)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, int64(1), total)
	require.Len(t, got, 1)
	require.Equal(t, hotTime, got[0].UpdatedAt)
	require.Equal(t, "hot-version", got[0].TextItems["text_01"])
	require.Equal(t, int32(30), got[0].Int32Items["integer_01"])
}

func TestStreamDuckDBFederatedQuery_GivenBaseAndDeltaRowsWithoutHotData_WhenQueried_ThenColdRowsAreReturned(t *testing.T) {
	withProductionDuckDBTemplateDescriptors(t)
	env := setupProductionFederatedE2EEnv(t)
	clearProductionFederatedData(t, env, 100)

	baseRowID := uuid.Must(uuid.NewV7())
	deltaRowID := uuid.Must(uuid.NewV7())
	now := time.Now()

	writeProductionParquet(t, env, "base", "cold_only_base.parquet", []productionTestRecord{{
		RowID:     baseRowID,
		SchemaID:  100,
		Name:      "base-only",
		Age:       14,
		Tag:       "base-tag",
		ChangedAt: now.Add(-72 * time.Hour).UnixMilli(),
	}})
	writeProductionParquet(t, env, "delta", "cold_only_delta.parquet", []productionTestRecord{{
		RowID:     deltaRowID,
		SchemaID:  100,
		Name:      "delta-only",
		Age:       28,
		Tag:       "delta-tag",
		ChangedAt: now.Add(-12 * time.Hour).UnixMilli(),
	}})

	plan := &ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
	opts := &FederatedQueryOptions{IncludeExecutionPlan: true, ExecutionPlan: plan}
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{SchemaID: 100, Limit: 10, Offset: 0},
		DuckDBHints: &DuckDBRenderHints{
			S3ParquetPathTemplate: fmt.Sprintf("s3://%s/%s/{{.SchemaID}}/base/*.parquet, s3://%s/%s/{{.SchemaID}}/delta/*.parquet", env.bucket, env.prefix, env.bucket, env.prefix),
		},
	}

	var got []*PersistentRecord
	total, err := env.repo.StreamDuckDBFederatedQuery(env.ctx, env.tables, q, 10, 0, nil, opts, func(_ context.Context, record *PersistentRecord) error {
		got = append(got, record)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, int64(2), total)
	require.Len(t, got, 2)

	names := map[string]bool{}
	for _, record := range got {
		names[record.TextItems["text_01"]] = true
	}
	require.Equal(t, map[string]bool{"base-only": true, "delta-only": true}, names)
	requireExecutionPlanHasSource(t, opts.ExecutionPlan.Sources, "postgres", "dirty", nil)
	requireExecutionPlanHasSource(t, opts.ExecutionPlan.Sources, "duckdb", "template rendered", nil)
	require.Contains(t, opts.ExecutionPlan.Timings, "duckdb_fetch")
	require.Contains(t, opts.ExecutionPlan.Timings, "total")
}

func TestStreamDuckDBFederatedQuery_GivenSoftDeletedRowsAcrossMixedTiers_WhenQueried_ThenDeletedRowsAreExcluded(t *testing.T) {
	withProductionDuckDBTemplateDescriptors(t)
	env := setupProductionFederatedE2EEnv(t)
	clearProductionFederatedData(t, env, 100)

	baseDeletedRowID := uuid.Must(uuid.NewV7())
	hotDeletedRowID := uuid.Must(uuid.NewV7())
	now := time.Now()
	deletedAt := now.UnixMilli()

	writeProductionParquet(t, env, "base", "deleted_base.parquet", []productionTestRecord{{
		RowID:     baseDeletedRowID,
		SchemaID:  100,
		Name:      "deleted-base",
		Age:       7,
		Tag:       "deleted-base-tag",
		ChangedAt: now.Add(-24 * time.Hour).UnixMilli(),
		DeletedAt: deletedAt,
	}})
	writeProductionParquet(t, env, "delta", "deleted_delta.parquet", []productionTestRecord{{
		RowID:     uuid.Must(uuid.NewV7()),
		SchemaID:  100,
		Name:      "placeholder",
		Age:       0,
		Tag:       "placeholder",
		ChangedAt: 0,
		DeletedAt: deletedAt,
	}})
	insertProductionDeletedHotRecord(t, env, 100, hotDeletedRowID, now.Add(-1*time.Hour).UnixMilli(), deletedAt, "deleted-hot", 9)

	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{SchemaID: 100, Limit: 10, Offset: 0},
		DuckDBHints: &DuckDBRenderHints{
			S3ParquetPathTemplate: fmt.Sprintf("s3://%s/%s/{{.SchemaID}}/base/*.parquet, s3://%s/%s/{{.SchemaID}}/delta/*.parquet", env.bucket, env.prefix, env.bucket, env.prefix),
		},
	}

	var got []*PersistentRecord
	total, err := env.repo.StreamDuckDBFederatedQuery(env.ctx, env.tables, q, 10, 0, nil, nil, func(_ context.Context, record *PersistentRecord) error {
		got = append(got, record)
		return nil
	})

	require.NoError(t, err)
	require.Zero(t, total)
	require.Empty(t, got)
}

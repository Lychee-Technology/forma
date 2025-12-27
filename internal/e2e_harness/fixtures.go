package e2e_harness

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lychee-technology/forma/internal"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// SeedPostgres creates minimal tables and inserts seed rows (row_id 1..5).
func SeedPostgres(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS change_log (
  schema_id INTEGER,
  row_id INTEGER,
  changed_at BIGINT,
  deleted_at BIGINT,
  flushed_at INTEGER
);`,
		`CREATE TABLE IF NOT EXISTS entity_main (
  ltbase_schema_id INTEGER,
  ltbase_row_id INTEGER,
  ltbase_created_at BIGINT,
  ltbase_updated_at BIGINT,
  ltbase_deleted_at BIGINT,
  text_01 TEXT,
  integer_01 INTEGER
);`,
		`CREATE TABLE IF NOT EXISTS eav_data (
  schema_id INTEGER,
  row_id INTEGER,
  attr_id INTEGER,
  value_text TEXT
);`,
	}

	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("create table: %w", err)
		}
	}

	// Insert seeds for row_id 1..5
	now := time.Now().Unix()
	for i := 1; i <= 5; i++ {
		name := ""
		age := 20 + i
		tag := "developer"
		switch i {
		case 3:
			name = "Johnny Jr"
			age = 17
			tag = "intern"
		case 4:
			name = "Joan"
			age = 45
		case 5:
			name = "Jane"
			age = 32
		default:
			name = fmt.Sprintf("John %d", i)
		}
		if _, err := db.ExecContext(ctx, `
INSERT INTO entity_main (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at, text_01, integer_01)
VALUES ($1,$2,$3,$4,$5,$6,$7)
`, 1, i, now-int64(100*i), now-int64(50*i), 0, name, age); err != nil {
			return fmt.Errorf("insert entity_main: %w", err)
		}

		// change_log: flushed_at 0 for row 2 and 5 to simulate dirty set (example)
		flushed := 1
		if i == 2 || i == 5 {
			flushed = 0
		}
		if _, err := db.ExecContext(ctx, `
INSERT INTO change_log (schema_id, row_id, changed_at, deleted_at, flushed_at)
VALUES ($1,$2,$3,$4,$5)
`, 1, i, now-int64(10*i), 0, flushed); err != nil {
			return fmt.Errorf("insert change_log: %w", err)
		}

		// eav_data: attr_id 205 => tag
		if _, err := db.ExecContext(ctx, `
INSERT INTO eav_data (schema_id, row_id, attr_id, value_text)
VALUES ($1,$2,$3,$4)
`, 1, i, 205, tag); err != nil {
			return fmt.Errorf("insert eav_data: %w", err)
		}
	}
	return nil
}

// WriteParquetFiles creates base.parquet and delta.parquet via DuckDB by loading CSV and exporting parquet.
// It returns local paths to generated parquet files.
func WriteParquetFiles(ctx context.Context, duck *internal.DuckDBClient, outDir string) (string, string, error) {
	if duck == nil || duck.DB == nil {
		return "", "", fmt.Errorf("duckdb client is nil")
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", "", err
	}
	csvPath := filepath.Join(outDir, "base.csv")
	f, err := os.Create(csvPath)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	// write header + rows
	_, err = f.WriteString("row_id,ltbase_created_at,ltbase_updated_at,ltbase_deleted_at,name,age,tag\n")
	if err != nil {
		return "", "", err
	}
	now := time.Now().Unix()
	rows := []struct {
		id   int
		name string
		age  int
		tag  string
	}{
		{1, "John 1", 31, "developer"},
		{2, "Johnny", 26, "developer"},
		{3, "Johnny Jr", 17, "intern"},
		{4, "Joan", 45, "developer"},
		{5, "Jane", 32, "developer"},
	}
	for _, r := range rows {
		line := fmt.Sprintf("%d,%d,%d,%d,%s,%d,%s\n", r.id, now-int64(100*r.id), now-int64(50*r.id), 0, r.name, r.age, r.tag)
		if _, err := f.WriteString(line); err != nil {
			return "", "", err
		}
	}
	f.Sync()

	baseParquet := filepath.Join(outDir, "base.parquet")
	deltaParquet := filepath.Join(outDir, "delta.parquet")

	// Use DuckDB to convert CSV -> Parquet
	ctxExec, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := duck.DB.ExecContext(ctxExec, fmt.Sprintf("CREATE TABLE tmp AS SELECT * FROM read_csv_auto('%s');", csvPath)); err != nil {
		return "", "", fmt.Errorf("create tmp from csv: %w", err)
	}
	if _, err := duck.DB.ExecContext(ctxExec, fmt.Sprintf("COPY tmp TO '%s' (FORMAT PARQUET);", baseParquet)); err != nil {
		return "", "", fmt.Errorf("export base parquet: %w", err)
	}
	// For delta, write a subset (e.g., rows 3..5)
	if _, err := duck.DB.ExecContext(ctxExec, "CREATE TABLE delta_tmp AS SELECT * FROM tmp WHERE row_id >= 3;"); err != nil {
		return "", "", fmt.Errorf("create delta_tmp: %w", err)
	}
	if _, err := duck.DB.ExecContext(ctxExec, fmt.Sprintf("COPY delta_tmp TO '%s' (FORMAT PARQUET);", deltaParquet)); err != nil {
		return "", "", fmt.Errorf("export delta parquet: %w", err)
	}
	return baseParquet, deltaParquet, nil
}

/*
UploadFileToS3 is adapted for the E2E harness to support a RustFS/httpfs flow.
It copies the local parquet file into ./e2e_artifacts/<bucket>/<objectName>, which
can be served by a lightweight HTTP file server (e.g., a RustFS container mounted to the project).
This avoids adding an S3 SDK dependency in the harness while keeping the artifacts accessible
to DuckDB via httpfs or a mounted volume in CI.
*/
func UploadFileToS3(ctx context.Context, endpoint, accessKey, secretKey, bucket, objectName, filePath string) error {

	// Validate endpoint URL
	// Build AWS SDK config with static credentials and custom endpoint if provided.
	loadOpts := []func(*config.LoadOptions) error{
		config.WithRegion("us-east-1"), // region required by SDK; endpoint will be used for custom endpoints like MinIO
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	}
	if endpoint != "" {
		loadOpts = append(loadOpts, config.WithBaseEndpoint(endpoint))
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	uploader := manager.NewUploader(s3Client)

	in, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open src: %w", err)
	}
	defer in.Close()

	// Perform upload
	// ensure bucket exists
	if _, err := s3Client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		// try to create the bucket
		if _, cerr := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); cerr != nil {
			var apiErr smithy.APIError
			if errors.As(cerr, &apiErr) {
				code := apiErr.ErrorCode()
				if code != "BucketAlreadyOwnedByYou" && code != "BucketAlreadyExists" {
					return fmt.Errorf("create bucket: %w", cerr)
				}
				// ignore - bucket already exists/owned
			} else {
				return fmt.Errorf("create bucket: %w", cerr)
			}
		}
	}

	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectName),
		Body:   in,
	})
	if err != nil {
		return fmt.Errorf("s3 upload: %w", err)
	}
	return nil
}

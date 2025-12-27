package e2e_harness

import (
	"context"
	"testing"
	"time"

	forma "github.com/lychee-technology/forma"
)

func TestE2EHarnessMinimal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E harness in -short mode")
	}
	ctx := context.Background()
	h := &TestHarness{}

	// Start Postgres
	if _, err := h.StartPostgres(ctx); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer h.StopPostgres(ctx)

	// Start S3
	if _, err := h.StartS3(ctx); err != nil {
		t.Fatalf("start rustfs: %v", err)
	}
	defer h.StopS3(ctx)

	// Start DuckDB
	cfg := forma.DuckDBConfig{
		Enabled:       true,
		DBPath:        "",
		EnableS3:      true,
		EnableParquet: true,
		S3Endpoint:    h.S3Endpoint,
		S3AccessKey:   "minio",
		S3SecretKey:   "minio",
	}
	if err := h.StartDuckDB(cfg); err != nil {
		t.Fatalf("start duckdb: %v", err)
	}
	defer h.StopDuckDB()

	// Seed Postgres
	if err := SeedPostgres(ctx, h.PGDB); err != nil {
		t.Fatalf("seed postgres: %v", err)
	}

	// Generate parquet files
	tmpDir := t.TempDir()
	base, delta, err := WriteParquetFiles(ctx, h.Duck, tmpDir)
	if err != nil {
		t.Fatalf("write parquet: %v", err)
	}

	// Upload to MinIO
	if err := UploadFileToS3(ctx, h.S3Endpoint, "minio", "minio", "test-bucket", "base/base.parquet", base); err != nil {
		t.Fatalf("upload base: %v", err)
	}
	if err := UploadFileToS3(ctx, h.S3Endpoint, "minio", "minio", "test-bucket", "delta/delta.parquet", delta); err != nil {
		t.Fatalf("upload delta: %v", err)
	}

	// Simple DuckDB check: list parquet files via read_parquet
	rows, err := h.Duck.DB.QueryContext(ctx, "SELECT count(*) FROM read_parquet('s3://test-bucket/base/*.parquet');")
	if err != nil {
		t.Fatalf("duckdb read_parquet query failed: %v", err)
	}
	defer rows.Close()
	var cnt int
	if rows.Next() {
		if err := rows.Scan(&cnt); err != nil {
			t.Fatalf("scan count: %v", err)
		}
	}
	if cnt <= 0 {
		t.Fatalf("expected >0 rows from parquet, got %d", cnt)
	}

	// All good
	time.Sleep(500 * time.Millisecond)
}

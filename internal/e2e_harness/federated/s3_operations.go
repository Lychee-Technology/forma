package federated

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// WriteParquet writes records to a parquet file in S3.
func (h *FederatedTestHarness) WriteParquet(ctx context.Context, tier, filename string, records []TestRecord) error {
	if h.s3Disabled {
		return fmt.Errorf("S3 is disabled (simulated failure)")
	}

	// Build CSV content for DuckDB conversion
	csvPath := filepath.Join(h.tmpDir, "temp.csv")
	parquetPath := filepath.Join(h.tmpDir, filename)

	if err := h.writeRecordsToCSV(csvPath, records); err != nil {
		return fmt.Errorf("write csv: %w", err)
	}

	// Use DuckDB to convert CSV to Parquet
	if err := h.convertCSVToParquet(ctx, csvPath, parquetPath); err != nil {
		return err
	}

	// Upload to S3
	s3Key := fmt.Sprintf("%s/%d/%s/%s", h.S3Prefix, h.SchemaID, tier, filename)
	if err := h.uploadToS3(ctx, parquetPath, s3Key); err != nil {
		return fmt.Errorf("upload to s3: %w", err)
	}

	h.flushedFiles = append(h.flushedFiles, s3Key)
	return nil
}

// convertCSVToParquet uses DuckDB to convert a CSV file to Parquet format.
func (h *FederatedTestHarness) convertCSVToParquet(ctx context.Context, csvPath, parquetPath string) error {
	createSQL := fmt.Sprintf(`CREATE OR REPLACE TABLE temp_export AS SELECT * FROM read_csv_auto('%s')`, csvPath)
	if _, err := h.Duck.DB.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("create temp table: %w", err)
	}

	exportSQL := fmt.Sprintf(`COPY temp_export TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`, parquetPath)
	if _, err := h.Duck.DB.ExecContext(ctx, exportSQL); err != nil {
		return fmt.Errorf("export parquet: %w", err)
	}

	return nil
}

// writeRecordsToCSV writes test records to a CSV file.
func (h *FederatedTestHarness) writeRecordsToCSV(path string, records []TestRecord) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Header
	_, _ = f.WriteString("row_id,schema_id,changed_at,deleted_at,name,version\n")

	for _, r := range records {
		name := ""
		version := 0
		if v, ok := r.Attributes["name"].(string); ok {
			name = v
		}
		if v, ok := r.Attributes["version"].(int); ok {
			version = v
		}

		line := fmt.Sprintf("%s,%d,%d,%d,%s,%d\n",
			r.RowID.String(), r.SchemaID, r.ChangedAt, r.DeletedAt, name, version)
		_, _ = f.WriteString(line)
	}

	return nil
}

// uploadToS3 uploads a local file to S3.
func (h *FederatedTestHarness) uploadToS3(ctx context.Context, localPath, s3Key string) error {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}

	_, err = h.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(h.S3Bucket),
		Key:    aws.String(s3Key),
		Body:   bytes.NewReader(data),
	})
	return err
}

// ListParquetFiles lists parquet files in a tier.
func (h *FederatedTestHarness) ListParquetFiles(ctx context.Context, tier string) ([]string, error) {
	prefix := fmt.Sprintf("%s/%d/%s/", h.S3Prefix, h.SchemaID, tier)

	resp, err := h.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(h.S3Bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	var files []string
	for _, obj := range resp.Contents {
		if strings.HasSuffix(*obj.Key, ".parquet") {
			files = append(files, *obj.Key)
		}
	}
	return files, nil
}

// ReadParquetMetadata reads metadata from a parquet file.
func (h *FederatedTestHarness) ReadParquetMetadata(ctx context.Context, s3Key string) (*ParquetMetadata, error) {
	s3Path := fmt.Sprintf("s3://%s/%s", h.S3Bucket, s3Key)

	rows, err := h.Duck.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT 
			COUNT(*) as row_count,
			MIN(row_id) as row_id_min,
			MAX(row_id) as row_id_max,
			MIN(changed_at) as created_min,
			MAX(changed_at) as created_max
		FROM read_parquet('%s')
	`, s3Path))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	meta := &ParquetMetadata{}
	if rows.Next() {
		_ = rows.Scan(&meta.RowCount, &meta.RowIDMin, &meta.RowIDMax, &meta.CreatedMin, &meta.CreatedMax)
	}

	// Get file size
	head, err := h.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(h.S3Bucket),
		Key:    aws.String(s3Key),
	})
	if err == nil && head.ContentLength != nil {
		meta.SizeBytes = *head.ContentLength
	}

	return meta, nil
}

// deleteS3Prefix deletes all objects under a prefix.
func (h *FederatedTestHarness) deleteS3Prefix(ctx context.Context, prefix string) error {
	resp, err := h.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(h.S3Bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return err
	}

	for _, obj := range resp.Contents {
		_, _ = h.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(h.S3Bucket),
			Key:    obj.Key,
		})
	}
	return nil
}

// SimulateS3Failure disables S3 operations and returns a restore function.
func (h *FederatedTestHarness) SimulateS3Failure() func() {
	h.s3Disabled = true
	return func() {
		h.s3Disabled = false
	}
}

// DownloadS3File downloads a file from S3.
func (h *FederatedTestHarness) DownloadS3File(ctx context.Context, key string) ([]byte, error) {
	resp, err := h.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(h.S3Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

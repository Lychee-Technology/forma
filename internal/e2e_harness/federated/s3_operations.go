package federated

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
//
// The system columns are CAST explicitly rather than left to read_csv_auto's
// sniffer, whose choice is value-dependent: a fixture whose timestamps happen
// to be small sniffs as INTEGER, and the parquetcheck invariant requires
// exactly BIGINT. Leaving that to inference made a fixture's VALUES decide
// whether the object passed pre-read validation (#460).
func (h *FederatedTestHarness) convertCSVToParquet(ctx context.Context, csvPath, parquetPath string) error {
	createSQL := fmt.Sprintf(`CREATE OR REPLACE TABLE temp_export AS
		SELECT * REPLACE (
			CAST(ltbase_created_at AS BIGINT) AS ltbase_created_at,
			CAST(changed_at AS BIGINT) AS changed_at,
			CAST(deleted_at AS BIGINT) AS deleted_at
		)
		FROM read_csv_auto('%s')`, csvPath)
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

	attrKeys := collectCSVAttributeKeys(records)
	writer := csv.NewWriter(f)
	// ltbase_created_at mirrors the production export: both CDC exporters
	// write the row's true creation time next to the LWW version stamp, and
	// the federated reader projects it into the created_at slot (#460).
	header := []string{"row_id", "schema_id", "ltbase_created_at", "changed_at", "deleted_at", "name", "version"}
	header = append(header, attrKeys...)
	if err := writer.Write(header); err != nil {
		return err
	}

	for _, r := range records {
		row := []string{
			r.RowID.String(),
			strconv.FormatInt(int64(r.SchemaID), 10),
			strconv.FormatInt(r.CreationStamp(), 10),
			strconv.FormatInt(r.ChangedAt, 10),
			strconv.FormatInt(r.DeletedAt, 10),
			attributeStringValue(r.Attributes, "name"),
			attributeStringValue(r.Attributes, "version"),
		}
		for _, key := range attrKeys {
			row = append(row, attributeStringValue(r.Attributes, key))
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}

	return nil
}

func collectCSVAttributeKeys(records []TestRecord) []string {
	keys := make(map[string]struct{})
	for _, record := range records {
		for key := range record.Attributes {
			if key == "name" || key == "version" {
				continue
			}
			keys[key] = struct{}{}
		}
	}
	result := make([]string, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func attributeStringValue(attrs map[string]any, key string) string {
	if attrs == nil {
		return ""
	}
	value, ok := attrs[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprint(v)
	}
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

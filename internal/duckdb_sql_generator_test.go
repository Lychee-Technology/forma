package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ============================================================================
// Tests for RenderS3ParquetPath
// ============================================================================

func TestRenderS3ParquetPath_EmptyTemplate(t *testing.T) {
	result, err := RenderS3ParquetPath("", 42)
	require.Error(t, err)
	require.Equal(t, "", result)
	require.Contains(t, err.Error(), "template string is empty")
}

func TestRenderS3ParquetPath_SimpleTemplate(t *testing.T) {
	tmpl := "s3://bucket/schema_{{.SchemaID}}/data.parquet"
	result, err := RenderS3ParquetPath(tmpl, 42)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/schema_42/data.parquet", result)
}

func TestRenderS3ParquetPath_ComplexTemplate(t *testing.T) {
	tmpl := "s3://my-bucket/data/year=2024/schema_id={{.SchemaID}}/partitions/data.parquet"
	result, err := RenderS3ParquetPath(tmpl, 99)
	require.NoError(t, err)
	require.Equal(t, "s3://my-bucket/data/year=2024/schema_id=99/partitions/data.parquet", result)
}

func TestRenderS3ParquetPath_TemplateWithoutPlaceholder(t *testing.T) {
	tmpl := "s3://bucket/fixed/path/data.parquet"
	result, err := RenderS3ParquetPath(tmpl, 42)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/fixed/path/data.parquet", result)
}

func TestRenderS3ParquetPath_InvalidTemplate(t *testing.T) {
	// Invalid Go template syntax
	tmpl := "s3://bucket/schema_{{.SchemaID/data.parquet"
	result, err := RenderS3ParquetPath(tmpl, 42)
	require.Error(t, err)
	require.Equal(t, "", result)
	require.Contains(t, err.Error(), "parse template")
}

func TestRenderS3ParquetPath_ZeroSchemaID(t *testing.T) {
	tmpl := "s3://bucket/schema_{{.SchemaID}}/data.parquet"
	result, err := RenderS3ParquetPath(tmpl, 0)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/schema_0/data.parquet", result)
}

func TestRenderS3ParquetPath_LargeSchemaID(t *testing.T) {
	tmpl := "s3://bucket/schema_{{.SchemaID}}/data.parquet"
	result, err := RenderS3ParquetPath(tmpl, 32767) // max int16
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/schema_32767/data.parquet", result)
}

func TestRenderS3ParquetPath_NegativeSchemaID(t *testing.T) {
	tmpl := "s3://bucket/schema_{{.SchemaID}}/data.parquet"
	result, err := RenderS3ParquetPath(tmpl, -1)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/schema_-1/data.parquet", result)
}

func TestRenderS3ParquetPath_MultipleOccurrences(t *testing.T) {
	// Template with multiple SchemaID placeholders
	tmpl := "s3://bucket/from={{.SchemaID}}/to={{.SchemaID}}/data.parquet"
	result, err := RenderS3ParquetPath(tmpl, 42)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/from=42/to=42/data.parquet", result)
}

func TestRenderS3ParquetPath_GCSPath(t *testing.T) {
	// Test with GCS-style path (even though function name says S3)
	tmpl := "gs://bucket/schema_{{.SchemaID}}/data.parquet"
	result, err := RenderS3ParquetPath(tmpl, 42)
	require.NoError(t, err)
	require.Equal(t, "gs://bucket/schema_42/data.parquet", result)
}

func TestRenderS3ParquetPath_LocalFilePath(t *testing.T) {
	// Test with local file path
	tmpl := "/local/path/schema_{{.SchemaID}}/data.parquet"
	result, err := RenderS3ParquetPath(tmpl, 42)
	require.NoError(t, err)
	require.Equal(t, "/local/path/schema_42/data.parquet", result)
}

func TestRenderS3ParquetPath_UndefinedField(t *testing.T) {
	// Template referencing a field that doesn't exist in the data struct
	tmpl := "s3://bucket/schema_{{.NonExistentField}}/data.parquet"
	result, err := RenderS3ParquetPath(tmpl, 42)
	require.NoError(t, err)
	// Go templates render undefined fields as "<no value>"
	require.Equal(t, "s3://bucket/schema_<no value>/data.parquet", result)
}

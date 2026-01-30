package internal

import (
	"errors"
	"testing"

	"github.com/lychee-technology/forma"
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

// ============================================================================
// Tests for BuildListPredicate
// ============================================================================

func TestBuildListPredicate_Equals(t *testing.T) {
	sql, param, err := BuildListPredicate("tags", "equals", "foo", forma.ValueTypeText)
	require.NoError(t, err)
	require.Equal(t, "list_contains(tags, ?)", sql)
	require.Equal(t, "foo", param)
}

func TestBuildListPredicate_EqualsNumeric(t *testing.T) {
	sql, param, err := BuildListPredicate("scores", "equals", "42", forma.ValueTypeInteger)
	require.NoError(t, err)
	require.Equal(t, "list_contains(scores, CAST(? AS INTEGER))", sql)
	require.Equal(t, "42", param)
}

func TestBuildListPredicate_NotEquals(t *testing.T) {
	sql, param, err := BuildListPredicate("tags", "not_equals", "bar", forma.ValueTypeText)
	require.NoError(t, err)
	require.Equal(t, "NOT list_contains(tags, ?)", sql)
	require.Equal(t, "bar", param)
}

func TestBuildListPredicate_Contains(t *testing.T) {
	sql, param, err := BuildListPredicate("names", "contains", "john", forma.ValueTypeText)
	require.NoError(t, err)
	require.Equal(t, "list_any_match(names, x -> x LIKE ?)", sql)
	require.Equal(t, "%john%", param)
}

func TestBuildListPredicate_StartsWith(t *testing.T) {
	sql, param, err := BuildListPredicate("prefixes", "starts_with", "pre", forma.ValueTypeText)
	require.NoError(t, err)
	require.Equal(t, "list_any_match(prefixes, x -> x LIKE ?)", sql)
	require.Equal(t, "pre%", param)
}

func TestBuildListPredicate_Gt(t *testing.T) {
	sql, param, err := BuildListPredicate("values", "gt", "10", forma.ValueTypeNumeric)
	require.NoError(t, err)
	require.Equal(t, "list_any_match(values, x -> x > CAST(? AS DOUBLE))", sql)
	require.Equal(t, "10", param)
}

func TestBuildListPredicate_Gte(t *testing.T) {
	sql, param, err := BuildListPredicate("values", "gte", "5", forma.ValueTypeInteger)
	require.NoError(t, err)
	require.Equal(t, "list_any_match(values, x -> x >= CAST(? AS INTEGER))", sql)
	require.Equal(t, "5", param)
}

func TestBuildListPredicate_Lt(t *testing.T) {
	sql, param, err := BuildListPredicate("values", "lt", "100", forma.ValueTypeBigInt)
	require.NoError(t, err)
	require.Equal(t, "list_any_match(values, x -> x < CAST(? AS BIGINT))", sql)
	require.Equal(t, "100", param)
}

func TestBuildListPredicate_Lte(t *testing.T) {
	sql, param, err := BuildListPredicate("values", "lte", "50", forma.ValueTypeSmallInt)
	require.NoError(t, err)
	require.Equal(t, "list_any_match(values, x -> x <= CAST(? AS SMALLINT))", sql)
	require.Equal(t, "50", param)
}

func TestBuildListPredicate_UnsupportedOperator(t *testing.T) {
	_, _, err := BuildListPredicate("tags", "regex", ".*", forma.ValueTypeText)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported operator 'regex' for LIST column")
}

// ============================================================================
// Tests for ValidateOrderByForListTypes
// ============================================================================

func TestValidateOrderByForListTypes_NoListTypes(t *testing.T) {
	orderBy := []forma.OrderBy{
		{Attribute: "name", SortOrder: forma.SortOrderAsc},
		{Attribute: "age", SortOrder: forma.SortOrderDesc},
	}
	getType := func(attr string) (forma.ValueType, bool) {
		switch attr {
		case "name":
			return forma.ValueTypeText, true
		case "age":
			return forma.ValueTypeInteger, true
		}
		return "", false
	}
	err := ValidateOrderByForListTypes(orderBy, getType)
	require.NoError(t, err)
}

func TestValidateOrderByForListTypes_WithListType(t *testing.T) {
	orderBy := []forma.OrderBy{
		{Attribute: "name", SortOrder: forma.SortOrderAsc},
		{Attribute: "tags", SortOrder: forma.SortOrderAsc},
	}
	getType := func(attr string) (forma.ValueType, bool) {
		switch attr {
		case "name":
			return forma.ValueTypeText, true
		case "tags":
			return forma.ValueTypeList, true
		}
		return "", false
	}
	err := ValidateOrderByForListTypes(orderBy, getType)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrListInOrderBy))
	require.Contains(t, err.Error(), "tags")
}

func TestValidateOrderByForListTypes_EmptyOrderBy(t *testing.T) {
	getType := func(attr string) (forma.ValueType, bool) {
		return forma.ValueTypeList, true
	}
	err := ValidateOrderByForListTypes(nil, getType)
	require.NoError(t, err)
}

func TestValidateOrderByForListTypes_UnknownAttribute(t *testing.T) {
	orderBy := []forma.OrderBy{
		{Attribute: "unknown", SortOrder: forma.SortOrderAsc},
	}
	getType := func(attr string) (forma.ValueType, bool) {
		return "", false // attribute not found
	}
	err := ValidateOrderByForListTypes(orderBy, getType)
	require.NoError(t, err) // unknown attributes are not flagged as LIST
}

// ============================================================================
// Tests for ValidateOrderByAttributesForListTypes
// ============================================================================

func TestValidateOrderByAttributesForListTypes_NoListTypes(t *testing.T) {
	orderBy := []AttributeOrder{
		{AttrID: 1, ValueType: forma.ValueTypeText},
		{AttrID: 2, ValueType: forma.ValueTypeInteger},
	}
	err := ValidateOrderByAttributesForListTypes(orderBy)
	require.NoError(t, err)
}

func TestValidateOrderByAttributesForListTypes_WithListType(t *testing.T) {
	orderBy := []AttributeOrder{
		{AttrID: 1, ValueType: forma.ValueTypeText},
		{AttrID: 2, ValueType: forma.ValueTypeList},
	}
	err := ValidateOrderByAttributesForListTypes(orderBy)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrListInOrderBy))
	require.Contains(t, err.Error(), "2")
}

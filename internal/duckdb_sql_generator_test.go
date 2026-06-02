package internal

import (
	"errors"
	"testing"
	"time"

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

// ============================================================================
// Focused helper behavior tests
// ============================================================================

func TestDuckDBSQLOperator_RewritesLikePatterns(t *testing.T) {
	tests := []struct {
		name      string
		op        string
		value     string
		wantOp    string
		wantValue string
	}{
		{name: "equals", op: "equals", value: "alice", wantOp: "=", wantValue: "alice"},
		{name: "starts with", op: "starts_with", value: "pre", wantOp: "LIKE", wantValue: "pre%"},
		{name: "contains", op: "contains", value: "mid", wantOp: "LIKE", wantValue: "%mid%"},
		{name: "not equals", op: "not_equals", value: "x", wantOp: "!=", wantValue: "x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOp, gotValue, err := duckDBSQLOperator(tt.op, tt.value)
			require.NoError(t, err)
			require.Equal(t, tt.wantOp, gotOp)
			require.Equal(t, tt.wantValue, gotValue)
		})
	}

	_, _, err := duckDBSQLOperator("regex", ".*")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported operator")
}

func TestDetectDuckDBValueType_RecognizesTemporalAndScalarValues(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want forma.ValueType
	}{
		{name: "bool true", raw: "true", want: forma.ValueTypeBool},
		{name: "uuid", raw: "550e8400-e29b-41d4-a716-446655440000", want: forma.ValueTypeUUID},
		{name: "rfc3339", raw: "2020-01-02T03:04:05Z", want: forma.ValueTypeDateTime},
		{name: "epoch millis", raw: "1700000000000", want: forma.ValueTypeDateTime},
		{name: "float", raw: "42.5", want: forma.ValueTypeNumeric},
		{name: "text", raw: "hello", want: forma.ValueTypeText},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, detectDuckDBValueType(tt.raw))
		})
	}
}

func TestParseDuckDBParamValue_ParsesTemporalAndBooleanValues(t *testing.T) {
	t.Run("rfc3339 datetime", func(t *testing.T) {
		got := parseDuckDBParamValue("2020-01-02T03:04:05Z", forma.ValueTypeDateTime)
		require.IsType(t, time.Time{}, got)
		require.Equal(t, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), got)
	})

	t.Run("epoch millis datetime", func(t *testing.T) {
		got := parseDuckDBParamValue("1700000000000", forma.ValueTypeDateTime)
		require.IsType(t, time.Time{}, got)
		require.Equal(t, time.UnixMilli(1700000000000).UTC(), got)
	})

	t.Run("bool one", func(t *testing.T) {
		got := parseDuckDBParamValue("1", forma.ValueTypeBool)
		require.Equal(t, true, got)
	})
}

func TestGenerateDuckDBWhereClause_GivenBareRFC3339Literal_WhenClauseBuilt_ThenItDefaultsToEqualsTimestamp(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "created_at",
				Value: "2020-01-02T03:04:05Z",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(q)
	require.NoError(t, err)
	require.Equal(t, "created_at = CAST(? AS TIMESTAMP)", clause)
	require.Len(t, args, 1)
	require.Equal(t, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), args[0])
}

func TestGenerateDuckDBWhereClause_GivenEpochMillisLiteral_WhenClauseBuilt_ThenItUsesTimestampCast(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "created_at",
				Value: "1700000000000",
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(q)
	require.NoError(t, err)
	require.Equal(t, "created_at = CAST(? AS TIMESTAMP)", clause)
	require.Len(t, args, 1)
	require.Equal(t, time.UnixMilli(1700000000000).UTC(), args[0])
}

func TestGenerateDuckDBWhereClause_GivenNestedAndOrConditions_WhenClauseBuilt_ThenGroupingAndArgumentOrderArePreserved(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.CompositeCondition{
				Logic: forma.LogicAnd,
				Conditions: []forma.Condition{
					&forma.KvCondition{Attr: "status", Value: "active"},
					&forma.CompositeCondition{
						Logic: forma.LogicOr,
						Conditions: []forma.Condition{
							&forma.KvCondition{Attr: "score", Value: "gt:10"},
							&forma.KvCondition{Attr: "email", Value: "starts_with:ops"},
						},
					},
				},
			},
		},
	}

	clause, args, err := GenerateDuckDBWhereClause(q)
	require.NoError(t, err)
	require.Equal(t, "(status = ?) AND ((score > CAST(? AS DOUBLE)) OR (email LIKE ?))", clause)
	require.Equal(t, []any{"active", 10.0, "ops%"}, args)
}

// TestEAVValueColumn_BoolUsesValueNumeric verifies that eavValueColumn returns
// "value_numeric" for ValueTypeBool, matching the storage path where booleans
// are written as float64 into value_numeric (see attribute_converter.go).
func TestEAVValueColumn_BoolUsesValueNumeric(t *testing.T) {
	got := eavValueColumn(forma.ValueTypeBool)
	require.Equal(t, "value_numeric", got,
		"bool EAV values are stored as float64 in value_numeric, not value_text")
}

// TestBuildSchemaProjection_BoolEAVOnly_PivotReturnsBoolExpr verifies that an
// EAV-only bool attribute produces a boolean-typed pivot expression
// "(MAX(CASE WHEN attr_id = X THEN value_numeric END) <> 0) AS active" so that
// the unified column type is BOOLEAN and DuckDB WHERE comparisons are type-safe.
func TestBuildSchemaProjection_BoolEAVOnly_PivotReturnsBoolExpr(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"active": forma.AttributeMetadata{
			AttributeID: 7,
			ValueType:   forma.ValueTypeBool,
			// No ColumnBinding → EAV-only
		},
	}
	sp, err := BuildSchemaProjection(1, cache)
	require.NoError(t, err)

	// EAV pivot must use a boolean comparison, not raw value_numeric
	require.Contains(t, sp.EAVPivotSelect,
		"(MAX(CASE WHEN attr_id = 7 THEN value_numeric END) <> 0) AS active",
		"bool EAV pivot expression must be boolean-typed via <> 0 comparison")

	// Must NOT contain a raw (non-wrapped) value_numeric for active
	require.NotContains(t, sp.EAVPivotSelect,
		"THEN value_numeric END) AS active",
		"raw value_numeric pivot must not be used for bool; wrap with <> 0")
}

// TestBuildSchemaProjection_BoolColumnBound_SmallintCOALESCE verifies that a
// bool_smallint column-bound attribute emits a type-safe COALESCE expression
// "COALESCE(hot_vals.active, m.smallint_01 <> 0) AS active" in PGSourceSelect,
// so that both sides of the COALESCE are BOOLEAN and there is no type mismatch.
func TestBuildSchemaProjection_BoolColumnBound_SmallintCOALESCE(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"active": forma.AttributeMetadata{
			AttributeID: 7,
			ValueType:   forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("smallint_01"),
				Encoding:   forma.MainColumnEncodingBoolInt,
			},
		},
	}
	sp, err := BuildSchemaProjection(1, cache)
	require.NoError(t, err)

	// PGSourceSelect must normalize the main col to BOOLEAN via <> 0
	require.Contains(t, sp.PGSourceSelect,
		"COALESCE(hot_vals.active, m.smallint_01 <> 0) AS active",
		"bool_smallint column-bound COALESCE must normalize main col to boolean")

	// EAV pivot must also produce a boolean expression for this attribute
	require.Contains(t, sp.EAVPivotSelect,
		"(MAX(CASE WHEN attr_id = 7 THEN value_numeric END) <> 0) AS active",
		"bool column-bound EAV pivot must also be boolean-typed")
}

// TestBuildSchemaProjection_BoolColumnBound_TextCOALESCE verifies that a
// bool_text column-bound attribute emits "COALESCE(hot_vals.active, m.text_01 = '1')
// AS active" in PGSourceSelect, normalising the text main column to BOOLEAN.
func TestBuildSchemaProjection_BoolColumnBound_TextCOALESCE(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"active": forma.AttributeMetadata{
			AttributeID: 7,
			ValueType:   forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumn("text_01"),
				Encoding:   forma.MainColumnEncodingBoolText,
			},
		},
	}
	sp, err := BuildSchemaProjection(1, cache)
	require.NoError(t, err)

	// PGSourceSelect must normalize the text main col to BOOLEAN via = '1'
	require.Contains(t, sp.PGSourceSelect,
		"COALESCE(hot_vals.active, m.text_01 = '1') AS active",
		"bool_text column-bound COALESCE must normalize main col to boolean")
}

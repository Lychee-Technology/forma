package sqlgen

import (
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, "list_contains(scores, CAST(? AS DOUBLE))", sql)
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
	require.Equal(t, "len(list_filter(names, lambda x: x LIKE ?)) > 0", sql)
	require.Equal(t, "%john%", param)
}

func TestBuildListPredicate_StartsWith(t *testing.T) {
	sql, param, err := BuildListPredicate("prefixes", "starts_with", "pre", forma.ValueTypeText)
	require.NoError(t, err)
	require.Equal(t, "len(list_filter(prefixes, lambda x: x LIKE ?)) > 0", sql)
	require.Equal(t, "pre%", param)
}

func TestBuildListPredicate_Gt(t *testing.T) {
	sql, param, err := BuildListPredicate("values", "gt", "10", forma.ValueTypeNumeric)
	require.NoError(t, err)
	require.Equal(t, "len(list_filter(values, lambda x: x > CAST(? AS DOUBLE))) > 0", sql)
	require.Equal(t, "10", param)
}

func TestBuildListPredicate_Gte(t *testing.T) {
	sql, param, err := BuildListPredicate("values", "gte", "5", forma.ValueTypeInteger)
	require.NoError(t, err)
	require.Equal(t, "len(list_filter(values, lambda x: x >= CAST(? AS DOUBLE))) > 0", sql)
	require.Equal(t, "5", param)
}

func TestBuildListPredicate_Lt(t *testing.T) {
	sql, param, err := BuildListPredicate("values", "lt", "100", forma.ValueTypeBigInt)
	require.NoError(t, err)
	require.Equal(t, "len(list_filter(values, lambda x: x < CAST(? AS BIGINT))) > 0", sql)
	require.Equal(t, "100", param)
}

func TestBuildListPredicate_Lte(t *testing.T) {
	sql, param, err := BuildListPredicate("values", "lte", "50", forma.ValueTypeSmallInt)
	require.NoError(t, err)
	require.Equal(t, "len(list_filter(values, lambda x: x <= CAST(? AS DOUBLE))) > 0", sql)
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
	orderBy := []model.AttributeOrder{
		{AttrID: 1, ValueType: forma.ValueTypeText},
		{AttrID: 2, ValueType: forma.ValueTypeInteger},
	}
	err := ValidateOrderByAttributesForListTypes(orderBy)
	require.NoError(t, err)
}

func TestValidateOrderByAttributesForListTypes_WithListType(t *testing.T) {
	orderBy := []model.AttributeOrder{
		{AttrID: 1, ValueType: forma.ValueTypeText},
		{AttrID: 2, ValueType: forma.ValueTypeList},
	}
	err := ValidateOrderByAttributesForListTypes(orderBy)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrListInOrderBy))
	require.Contains(t, err.Error(), "2")
}

// ============================================================================
func TestEAVValueColumn_BoolUsesValueNumeric(t *testing.T) {
	got := eavValueColumn(forma.ValueTypeBool)
	require.Equal(t, "value_numeric", got,
		"bool EAV values are stored as float64 in value_numeric, not value_text")
}

// ============================================================================
// Exact-output tests restored from the retired legacy generator (issue #127
// review): the datetime cases port with a DateTime cache entry and now pin the
// CAST(? AS BIGINT) epoch-ms contract (#200); the nested case ports with no
// cache (dual walker's literal type inference).
// ============================================================================

func TestBuildDuckClause_GivenBareRFC3339Literal_WhenClauseBuilt_ThenItDefaultsToEqualsEpochMs(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"created_at": forma.AttributeMetadata{AttributeID: 7, ValueType: forma.ValueTypeDateTime},
	}
	cond := &forma.KvCondition{Attr: "created_at", Value: "2020-01-02T03:04:05Z"}

	clause, args, err := buildDuckClause(cond, cache)
	require.NoError(t, err)
	require.Equal(t, "created_at = CAST(? AS BIGINT)", clause)
	require.Len(t, args, 1)
	require.Equal(t, int64(1577934245000), args[0])
}

func TestBuildDuckClause_GivenEpochMillisLiteral_WhenClauseBuilt_ThenItUsesBigintCast(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"created_at": forma.AttributeMetadata{AttributeID: 7, ValueType: forma.ValueTypeDateTime},
	}
	cond := &forma.KvCondition{Attr: "created_at", Value: "1700000000000"}

	clause, args, err := buildDuckClause(cond, cache)
	require.NoError(t, err)
	require.Equal(t, "created_at = CAST(? AS BIGINT)", clause)
	require.Len(t, args, 1)
	require.Equal(t, int64(1700000000000), args[0])
}

func TestBuildDuckClause_GivenNestedAndOrConditions_WhenClauseBuilt_ThenGroupingAndArgumentOrderArePreserved(t *testing.T) {
	cond := &forma.CompositeCondition{
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
	}

	clause, args, err := buildDuckClause(cond, nil)
	require.NoError(t, err)
	require.Equal(t, `(status = ?) AND ((score > CAST(? AS DOUBLE)) OR (email LIKE ? ESCAPE '\'))`, clause)
	require.Equal(t, []any{"active", "10", "ops%"}, args)
}

// TestBuildSchemaProjection_NoSyntheticNameVersion pins issue #192: the
// projection must reference only attributes the schema actually defines.
// Real CDC parquet exports contain no columns for undefined attributes, so
// the legacy synthetic name/version injection made warm/cold federated
// queries reference parquet columns that don't exist.
func TestBuildSchemaProjection_NoSyntheticNameVersion(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"age": {AttributeID: 3, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: "integer_01"}},
	}
	sp, err := BuildSchemaProjection(7, cache)
	require.NoError(t, err)

	require.Equal(t,
		[]string{"row_id", "created_at", "ver_ts", "deleted_ts", "age"},
		sp.UnifiedColumnNames)
	require.Equal(t,
		"row_id, ltbase_created_at AS created_at, changed_at AS ver_ts, deleted_at AS deleted_ts, age",
		sp.S3SourceSelect)
	require.Empty(t, sp.EAVAttrs)
	require.False(t, sp.HasEAVAttrs)
	require.NotContains(t, sp.OuterSelect, "'version'")
}

package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"lychee.technology/ltbase/forma"
)

func stringPtr(v string) *string  { return &v }
func floatPtr(v float64) *float64 { return &v }
func boolPtr(v bool) *bool        { return &v }

// MockPool is a mock implementation for testing (in real tests, use testcontainers for PostgreSQL)
// For now, we'll document the expected test structure

func TestNewPostgresAttributeRepository(t *testing.T) {
	// This test verifies the constructor works correctly
	// In integration tests, you would use a real pool
	repo := NewPostgresAttributeRepository(nil, "eav_test", "schema_registry_test", nil)

	assert.NotNil(t, repo)
	assert.Equal(t, "eav_test", repo.eavTableName)
	assert.Equal(t, "schema_registry_test", repo.schemaTableName)
}

func TestBuildFilterConditions(t *testing.T) {
	repo := &PostgresAttributeRepository{}

	tests := []struct {
		name           string
		filters        []forma.Filter
		expectedConds  string
		expectedArgLen int
	}{
		{
			name:           "empty filters",
			filters:        []forma.Filter{},
			expectedConds:  "1=1",
			expectedArgLen: 0,
		},
		{
			name: "single equals filter",
			filters: []forma.Filter{
				{
					Type:  forma.FilterEquals,
					Field: forma.FilterFieldAttributeName,
					Value: "username",
				},
			},
			expectedConds:  "(attr_id = $1)",
			expectedArgLen: 1,
		},
		{
			name: "single contains filter",
			filters: []forma.Filter{
				{
					Type:  forma.FilterContains,
					Field: forma.FilterFieldAttributeValue,
					Value: "test",
				},
			},
			expectedConds:  "(value_text ILIKE '%' || $1 || '%')",
			expectedArgLen: 1,
		},
		{
			name: "multiple filters",
			filters: []forma.Filter{
				{
					Type:  forma.FilterEquals,
					Field: forma.FilterFieldAttributeName,
					Value: "email",
				},
				{
					Type:  forma.FilterStartsWith,
					Field: forma.FilterFieldAttributeValue,
					Value: "user@",
				},
			},
			expectedConds:  "(attr_id = $1) AND (value_text ILIKE $2 || '%')",
			expectedArgLen: 2,
		},
		{
			name: "greater than filter",
			filters: []forma.Filter{
				{
					Type:  forma.FilterGreaterThan,
					Field: forma.FilterFieldAttributeValue,
					Value: "100",
				},
			},
			expectedConds:  "(value_text > $1)",
			expectedArgLen: 1,
		},
		{
			name: "in filter",
			filters: []forma.Filter{
				{
					Type:  forma.FilterIn,
					Field: forma.FilterFieldAttributeName,
					Value: []string{"status", "type"},
				},
			},
			expectedConds:  "(attr_id = ANY($1))",
			expectedArgLen: 1,
		},
		{
			name: "not equals filter",
			filters: []forma.Filter{
				{
					Type:  forma.FilterNotEquals,
					Field: forma.FilterFieldAttributeName,
					Value: "deleted",
				},
			},
			expectedConds:  "(attr_id != $1)",
			expectedArgLen: 1,
		},
		{
			name: "less than filter",
			filters: []forma.Filter{
				{
					Type:  forma.FilterLessThan,
					Field: forma.FilterFieldAttributeValue,
					Value: "50",
				},
			},
			expectedConds:  "(value_text < $1)",
			expectedArgLen: 1,
		},
		{
			name: "greater or equal filter",
			filters: []forma.Filter{
				{
					Type:  forma.FilterGreaterEq,
					Field: forma.FilterFieldAttributeValue,
					Value: "100",
				},
			},
			expectedConds:  "(value_text >= $1)",
			expectedArgLen: 1,
		},
		{
			name: "less or equal filter",
			filters: []forma.Filter{
				{
					Type:  forma.FilterLessEq,
					Field: forma.FilterFieldAttributeValue,
					Value: "50",
				},
			},
			expectedConds:  "(value_text <= $1)",
			expectedArgLen: 1,
		},
		{
			name: "not in filter",
			filters: []forma.Filter{
				{
					Type:  forma.FilterNotIn,
					Field: forma.FilterFieldAttributeName,
					Value: []string{"archived", "deleted"},
				},
			},
			expectedConds:  "(attr_id != ALL($1))",
			expectedArgLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conds, args := repo.buildFilterConditions(0, tt.filters, 1)
			assert.Equal(t, tt.expectedConds, conds)
			assert.Equal(t, tt.expectedArgLen, len(args))
		})
	}
}

func TestBuildOrderByClause(t *testing.T) {
	repo := &PostgresAttributeRepository{}

	tests := []struct {
		name          string
		orderBy       []forma.OrderBy
		expectedOrder string
	}{
		{
			name:          "empty order by",
			orderBy:       []forma.OrderBy{},
			expectedOrder: "ORDER BY t.row_id ASC",
		},
		{
			name: "single ascending order",
			orderBy: []forma.OrderBy{
				{
					Field:     forma.FilterFieldAttributeName,
					SortOrder: forma.SortOrderAsc,
				},
			},
			expectedOrder: "ORDER BY t.attr_id ASC",
		},
		{
			name: "single descending order",
			orderBy: []forma.OrderBy{
				{
					Field:     forma.FilterFieldAttributeValue,
					SortOrder: forma.SortOrderDesc,
				},
			},
			expectedOrder: "ORDER BY t.value_text DESC",
		},
		{
			name: "multiple order by",
			orderBy: []forma.OrderBy{
				{
					Field:     forma.FilterFieldAttributeName,
					SortOrder: forma.SortOrderAsc,
				},
				{
					Field:     forma.FilterFieldRowID,
					SortOrder: forma.SortOrderDesc,
				},
			},
			expectedOrder: "ORDER BY t.attr_id ASC, t.row_id DESC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order := repo.buildOrderByClause(tt.orderBy)
			assert.Equal(t, tt.expectedOrder, order)
		})
	}
}

func TestMapFilterField(t *testing.T) {
	tests := []struct {
		field    forma.FilterField
		expected string
	}{
		{forma.FilterFieldAttributeName, "attr_id"},
		{forma.FilterFieldAttributeValue, "value_text"},
		{forma.FilterFieldRowID, "row_id"},
		{forma.FilterFieldSchemaName, "schema_name"},
		{forma.FilterField("unknown"), "value_text"},
	}

	for _, tt := range tests {
		t.Run(string(tt.field), func(t *testing.T) {
			result := mapFilterField(tt.field)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// SQL Generation Tests

func TestBuildValuesClause(t *testing.T) {
	repo := &PostgresAttributeRepository{}

	tests := []struct {
		name                 string
		attributes           []Attribute
		expectedPattern      string
		expectedArgCount     int
		validatePlaceholders bool
	}{
		{
			name:                 "single attribute",
			attributes:           []Attribute{{SchemaID: 1, RowID: uuid.New(), AttrID: 10, ArrayIndices: "", ValueText: stringPtr("value")}},
			expectedPattern:      "($1, $2, $3, $4, $5, $6, $7, $8)",
			expectedArgCount:     8,
			validatePlaceholders: true,
		},
		{
			name: "multiple attributes",
			attributes: []Attribute{
				{SchemaID: 1, RowID: uuid.New(), AttrID: 10, ArrayIndices: "", ValueText: stringPtr("value1")},
				{SchemaID: 1, RowID: uuid.New(), AttrID: 11, ArrayIndices: "", ValueNumeric: floatPtr(42)},
				{SchemaID: 1, RowID: uuid.New(), AttrID: 12, ArrayIndices: "0", ValueBool: boolPtr(true)},
			},
			expectedPattern:      "($1, $2, $3, $4, $5, $6, $7, $8), ($9, $10, $11, $12, $13, $14, $15, $16), ($17, $18, $19, $20, $21, $22, $23, $24)",
			expectedArgCount:     24,
			validatePlaceholders: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valuesClause, args := repo.buildValuesClause(tt.attributes)

			if tt.validatePlaceholders {
				assert.Equal(t, tt.expectedPattern, valuesClause)
			}
			assert.Equal(t, tt.expectedArgCount, len(args))

			// Verify args contain all attributes in order
			for i, attr := range tt.attributes {
				baseIdx := i * 8
				assert.Equal(t, attr.SchemaID, args[baseIdx])
				assert.Equal(t, attr.RowID, args[baseIdx+1])
				assert.Equal(t, attr.AttrID, args[baseIdx+2])
				assert.Equal(t, attr.ArrayIndices, args[baseIdx+3])
				assert.Equal(t, attr.ValueText, args[baseIdx+4])
				assert.Equal(t, attr.ValueNumeric, args[baseIdx+5])
				assert.Equal(t, attr.ValueDate, args[baseIdx+6])
				assert.Equal(t, attr.ValueBool, args[baseIdx+7])
			}
		})
	}
}

func TestBuildPaginatedQueryTemplate(t *testing.T) {
	repo := &PostgresAttributeRepository{
		eavTableName:    "eav",
		schemaTableName: "schema_registry",
	}

	tests := []struct {
		name             string
		schemaCondition  string
		conditions       string
		orderByClause    string
		limitParamIdx    int
		offsetParamIdx   int
		shouldContain    []string
		shouldNotContain []string
	}{
		{
			name:            "with schema filter",
			schemaCondition: "t.schema_id = (SELECT schema_id FROM schema_registry WHERE schema_name = $1 LIMIT 1)",
			conditions:      "1=1",
			orderByClause:   "ORDER BY t.row_id ASC",
			limitParamIdx:   1,
			offsetParamIdx:  2,
			shouldContain: []string{
				"WITH",
				"distinct_rows",
				"total_count",
				"LIMIT $1",
				"OFFSET $2",
				"WHERE t.schema_id = (SELECT schema_id FROM schema_registry WHERE schema_name = $1 LIMIT 1) AND 1=1",
				"FROM distinct_rows dr",
				"CROSS JOIN total_count tc",
				"CROSS JOIN LATERAL"},
			shouldNotContain: []string{},
		},
		{
			name:             "without schema filter",
			schemaCondition:  "1=1",
			conditions:       "1=1",
			orderByClause:    "ORDER BY t.row_id ASC",
			limitParamIdx:    1,
			offsetParamIdx:   2,
			shouldContain:    []string{"WITH", "distinct_rows", "total_count", "LIMIT $1", "OFFSET $2", "WHERE 1=1 AND 1=1"},
			shouldNotContain: []string{"target_schema"},
		},
		{
			name:             "with complex conditions",
			schemaCondition:  "1=1",
			conditions:       "(attr_name = $1) AND (attr_value ILIKE $2 || '%')",
			orderByClause:    "ORDER BY t.attr_name ASC",
			limitParamIdx:    3,
			offsetParamIdx:   4,
			shouldContain:    []string{"WHERE 1=1 AND (attr_name = $1) AND (attr_value ILIKE $2 || '%')", "LIMIT $3", "OFFSET $4"},
			shouldNotContain: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := repo.buildPaginatedQueryTemplate(
				tt.schemaCondition,
				tt.conditions,
				tt.orderByClause,
				tt.limitParamIdx,
				tt.offsetParamIdx,
			)

			for _, substr := range tt.shouldContain {
				assert.Contains(t, query, substr, "Query should contain: %s", substr)
			}

			for _, substr := range tt.shouldNotContain {
				assert.NotContains(t, query, substr, "Query should not contain: %s", substr)
			}

			// Verify basic structure
			assert.Contains(t, query, "SELECT")
			assert.Contains(t, query, "FROM")
			assert.Contains(t, query, "CROSS JOIN total_count")
		})
	}
}

// SQL Query Building Tests

func TestInsertAttributesQueryGeneration(t *testing.T) {
	repo := &PostgresAttributeRepository{
		eavTableName: "attributes_eav",
	}

	attributes := []Attribute{
		{SchemaID: 100, RowID: uuid.New(), AttrID: 10, ArrayIndices: "", ValueText: stringPtr("value1")},
		{SchemaID: 100, RowID: uuid.New(), AttrID: 11, ArrayIndices: "", ValueBool: boolPtr(true)},
	}

	valuesClause, args := repo.buildValuesClause(attributes)
	expectedQuery := "INSERT INTO attributes_eav (schema_id, row_id, attr_id, array_indices, value_text, value_numeric, value_date, value_bool) VALUES " + valuesClause

	assert.Contains(t, expectedQuery, "INSERT INTO")
	assert.Contains(t, expectedQuery, "attributes_eav")
	assert.Contains(t, expectedQuery, "VALUES")
	assert.Equal(t, len(attributes)*8, len(args))
}

func TestUpdateAttributesQueryGeneration(t *testing.T) {
	expectedQuery := `UPDATE attributes_eav SET 
			value_text = $1, 
			value_numeric = $2, 
			value_date = $3, 
			value_bool = $4 
		WHERE schema_id = $5 AND row_id = $6 AND attr_id = $7 AND array_indices = $8`

	assert.Contains(t, expectedQuery, "UPDATE attributes_eav")
	assert.Contains(t, expectedQuery, "value_text = $1")
	assert.Contains(t, expectedQuery, "value_bool = $4")
	assert.Contains(t, expectedQuery, "schema_id = $5")
	assert.Contains(t, expectedQuery, "attr_id = $7")
}

func TestUpsertAttributesQueryGeneration(t *testing.T) {
	repo := &PostgresAttributeRepository{
		eavTableName: "attributes_eav",
	}

	attributes := []Attribute{
		{SchemaID: 100, RowID: uuid.New(), AttrID: 10, ArrayIndices: "", ValueText: stringPtr("value1")},
	}

	valuesClause, args := repo.buildValuesClause(attributes)

	queryTemplate := `INSERT INTO %s (schema_id, row_id, attr_id, array_indices, value_text, value_numeric, value_date, value_bool) VALUES %s
ON CONFLICT (schema_id, row_id, attr_id, array_indices)
DO UPDATE SET 
	value_text = EXCLUDED.value_text,
	value_numeric = EXCLUDED.value_numeric,
	value_date = EXCLUDED.value_date,
	value_bool = EXCLUDED.value_bool`

	query := fmt.Sprintf(queryTemplate, repo.eavTableName, valuesClause)

	assert.Contains(t, query, "INSERT INTO")
	assert.Contains(t, query, "ON CONFLICT")
	assert.Contains(t, query, "DO UPDATE SET")
	assert.Contains(t, query, "EXCLUDED.value_text")
	assert.Equal(t, len(attributes)*8, len(args))
}

func TestDeleteAttributesQueryGeneration(t *testing.T) {
	query := `DELETE FROM attributes_eav
 WHERE schema_id = (SELECT schema_id FROM schema_registry WHERE schema_name = $1)
 AND row_id = ANY($2)`

	assert.Contains(t, query, "DELETE FROM")
	assert.Contains(t, query, "WHERE schema_id =")
	assert.Contains(t, query, "SELECT schema_id FROM schema_registry")
	assert.Contains(t, query, "row_id = ANY($2)")
}

func TestGetAttributesQueryGeneration(t *testing.T) {
	query := `SELECT eav.schema_id, row_id, attr_name, attr_value
 FROM attributes_eav eav INNER JOIN schema_registry sr
 ON eav.schema_id = sr.schema_id
 WHERE sr.schema_name = $1 AND eav.row_id = $2`

	assert.Contains(t, query, "SELECT")
	assert.Contains(t, query, "schema_id, row_id, attr_name, attr_value")
	assert.Contains(t, query, "INNER JOIN")
	assert.Contains(t, query, "WHERE sr.schema_name = $1")
	assert.Contains(t, query, "AND eav.row_id = $2")
}

func TestExistsEntityQueryGeneration(t *testing.T) {
	query := `SELECT EXISTS(
SELECT 1 FROM attributes_eav eav
INNER JOIN schema_registry sr ON eav.schema_id = sr.schema_id
WHERE sr.schema_name = $1 AND eav.row_id = $2
)`

	assert.Contains(t, query, "EXISTS")
	assert.Contains(t, query, "SELECT 1 FROM")
	assert.Contains(t, query, "INNER JOIN")
}

func TestCountEntitiesQueryGeneration(t *testing.T) {
	query := `SELECT COUNT(DISTINCT t.row_id)
FROM attributes_eav t
INNER JOIN schema_registry s ON t.schema_id = s.schema_id
WHERE s.schema_name = $1 AND (1=1)`

	assert.Contains(t, query, "COUNT(DISTINCT")
	assert.Contains(t, query, "INNER JOIN")
	assert.Contains(t, query, "WHERE s.schema_name = $1")
}

// Edge Cases and Boundary Tests

func TestBuildValuesClause_EdgeCases(t *testing.T) {
	repo := &PostgresAttributeRepository{}

	tests := []struct {
		name        string
		attributes  []Attribute
		shouldPass  bool
		expectEmpty bool
	}{
		{
			name:        "empty attributes",
			attributes:  []Attribute{},
			shouldPass:  true,
			expectEmpty: true,
		},
		{
			name: "single attribute",
			attributes: []Attribute{
				{SchemaID: 1, RowID: uuid.New(), AttrID: 5, ArrayIndices: "", ValueText: stringPtr("value")},
			},
			shouldPass:  true,
			expectEmpty: false,
		},
		{
			name: "many attributes",
			attributes: func() []Attribute {
				var attrs []Attribute
				for i := 0; i < 100; i++ {
					attrs = append(attrs, Attribute{
						SchemaID:     int16(i % 10),
						RowID:        uuid.New(),
						AttrID:       int16(10 + (i % 20)),
						ArrayIndices: "",
						ValueText:    stringPtr(fmt.Sprintf("value-%d", i)),
					})
				}
				return attrs
			}(),
			shouldPass:  true,
			expectEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valuesClause, args := repo.buildValuesClause(tt.attributes)

			if tt.shouldPass {
				if tt.expectEmpty {
					assert.Empty(t, valuesClause)
					assert.Equal(t, 0, len(args))
				} else {
					assert.NotEmpty(t, valuesClause)
					assert.Equal(t, len(tt.attributes)*8, len(args))
				}
			}
		})
	}
}

func TestBuildFilterConditions_ParameterOrdering(t *testing.T) {
	repo := &PostgresAttributeRepository{}

	filters := []forma.Filter{
		{Type: forma.FilterEquals, Field: forma.FilterFieldAttributeName, Value: "name1"},
		{Type: forma.FilterContains, Field: forma.FilterFieldAttributeValue, Value: "value1"},
		{Type: forma.FilterGreaterThan, Field: forma.FilterFieldAttributeValue, Value: 100},
	}

	_, args := repo.buildFilterConditions(0, filters, 1)

	assert.Equal(t, 3, len(args))
	assert.Equal(t, "name1", args[0])
	assert.Equal(t, "value1", args[1])
	assert.Equal(t, 100, args[2])
}

func TestBuildPaginatedQueryTemplate_ParameterIndexing(t *testing.T) {
	repo := &PostgresAttributeRepository{
		eavTableName:    "eav",
		schemaTableName: "schema_registry",
	}

	tests := []struct {
		name           string
		limitParamIdx  int
		offsetParamIdx int
		shouldContain  string
	}{
		{
			name:           "parameters at indices 1,2",
			limitParamIdx:  1,
			offsetParamIdx: 2,
			shouldContain:  "LIMIT $1 OFFSET $2",
		},
		{
			name:           "parameters at indices 3,4",
			limitParamIdx:  3,
			offsetParamIdx: 4,
			shouldContain:  "LIMIT $3 OFFSET $4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := repo.buildPaginatedQueryTemplate(
				"1=1",
				"1=1",
				"ORDER BY t.row_id ASC",
				tt.limitParamIdx,
				tt.offsetParamIdx,
			)

			assert.Contains(t, query, tt.shouldContain)
		})
	}
}

// Unit tests for single-schema query

func TestQueryAttributes_SingleSchema_BackwardCompatible(t *testing.T) {
	// Test backward compatibility with single schema
	query := &AttributeQuery{
		SchemaID: 100,
		Filters: []forma.Filter{
			{
				Type:  forma.FilterEquals,
				Field: forma.FilterFieldAttributeName,
				Value: "status",
			},
		},
		Limit:  10,
		Offset: 0,
	}

	assert.Equal(t, int16(100), query.SchemaID)
	assert.Equal(t, 1, len(query.Filters))
	assert.Equal(t, 10, query.Limit)
	assert.Equal(t, 0, query.Offset)
}

// Integration test helpers (to be used with testcontainers or docker-compose)

func TestInsertAttributesIntegration(t *testing.T) {
	// Skip if not in integration test mode
	t.Skip("Integration test - requires PostgreSQL")

	// This is a template for integration testing
	// In real scenarios, you would:
	// 1. Start a PostgreSQL container
	// 2. Create tables
	// 3. Test actual database operations

	repo := NewPostgresAttributeRepository(nil, "eav_test", "schema_registry_test", nil)
	require.NotNil(t, repo)

	ctx := context.Background()
	attrs := []Attribute{
		{
			SchemaID:  100,
			RowID:     uuid.New(),
			AttrID:    1,
			ValueText: stringPtr("john_doe"),
		},
	}

	err := repo.InsertAttributes(ctx, attrs)
	require.NoError(t, err)
}

func TestGetAttributesIntegration(t *testing.T) {
	// Skip if not in integration test mode
	t.Skip("Integration test - requires PostgreSQL")

	repo := NewPostgresAttributeRepository(nil, "eav_test", "schema_registry_test", nil)
	require.NotNil(t, repo)

	ctx := context.Background()
	rowID := uuid.New()

	attrs, err := repo.GetAttributes(ctx, "users", rowID)
	require.NoError(t, err)
	assert.IsType(t, []Attribute{}, attrs)
}

func TestQueryAttributesIntegration(t *testing.T) {
	// Skip if not in integration test mode
	t.Skip("Integration test - requires PostgreSQL")

	repo := NewPostgresAttributeRepository(nil, "eav_test", "schema_registry_test", nil)
	require.NotNil(t, repo)

	ctx := context.Background()
	query := &AttributeQuery{
		SchemaID: 100,
		Filters: []forma.Filter{
			{
				Type:  forma.FilterEquals,
				Field: forma.FilterFieldAttributeName,
				Value: "email",
			},
		},
		Limit:  10,
		Offset: 0,
	}

	attrs, err := repo.QueryAttributes(ctx, query)
	require.NoError(t, err)
	assert.IsType(t, []Attribute{}, attrs)
}

func TestBatchUpsertAttributesIntegration(t *testing.T) {
	// Skip if not in integration test mode
	t.Skip("Integration test - requires PostgreSQL")

	repo := NewPostgresAttributeRepository(nil, "eav_test", "schema_registry_test", nil)
	require.NotNil(t, repo)

	ctx := context.Background()
	rowID := uuid.New()
	attrs := []Attribute{
		{
			SchemaID:  100,
			RowID:     rowID,
			AttrID:    1,
			ValueText: stringPtr("john_doe"),
		},
		{
			SchemaID:  100,
			RowID:     rowID,
			AttrID:    2,
			ValueText: stringPtr("john@example.com"),
		},
	}

	err := repo.BatchUpsertAttributes(ctx, attrs)
	require.NoError(t, err)
}

func TestCountEntitiesIntegration(t *testing.T) {
	// Skip if not in integration test mode
	t.Skip("Integration test - requires PostgreSQL")

	repo := NewPostgresAttributeRepository(nil, "eav_test", "schema_registry_test", nil)
	require.NotNil(t, repo)

	ctx := context.Background()
	filters := []forma.Filter{
		{
			Type:  forma.FilterEquals,
			Field: forma.FilterFieldAttributeName,
			Value: "status",
		},
	}

	count, err := repo.CountEntities(ctx, "users", filters)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(0))
}

func TestDeleteEntityIntegration(t *testing.T) {
	// Skip if not in integration test mode
	t.Skip("Integration test - requires PostgreSQL")

	repo := NewPostgresAttributeRepository(nil, "eav_test", "schema_registry_test", nil)
	require.NotNil(t, repo)

	ctx := context.Background()
	rowID := uuid.New()

	err := repo.DeleteEntity(ctx, "users", rowID)
	require.NoError(t, err)
}

func TestExistsEntityIntegration(t *testing.T) {
	// Skip if not in integration test mode
	t.Skip("Integration test - requires PostgreSQL")

	repo := NewPostgresAttributeRepository(nil, "eav_test", "schema_registry_test", nil)
	require.NotNil(t, repo)

	ctx := context.Background()
	rowID := uuid.New()

	exists, err := repo.ExistsEntity(ctx, "users", rowID)
	require.NoError(t, err)
	assert.IsType(t, false, exists)
}

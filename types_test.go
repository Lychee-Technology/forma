package forma

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Condition Interface Tests
// =============================================================================

func TestCompositeCondition_IsLeaf(t *testing.T) {
	c := &CompositeCondition{Logic: LogicAnd}
	assert.False(t, c.IsLeaf())
}

func TestKvCondition_IsLeaf(t *testing.T) {
	kv := &KvCondition{Attr: "name", Value: "test"}
	assert.True(t, kv.IsLeaf())
}

// =============================================================================
// CompositeCondition UnmarshalJSON Tests
// =============================================================================

func TestCompositeCondition_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		wantLogic Logic
		wantLen   int
		wantErr   bool
		errSubstr string
	}{
		{
			name:      "valid AND with kv conditions",
			json:      `{"l":"and","c":[{"a":"name","v":"test"},{"a":"age","v":"30"}]}`,
			wantLogic: LogicAnd,
			wantLen:   2,
			wantErr:   false,
		},
		{
			name:      "valid OR with kv conditions",
			json:      `{"l":"or","c":[{"a":"status","v":"active"}]}`,
			wantLogic: LogicOr,
			wantLen:   1,
			wantErr:   false,
		},
		{
			name:      "empty conditions array",
			json:      `{"l":"and","c":[]}`,
			wantLogic: LogicAnd,
			wantLen:   0,
			wantErr:   false,
		},
		{
			name:      "missing logic field",
			json:      `{"c":[{"a":"name","v":"test"}]}`,
			wantErr:   true,
			errSubstr: "missing logic",
		},
		{
			name:      "unknown logic value",
			json:      `{"l":"xor","c":[]}`,
			wantErr:   true,
			errSubstr: "unknown logic",
		},
		{
			name:      "nested composite conditions",
			json:      `{"l":"and","c":[{"l":"or","c":[{"a":"x","v":"1"},{"a":"y","v":"2"}]},{"a":"z","v":"3"}]}`,
			wantLogic: LogicAnd,
			wantLen:   2,
			wantErr:   false,
		},
		{
			name:    "invalid JSON",
			json:    `{invalid}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c CompositeCondition
			err := json.Unmarshal([]byte(tt.json), &c)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errSubstr != "" {
					assert.Contains(t, err.Error(), tt.errSubstr)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantLogic, c.Logic)
				assert.Len(t, c.Conditions, tt.wantLen)
			}
		})
	}
}

// =============================================================================
// KvCondition UnmarshalJSON Tests
// =============================================================================

func TestKvCondition_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		wantAttr  string
		wantValue string
		wantErr   bool
		errSubstr string
	}{
		{
			name:      "valid kv condition",
			json:      `{"a":"name","v":"John"}`,
			wantAttr:  "name",
			wantValue: "John",
			wantErr:   false,
		},
		{
			name:      "missing attr field",
			json:      `{"v":"value"}`,
			wantErr:   true,
			errSubstr: "missing attr",
		},
		{
			name:      "missing value field",
			json:      `{"a":"name"}`,
			wantErr:   true,
			errSubstr: "missing value",
		},
		{
			name:      "empty attr field",
			json:      `{"a":"","v":"test"}`,
			wantErr:   true,
			errSubstr: "missing attr",
		},
		{
			name:      "empty value field",
			json:      `{"a":"name","v":""}`,
			wantErr:   true,
			errSubstr: "missing value",
		},
		{
			name:    "invalid JSON",
			json:    `{invalid}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var kv KvCondition
			err := json.Unmarshal([]byte(tt.json), &kv)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errSubstr != "" {
					assert.Contains(t, err.Error(), tt.errSubstr)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantAttr, kv.Attr)
				assert.Equal(t, tt.wantValue, kv.Value)
			}
		})
	}
}

// =============================================================================
// unmarshalCondition Tests
// =============================================================================

func TestUnmarshalCondition(t *testing.T) {
	tests := []struct {
		name         string
		json         string
		wantType     string // "composite" or "kv"
		wantErr      bool
		errSubstr    string
	}{
		{
			name:     "composite condition with logic",
			json:     `{"l":"and","c":[{"a":"name","v":"test"}]}`,
			wantType: "composite",
			wantErr:  false,
		},
		{
			name:     "kv condition with attr",
			json:     `{"a":"name","v":"test"}`,
			wantType: "kv",
			wantErr:  false,
		},
		{
			name:      "invalid - no logic or attr",
			json:      `{"x":"y"}`,
			wantErr:   true,
			errSubstr: "invalid condition payload",
		},
		{
			name:    "invalid JSON",
			json:    `{invalid}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond, err := unmarshalCondition([]byte(tt.json))

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errSubstr != "" {
					assert.Contains(t, err.Error(), tt.errSubstr)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, cond)

				switch tt.wantType {
				case "composite":
					_, ok := cond.(*CompositeCondition)
					assert.True(t, ok, "expected CompositeCondition")
				case "kv":
					_, ok := cond.(*KvCondition)
					assert.True(t, ok, "expected KvCondition")
				}
			}
		})
	}
}

// =============================================================================
// QueryRequest JSON Tests
// =============================================================================

func TestQueryRequest_UnmarshalJSON(t *testing.T) {
	rowID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	tests := []struct {
		name           string
		json           string
		wantSchemaName string
		wantPage       int
		wantCondition  bool
		wantErr        bool
	}{
		{
			name:           "basic query request",
			json:           `{"schema_name":"users","page":1,"items_per_page":10}`,
			wantSchemaName: "users",
			wantPage:       1,
			wantCondition:  false,
			wantErr:        false,
		},
		{
			name:           "with kv condition",
			json:           `{"schema_name":"users","page":1,"items_per_page":10,"condition":{"a":"name","v":"John"}}`,
			wantSchemaName: "users",
			wantPage:       1,
			wantCondition:  true,
			wantErr:        false,
		},
		{
			name:           "with composite condition",
			json:           `{"schema_name":"users","page":1,"items_per_page":10,"condition":{"l":"and","c":[{"a":"age","v":"30"}]}}`,
			wantSchemaName: "users",
			wantPage:       1,
			wantCondition:  true,
			wantErr:        false,
		},
		{
			name:           "with null condition",
			json:           `{"schema_name":"users","page":1,"items_per_page":10,"condition":null}`,
			wantSchemaName: "users",
			wantPage:       1,
			wantCondition:  false,
			wantErr:        false,
		},
		{
			name:           "with row_id",
			json:           `{"schema_name":"users","page":1,"items_per_page":10,"row_id":"550e8400-e29b-41d4-a716-446655440000"}`,
			wantSchemaName: "users",
			wantPage:       1,
			wantCondition:  false,
			wantErr:        false,
		},
		{
			name:    "invalid JSON",
			json:    `{invalid}`,
			wantErr: true,
		},
		{
			name:    "invalid condition",
			json:    `{"schema_name":"users","page":1,"items_per_page":10,"condition":{"x":"y"}}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req QueryRequest
			err := json.Unmarshal([]byte(tt.json), &req)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantSchemaName, req.SchemaName)
				assert.Equal(t, tt.wantPage, req.Page)
				if tt.wantCondition {
					assert.NotNil(t, req.Condition)
				} else {
					assert.Nil(t, req.Condition)
				}
			}
		})
	}

	// Test with row_id specifically
	t.Run("row_id parsed correctly", func(t *testing.T) {
		var req QueryRequest
		err := json.Unmarshal([]byte(`{"schema_name":"users","page":1,"items_per_page":10,"row_id":"550e8400-e29b-41d4-a716-446655440000"}`), &req)
		require.NoError(t, err)
		require.NotNil(t, req.RowID)
		assert.Equal(t, rowID, *req.RowID)
	})
}

func TestQueryRequest_MarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		req     QueryRequest
		wantErr bool
	}{
		{
			name: "basic query request",
			req: QueryRequest{
				SchemaName:   "users",
				Page:         1,
				ItemsPerPage: 10,
			},
			wantErr: false,
		},
		{
			name: "with kv condition",
			req: QueryRequest{
				SchemaName:   "users",
				Page:         1,
				ItemsPerPage: 10,
				Condition:    &KvCondition{Attr: "name", Value: "John"},
			},
			wantErr: false,
		},
		{
			name: "with composite condition",
			req: QueryRequest{
				SchemaName:   "users",
				Page:         1,
				ItemsPerPage: 10,
				Condition: &CompositeCondition{
					Logic:      LogicAnd,
					Conditions: []Condition{&KvCondition{Attr: "age", Value: "30"}},
				},
			},
			wantErr: false,
		},
		{
			name: "with sort options",
			req: QueryRequest{
				SchemaName:   "users",
				Page:         1,
				ItemsPerPage: 10,
				SortBy:       []string{"name", "age"},
				SortOrder:    SortOrderDesc,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.req)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, data)

				// Verify it can be unmarshaled back
				var req2 QueryRequest
				err = json.Unmarshal(data, &req2)
				require.NoError(t, err)
				assert.Equal(t, tt.req.SchemaName, req2.SchemaName)
				assert.Equal(t, tt.req.Page, req2.Page)
			}
		})
	}
}

// =============================================================================
// CrossSchemaRequest JSON Tests
// =============================================================================

func TestCrossSchemaRequest_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name            string
		json            string
		wantSchemaNames []string
		wantSearchTerm  string
		wantCondition   bool
		wantErr         bool
	}{
		{
			name:            "basic cross-schema request",
			json:            `{"schema_names":["users","posts"],"search_term":"test","page":1,"items_per_page":10}`,
			wantSchemaNames: []string{"users", "posts"},
			wantSearchTerm:  "test",
			wantCondition:   false,
			wantErr:         false,
		},
		{
			name:            "with kv condition",
			json:            `{"schema_names":["users"],"search_term":"test","page":1,"items_per_page":10,"condition":{"a":"status","v":"active"}}`,
			wantSchemaNames: []string{"users"},
			wantSearchTerm:  "test",
			wantCondition:   true,
			wantErr:         false,
		},
		{
			name:            "with null condition",
			json:            `{"schema_names":["users"],"search_term":"test","page":1,"items_per_page":10,"condition":null}`,
			wantSchemaNames: []string{"users"},
			wantSearchTerm:  "test",
			wantCondition:   false,
			wantErr:         false,
		},
		{
			name:    "invalid JSON",
			json:    `{invalid}`,
			wantErr: true,
		},
		{
			name:    "invalid condition",
			json:    `{"schema_names":["users"],"search_term":"test","page":1,"items_per_page":10,"condition":{"x":"y"}}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req CrossSchemaRequest
			err := json.Unmarshal([]byte(tt.json), &req)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantSchemaNames, req.SchemaNames)
				assert.Equal(t, tt.wantSearchTerm, req.SearchTerm)
				if tt.wantCondition {
					assert.NotNil(t, req.Condition)
				} else {
					assert.Nil(t, req.Condition)
				}
			}
		})
	}
}

func TestCrossSchemaRequest_MarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		req     CrossSchemaRequest
		wantErr bool
	}{
		{
			name: "basic cross-schema request",
			req: CrossSchemaRequest{
				SchemaNames:  []string{"users", "posts"},
				SearchTerm:   "test",
				Page:         1,
				ItemsPerPage: 10,
			},
			wantErr: false,
		},
		{
			name: "with kv condition",
			req: CrossSchemaRequest{
				SchemaNames:  []string{"users"},
				SearchTerm:   "test",
				Page:         1,
				ItemsPerPage: 10,
				Condition:    &KvCondition{Attr: "status", Value: "active"},
			},
			wantErr: false,
		},
		{
			name: "with attrs",
			req: CrossSchemaRequest{
				SchemaNames:  []string{"users"},
				SearchTerm:   "test",
				Page:         1,
				ItemsPerPage: 10,
				Attrs:        []string{"name", "email"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.req)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, data)

				// Verify it can be unmarshaled back
				var req2 CrossSchemaRequest
				err = json.Unmarshal(data, &req2)
				require.NoError(t, err)
				assert.Equal(t, tt.req.SchemaNames, req2.SchemaNames)
				assert.Equal(t, tt.req.SearchTerm, req2.SearchTerm)
			}
		})
	}
}

// =============================================================================
// Constant Value Tests
// =============================================================================

func TestFilterTypeConstants(t *testing.T) {
	assert.Equal(t, FilterType("equals"), FilterEquals)
	assert.Equal(t, FilterType("not_equals"), FilterNotEquals)
	assert.Equal(t, FilterType("starts_with"), FilterStartsWith)
	assert.Equal(t, FilterType("contains"), FilterContains)
	assert.Equal(t, FilterType("gt"), FilterGreaterThan)
	assert.Equal(t, FilterType("lt"), FilterLessThan)
	assert.Equal(t, FilterType("gte"), FilterGreaterEq)
	assert.Equal(t, FilterType("lte"), FilterLessEq)
	assert.Equal(t, FilterType("in"), FilterIn)
	assert.Equal(t, FilterType("not_in"), FilterNotIn)
}

func TestSortOrderConstants(t *testing.T) {
	assert.Equal(t, SortOrder("asc"), SortOrderAsc)
	assert.Equal(t, SortOrder("desc"), SortOrderDesc)
}

func TestFilterFieldConstants(t *testing.T) {
	assert.Equal(t, FilterField("attr_name"), FilterFieldAttributeName)
	assert.Equal(t, FilterField("value_text"), FilterFieldValueText)
	assert.Equal(t, FilterField("value_numeric"), FilterFieldValueNumeric)
	assert.Equal(t, FilterField("row_id"), FilterFieldRowID)
	assert.Equal(t, FilterField("schema_name"), FilterFieldSchemaName)
}

func TestOperationTypeConstants(t *testing.T) {
	assert.Equal(t, OperationType("create"), OperationCreate)
	assert.Equal(t, OperationType("read"), OperationRead)
	assert.Equal(t, OperationType("update"), OperationUpdate)
	assert.Equal(t, OperationType("delete"), OperationDelete)
	assert.Equal(t, OperationType("query"), OperationQuery)
}

func TestReferenceTypeConstants(t *testing.T) {
	assert.Equal(t, ReferenceType("single"), ReferenceTypeSingle)
	assert.Equal(t, ReferenceType("array"), ReferenceTypeArray)
	assert.Equal(t, ReferenceType("nested"), ReferenceTypeNested)
}

func TestLogicConstants(t *testing.T) {
	assert.Equal(t, Logic("and"), LogicAnd)
	assert.Equal(t, Logic("or"), LogicOr)
}

// =============================================================================
// Struct JSON Serialization Tests
// =============================================================================

func TestDataRecord_JSON(t *testing.T) {
	rowID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	record := DataRecord{
		SchemaName: "users",
		RowID:      rowID,
		Attributes: map[string]any{
			"name": "John",
			"age":  30,
		},
	}

	data, err := json.Marshal(record)
	require.NoError(t, err)

	var decoded DataRecord
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, record.SchemaName, decoded.SchemaName)
	assert.Equal(t, record.RowID, decoded.RowID)
	assert.Equal(t, "John", decoded.Attributes["name"])
}

func TestOrderBy_JSON(t *testing.T) {
	orderBy := OrderBy{
		Attribute: "name",
		SortOrder: SortOrderDesc,
	}

	data, err := json.Marshal(orderBy)
	require.NoError(t, err)

	var decoded OrderBy
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, orderBy.Attribute, decoded.Attribute)
	assert.Equal(t, orderBy.SortOrder, decoded.SortOrder)
}

func TestTableNames_JSON(t *testing.T) {
	tableNames := TableNames{
		SchemaRegistry: "schema_registry",
		EntityMain:     "entity_main",
		EAVData:        "eav_data",
	}

	data, err := json.Marshal(tableNames)
	require.NoError(t, err)

	var decoded TableNames
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, tableNames, decoded)
}

func TestEntityIdentifier_JSON(t *testing.T) {
	rowID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	identifier := EntityIdentifier{
		SchemaName: "users",
		RowID:      rowID,
	}

	data, err := json.Marshal(identifier)
	require.NoError(t, err)

	var decoded EntityIdentifier
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, identifier, decoded)
}

func TestEntityOperation_JSON(t *testing.T) {
	rowID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	op := EntityOperation{
		EntityIdentifier: EntityIdentifier{
			SchemaName: "users",
			RowID:      rowID,
		},
		Type: OperationCreate,
		Data: map[string]any{"name": "John"},
	}

	data, err := json.Marshal(op)
	require.NoError(t, err)

	var decoded EntityOperation
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, op.SchemaName, decoded.SchemaName)
	assert.Equal(t, op.RowID, decoded.RowID)
	assert.Equal(t, op.Type, decoded.Type)
}

func TestBatchOperation_JSON(t *testing.T) {
	rowID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	batch := BatchOperation{
		Operations: []EntityOperation{
			{
				EntityIdentifier: EntityIdentifier{
					SchemaName: "users",
					RowID:      rowID,
				},
				Type: OperationCreate,
			},
		},
		Atomic: true,
	}

	data, err := json.Marshal(batch)
	require.NoError(t, err)

	var decoded BatchOperation
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, batch.Atomic, decoded.Atomic)
	assert.Len(t, decoded.Operations, 1)
}

func TestBatchResult_JSON(t *testing.T) {
	result := BatchResult{
		Successful: []*DataRecord{
			{SchemaName: "users", RowID: uuid.New()},
		},
		Failed:     []OperationError{},
		TotalCount: 1,
		Duration:   1000,
	}

	data, err := json.Marshal(result)
	require.NoError(t, err)

	var decoded BatchResult
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, result.TotalCount, decoded.TotalCount)
	assert.Equal(t, result.Duration, decoded.Duration)
}

func TestOperationError_JSON(t *testing.T) {
	rowID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	opErr := OperationError{
		Operation: EntityOperation{
			EntityIdentifier: EntityIdentifier{
				SchemaName: "users",
				RowID:      rowID,
			},
			Type: OperationCreate,
		},
		Error:   "duplicate key",
		Code:    "DUPLICATE_KEY",
		Details: map[string]any{"field": "email"},
	}

	data, err := json.Marshal(opErr)
	require.NoError(t, err)

	var decoded OperationError
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, opErr.Error, decoded.Error)
	assert.Equal(t, opErr.Code, decoded.Code)
}

func TestEntityUpdate_JSON(t *testing.T) {
	rowID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	update := EntityUpdate{
		EntityIdentifier: EntityIdentifier{
			SchemaName: "users",
			RowID:      rowID,
		},
		Updates: map[string]any{"name": "Jane"},
	}

	data, err := json.Marshal(update)
	require.NoError(t, err)

	var decoded EntityUpdate
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, update.SchemaName, decoded.SchemaName)
	assert.Equal(t, update.RowID, decoded.RowID)
}

func TestReference_JSON(t *testing.T) {
	sourceRowID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	targetRowID := uuid.MustParse("660e8400-e29b-41d4-a716-446655440000")
	ref := Reference{
		SourceSchemaName: "posts",
		SourceRowID:      sourceRowID,
		SourceFieldName:  "author_id",
		TargetSchemaName: "users",
		TargetRowID:      targetRowID,
		ReferenceType:    ReferenceTypeSingle,
	}

	data, err := json.Marshal(ref)
	require.NoError(t, err)

	var decoded Reference
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, ref, decoded)
}

func TestQueryResult_JSON(t *testing.T) {
	result := QueryResult{
		Data:         []*DataRecord{},
		TotalRecords: 100,
		TotalPages:   10,
		CurrentPage:  1,
		ItemsPerPage: 10,
		HasNext:      true,
		HasPrevious:  false,
	}

	data, err := json.Marshal(result)
	require.NoError(t, err)

	var decoded QueryResult
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, result.TotalRecords, decoded.TotalRecords)
	assert.Equal(t, result.TotalPages, decoded.TotalPages)
	assert.Equal(t, result.HasNext, decoded.HasNext)
	assert.Equal(t, result.HasPrevious, decoded.HasPrevious)
}

func TestCursorQueryResult_JSON(t *testing.T) {
	result := CursorQueryResult{
		Data:       []*DataRecord{},
		NextCursor: "abc123",
		HasMore:    true,
	}

	data, err := json.Marshal(result)
	require.NoError(t, err)

	var decoded CursorQueryResult
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, result.NextCursor, decoded.NextCursor)
	assert.Equal(t, result.HasMore, decoded.HasMore)
}

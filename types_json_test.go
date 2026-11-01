package forma

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		ChangeLog:      "change_log",
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
	assert.NotContains(t, string(data), "error_id", "a value with no id must not serialise an empty one")

	opErr.ErrorID = "0b7c3a8e-6a53-4f0e-9d5b-3c1f2a4e5d6f"
	data, err = json.Marshal(opErr)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"error_id":"0b7c3a8e-6a53-4f0e-9d5b-3c1f2a4e5d6f"`,
		"the wire name matches the HTTP error body's error_id")
	decoded = OperationError{}
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, opErr.ErrorID, decoded.ErrorID)
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

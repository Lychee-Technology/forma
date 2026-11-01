package forma

import (
	"encoding/json"
	"testing"

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
		name      string
		json      string
		wantType  string // "composite" or "kv"
		wantErr   bool
		errSubstr string
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

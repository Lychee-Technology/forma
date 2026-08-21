package forma

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// QueryRequest JSON Tests
// =============================================================================

func TestQueryRequest_UnmarshalJSON(t *testing.T) { //nolint:funlen // #319 follow-up: oversized test functions, tracked separately
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
			name:           "with federated hints",
			json:           `{"schema_name":"users","page":1,"items_per_page":10,"federated":{"enabled":true,"preferred_tiers":["hot","warm","cold"],"prefer_hot":false,"use_main_as_anchor":true,"s3_parquet_path_template":"s3://bucket/prefix/{{.SchemaID}}/base/*.parquet, s3://bucket/prefix/{{.SchemaID}}/delta/*.parquet","allow_partial_degraded_mode":true}}`,
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
				if tt.name == "with federated hints" {
					require.NotNil(t, req.Federated)
					assert.True(t, req.Federated.Enabled)
					assert.Equal(t, []string{"hot", "warm", "cold"}, req.Federated.PreferredTiers)
					assert.True(t, req.Federated.UseMainAsAnchor)
					assert.True(t, req.Federated.AllowPartialDegradedMode)
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
		{
			name: "with federated hints",
			req: QueryRequest{
				SchemaName:   "users",
				Page:         1,
				ItemsPerPage: 10,
				Federated: &FederatedQueryRequest{
					Enabled:                  true,
					PreferredTiers:           []string{"hot", "warm", "cold"},
					UseMainAsAnchor:          true,
					S3ParquetPathTemplate:    "s3://bucket/prefix/{{.SchemaID}}/base/*.parquet, s3://bucket/prefix/{{.SchemaID}}/delta/*.parquet",
					AllowPartialDegradedMode: true,
				},
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
				if tt.req.Federated != nil {
					require.NotNil(t, req2.Federated)
					assert.Equal(t, tt.req.Federated.PreferredTiers, req2.Federated.PreferredTiers)
					assert.Equal(t, tt.req.Federated.S3ParquetPathTemplate, req2.Federated.S3ParquetPathTemplate)
				}
			}
		})
	}
}

func TestQueryRequestSortFieldJSONRoundTrip(t *testing.T) {
	payload := []byte(`{
		"schema_name": "orders",
		"sort": [
			{"attribute": "status"},
			{"attribute": "created_at", "sort_order": "desc"}
		],
		"condition": {"a": "status", "v": "equals:hot"}
	}`)

	var req QueryRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		t.Fatalf("unmarshal QueryRequest with sort: %v", err)
	}
	if len(req.Sort) != 2 {
		t.Fatalf("Sort length = %d, want 2", len(req.Sort))
	}
	assert.Equal(t, "status", req.Sort[0].Attribute)
	assert.Equal(t, SortOrder(""), req.Sort[0].SortOrder) // direction omitted stays empty at the type layer
	assert.Equal(t, "created_at", req.Sort[1].Attribute)
	assert.Equal(t, SortOrderDesc, req.Sort[1].SortOrder)

	// Round-trip: the custom MarshalJSON (alias-based) must carry the field back out.
	out, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal QueryRequest with sort: %v", err)
	}
	var decoded QueryRequest
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("re-unmarshal QueryRequest: %v", err)
	}
	assert.Equal(t, req.Sort, decoded.Sort)
}

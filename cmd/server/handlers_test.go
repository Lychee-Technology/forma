package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

type mockEntityManager struct {
	advancedResult    *forma.QueryResult
	advancedErr       error
	batchCreateResult *forma.BatchResult
	batchCreateErr    error
	batchCreateReq    *forma.BatchOperation
	crossSchemaResult *forma.QueryResult
	crossSchemaErr    error
	crossSchemaReq    *forma.CrossSchemaRequest
}

func (m *mockEntityManager) Create(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockEntityManager) Get(ctx context.Context, req *forma.QueryRequest) (*forma.DataRecord, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockEntityManager) Update(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockEntityManager) Delete(ctx context.Context, req *forma.EntityOperation) error {
	return fmt.Errorf("not implemented")
}

func (m *mockEntityManager) Query(ctx context.Context, req *forma.QueryRequest) (*forma.QueryResult, error) {
	if m.advancedResult != nil {
		return m.advancedResult, m.advancedErr
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *mockEntityManager) CrossSchemaSearch(ctx context.Context, req *forma.CrossSchemaRequest) (*forma.QueryResult, error) {
	m.crossSchemaReq = req
	if m.crossSchemaResult != nil {
		return m.crossSchemaResult, m.crossSchemaErr
	}
	if m.crossSchemaErr != nil {
		return nil, m.crossSchemaErr
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *mockEntityManager) BatchCreate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	m.batchCreateReq = req
	if m.batchCreateResult != nil {
		return m.batchCreateResult, m.batchCreateErr
	}
	if m.batchCreateErr != nil {
		return nil, m.batchCreateErr
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *mockEntityManager) BatchUpdate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockEntityManager) BatchDelete(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	return nil, fmt.Errorf("not implemented")
}

func TestHandleAdvancedQuerySuccess(t *testing.T) {
	result := &forma.QueryResult{
		Data: []*forma.DataRecord{
			{
				SchemaName: "lead",
				RowID:      uuid.New(),
				Attributes: map[string]any{"status": "hot"},
			},
		},
	}

	server := &Server{
		manager: &mockEntityManager{
			advancedResult: result,
		},
	}

	payload := []byte(`{
		"schema_name": "lead",
		"condition": {"l": "and", "c": [{"a": "status", "v": "equals:hot"}]},
		"page": 1,
		"items_per_page": 10
	}`)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	server.handleAdvancedQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestHandleAdvancedQueryValidation(t *testing.T) {
	server := &Server{
		manager: &mockEntityManager{},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query", bytes.NewReader([]byte(`{"schema_name": ""}`)))
	rec := httptest.NewRecorder()
	server.handleAdvancedQuery(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

func TestHandleCreateRejectsNonObjectArrayElements(t *testing.T) {
	server := &Server{
		manager: &mockEntityManager{},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/visit", bytes.NewReader([]byte(`[{"id":"ok"}, 1]`)))
	rec := httptest.NewRecorder()
	server.handleCreate(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

func TestHandleSearchValidationErrors(t *testing.T) {
	server := &Server{
		manager: &mockEntityManager{},
	}

	tests := []struct {
		name string
		url  string
	}{
		{name: "missing schemas", url: "/api/v1/search?q=lead"},
		{name: "missing q", url: "/api/v1/search?schemas=lead"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			rec := httptest.NewRecorder()
			server.handleSearch(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected status 400, got %d", rec.Code)
			}
		})
	}
}

func TestHandleSearchParsesCSVSchemas(t *testing.T) {
	result := &forma.QueryResult{Data: []*forma.DataRecord{}}
	manager := &mockEntityManager{
		crossSchemaResult: result,
	}
	server := &Server{manager: manager}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/search?schemas=lead,%20visit,lead&q=%20john%20", nil)
	rec := httptest.NewRecorder()
	server.handleSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if manager.crossSchemaReq == nil {
		t.Fatalf("expected CrossSchemaSearch request to be captured")
	}

	expectedSchemas := []string{"lead", "visit"}
	if !reflect.DeepEqual(expectedSchemas, manager.crossSchemaReq.SchemaNames) {
		t.Fatalf("expected schemas %v, got %v", expectedSchemas, manager.crossSchemaReq.SchemaNames)
	}
	if manager.crossSchemaReq.SearchTerm != "john" {
		t.Fatalf("expected trimmed search term 'john', got %q", manager.crossSchemaReq.SearchTerm)
	}
}

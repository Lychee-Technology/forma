package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

type mockEntityManager struct {
	advancedResult *forma.QueryResult
	advancedErr    error
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
	return nil, fmt.Errorf("not implemented")
}

func (m *mockEntityManager) BatchCreate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
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

package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
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
	advancedReq       *forma.QueryRequest
	getResult         *forma.DataRecord
	getErr            error
	batchCreateResult *forma.BatchResult
	batchCreateErr    error
	batchCreateReq    *forma.BatchOperation
	updateResult      *forma.DataRecord
	updateErr         error
	updateReq         *forma.EntityOperation
	crossSchemaResult *forma.QueryResult
	crossSchemaErr    error
	crossSchemaReq    *forma.CrossSchemaRequest
}

func (m *mockEntityManager) Create(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockEntityManager) Get(ctx context.Context, req *forma.QueryRequest) (*forma.DataRecord, error) {
	if m.getResult != nil || m.getErr != nil {
		return m.getResult, m.getErr
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *mockEntityManager) Update(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	m.updateReq = req
	if m.updateResult != nil || m.updateErr != nil {
		return m.updateResult, m.updateErr
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *mockEntityManager) Delete(ctx context.Context, req *forma.EntityOperation) error {
	return fmt.Errorf("not implemented")
}

func (m *mockEntityManager) Query(ctx context.Context, req *forma.QueryRequest) (*forma.QueryResult, error) {
	m.advancedReq = req
	if m.advancedResult != nil || m.advancedErr != nil {
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

// TestCreateAndUpdateDecodeEntityNumbersExactly guards #205: HTTP entity
// bodies must decode integer literals with json.Number so values above 2^53
// (including the full ±int64 range) reach the manager boundary undamaged,
// rather than being pre-rounded through float64 by the default decoder.
func TestCreateAndUpdateDecodeEntityNumbersExactly(t *testing.T) {
	signs := []string{"9223372036854775807", "-9223372036854775807"}

	for _, lit := range signs {
		t.Run("create/"+lit, func(t *testing.T) {
			manager := &mockEntityManager{batchCreateResult: &forma.BatchResult{}}
			server := &Server{manager: manager}

			body := fmt.Sprintf(`{"amount": %s}`, lit)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/visit", bytes.NewReader([]byte(body)))
			rec := httptest.NewRecorder()
			server.handleCreate(rec, req)

			if manager.batchCreateReq == nil {
				t.Fatalf("expected BatchCreate request to be captured")
			}
			if len(manager.batchCreateReq.Operations) != 1 {
				t.Fatalf("expected 1 operation, got %d", len(manager.batchCreateReq.Operations))
			}
			got := manager.batchCreateReq.Operations[0].Data["amount"]
			if want := json.Number(lit); got != want {
				t.Fatalf("Data[amount] = %#v (%T), want %#v", got, got, want)
			}
		})

		t.Run("update/"+lit, func(t *testing.T) {
			manager := &mockEntityManager{updateResult: &forma.DataRecord{}}
			server := &Server{manager: manager}

			rowID := uuid.New()
			body := fmt.Sprintf(`{"amount": %s}`, lit)
			url := fmt.Sprintf("/api/v1/visit/%s", rowID.String())
			req := httptest.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(body)))
			rec := httptest.NewRecorder()
			server.handleUpdate(rec, req)

			if manager.updateReq == nil {
				t.Fatalf("expected Update request to be captured")
			}
			want := json.Number(lit)
			if got := manager.updateReq.Data["amount"]; got != want {
				t.Fatalf("Data[amount] = %#v (%T), want %#v", got, got, want)
			}
			if got := manager.updateReq.Updates["amount"]; got != want {
				t.Fatalf("Updates[amount] = %#v (%T), want %#v", got, got, want)
			}
		})
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

func TestClassifyManagerError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "not found", err: fmt.Errorf("entity not found"), wantStatus: http.StatusNotFound},
		{name: "validation", err: fmt.Errorf("schema name is required"), wantStatus: http.StatusBadRequest},
		{name: "conflict", err: fmt.Errorf("duplicate key"), wantStatus: http.StatusConflict},
		{name: "internal", err: fmt.Errorf("db timeout"), wantStatus: http.StatusInternalServerError},
		// Sentinel error checks — wrapped errors must route via errors.Is.
		{name: "sentinel not found", err: fmt.Errorf("wrap: %w", forma.ErrNotFound), wantStatus: http.StatusNotFound},
		{name: "sentinel conflict", err: fmt.Errorf("wrap: %w", forma.ErrConflict), wantStatus: http.StatusConflict},
		{name: "sentinel invalid input", err: fmt.Errorf("wrap: %w", forma.ErrInvalidInput), wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyManagerError(tt.err)
			if got != tt.wantStatus {
				t.Fatalf("expected %d, got %d", tt.wantStatus, got)
			}
		})
	}
}

func TestHandleGetErrorMapping(t *testing.T) {
	rowID := uuid.New()

	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "not found", err: fmt.Errorf("entity not found"), wantStatus: http.StatusNotFound},
		{name: "internal", err: fmt.Errorf("db timeout"), wantStatus: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &Server{
				manager: &mockEntityManager{getErr: tt.err},
			}

			req := httptest.NewRequest(http.MethodGet, "/api/v1/lead/"+rowID.String(), nil)
			rec := httptest.NewRecorder()
			server.handleGet(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d", tt.wantStatus, rec.Code)
			}
		})
	}
}

func TestHandleQueryMapsValidationErrors(t *testing.T) {
	server := &Server{
		manager: &mockEntityManager{
			advancedErr: fmt.Errorf("cannot sort by unknown attribute 'x'"),
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/lead", nil)
	rec := httptest.NewRecorder()
	server.handleQuery(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

func TestHandleSearchMapsConflictErrors(t *testing.T) {
	server := &Server{
		manager: &mockEntityManager{
			crossSchemaErr: fmt.Errorf("duplicate request conflict"),
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/search?schemas=lead&q=john", nil)
	rec := httptest.NewRecorder()
	server.handleSearch(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected status 409, got %d", rec.Code)
	}
}

func TestNewServer_HealthRouteDisabledByDefault(t *testing.T) {
	server := NewServer(&mockEntityManager{}, Options{})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	// /health is not registered; the mux falls through to the catch-all
	// /api/v1/ handler which returns an error, not 200.
	if rec.Code == http.StatusOK {
		t.Fatalf("expected /health to not respond 200 when disabled, got %d", rec.Code)
	}
}

func TestNewServer_HealthRouteEnabledWhenOptionSet(t *testing.T) {
	server := NewServer(&mockEntityManager{}, Options{EnableHealth: true})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for /health when EnableHealth=true, got %d", rec.Code)
	}
}

// TestHandleAdvancedQueryStructuredSort pins the #240 wire contract: a JSON
// body carrying the structured "sort" field reaches the manager with per-key
// directions intact — the handler must not flatten or drop it.

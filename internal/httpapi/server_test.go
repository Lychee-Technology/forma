package httpapi

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
	advancedReq       *forma.QueryRequest
	getResult         *forma.DataRecord
	getErr            error
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
	if m.getResult != nil || m.getErr != nil {
		return m.getResult, m.getErr
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *mockEntityManager) Update(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
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

// TestHandleAdvancedQueryFederatedExecutionPlan pins the #243 wire contract:
// a federated request with include_execution_plan reaches the manager as a
// Federated hint, and the ExecutionPlan the manager returns marshals onto the
// HTTP response so a caller (e.g. the k6 suite) can read the actual route.
func TestHandleAdvancedQueryFederatedExecutionPlan(t *testing.T) {
	result := &forma.QueryResult{
		Data: []*forma.DataRecord{},
		ExecutionPlan: &forma.ExecutionPlan{
			Routing: forma.ExecutionRouting{UsedDuckDB: true, Tiers: []string{"hot", "warm", "cold"}, Reason: "hybrid"},
		},
	}
	mgr := &mockEntityManager{advancedResult: result}
	server := &Server{manager: mgr}

	payload := []byte(`{
		"schema_name": "lead",
		"condition": {"a": "status", "v": "equals:hot"},
		"page": 1,
		"items_per_page": 10,
		"federated": {"enabled": true, "include_execution_plan": true}
	}`)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	server.handleAdvancedQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Request side: the federated hint (incl. include_execution_plan) reached the manager.
	if mgr.advancedReq == nil || mgr.advancedReq.Federated == nil {
		t.Fatalf("expected federated hint on the manager request, got %+v", mgr.advancedReq)
	}
	if !mgr.advancedReq.Federated.Enabled || !mgr.advancedReq.Federated.IncludeExecutionPlan {
		t.Fatalf("federated flags not parsed: %+v", mgr.advancedReq.Federated)
	}

	// Response side: execution_plan is present on the wire with the real route.
	body := rec.Body.String()
	for _, want := range []string{`"execution_plan"`, `"used_duckdb":true`, `"reason":"hybrid"`} {
		if !bytes.Contains(rec.Body.Bytes(), []byte(want)) {
			t.Fatalf("response body missing %q; body: %s", want, body)
		}
	}
}

// TestHandleAdvancedQueryNoPlanOmitted pins that a plain (non-federated)
// response omits execution_plan entirely, so the field never appears unless a
// route was actually recorded.
func TestHandleAdvancedQueryNoPlanOmitted(t *testing.T) {
	mgr := &mockEntityManager{advancedResult: &forma.QueryResult{Data: []*forma.DataRecord{}}}
	server := &Server{manager: mgr}

	payload := []byte(`{"schema_name": "lead", "condition": {"a": "status", "v": "equals:hot"}, "page": 1, "items_per_page": 10}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	server.handleAdvancedQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if bytes.Contains(rec.Body.Bytes(), []byte(`"execution_plan"`)) {
		t.Fatalf("expected execution_plan omitted; body: %s", rec.Body.String())
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

func TestHandleAdvancedQueryURLAttrsOverrideBodyAttrs(t *testing.T) {
	manager := &mockEntityManager{
		advancedResult: &forma.QueryResult{Data: []*forma.DataRecord{}},
	}
	server := &Server{manager: manager}

	payload := []byte(`{
		"schema_name": "lead",
		"condition": {"l": "and", "c": [{"a": "status", "v": "equals:hot"}]},
		"attrs": ["body_attr"],
		"page": 1,
		"items_per_page": 10
	}`)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query?attrs=url_one,url_two", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	server.handleAdvancedQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if manager.advancedReq == nil {
		t.Fatalf("expected Query request to be captured")
	}

	expectedAttrs := []string{"url_one", "url_two"}
	if !reflect.DeepEqual(expectedAttrs, manager.advancedReq.Attrs) {
		t.Fatalf("expected attrs %v, got %v", expectedAttrs, manager.advancedReq.Attrs)
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

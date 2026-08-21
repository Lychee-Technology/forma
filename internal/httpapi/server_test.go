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
	batchDeleteResult *forma.BatchResult
	batchDeleteErr    error
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
	if m.batchDeleteResult != nil || m.batchDeleteErr != nil {
		return m.batchDeleteResult, m.batchDeleteErr
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *mockEntityManager) Close() error { return nil }

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
	// Contract since #301: classification is by sentinel evidence alone. The
	// substring heuristic is gone, so an error whose *text* reads like a client
	// fault but wraps no sentinel is a 500 — driver prose trips those words
	// (DuckDB renders a missing S3 object as "404 (Not Found)") and answering 404
	// tells clients and caches a storage failure means the resource is absent.
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "text-only not found is no longer 404", err: fmt.Errorf("entity not found"), wantStatus: http.StatusInternalServerError},
		{name: "text-only validation is no longer 400", err: fmt.Errorf("schema name is required"), wantStatus: http.StatusInternalServerError},
		{name: "text-only conflict is no longer 409", err: fmt.Errorf("duplicate key"), wantStatus: http.StatusInternalServerError},
		{name: "internal", err: fmt.Errorf("db timeout"), wantStatus: http.StatusInternalServerError},
		// Sentinel error checks — wrapped errors must route via errors.Is.
		{name: "sentinel not found", err: fmt.Errorf("wrap: %w", forma.ErrNotFound), wantStatus: http.StatusNotFound},
		{name: "sentinel conflict", err: fmt.Errorf("wrap: %w", forma.ErrConflict), wantStatus: http.StatusConflict},
		{name: "sentinel invalid input", err: fmt.Errorf("wrap: %w", forma.ErrInvalidInput), wantStatus: http.StatusBadRequest},
		{name: "nil", err: nil, wantStatus: http.StatusInternalServerError},
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

// TestGetByRowIDMapsManagerErrors drives the request through the registered mux
// so the assertion covers the chain production actually uses: apiHandler's GET
// branch -> handleQuery's row-id branch -> executeGet. Invoking a handler method
// directly would leave that routing unpinned, which is how #368's dead handler
// kept a test to itself after the live path had moved on.
func TestGetByRowIDMapsManagerErrors(t *testing.T) {
	rowID := uuid.New()

	// Since #301 only a sentinel earns the 404; "entity not found" as bare prose
	// is an unclassifiable failure and answers 500.
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "sentinel not found", err: fmt.Errorf("entity: %w", forma.ErrNotFound), wantStatus: http.StatusNotFound},
		{name: "text-only not found", err: fmt.Errorf("entity not found"), wantStatus: http.StatusInternalServerError},
		{name: "internal", err: fmt.Errorf("db timeout"), wantStatus: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := NewServer(&mockEntityManager{getErr: tt.err}, Options{})

			req := httptest.NewRequest(http.MethodGet, "/api/v1/lead/"+rowID.String(), nil)
			rec := httptest.NewRecorder()
			server.Handler().ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d", tt.wantStatus, rec.Code)
			}
		})
	}
}

// TestHandleQueryMapsValidationErrors uses the real post-#296 shape of the
// unknown-sort-attribute error: internal/entity_query_sort.go now wraps
// forma.ErrInvalidInput, which is what keeps it a 400 after #301 removed the
// substring heuristic. The bare-text version it used to assert on is a 500.
func TestHandleQueryMapsValidationErrors(t *testing.T) {
	server := &Server{
		manager: &mockEntityManager{
			advancedErr: fmt.Errorf("cannot sort by unknown attribute 'x' in schema 'lead': %w", forma.ErrInvalidInput),
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/lead", nil)
	rec := httptest.NewRecorder()
	server.handleQuery(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

// TestHandleSearchMapsConflictErrors pins that a 409 still reaches the client —
// but only via forma.ErrConflict. Since #301 the word "conflict" in the message
// carries no classification weight.
func TestHandleSearchMapsConflictErrors(t *testing.T) {
	server := &Server{
		manager: &mockEntityManager{
			crossSchemaErr: fmt.Errorf("duplicate request: %w", forma.ErrConflict),
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

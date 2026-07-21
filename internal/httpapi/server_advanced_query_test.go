package httpapi

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

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
			Sources: []forma.ExecutionSource{
				{Tier: "cold", Engine: "duckdb", ActualRows: 5, PredicatePushdown: true},
			},
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

	// Response side: execution_plan carries routing AND sources on the wire (#243).
	body := rec.Body.String()
	for _, want := range []string{`"execution_plan"`, `"used_duckdb":true`, `"reason":"hybrid"`, `"sources"`, `"tier":"cold"`} {
		if !bytes.Contains(rec.Body.Bytes(), []byte(want)) {
			t.Fatalf("response body missing %q; body: %s", want, body)
		}
	}
	// Security: the raw SQL / bind params must never reach the wire (P0).
	for _, forbidden := range []string{`"sql"`, `postgres_scan`, `"params"`, `password=`} {
		if bytes.Contains(rec.Body.Bytes(), []byte(forbidden)) {
			t.Fatalf("response body must not contain %q; body: %s", forbidden, body)
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

func TestHandleAdvancedQueryStructuredSort(t *testing.T) {
	mgr := &mockEntityManager{advancedResult: &forma.QueryResult{Data: []*forma.DataRecord{}}}
	server := &Server{manager: mgr}

	payload := []byte(`{
		"schema_name": "lead",
		"condition": {"a": "status", "v": "equals:hot"},
		"sort": [
			{"attribute": "status"},
			{"attribute": "created_at", "sort_order": "desc"}
		]
	}`)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	server.handleAdvancedQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if mgr.advancedReq == nil || len(mgr.advancedReq.Sort) != 2 {
		t.Fatalf("manager saw Sort = %+v, want 2 entries", mgr.advancedReq)
	}
	if mgr.advancedReq.Sort[1].Attribute != "created_at" || mgr.advancedReq.Sort[1].SortOrder != forma.SortOrderDesc {
		t.Fatalf("manager saw Sort[1] = %+v, want created_at desc", mgr.advancedReq.Sort[1])
	}
}

// TestHandleAdvancedQueryInvalidInputMapsTo400 pins that a manager-side
// validation failure (e.g. #240's sort vs sort_by mutual exclusion) surfaces
// as HTTP 400, not 500.
func TestHandleAdvancedQueryInvalidInputMapsTo400(t *testing.T) {
	mgr := &mockEntityManager{
		advancedErr: fmt.Errorf("sort cannot be combined with sort_by/sort_order in schema 'lead': %w", forma.ErrInvalidInput),
	}
	server := &Server{manager: mgr}

	payload := []byte(`{
		"schema_name": "lead",
		"condition": {"a": "status", "v": "equals:hot"},
		"sort": [{"attribute": "status"}],
		"sort_by": ["status"]
	}`)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	server.handleAdvancedQuery(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

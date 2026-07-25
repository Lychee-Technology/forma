package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// endpointCase drives one HTTP entry point with a failing manager.
type endpointCase struct {
	name    string
	method  string
	target  string
	body    string
	failing func(*mockEntityManager, error)
}

func allEndpointCases(rowID uuid.UUID) []endpointCase {
	return []endpointCase{
		{
			name: "create", method: http.MethodPost, target: "/api/v1/lead",
			body:    `{"name":"x"}`,
			failing: func(m *mockEntityManager, err error) { m.batchCreateErr = err },
		},
		{
			name: "get", method: http.MethodGet, target: "/api/v1/lead/" + rowID.String(),
			failing: func(m *mockEntityManager, err error) { m.getErr = err },
		},
		{
			name: "query", method: http.MethodGet, target: "/api/v1/lead?page=1&items_per_page=10",
			failing: func(m *mockEntityManager, err error) { m.advancedErr = err },
		},
		{
			name: "update", method: http.MethodPut, target: "/api/v1/lead/" + rowID.String(),
			body:    `{"name":"y"}`,
			failing: func(m *mockEntityManager, err error) { m.updateErr = err },
		},
		{
			name: "single delete", method: http.MethodDelete, target: "/api/v1/lead/" + rowID.String(),
			failing: func(m *mockEntityManager, err error) { m.batchDeleteErr = err },
		},
		{
			name: "batch delete", method: http.MethodDelete, target: "/api/v1/lead",
			body:    `["` + rowID.String() + `"]`,
			failing: func(m *mockEntityManager, err error) { m.batchDeleteErr = err },
		},
		{
			name: "search", method: http.MethodGet, target: "/api/v1/search?schemas=lead&q=john",
			failing: func(m *mockEntityManager, err error) { m.crossSchemaErr = err },
		},
		{
			name: "advanced query", method: http.MethodPost, target: "/api/v1/advanced_query",
			body: `{"schema_name":"lead",` +
				`"condition":{"l":"and","c":[{"a":"status","v":"equals:hot"}]},` +
				`"page":1,"items_per_page":10}`,
			failing: func(m *mockEntityManager, err error) { m.advancedErr = err },
		},
	}
}

// TestNoEndpointLeaksOperatorDetail is the #301 acceptance test. Every HTTP
// entry point is driven with the error chain the federated engine really
// produces, and no response body may carry any part of it. The log must carry
// all of it.
func TestNoEndpointLeaksOperatorDetail(t *testing.T) {
	rowID := uuid.New()

	for _, tc := range allEndpointCases(rowID) {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zap.ErrorLevel)
			restore := zap.ReplaceGlobals(zap.New(core))
			defer restore()

			manager := &mockEntityManager{}
			tc.failing(manager, operatorDetailError())
			srv := NewServer(manager, Options{})

			var reader *bytes.Reader
			if tc.body == "" {
				reader = bytes.NewReader(nil)
			} else {
				reader = bytes.NewReader([]byte(tc.body))
			}
			req := httptest.NewRequest(tc.method, tc.target, reader)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("expected status 500, got %d; body: %s", rec.Code, rec.Body.String())
			}

			body := rec.Body.String()
			for _, forbidden := range []string{
				canaryPassword, canaryKey, "password=", "s3://", "IO Error",
				"manifest lists", "schema 22", "postgres_scan",
			} {
				if strings.Contains(body, forbidden) {
					t.Fatalf("%s body leaked %q; body: %s", tc.name, forbidden, body)
				}
			}

			var resp APIResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("body is not valid JSON: %v", err)
			}
			if resp.ErrorClass != errorClassParquetSetInconsistent {
				t.Fatalf("expected class %q, got %q", errorClassParquetSetInconsistent, resp.ErrorClass)
			}
			if _, err := uuid.Parse(resp.ErrorID); err != nil {
				t.Fatalf("error_id %q is not a UUID: %v", resp.ErrorID, err)
			}

			entries := logs.All()
			if len(entries) == 0 {
				t.Fatalf("%s logged no ERROR entry, so the detail was destroyed rather than relocated", tc.name)
			}
			logged, _ := entries[len(entries)-1].ContextMap()["error"].(string)
			for _, required := range []string{canaryPassword, canaryKey} {
				if !strings.Contains(logged, required) {
					t.Fatalf("%s operator log lost %q; logged: %s", tc.name, required, logged)
				}
			}
		})
	}
}

// TestCreateValidationErrorIsClientError pins the #301 decision that
// handleCreate stops reporting write-path validation as 500. The documented
// contract is that ErrInvalidInput surfaces as 4xx, and only that
// reclassification keeps create failures legible once 5xx bodies are redacted.
func TestCreateValidationErrorIsClientError(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	manager := &mockEntityManager{
		batchCreateErr: fmt.Errorf("attribute 'age' (attrID=2): %w: cannot convert string to float64", forma.ErrInvalidInput),
	}
	srv := NewServer(manager, Options{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/lead", bytes.NewReader([]byte(`{"age":"x"}`)))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 for a validation error, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "cannot convert string to float64") {
		t.Fatalf("expected the validation detail preserved, got %s", rec.Body.String())
	}
}

// TestReadPathErrorsClassifyAs5xx pins status accuracy for the three read-path
// carriers: classifyManagerError falls back to substring matching, and
// ManifestSchemaMismatchError already says "must resolve", one word away from
// its "must be" probe. A misclassification here would return a misleading 4xx.
//
// It is NOT what keeps object keys out of the body — Task 3's revised gate makes
// disclosure depend on positive sentinel evidence (isClientError), not on the
// status, precisely because driver text trips these heuristics in practice
// (DuckDB renders a missing object as "404 (Not Found)"). This test guards the
// status; error_response_test.go guards the disclosure.
func TestReadPathErrorsClassifyAs5xx(t *testing.T) {
	errs := map[string]error{
		"parquet set inconsistent": &forma.ParquetSetInconsistentError{
			SchemaID: 22, MissingKeys: []string{"base/k.parquet"},
		},
		"no parquet paths configured":  &forma.NoParquetPathsError{SchemaID: 7, SourceConfigured: false},
		"no parquet paths from source": &forma.NoParquetPathsError{SchemaID: 7, SourceConfigured: true},
		"manifest schema mismatch": &forma.ManifestSchemaMismatchError{
			RequestedSchemaID: 1, ManifestSchemaID: 2, Path: "manifests/1.json",
		},
	}

	for name, err := range errs {
		t.Run(name, func(t *testing.T) {
			wrapped := fmt.Errorf("execute duckdb query: %w", err)
			if got := classifyManagerError(wrapped); got != http.StatusInternalServerError {
				t.Fatalf("read-path error classified as %d, which escapes redaction; message was: %v", got, wrapped)
			}
		})
	}
}

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
// produces, and no response body may carry any part of it. The log must carry the
// diagnosis — but not the credential: the log line this PR introduced is itself a
// destination, so the password canary is asserted absent there and the object key
// asserted present.
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
				canaryPassword, canarySecret, canarySecretHead, canarySecretTail, canaryKey, "password=", "s3://", "IO Error",
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
			for _, required := range []string{canaryKey, "password=***REDACTED***"} {
				if !strings.Contains(logged, required) {
					t.Fatalf("%s operator log lost %q; logged: %s", tc.name, required, logged)
				}
			}
			if leaksCanarySecret(logged) {
				t.Fatalf("%s operator log leaked the credential; logged: %s", tc.name, logged)
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
		batchCreateErr: forma.InvalidInputf("attribute 'age' (attrID=2): cannot convert string to float64"),
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

// TestCreateUnknownSchemaIs404AndVerbatim pins the externally visible
// behaviour change from #301: handleCreate answers its classified status, so
// POST to an unknown schema returns 404 instead of 500.
//
// It also pins that this 404 publishes. The chain is the shape the batch
// service now builds — the registry's name-keyed forma.NotFoundf leaf under a
// plain phase wrap under the batch loop's forma.WrapPublicf — so the body
// names the schema the caller sent plus the batch index, while the phase
// context ("failed to get schema") stays operator-only (#362 review, P2) and
// the body stays free of error_class/error_id, which is what distinguishes it
// from an error carrying no publication at all
// (TestUnconvertedSentinelIsRedacted4xx).
func TestCreateUnknownSchemaIs404AndVerbatim(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	manager := &mockEntityManager{
		batchCreateErr: forma.WrapPublicf(
			fmt.Errorf("failed to get schema: %w",
				forma.NotFoundf("schema not found: %s", "nosuchschema")),
			"operation[0]"),
	}
	srv := NewServer(manager, Options{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/nosuchschema", bytes.NewReader([]byte(`{"name":"x"}`)))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for an unknown schema on create, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if !strings.Contains(resp.Error, "operation[0]: schema not found: nosuchschema") {
		t.Fatalf("expected the published message naming the index and schema, got %q", resp.Error)
	}
	if strings.Contains(resp.Error, "failed to get schema") {
		t.Fatalf("an internal phase name reached the 404 body: %q", resp.Error)
	}
	if resp.ErrorClass != "" || resp.ErrorID != "" {
		t.Fatalf("a sentinel-carrying error took the redacted branch: error_class=%q error_id=%q",
			resp.ErrorClass, resp.ErrorID)
	}
}

// TestReadPathErrorsClassifyAs5xx pins status accuracy for the three read-path
// carriers. It is now structural rather than a near-miss check: classification
// reads sentinels only, and none of these carriers wraps a client sentinel, so
// each is a 500. It used to be delicate — the substring heuristic matched
// "must be", and ManifestSchemaMismatchError says "must resolve", one word away
// from a bogus 400. Removing the heuristic removed that hazard class entirely.
//
// It is NOT what keeps object keys out of the body — disclosure depends on
// positive sentinel evidence (isClientError). This test guards the status;
// error_response_test.go guards the disclosure.
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
		"manifest unstamped": &forma.ManifestUnstampedError{
			RequestedSchemaID: 1, Path: "manifests/shared.json", Entries: 2,
		},
		"parquet type incompatible": &forma.ParquetTypeIncompatibleError{
			SchemaID: 1, Attribute: "ratio", Declared: "bigint", Path: "s3://b/delta/k.parquet", FileType: "DOUBLE",
			Evidence: "it holds 1000.5, which is not a whole number in [-9007199254740992, 9007199254740992]",
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

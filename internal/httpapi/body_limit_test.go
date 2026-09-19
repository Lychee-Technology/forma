package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// TestOversizedBodyIsRefusedBeforeTheManager pins #465: every body-carrying
// endpoint refuses a body past Options.MaxBodyBytes with a disclosed 413 that
// names the limit, and the probe manager proves the request was rejected
// before any manager work — the body was never materialized into a batch.
func TestOversizedBodyIsRefusedBeforeTheManager(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	const limit = 64
	// A syntactically valid JSON body of well over 64 bytes: a decoder that
	// ignored the cap would parse it and reach the manager.
	big := `[` + strings.Repeat(`{"name":"x"},`, 20) + `{"name":"x"}]`
	if len(big) <= limit {
		t.Fatalf("test body must exceed the limit: %d <= %d", len(big), limit)
	}
	rowIDs := `["` + strings.Repeat(`0190b7b8-0000-7000-8000-000000000000","`, 3) +
		`0190b7b8-0000-7000-8000-000000000000"]`

	cases := []struct {
		name   string
		method string
		target string
		body   string
	}{
		{"create", http.MethodPost, "/api/v1/lead", big},
		{"update", http.MethodPut, "/api/v1/lead/0190b7b8-0000-7000-8000-000000000000", big},
		{"batch delete", http.MethodDelete, "/api/v1/lead", rowIDs},
		{"advanced query", http.MethodPost, "/api/v1/advanced_query", big},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(newParseProbeManager(), Options{MaxBodyBytes: limit})
			req := httptest.NewRequest(tc.method, tc.target, bytes.NewReader([]byte(tc.body)))
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("expected 413, got %d; body: %s", rec.Code, rec.Body.String())
			}
			var resp APIResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("body is not valid JSON: %v", err)
			}
			want := fmt.Sprintf("request body too large: request body exceeds %d bytes", limit)
			if resp.Error != want {
				t.Fatalf("body %q, want %q", resp.Error, want)
			}
			if resp.ErrorClass != "" || resp.ErrorID != "" {
				t.Fatalf("a body-size rejection took the redacted branch: error_class=%q error_id=%q",
					resp.ErrorClass, resp.ErrorID)
			}
		})
	}
}

// TestBodyUnderTheLimitReachesTheManager is the other half of the cap: the
// limit is a ceiling, not a tax on ordinary requests.
func TestBodyUnderTheLimitReachesTheManager(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	manager := &mockEntityManager{advancedResult: &forma.QueryResult{Data: []*forma.DataRecord{}}}
	srv := NewServer(manager, Options{MaxBodyBytes: 64})
	body := `{"schema_name":"lead","condition":{"a":"s","v":"equals:x"}}`
	if len(body) > 64 {
		t.Fatalf("test body must fit under the limit: %d", len(body))
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query", strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
	if manager.advancedReq == nil || manager.advancedReq.SchemaName != "lead" {
		t.Fatalf("manager did not receive the decoded request: %+v", manager.advancedReq)
	}
}

// TestDefaultBodyLimitIsTheEntitySizeLimit pins the zero-value contract:
// a Server built without a limit caps bodies at forma's MaxEntitySize.
func TestDefaultBodyLimitIsTheEntitySizeLimit(t *testing.T) {
	want := int64(forma.DefaultConfig(nil).Entity.MaxEntitySize)
	if want != 1<<20 {
		t.Fatalf("default MaxEntitySize moved to %d; update the README", want)
	}
	if got := NewServer(nil, Options{}).bodyLimit(); got != want {
		t.Fatalf("default body limit = %d, want %d", got, want)
	}
	if got := NewServer(nil, Options{MaxBodyBytes: 10}).bodyLimit(); got != 10 {
		t.Fatalf("configured body limit = %d, want 10", got)
	}
}

// TestDeadlineExceededAnswers504 pins the status and class a server-set
// timeout earns (#465): 504 with the "timeout" class and a fixed message, on
// the redacted branch like every other non-4xx.
func TestDeadlineExceededAnswers504(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	err := fmt.Errorf("failed to query records: %w", context.DeadlineExceeded)
	if got := classifyManagerError(err); got != http.StatusGatewayTimeout {
		t.Fatalf("classifyManagerError = %d, want 504", got)
	}
	if got := classifyManagerError(context.Canceled); got != http.StatusInternalServerError {
		t.Fatalf("context.Canceled must stay 500, got %d", got)
	}

	manager := &mockEntityManager{advancedErr: err}
	srv := NewServer(manager, Options{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query",
		strings.NewReader(`{"schema_name":"lead","condition":{"a":"s","v":"equals:x"}}`))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504, got %d; body: %s", rec.Code, rec.Body.String())
	}
	var resp APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if resp.Error != "request timed out" || resp.ErrorClass != errorClassTimeout || resp.ErrorID == "" {
		t.Fatalf("unexpected timeout body: %+v", resp)
	}
}

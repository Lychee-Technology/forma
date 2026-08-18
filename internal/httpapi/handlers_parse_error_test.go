package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/redact"
	"go.uber.org/zap"
)

// newParseProbeManager returns a manager whose every entry point fails with an
// operator error. A parse failure must reject the request before the manager
// is touched, so each case below proving a 400 with our exact published body
// (not a redacted 500) is also proof the manager was never reached.
func newParseProbeManager() *mockEntityManager {
	probe := errors.New("manager must not be reached by a parse failure")
	return &mockEntityManager{
		batchCreateErr: probe, getErr: probe, advancedErr: probe,
		updateErr: probe, batchDeleteErr: probe, crossSchemaErr: probe,
	}
}

// TestParseFailuresPublishThroughTheGate pins #360: every request-parsing
// rejection is a disclosed 400 built from a published message — same status
// and (mostly) same text as the pre-#360 direct writeError bodies, but now on
// the gated branch: no error_class, no error_id, scrub applied.
func TestParseFailuresPublishThroughTheGate(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	cases := []struct {
		name     string
		method   string
		target   string
		body     string
		wantBody string
	}{
		{"create bad path", http.MethodPost, "/api/v1/a/b/c", `{}`,
			"invalid path: invalid path format"},
		{"query empty schema", http.MethodGet, "/api/v1/", "",
			"invalid path: empty schema name"},
		{"create bad json", http.MethodPost, "/api/v1/lead", `{`,
			"invalid json body: unexpected EOF"},
		{"create empty array", http.MethodPost, "/api/v1/lead", `[]`,
			"invalid json body: empty array not allowed"},
		{"create non-object element", http.MethodPost, "/api/v1/lead", `[{"a":1},42]`,
			"invalid json body: body[1] must be an object"},
		{"create scalar body", http.MethodPost, "/api/v1/lead", `"x"`,
			"invalid json body: body must be an object or array"},
		{"get bad row_id", http.MethodGet, "/api/v1/lead/nope", "",
			"invalid row_id: invalid UUID length: 4"},
		{"query bad sort", http.MethodGet, "/api/v1/lead?sort_by=age&sort_order=up", "",
			"invalid sort parameters: invalid sort_order: up"},
		{"update bad row_id", http.MethodPut, "/api/v1/lead/nope", `{}`,
			"invalid row_id: invalid UUID length: 4"},
		{"update bad json", http.MethodPut, "/api/v1/lead/0190b7b8-0000-7000-8000-000000000000", `{`,
			"invalid json body: unexpected EOF"},
		{"single delete bad row_id", http.MethodDelete, "/api/v1/lead/nope", "",
			"invalid row_id: invalid UUID length: 4"},
		{"batch delete bad json", http.MethodDelete, "/api/v1/lead", `{`,
			"invalid json body: unexpected EOF"},
		{"batch delete bad element", http.MethodDelete, "/api/v1/lead", `["a"]`,
			"invalid row_id: index 0: invalid UUID length: 1"},
		{"advanced query bad json", http.MethodPost, "/api/v1/advanced_query", `{`,
			"invalid json body: unexpected EOF"},
		{"delete bad path", http.MethodDelete, "/api/v1/a/b/c", "",
			"invalid path: invalid path format"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(newParseProbeManager(), Options{})
			req := httptest.NewRequest(tc.method, tc.target, bytes.NewReader([]byte(tc.body)))
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d; body: %s", rec.Code, rec.Body.String())
			}
			var resp APIResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("body is not valid JSON: %v", err)
			}
			if resp.Error != tc.wantBody {
				t.Fatalf("body %q, want %q", resp.Error, tc.wantBody)
			}
			if resp.ErrorClass != "" || resp.ErrorID != "" {
				t.Fatalf("a parse failure took the redacted branch: error_class=%q error_id=%q",
					resp.ErrorClass, resp.ErrorID)
			}
		})
	}
}

// TestParseFailureScrubsPublishedCredential pins the concrete win of the
// conversion: sort_order is caller text interpolated into a published message,
// and before #360 the direct writeError path echoed it with no scrub at all.
func TestParseFailureScrubsPublishedCredential(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	srv := NewServer(newParseProbeManager(), Options{})
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/lead?sort_by=age&sort_order=password%3Ds3cr3t", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, "s3cr3t") {
		t.Fatalf("published parse message leaked credential-shaped caller text: %s", body)
	}
	if !strings.Contains(body, redact.Placeholder) {
		t.Fatalf("expected the scrub placeholder in the body, got: %s", body)
	}
}

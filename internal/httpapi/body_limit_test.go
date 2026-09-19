package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

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

// TestBodyCapCoversBytesAfterTheFirstValue pins the review finding on #465:
// json.Decoder stops at the end of the first value, so a body whose first
// value fits under the cap and whose trailing bytes do not must still answer
// 413. Before the drain, such a body decoded cleanly and reached the manager.
func TestBodyCapCoversBytesAfterTheFirstValue(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	const limit = 64
	small := `{"schema_name":"lead","condition":{"a":"s","v":"equals:x"}}`
	if len(small) > limit {
		t.Fatalf("the first value must fit under the limit: %d > %d", len(small), limit)
	}

	cases := []struct {
		name string
		tail string
	}{
		{"trailing whitespace", strings.Repeat(" ", 1000)},
		{"trailing second value", strings.Repeat(" ", 1000) + `{}`},
		// The second value begins under the cap and its tail crosses it: the
		// cap is still the verdict, because the drain reads to the end
		// before deciding (#465 review). Diagnosing the second value first
		// would answer 400 for a body the documented contract calls 413.
		{"second value then over the cap", ` {` + strings.Repeat(" ", 1000)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manager := newParseProbeManager()
			srv := NewServer(manager, Options{MaxBodyBytes: limit})
			req := httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query", strings.NewReader(small+tc.tail))
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
		})
	}
}

// TestSecondJSONValueUnderTheCapIsInvalidInput pins the other half of the
// drain: a body that fits under the cap but carries a second value after the
// first is a 400, not a request whose tail is silently discarded. Trailing
// whitespace stays legal, so newline-terminated bodies keep working.
func TestSecondJSONValueUnderTheCapIsInvalidInput(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	first := `{"schema_name":"lead","condition":{"a":"s","v":"equals:x"}}`
	cases := []struct {
		name     string
		body     string
		wantCode int
		wantErr  string
	}{
		{"second object", first + ` {}`, http.StatusBadRequest, "invalid json body: unexpected data after the JSON body"},
		{"second scalar", first + "\n1", http.StatusBadRequest, "invalid json body: unexpected data after the JSON body"},
		{"trailing garbage", first + "x", http.StatusBadRequest, "invalid json body: unexpected data after the JSON body"},
		{"trailing form feed", first + "\f", http.StatusBadRequest, "invalid json body: unexpected data after the JSON body"},
		{"trailing newline", first + "\n", http.StatusOK, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manager := &mockEntityManager{advancedResult: &forma.QueryResult{Data: []*forma.DataRecord{}}}
			srv := NewServer(manager, Options{})
			req := httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Fatalf("expected %d, got %d; body: %s", tc.wantCode, rec.Code, rec.Body.String())
			}
			if tc.wantCode == http.StatusOK {
				if manager.advancedReq == nil {
					t.Fatal("a newline-terminated body must reach the manager")
				}
				return
			}
			if manager.advancedReq != nil {
				t.Fatal("a refused body must not reach the manager")
			}
			var resp APIResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("body is not valid JSON: %v", err)
			}
			if resp.Error != tc.wantErr {
				t.Fatalf("body %q, want %q", resp.Error, tc.wantErr)
			}
		})
	}
}

// failingBody is a request body that yields prefix and then fails with err,
// standing in for a transport that gave up mid-upload.
type failingBody struct {
	prefix io.Reader
	err    error
}

func (b *failingBody) Read(p []byte) (int, error) {
	n, err := b.prefix.Read(p)
	if errors.Is(err, io.EOF) {
		return n, b.err
	}
	return n, err
}

func (b *failingBody) Close() error { return nil }

// TestBodyReadFailureIsNotMalformedJSON pins the #465 review finding: an
// error the transport returns while the body is being read is not the
// caller's JSON and must not be published as `invalid json body`. A read
// timeout (http.Server.ReadTimeout expiring mid-upload) is a 408 on the
// redacted branch with the timeout class and Connection: close; any other
// transport failure is the client going away and stays a redacted 500.
func TestBodyReadFailureIsNotMalformedJSON(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	timeout := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	reset := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
	cases := []struct {
		name       string
		err        error
		wantCode   int
		wantClass  string
		wantMsg    string
		wantClosed bool
	}{
		{"read timeout", timeout, http.StatusRequestTimeout, errorClassTimeout, "request timed out", true},
		{"connection reset", reset, http.StatusInternalServerError, errorClassInternal, "internal error", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manager := newParseProbeManager()
			srv := NewServer(manager, Options{})
			body := &failingBody{prefix: strings.NewReader(`{"schema_name":"lead","cond`), err: tc.err}
			req := httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query", body)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Fatalf("expected %d, got %d; body: %s", tc.wantCode, rec.Code, rec.Body.String())
			}
			var resp APIResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("body is not valid JSON: %v", err)
			}
			if resp.Error != tc.wantMsg || resp.ErrorClass != tc.wantClass || resp.ErrorID == "" {
				t.Fatalf("a transport failure must take the redacted branch: %+v", resp)
			}
			if strings.Contains(rec.Body.String(), "tcp") {
				t.Fatalf("network prose reached the body: %s", rec.Body.String())
			}
			if closed := rec.Header().Get("Connection") == "close"; closed != tc.wantClosed {
				t.Fatalf("Connection: close = %v, want %v", closed, tc.wantClosed)
			}
		})
	}
}

// TestReadTimeoutWhileUploadingAnswers408 is the real-connection form of the
// case above: an http.Server with a ReadTimeout, a client that sends the
// headers and half the body and then stalls. The recorder tests cannot see
// this because httptest enforces no socket deadlines.
func TestReadTimeoutWhileUploadingAnswers408(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	manager := newParseProbeManager()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: NewServer(manager, Options{}).Handler(), ReadTimeout: 300 * time.Millisecond}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	partial := `{"schema_name":"lead",`
	head := fmt.Sprintf("POST /api/v1/advanced_query HTTP/1.1\r\nHost: forma\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", len(partial)+64)
	if _, err := io.WriteString(conn, head+partial); err != nil {
		t.Fatalf("write: %v", err)
	}
	// The rest of the body never comes; the server's read deadline fires
	// inside readJSONBody and the handler must still get its answer out.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("no response after the read timeout: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("expected 408, got %d", resp.StatusCode)
	}
	if !resp.Close {
		t.Fatalf("a 408 must close the connection")
	}
	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if apiResp.ErrorClass != errorClassTimeout || apiResp.Error != "request timed out" {
		t.Fatalf("unexpected 408 body: %+v", apiResp)
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

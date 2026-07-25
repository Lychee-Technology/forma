package httpapi

import (
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

// TestErrorClassResolvesSentinelsThroughWraps pins that the public error-class
// vocabulary is resolved with errors.Is through arbitrary wrap depth, never by
// matching message text — the #301 body carries no message text, so the token is
// the only thing a client can discriminate on.
func TestErrorClassResolvesSentinelsThroughWraps(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "parquet set inconsistent behind two wraps",
			err: fmt.Errorf("execute duckdb query: %w: %w",
				&forma.ParquetSetInconsistentError{SchemaID: 22, MissingKeys: []string{"base/k.parquet"}},
				fmt.Errorf("IO Error: driver text")),
			want: errorClassParquetSetInconsistent,
		},
		{
			name: "no parquet paths",
			err:  fmt.Errorf("resolve paths: %w", &forma.NoParquetPathsError{SchemaID: 7, SourceConfigured: true}),
			want: errorClassNoParquetPaths,
		},
		{
			name: "manifest schema mismatch",
			err: fmt.Errorf("manifest parquet source: %w",
				&forma.ManifestSchemaMismatchError{RequestedSchemaID: 1, ManifestSchemaID: 2, Path: "manifests/1.json"}),
			want: errorClassManifestSchemaMismatch,
		},
		{
			name: "unclassified falls back to internal",
			err:  fmt.Errorf("db timeout"),
			want: errorClassInternal,
		},
		{
			name: "federated read failure is not given its own token",
			err:  fmt.Errorf("execute duckdb query: federated read failed: IO Error"),
			want: errorClassInternal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorClass(tt.err); got != tt.want {
				t.Fatalf("errorClass = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestPublicErrorMessageCarriesNoDetail pins that the only prose a redacted 5xx
// body carries is a fixed string per class — no schema id, no path, no driver
// text.
func TestPublicErrorMessageCarriesNoDetail(t *testing.T) {
	tests := map[string]string{
		errorClassParquetSetInconsistent: "internal read error",
		errorClassNoParquetPaths:         "internal read error",
		errorClassManifestSchemaMismatch: "internal read error",
		errorClassInternal:               "internal error",
	}

	for class, want := range tests {
		if got := publicErrorMessage(class); got != want {
			t.Fatalf("publicErrorMessage(%q) = %q, want %q", class, got, want)
		}
	}
}

// leakCanaries are the two strings that must never appear in a response body.
// The password one is the real shape: DuckDB's postgres_scan attach failure puts
// the connection string — password included — into its own prose, verified
// against duckdb-go v2.5.6. The key one is ParquetSetInconsistentError.MissingKeys.
const (
	canaryPassword = "password=SUPERSECRET-CANARY"
	canaryKey      = "base/schema_22/CANARY-KEY.parquet"
)

// operatorDetailError builds an error chain shaped like the one the federated
// engine actually returns: a typed read-path carrier wrapping raw driver text.
func operatorDetailError() error {
	return fmt.Errorf("execute duckdb query: %w: %w",
		&forma.ParquetSetInconsistentError{SchemaID: 22, MissingKeys: []string{canaryKey}},
		fmt.Errorf(`IO Error: Unable to connect to Postgres at "host=h port=5432 user=u %s dbname=d"`, canaryPassword))
}

// TestRespondErrorRedacts5xxAndLogsFullChain is the load-bearing #301 test. The
// body must carry no operator detail; the log must carry all of it, under an
// error_id matching the body's, so redaction relocates detail instead of
// destroying it. Before #301 there was no handler error logging at all, so the
// log half is not incidental.
func TestRespondErrorRedacts5xxAndLogsFullChain(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	rec := httptest.NewRecorder()
	respondError(rec, "query failed", operatorDetailError(), "schema", "orders")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", rec.Code)
	}

	body := rec.Body.String()
	for _, forbidden := range []string{
		canaryPassword, canaryKey, "password=", "s3://", "IO Error",
		"manifest lists", "schema 22", "postgres_scan",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("5xx body leaked %q; body: %s", forbidden, body)
		}
	}

	var resp APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if resp.Success {
		t.Fatalf("expected success=false")
	}
	if resp.Error != "internal read error" {
		t.Fatalf("expected generic message, got %q", resp.Error)
	}
	if resp.ErrorClass != errorClassParquetSetInconsistent {
		t.Fatalf("expected class %q, got %q", errorClassParquetSetInconsistent, resp.ErrorClass)
	}
	if _, err := uuid.Parse(resp.ErrorID); err != nil {
		t.Fatalf("error_id %q is not a UUID: %v", resp.ErrorID, err)
	}

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 ERROR log entry, got %d", len(entries))
	}
	entry := entries[0]
	if entry.Message != "query failed" {
		t.Fatalf("expected log message %q, got %q", "query failed", entry.Message)
	}

	fields := entry.ContextMap()
	logged, _ := fields["error"].(string)
	for _, required := range []string{canaryPassword, canaryKey, "schema 22"} {
		if !strings.Contains(logged, required) {
			t.Fatalf("operator log lost %q; logged error was: %s", required, logged)
		}
	}
	if fields["error_id"] != resp.ErrorID {
		t.Fatalf("log error_id %v does not match body %q", fields["error_id"], resp.ErrorID)
	}
	if fields["error_class"] != errorClassParquetSetInconsistent {
		t.Fatalf("log error_class = %v", fields["error_class"])
	}
	if fields["schema"] != "orders" {
		t.Fatalf("caller log field lost: schema = %v", fields["schema"])
	}
}

// TestRespondErrorKeeps4xxMessageVerbatim pins the other half of the contract:
// a client error describes caller-supplied input, so the caller still gets the
// full message and no correlation fields are emitted.
func TestRespondErrorKeeps4xxMessageVerbatim(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	err := fmt.Errorf("attribute 'age' (attrID=2): %w: cannot convert string to float64", forma.ErrInvalidInput)
	rec := httptest.NewRecorder()
	respondError(rec, "create failed", err, "schema", "orders")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}

	var resp APIResponse
	if uerr := json.Unmarshal(rec.Body.Bytes(), &resp); uerr != nil {
		t.Fatalf("body is not valid JSON: %v", uerr)
	}
	if !strings.Contains(resp.Error, "create failed: attribute 'age' (attrID=2)") {
		t.Fatalf("4xx message was not preserved verbatim: %q", resp.Error)
	}
	if !strings.Contains(resp.Error, "cannot convert string to float64") {
		t.Fatalf("4xx message was truncated: %q", resp.Error)
	}
	if resp.ErrorClass != "" || resp.ErrorID != "" {
		t.Fatalf("4xx must not emit correlation fields, got class=%q id=%q", resp.ErrorClass, resp.ErrorID)
	}
	if logs.Len() != 0 {
		t.Fatalf("a client error must not log at ERROR level, got %d entries", logs.Len())
	}
}

// TestRespondErrorWithStatusHonoursCallerStatus pins the executeGet path, which
// picks its message from the classified status and so passes the status in.
func TestRespondErrorWithStatusHonoursCallerStatus(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	rec := httptest.NewRecorder()
	respondErrorWithStatus(rec, http.StatusNotFound, "record not found",
		fmt.Errorf("wrap: %w", forma.ErrNotFound), "schema", "orders")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "record not found: wrap") {
		t.Fatalf("expected verbatim 404 message, got %s", rec.Body.String())
	}
}

// misclassifiedReadPathError reproduces the #301 leak that status-based gating
// missed. DuckDB reports a missing S3 object with the literal words "404 (Not
// Found)", which classifyManagerError's substring fallback reads as a client
// 404 — routing a read-path failure, S3 URL and connection string included,
// straight at the verbatim branch.
func misclassifiedReadPathError() error {
	return fmt.Errorf("execute duckdb query: %w: %w",
		&forma.ParquetSetInconsistentError{SchemaID: 22, MissingKeys: []string{canaryKey}},
		fmt.Errorf(`HTTP Error: Unable to connect to URL "https://b.s3.amazonaws.com/%s": 404 (Not Found). `+
			`Also failed to attach postgres_scan with "host=h user=u %s"`, canaryKey, canaryPassword))
}

// TestRespondErrorRedactsMisclassified4xxReadPathError is the regression test for
// the fix: disclosure is gated on positive sentinel evidence, not on the status,
// so an error the heuristic mislabels 4xx is still redacted. The status stays 404
// on purpose — only the body is a leak — and the log must still receive the chain,
// because at 4xx the old code logged at Debug and production runs at Info.
func TestRespondErrorRedactsMisclassified4xxReadPathError(t *testing.T) {
	err := misclassifiedReadPathError()

	// Prove the hazard is real rather than a strawman: the heuristic genuinely
	// calls this read-path failure a client 404.
	if got := classifyManagerError(err); got != http.StatusNotFound {
		t.Fatalf("precondition failed: expected the heuristic to misclassify as 404, got %d", got)
	}

	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	rec := httptest.NewRecorder()
	respondError(rec, "query failed", err, "schema", "orders")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status must stay as classified (404), got %d", rec.Code)
	}

	body := rec.Body.String()
	for _, forbidden := range []string{
		canaryPassword, canaryKey, "password=", "s3://", "https://",
		"404 (Not Found)", "HTTP Error", "postgres_scan",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("misclassified 4xx body leaked %q; body: %s", forbidden, body)
		}
	}

	var resp APIResponse
	if uerr := json.Unmarshal(rec.Body.Bytes(), &resp); uerr != nil {
		t.Fatalf("body is not valid JSON: %v", uerr)
	}
	if resp.ErrorClass != errorClassParquetSetInconsistent {
		t.Fatalf("expected class %q, got %q", errorClassParquetSetInconsistent, resp.ErrorClass)
	}
	if _, perr := uuid.Parse(resp.ErrorID); perr != nil {
		t.Fatalf("error_id %q is not a UUID: %v", resp.ErrorID, perr)
	}

	// A redacted 4xx must still reach operators at ERROR level.
	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 ERROR log entry for a redacted 404, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	logged, _ := fields["error"].(string)
	for _, required := range []string{canaryPassword, canaryKey, "404 (Not Found)"} {
		if !strings.Contains(logged, required) {
			t.Fatalf("operator log lost %q; logged error was: %s", required, logged)
		}
	}
	if fields["error_id"] != resp.ErrorID {
		t.Fatalf("log error_id %v does not match body %q", fields["error_id"], resp.ErrorID)
	}
}

// TestRespondErrorRedactsHeuristicOnly4xx pins an accepted consequence of gating
// on sentinels: a genuine client error that wraps no sentinel and is recognised
// only by substring matching now gets a redacted body, so its guidance no longer
// reaches the caller.
//
// This is human-approved. Such messages stay opaque until they are made to wrap
// forma.ErrInvalidInput, which issue #296 tracks; the alternative — trusting the
// heuristic — leaks S3 URLs and the Postgres password, which is strictly worse.
// The status is still a correct 400, so clients keying on status are unaffected.
func TestRespondErrorRedactsHeuristicOnly4xx(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	err := fmt.Errorf("cannot sort by unknown attribute %q", "nope")
	if got := classifyManagerError(err); got != http.StatusBadRequest {
		t.Fatalf("precondition failed: expected heuristic 400, got %d", got)
	}
	if isClientError(err) {
		t.Fatalf("precondition failed: this error wraps no sentinel")
	}

	rec := httptest.NewRecorder()
	respondError(rec, "query failed", err, "schema", "orders")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status must stay 400, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "unknown attribute") {
		t.Fatalf("expected a redacted body for a sentinel-less error, got %s", rec.Body.String())
	}

	var resp APIResponse
	if uerr := json.Unmarshal(rec.Body.Bytes(), &resp); uerr != nil {
		t.Fatalf("body is not valid JSON: %v", uerr)
	}
	if resp.Error != "internal error" {
		t.Fatalf("expected the generic internal message, got %q", resp.Error)
	}
}

// TestRespondErrorLogLevels pins the log level of each branch. Levels are now
// contract: the redacted branch must be ERROR so it survives production's Info
// threshold, and the verbatim branch must stay DEBUG so client mistakes do not
// page anyone. Asserting Debug positively also closes a hole in
// TestRespondErrorKeeps4xxMessageVerbatim, whose zero-ERROR-entries check would
// pass even if nothing were logged at all.
func TestRespondErrorLogLevels(t *testing.T) {
	t.Run("verbatim client error logs at debug", func(t *testing.T) {
		core, logs := observer.New(zap.DebugLevel)
		restore := zap.ReplaceGlobals(zap.New(core))
		defer restore()

		rec := httptest.NewRecorder()
		respondError(rec, "create failed",
			fmt.Errorf("attribute 'age': %w", forma.ErrInvalidInput), "schema", "orders")

		entries := logs.All()
		if len(entries) != 1 {
			t.Fatalf("expected exactly 1 log entry, got %d", len(entries))
		}
		if entries[0].Level != zap.DebugLevel {
			t.Fatalf("expected the verbatim branch to log at DEBUG, got %s", entries[0].Level)
		}
	})

	t.Run("redacted error logs at error", func(t *testing.T) {
		core, logs := observer.New(zap.DebugLevel)
		restore := zap.ReplaceGlobals(zap.New(core))
		defer restore()

		rec := httptest.NewRecorder()
		respondError(rec, "query failed", operatorDetailError(), "schema", "orders")

		entries := logs.All()
		if len(entries) != 1 {
			t.Fatalf("expected exactly 1 log entry, got %d", len(entries))
		}
		if entries[0].Level != zap.ErrorLevel {
			t.Fatalf("expected the redacted branch to log at ERROR, got %s", entries[0].Level)
		}
	})
}

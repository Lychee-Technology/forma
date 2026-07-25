package httpapi

import (
	"encoding/json"
	"errors"
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
// body must carry no operator detail; the log must carry all of it *except the
// credential*, under an error_id matching the body's, so redaction relocates
// detail instead of destroying it. Before #301 there was no handler error logging
// at all, so the log half is not incidental — and for the same reason the log is a
// new exposure surface, which is why the password canary is asserted absent from
// it while the object key is asserted present.
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
	// Non-secret operator detail must survive: without the object key and schema id
	// the error_id correlates to nothing actionable.
	for _, required := range []string{canaryKey, "schema 22", "IO Error"} {
		if !strings.Contains(logged, required) {
			t.Fatalf("operator log lost %q; logged error was: %s", required, logged)
		}
	}
	// The credential must not: this log line is written by every redacted response
	// and flows into whatever collector and retention the deployment runs.
	if strings.Contains(logged, canaryPassword) {
		t.Fatalf("operator log leaked the credential; logged error was: %s", logged)
	}
	if !strings.Contains(logged, "password=***REDACTED***") {
		t.Fatalf("expected the credential to be replaced in place; logged error was: %s", logged)
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

// driverNotFoundTextError reproduces the #301 leak that status-based gating
// missed. DuckDB reports a missing S3 object with the literal words "404 (Not
// Found)", which the old substring heuristic read as a client 404 — routing a
// read-path failure, S3 URL and connection string included, at the verbatim
// branch, and answering a storage failure with the HTTP status for "the resource
// does not exist".
func driverNotFoundTextError() error {
	return fmt.Errorf("execute duckdb query: %w: %w",
		&forma.ParquetSetInconsistentError{SchemaID: 22, MissingKeys: []string{canaryKey}},
		fmt.Errorf(`HTTP Error: Unable to connect to URL "https://b.s3.amazonaws.com/%s": 404 (Not Found). `+
			`Also failed to attach postgres_scan with "host=h user=u %s"`, canaryKey, canaryPassword))
}

// TestReadPathDriverErrorIs500AndRedacted pins both halves of the fixed contract
// for a read-path failure whose driver prose reads like a client error.
//
// It used to assert 404 with a redacted body, on the reasoning that only the body
// was a leak. That was wrong: status is protocol semantics. A client, cache, or
// alerting rule that receives 404 concludes the resource is absent, stops
// retrying, and may cache the negative result — for what is actually an S3 or
// credential failure. Classification now reads sentinels only, so this chain
// (which wraps no client sentinel) is a 500. The log must still receive the
// detail, minus the credential.
func TestReadPathDriverErrorIs500AndRedacted(t *testing.T) {
	err := driverNotFoundTextError()

	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	rec := httptest.NewRecorder()
	respondError(rec, "query failed", err, "schema", "orders")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("a read-path driver failure must answer 500, got %d", rec.Code)
	}

	body := rec.Body.String()
	for _, forbidden := range []string{
		canaryPassword, canaryKey, "password=", "s3://", "https://",
		"404 (Not Found)", "HTTP Error", "postgres_scan",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("read-path 500 body leaked %q; body: %s", forbidden, body)
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

	// A redacted response must still reach operators at ERROR level, with the
	// diagnosis intact and only the credential removed.
	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 ERROR log entry, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	logged, _ := fields["error"].(string)
	for _, required := range []string{canaryKey, "404 (Not Found)", "postgres_scan"} {
		if !strings.Contains(logged, required) {
			t.Fatalf("operator log lost %q; logged error was: %s", required, logged)
		}
	}
	if strings.Contains(logged, canaryPassword) {
		t.Fatalf("operator log leaked the credential; logged error was: %s", logged)
	}
	if fields["error_id"] != resp.ErrorID {
		t.Fatalf("log error_id %v does not match body %q", fields["error_id"], resp.ErrorID)
	}
}

// TestUnknownSortAttributeIs400AndVerbatim closes the loop between #301 and #296.
//
// Removing the substring heuristic would have regressed the one genuine client
// error that relied on it — `cannot sort by unknown attribute` — from a 400 with
// usable guidance to a 500 with an opaque body. internal/entity_query_sort.go now
// wraps forma.ErrInvalidInput instead, so it reaches 400 through the sentinel
// branch and keeps its verbatim message. This is the error shape that function
// actually produces, prose unchanged.
func TestUnknownSortAttributeIs400AndVerbatim(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	err := fmt.Errorf("cannot sort by unknown attribute '%s' in schema '%s': %w",
		"nope", "lead", forma.ErrInvalidInput)

	rec := httptest.NewRecorder()
	respondError(rec, "query failed", err, "schema", "lead")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var resp APIResponse
	if uerr := json.Unmarshal(rec.Body.Bytes(), &resp); uerr != nil {
		t.Fatalf("body is not valid JSON: %v", uerr)
	}
	if !strings.Contains(resp.Error, "cannot sort by unknown attribute 'nope' in schema 'lead'") {
		t.Fatalf("expected the guidance preserved verbatim, got %q", resp.Error)
	}
	if resp.ErrorClass != "" || resp.ErrorID != "" {
		t.Fatalf("a sentinel-carrying 400 must not emit correlation fields, got class=%q id=%q",
			resp.ErrorClass, resp.ErrorID)
	}
}

// TestSentinelLessErrorIsRedacted500 keeps a redacted-non-sentinel case that does
// not depend on any trigger word: an error with no sentinel evidence at all is a
// 500 with an opaque body, whatever its prose.
func TestSentinelLessErrorIsRedacted500(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	err := fmt.Errorf("connection reset by peer while streaming %s", canaryKey)
	if isClientError(err) {
		t.Fatalf("precondition failed: this error wraps no sentinel")
	}

	rec := httptest.NewRecorder()
	respondError(rec, "query failed", err, "schema", "orders")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), canaryKey) {
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

// TestMixedChainVerbatimBodyCarriesNoCredential is the Finding 3 canary.
//
// isClientError uses errors.Is, which matches any leaf, so a joined chain
// carrying both a client sentinel and a driver cause takes the *verbatim* branch
// and discloses the driver text. `fmt.Errorf("%w: %w", …)` of exactly this shape
// is used throughout internal/federated, so the shape is not hypothetical.
//
// What this test guarantees is the credential half: redactCredentials runs on the
// verbatim body too, so no chain shape can put the password in a response.
//
// RESIDUAL, deliberately asserted rather than fixed here: the same body still
// discloses the *non-credential* operator cause — the S3 object key below reaches
// the client in a 400. That follows from the verbatim branch echoing raw chain
// text at all. Closing it means giving client errors typed public messages
// instead of echoing the chain, which is a redesign of the 4xx surface and larger
// than #301. Tracked as a follow-up.
func TestMixedChainVerbatimBodyCarriesNoCredential(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	driver := fmt.Errorf(`IO Error: Unable to connect to Postgres at "host=h user=u %s dbname=d" reading %s`,
		canaryPassword, canaryKey)
	err := errors.Join(forma.ErrInvalidInput, driver)

	rec := httptest.NewRecorder()
	respondError(rec, "query failed", err, "schema", "orders")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 from the sentinel leaf, got %d", rec.Code)
	}

	var resp APIResponse
	if uerr := json.Unmarshal(rec.Body.Bytes(), &resp); uerr != nil {
		t.Fatalf("body is not valid JSON: %v", uerr)
	}
	if resp.ErrorClass != "" || resp.ErrorID != "" {
		t.Fatalf("precondition failed: expected the verbatim branch, got class=%q id=%q",
			resp.ErrorClass, resp.ErrorID)
	}
	if !strings.Contains(resp.Error, "IO Error") {
		t.Fatalf("precondition failed: expected the verbatim branch to echo the driver cause, got %q", resp.Error)
	}

	if strings.Contains(resp.Error, canaryPassword) {
		t.Fatalf("a mixed chain leaked the credential into a 400 body: %q", resp.Error)
	}
	if !strings.Contains(resp.Error, "password=***REDACTED***") {
		t.Fatalf("expected the credential replaced in place, got %q", resp.Error)
	}

	// The residual, pinned so it is a decision and not a surprise.
	if !strings.Contains(resp.Error, canaryKey) {
		t.Fatalf("expected the documented residual (non-credential operator detail "+
			"reaching a mixed-chain 400 body) to still hold; if this now passes, the "+
			"4xx surface was redesigned and this comment is stale: %q", resp.Error)
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

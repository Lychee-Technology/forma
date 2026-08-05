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
		canaryPassword, canarySecret, canarySecretHead, canarySecretTail, canaryKey, "password=", "s3://", "IO Error",
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
	if leaksCanarySecret(logged) {
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

// TestRespondError4xxPublishesOnlyTheCarriersMessage pins the other half of
// the contract (#313): the caller gets the deliberately published message —
// and only that. Operator detail attached to the same error stays out of the
// body, and its presence promotes the log line to WARN because the boundary
// just withheld text whose only remaining copy is that line.
func TestRespondError4xxPublishesOnlyTheCarriersMessage(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	err := forma.WithOperatorDetail(
		forma.InvalidInputf("attribute 'age' (attrID=2): cannot convert string to float64"),
		fmt.Errorf("while flushing %s", canaryKey))
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
		t.Fatalf("published message was not preserved: %q", resp.Error)
	}
	if !strings.Contains(resp.Error, "cannot convert string to float64") {
		t.Fatalf("published message was truncated: %q", resp.Error)
	}
	if strings.Contains(rec.Body.String(), canaryKey) {
		t.Fatalf("operator detail leaked into the 400 body: %s", rec.Body.String())
	}
	if resp.ErrorClass != "" || resp.ErrorID != "" {
		t.Fatalf("4xx must not emit correlation fields, got class=%q id=%q", resp.ErrorClass, resp.ErrorID)
	}

	entries := logs.All()
	if len(entries) != 1 || entries[0].Level != zap.WarnLevel {
		t.Fatalf("withheld operator detail must log at WARN, got %d entries %v", len(entries), entries)
	}
	logged, _ := entries[0].ContextMap()["error"].(string)
	if !strings.Contains(logged, canaryKey) {
		t.Fatalf("operator log lost the withheld detail; logged: %s", logged)
	}
}

// TestRespondErrorWithStatusHonoursCallerStatus pins the executeGet path, which
// picks its message from the classified status and so passes the status in.
func TestRespondErrorWithStatusHonoursCallerStatus(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	rec := httptest.NewRecorder()
	respondErrorWithStatus(rec, http.StatusNotFound, "record not found",
		forma.NotFoundf("wrap"), "schema", "orders")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "record not found: wrap") {
		t.Fatalf("expected verbatim 404 message, got %s", rec.Body.String())
	}
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
		canaryPassword, canarySecret, canarySecretHead, canarySecretTail, canaryKey, "password=", "s3://", "https://",
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
	if leaksCanarySecret(logged) {
		t.Fatalf("operator log leaked the credential; logged error was: %s", logged)
	}
	if fields["error_id"] != resp.ErrorID {
		t.Fatalf("log error_id %v does not match body %q", fields["error_id"], resp.ErrorID)
	}
}

// TestUnknownSortAttributeIs400AndPublished closes the loop between #301,
// #296, and #313: the unknown-sort-attribute error keeps its 400 and its
// guidance, now as a published carrier message. The sentinel suffix that #309
// kept as body contract no longer reaches the body at all — only Error()
// carries it — which the negative assertion pins. This is the error shape
// internal/entity_query_sort.go actually produces, prose unchanged.
func TestUnknownSortAttributeIs400AndPublished(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	err := forma.InvalidInputf("cannot sort by unknown attribute '%s' in schema '%s'",
		"nope", "lead")

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
		t.Fatalf("expected the guidance preserved, got %q", resp.Error)
	}
	if strings.Contains(resp.Error, ": invalid input") {
		t.Fatalf("the sentinel suffix must stay out of the body (#313), got %q", resp.Error)
	}
	if resp.ErrorClass != "" || resp.ErrorID != "" {
		t.Fatalf("a published 400 must not emit correlation fields, got class=%q id=%q",
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

// TestRespondErrorLogLevels pins the log level of each branch. Levels are
// contract: the redacted branch must be ERROR so it survives production's Info
// threshold; a disclosed 4xx with no withheld detail stays DEBUG so client
// mistakes do not page anyone; a disclosed 4xx that withholds operator detail
// logs at WARN — that line is the only remaining copy of the detail, and it
// must clear the Info threshold without inheriting ERROR's alerting weight
// (a caller can trigger 4xx at will).
func TestRespondErrorLogLevels(t *testing.T) {
	t.Run("published client error logs at debug", func(t *testing.T) {
		core, logs := observer.New(zap.DebugLevel)
		restore := zap.ReplaceGlobals(zap.New(core))
		defer restore()

		rec := httptest.NewRecorder()
		respondError(rec, "create failed",
			forma.InvalidInputf("attribute 'age'"), "schema", "orders")

		entries := logs.All()
		if len(entries) != 1 {
			t.Fatalf("expected exactly 1 log entry, got %d", len(entries))
		}
		if entries[0].Level != zap.DebugLevel {
			t.Fatalf("expected the published branch to log at DEBUG, got %s", entries[0].Level)
		}
	})

	t.Run("published client error with withheld detail logs at warn", func(t *testing.T) {
		core, logs := observer.New(zap.DebugLevel)
		restore := zap.ReplaceGlobals(zap.New(core))
		defer restore()

		rec := httptest.NewRecorder()
		respondError(rec, "create failed",
			forma.WithOperatorDetail(forma.InvalidInputf("attribute 'age'"),
				fmt.Errorf("operator cause")), "schema", "orders")

		entries := logs.All()
		if len(entries) != 1 {
			t.Fatalf("expected exactly 1 log entry, got %d", len(entries))
		}
		if entries[0].Level != zap.WarnLevel {
			t.Fatalf("expected the withheld-detail branch to log at WARN, got %s", entries[0].Level)
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

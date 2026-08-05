package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// This file gates the disclosure decision (#301 Finding 3, redesigned by
// #313): a 4xx body carries a deliberately published message or no message at
// all. Chain shape stopped being load-bearing — a bare sentinel wrap is
// denied whether its chain is single- or multi-cause, and a carrier's
// publication survives any amount of re-wrapping or joining.

// TestMixedChainIsRedacted is the Finding 3 regression, now serving as the
// deny-shape pin: a chain that carries client-sentinel evidence but no
// published message is redacted, so a joined driver cause can never reach a
// 400 body. The *status* still follows the sentinel — a 400 with a redacted
// body is the deny shape itself.
func TestMixedChainIsRedacted(t *testing.T) {
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
	if resp.ErrorClass == "" || resp.ErrorID == "" {
		t.Fatalf("expected the redacted branch, got class=%q id=%q", resp.ErrorClass, resp.ErrorID)
	}
	if resp.Error != "internal error" {
		t.Fatalf("expected the generic message, got %q", resp.Error)
	}
	if strings.Contains(resp.Error, canaryKey) {
		t.Fatalf("a mixed chain leaked the object key into a 400 body: %q", resp.Error)
	}
	if leaksCanarySecret(rec.Body.String()) {
		t.Fatalf("a mixed chain leaked the credential into a 400 body: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "IO Error") {
		t.Fatalf("a mixed chain echoed the driver cause into a 400 body: %s", rec.Body.String())
	}
}

// TestMultiVerbWrapChainIsRedacted covers the other producer of the same
// carrier-less mixed shape: fmt.Errorf with two %w verbs, wrapped once more so
// the fan-out is not the outermost node.
func TestMultiVerbWrapChainIsRedacted(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	driver := fmt.Errorf("HTTP Error: 403 reading %s", canaryKey)
	err := fmt.Errorf("advanced query: %w",
		fmt.Errorf("filter rejected: %w: %w", forma.ErrInvalidInput, driver))

	rec := httptest.NewRecorder()
	respondError(rec, "query failed", err, "schema", "orders")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 from the sentinel leaf, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), canaryKey) {
		t.Fatalf("a nested multi-cause chain leaked the object key: %s", rec.Body.String())
	}
}

// TestUnconvertedSentinelIsRedacted4xx pins deny-by-default explicitly: a
// bare fmt.Errorf wrap of a client sentinel — the pre-#313 idiom — earns its
// status but not a body. This is the enforcement mechanism for future wrap
// sites: forgetting the carrier degrades the caller's 400 to an opaque one,
// which the site's own feature test should catch.
func TestUnconvertedSentinelIsRedacted4xx(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	err := fmt.Errorf("advanced query: %w",
		fmt.Errorf("attribute not found in cache: %s: %w", "nosuchattr", forma.ErrInvalidInput))

	rec := httptest.NewRecorder()
	respondError(rec, "query failed", err, "schema", "orders")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var resp APIResponse
	if uerr := json.Unmarshal(rec.Body.Bytes(), &resp); uerr != nil {
		t.Fatalf("body is not valid JSON: %v", uerr)
	}
	if resp.ErrorClass == "" || resp.ErrorID == "" {
		t.Fatalf("a publication-less sentinel wrap must take the redacted branch, got class=%q id=%q",
			resp.ErrorClass, resp.ErrorID)
	}
	if strings.Contains(resp.Error, "nosuchattr") {
		t.Fatalf("raw chain text reached the 400 body: %q", resp.Error)
	}
}

// TestClientErrorPublishesItsMessage is the other side of the gate: the shape
// every converted client error now has — a carrier under plain context wraps —
// reaches the caller with its published message intact.
func TestClientErrorPublishesItsMessage(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	err := fmt.Errorf("advanced query: %w",
		forma.InvalidInputf("attribute not found in cache: %s", "nosuchattr"))

	rec := httptest.NewRecorder()
	respondError(rec, "query failed", err, "schema", "orders")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var resp APIResponse
	if uerr := json.Unmarshal(rec.Body.Bytes(), &resp); uerr != nil {
		t.Fatalf("body is not valid JSON: %v", uerr)
	}
	if resp.ErrorClass != "" || resp.ErrorID != "" {
		t.Fatalf("a published client error took the redacted branch: class=%q id=%q",
			resp.ErrorClass, resp.ErrorID)
	}
	if !strings.Contains(resp.Error, "nosuchattr") {
		t.Fatalf("expected the published message naming the attribute, got %q", resp.Error)
	}
}

// TestMixedChainPublishesClientTextOnly is the #313 acceptance test, using the
// shape internal/federated/duckdb_query_build.go actually produces for an
// unrenderable caller-supplied path template: a carrier with the render error
// attached as operator detail. Pre-#313 this chain collapsed to an opaque
// redacted 400; now the caller learns what to fix while the operator cause
// stays out of the body.
func TestMixedChainPublishesClientTextOnly(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	renderErr := fmt.Errorf(`template: s3path:1: unclosed action reading %s`, canaryKey)
	err := fmt.Errorf("render parquet path hint: %w", forma.WithOperatorDetail(
		forma.InvalidInputf("render s3 parquet path template %q: the template is not renderable",
			"s3://b/{{.Broken"), renderErr))

	rec := httptest.NewRecorder()
	respondError(rec, "advanced query failed", err, "schema", "orders")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var resp APIResponse
	if uerr := json.Unmarshal(rec.Body.Bytes(), &resp); uerr != nil {
		t.Fatalf("body is not valid JSON: %v", uerr)
	}
	if !strings.Contains(resp.Error, `"s3://b/{{.Broken"`) || !strings.Contains(resp.Error, "not renderable") {
		t.Fatalf("expected the published template guidance, got %q", resp.Error)
	}
	if strings.Contains(rec.Body.String(), canaryKey) {
		t.Fatalf("operator detail leaked into the 400 body: %s", rec.Body.String())
	}
	if strings.Contains(resp.Error, "unclosed action") {
		t.Fatalf("text/template internals leaked into the 400 body: %q", resp.Error)
	}
	if resp.ErrorClass != "" || resp.ErrorID != "" {
		t.Fatalf("a published 400 must not emit correlation fields, got class=%q id=%q",
			resp.ErrorClass, resp.ErrorID)
	}
}

// TestCarrierSurvivesReWrapping pins the resolution depth: service layers add
// plain context wraps above WrapPublicf, and the boundary must still find the
// publication and emit the accumulated prefix + leaf.
func TestCarrierSurvivesReWrapping(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	leaf := forma.InvalidInputf("invalid value for attribute 'age' (attrID=2): cannot convert string to float64")
	err := forma.WrapPublicf(leaf, "operation[0]: failed to transform data to persistent record")
	err = fmt.Errorf("batch create: %w", err)
	err = fmt.Errorf("manager: %w", err)

	rec := httptest.NewRecorder()
	respondError(rec, "batch create failed", err)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, required := range []string{"operation[0]", "attribute 'age' (attrID=2)", "cannot convert string to float64"} {
		if !strings.Contains(body, required) {
			t.Fatalf("published message lost %q; body: %s", required, body)
		}
	}
}

// TestCarrierAtA5xxIsRedacted pins that a caller-supplied status can only hold
// disclosure back, never open it: a publishing carrier at a 5xx is redacted
// like any other operator failure.
func TestCarrierAtA5xxIsRedacted(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	rec := httptest.NewRecorder()
	respondErrorWithStatus(rec, http.StatusInternalServerError, "query failed",
		forma.InvalidInputf("bad filter naming %s", canaryKey))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), canaryKey) {
		t.Fatalf("a 5xx disclosed a published message: %s", rec.Body.String())
	}

	var resp APIResponse
	if uerr := json.Unmarshal(rec.Body.Bytes(), &resp); uerr != nil {
		t.Fatalf("body is not valid JSON: %v", uerr)
	}
	if resp.Error != "internal error" {
		t.Fatalf("expected the generic message, got %q", resp.Error)
	}
}

// TestPublishedMessageIsCredentialScrubbed keeps the boundary's stated
// invariant — every string leaving it passes redactCredentials — on the new
// disclosed branch: a wrap site that interpolates a DSN into its published
// message by accident does not ship the password.
func TestPublishedMessageIsCredentialScrubbed(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	err := forma.InvalidInputf("cannot use connection string %q",
		"host=h user=u password="+canarySecret+" dbname=d")

	rec := httptest.NewRecorder()
	respondError(rec, "create failed", err)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	if leaksCanarySecret(rec.Body.String()) {
		t.Fatalf("a published message shipped the credential: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "***REDACTED***") {
		t.Fatalf("expected the credential replaced in place, got %s", rec.Body.String())
	}
}

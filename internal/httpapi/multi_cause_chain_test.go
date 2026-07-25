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

// This file gates the second conjunct of the disclosure decision (#301,
// round-4 Finding 3): a chain whose provenance is ambiguous is redacted even
// when it carries client-sentinel evidence. Sentinel evidence alone lives in
// error_response_test.go.

// TestMixedChainIsRedacted is the Finding 3 regression.
//
// isClientError uses errors.Is, which matches any leaf, so a joined chain
// carrying both a client sentinel and a driver cause used to take the *verbatim*
// branch and disclose the driver text — an S3 object key reaching a public 400.
// `errors.Join` and the `fmt.Errorf("%w: %w", …)` shape used throughout
// internal/federated both produce that node, so it is not hypothetical.
//
// Disclosure now requires an unambiguous chain as well as sentinel evidence
// (canDiscloseVerbatim). A fan-out means the sentinel says nothing about the
// other branch, so the body is redacted. The *status* still follows the sentinel
// — this is a 400 with a redacted body, the one live shape where the two
// branches disagree.
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

// TestMultiVerbWrapChainIsRedacted covers the other producer of the same node:
// fmt.Errorf with two %w verbs, which is how internal/federated builds its
// classified read errors. errors.Unwrap returns nil at such a node, so a walk
// built on it would miss the fan-out entirely.
func TestMultiVerbWrapChainIsRedacted(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	driver := fmt.Errorf("HTTP Error: 403 reading %s", canaryKey)
	// Wrapped once more, so the fan-out is not the outermost node.
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

// TestSingleCauseClientErrorStaysVerbatim is the other side of the gate: the
// shape every real client error in this repo has — one %w around one sentinel,
// wrapped in context — must still reach the caller with its message intact.
func TestSingleCauseClientErrorStaysVerbatim(t *testing.T) {
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
	if resp.ErrorClass != "" || resp.ErrorID != "" {
		t.Fatalf("a single-cause client error took the redacted branch: class=%q id=%q",
			resp.ErrorClass, resp.ErrorID)
	}
	if !strings.Contains(resp.Error, "nosuchattr") {
		t.Fatalf("expected the verbatim message naming the attribute, got %q", resp.Error)
	}
}

// TestHasMultipleCauses pins the walk itself, including the two shapes a loop
// built on errors.Unwrap alone would get wrong.
func TestHasMultipleCauses(t *testing.T) {
	sentinel := forma.ErrInvalidInput
	other := errors.New("operator detail")

	cases := map[string]struct {
		err  error
		want bool
	}{
		"nil":                     {nil, false},
		"bare sentinel":           {sentinel, false},
		"single wrap":             {fmt.Errorf("ctx: %w", sentinel), false},
		"nested single wraps":     {fmt.Errorf("a: %w", fmt.Errorf("b: %w", sentinel)), false},
		"join of two":             {errors.Join(sentinel, other), true},
		"join of one":             {errors.Join(sentinel), false},
		"multi verb wrap":         {fmt.Errorf("ctx: %w: %w", sentinel, other), true},
		"join buried under wraps": {fmt.Errorf("a: %w", fmt.Errorf("b: %w", errors.Join(sentinel, other))), true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := hasMultipleCauses(tc.err); got != tc.want {
				t.Fatalf("hasMultipleCauses(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

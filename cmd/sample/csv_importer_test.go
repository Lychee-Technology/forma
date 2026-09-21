package main

import (
	"testing"

	"github.com/lychee-technology/forma"
)

// A live best-effort failure carries its correlation id, and the import log
// shows it beside the published message so an operator can find the full
// error on the server log line (#398).
func TestFailureReasonShowsTheCorrelationID(t *testing.T) {
	got := failureReason(forma.OperationError{
		Code:    "CREATE_FAILED",
		Error:   "internal error",
		ErrorID: "0b1a3c6e-5d2f-4a8b-9c7d-1e2f3a4b5c6d",
	})
	want := "CREATE_FAILED: internal error (error_id 0b1a3c6e-5d2f-4a8b-9c7d-1e2f3a4b5c6d)"
	if got != want {
		t.Fatalf("failureReason = %q, want %q", got, want)
	}
}

// error_id is omitempty on the wire, so an entry without one (hand-built, or
// a payload from before #398) must not render an empty handle.
func TestFailureReasonOmitsAnAbsentCorrelationID(t *testing.T) {
	got := failureReason(forma.OperationError{Code: "CREATE_FAILED", Error: "internal error"})
	if want := "CREATE_FAILED: internal error"; got != want {
		t.Fatalf("failureReason = %q, want %q", got, want)
	}
}

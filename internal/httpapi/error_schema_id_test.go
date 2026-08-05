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

// This file gates the one datum a redacted body carries beyond its class and
// correlation id: the schema the failed read was addressed to.
//
// It is a reversal, and the tests are written to keep it honest. #301 asked for
// "error class + schema id"; the design settled on error_class + error_id and
// docs/error-handling.md recorded "no schema id" as a constraint; the issue owner
// reinstated the schema id. So these assertions exist to prove the field is
// present and correct, while the canaries in error_response_test.go and
// error_leak_test.go still prove nothing else crossed with it.

// TestErrorSchemaIDResolvesCarriersThroughWraps pins that every typed read-path
// carrier yields its schema id through wrapping, and that an untyped chain yields
// zero.
//
// The depth is not decorative. The federated engine returns
// fmt.Errorf("execute duckdb query: %w: %w", carrier, driverErr), so a bare type
// assertion on err would resolve 0 on every real chain while passing any test
// that handed the carrier over undressed.
func TestErrorSchemaIDResolvesCarriersThroughWraps(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int16
	}{
		{
			name: "parquet set inconsistent",
			err: fmt.Errorf("a: %w", fmt.Errorf("b: %w",
				&forma.ParquetSetInconsistentError{SchemaID: 22, MissingKeys: []string{"base/k.parquet"}})),
			want: 22,
		},
		{
			name: "no parquet paths",
			err: fmt.Errorf("a: %w", fmt.Errorf("b: %w",
				&forma.NoParquetPathsError{SchemaID: 7, SourceConfigured: true})),
			want: 7,
		},
		{
			name: "manifest schema mismatch",
			err: fmt.Errorf("a: %w", fmt.Errorf("b: %w",
				&forma.ManifestSchemaMismatchError{RequestedSchemaID: 41, ManifestSchemaID: 93, Path: "manifests/41.json"})),
			want: 41,
		},
		{
			name: "multi-cause chain alongside driver text",
			err: fmt.Errorf("execute duckdb query: %w: %w",
				&forma.ParquetSetInconsistentError{SchemaID: 22, MissingKeys: []string{"base/k.parquet"}},
				fmt.Errorf("IO Error: driver text")),
			want: 22,
		},
		{
			name: "no typed carrier",
			err:  fmt.Errorf("a: %w", fmt.Errorf("b: %w", fmt.Errorf("db timeout"))),
			want: 0,
		},
		{
			name: "nil error",
			err:  nil,
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorSchemaID(tt.err); got != tt.want {
				t.Fatalf("errorSchemaID = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestErrorSchemaIDPrefersRequestedOverManifestID is the swap guard.
//
// ManifestSchemaMismatchError carries two ids, and only one of them is the
// client's. RequestedSchemaID is the schema the caller asked to read;
// ManifestSchemaID is a foreign stamp found on the object, belonging to whichever
// other schema misaddressed it. Reporting the manifest's id would answer a
// request about one schema with another schema's identity, and would disclose the
// existence of a schema the caller never named.
//
// The two ids differ, so a swapped field fails here rather than passing on a
// coincidence.
func TestErrorSchemaIDPrefersRequestedOverManifestID(t *testing.T) {
	const requested, stamped int16 = 41, 93

	err := fmt.Errorf("manifest parquet source: %w", &forma.ManifestSchemaMismatchError{
		RequestedSchemaID: requested,
		ManifestSchemaID:  stamped,
		Path:              "manifests/41.json",
	})

	got := errorSchemaID(err)
	if got == stamped {
		t.Fatalf("errorSchemaID returned the manifest's stamped id %d; it must return the requested id %d",
			stamped, requested)
	}
	if got != requested {
		t.Fatalf("errorSchemaID = %d, want the requested id %d", got, requested)
	}
}

// TestRedactedBodyCarriesSchemaID pins the reversal end to end: a redacted body
// from a real carrier chain carries schema_id alongside error_class and error_id,
// and still carries none of the operator detail the same chain holds.
func TestRedactedBodyCarriesSchemaID(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	rec := httptest.NewRecorder()
	// operatorDetailError is the shared fixture: a ParquetSetInconsistentError for
	// schema 22 wrapping a password-bearing driver message.
	respondError(rec, "query failed", operatorDetailError(), "schema", "orders")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", rec.Code)
	}

	var resp APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if resp.SchemaID != 22 {
		t.Fatalf("redacted body schema_id = %d, want 22", resp.SchemaID)
	}
	if resp.ErrorClass != errorClassParquetSetInconsistent {
		t.Fatalf("expected class %q, got %q", errorClassParquetSetInconsistent, resp.ErrorClass)
	}
	if _, err := uuid.Parse(resp.ErrorID); err != nil {
		t.Fatalf("error_id %q is not a UUID: %v", resp.ErrorID, err)
	}

	// Adding a field must not widen the disclosure: the same canaries the
	// load-bearing #301 test asserts still have to be absent.
	body := rec.Body.String()
	for _, forbidden := range []string{
		canaryPassword, canarySecret, canarySecretHead, canarySecretTail, canaryKey,
		"password=", "s3://", "IO Error", "manifest lists", "postgres_scan",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("redacted body leaked %q; body: %s", forbidden, body)
		}
	}

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 ERROR log entry, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	// Structured, not interpolated: operators filter on this field rather than
	// parsing it back out of the message. zap round-trips the int16 as an int16
	// through ContextMap, so assert the typed value — a string-formatted check
	// would pass on a field that had been folded into prose.
	loggedSchemaID, ok := fields["schema_id"]
	if !ok {
		t.Fatalf("operator log carries no schema_id field; fields: %v", fields)
	}
	if loggedSchemaID != int16(22) {
		t.Fatalf("log schema_id = %v (%T), want int16 22", loggedSchemaID, loggedSchemaID)
	}
}

// TestRedactedBodyOmitsSchemaIDWithoutACarrier pins the omitempty encoding on the
// serialized bytes rather than on the struct field.
//
// Asserting resp.SchemaID == 0 after a round trip would pass whether the key were
// absent or present-and-zero, and the whole reason omitempty is safe here is that
// schema IDs are always positive, so zero can only ever mean "absent". A key
// spelled `"schema_id":0` would contradict that and tell a client the read was
// addressed to schema 0.
func TestRedactedBodyOmitsSchemaIDWithoutACarrier(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	rec := httptest.NewRecorder()
	respondError(rec, "query failed", fmt.Errorf("execute duckdb query: %w", fmt.Errorf("db timeout")))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", rec.Code)
	}

	body := rec.Body.String()
	if strings.Contains(body, "schema_id") {
		t.Fatalf("an error with no typed carrier emitted schema_id; body: %s", body)
	}

	// The class fields must still be there, so the assertion above is proving
	// omission rather than an unrelated failure to reach the redacted branch.
	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if _, present := raw["schema_id"]; present {
		t.Fatalf("schema_id key present in decoded body: %v", raw)
	}
	if raw["error_class"] != errorClassInternal {
		t.Fatalf("expected error_class %q, got %v", errorClassInternal, raw["error_class"])
	}
	if _, present := raw["error_id"]; !present {
		t.Fatalf("redacted body carries no error_id: %v", raw)
	}
}

// TestPublished4xxBodyCarriesNoSchemaID pins that the schema-id reversal is
// confined to the redacted branch. A published client error keeps a body of
// message only: no correlation fields, no schema id — even when the chain
// deliberately carries a ParquetSetInconsistentError that errorSchemaID
// *would* resolve, attached as operator detail. The test fails if the field
// is populated before the disclosure gate instead of after it, and the
// object-key assertion makes it strictly stronger than its pre-#313 version:
// the detail's text must not surface either.
func TestPublished4xxBodyCarriesNoSchemaID(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	err := forma.WithOperatorDetail(forma.InvalidInputf("bad filter"),
		&forma.ParquetSetInconsistentError{SchemaID: 22, MissingKeys: []string{canaryKey}})

	rec := httptest.NewRecorder()
	respondError(rec, "query failed", err)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}

	body := rec.Body.String()
	for _, forbidden := range []string{"schema_id", "error_class", "error_id", canaryKey} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("published 4xx body carried %q; body: %s", forbidden, body)
		}
	}
	if !strings.Contains(body, "bad filter") {
		t.Fatalf("expected the published message, got %s", body)
	}
}

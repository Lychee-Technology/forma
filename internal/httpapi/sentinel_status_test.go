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

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/transform"
	"go.uber.org/zap"
)

// This file gates the sentinel contract end to end: an error that a real
// validator produces for real caller input must still reach the client as a 4xx
// with a usable message. Each test builds its error by running the production
// code path rather than hand-writing a chain, so removing a sentinel fails here
// instead of passing on a fixture that was updated to match.

// requiredAttrRegistry is the minimum forma.SchemaRegistry the transform write
// path needs, declaring one required EAV attribute.
type requiredAttrRegistry struct{}

func (r *requiredAttrRegistry) cache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"name":  {AttributeName: "name", AttributeID: 1, ValueType: forma.ValueTypeText},
		"email": {AttributeName: "email", AttributeID: 2, ValueType: forma.ValueTypeText, Required: true},
	}
}

func (r *requiredAttrRegistry) GetSchemaAttributeCacheByName(string) (int16, forma.SchemaAttributeCache, error) {
	return 500, r.cache(), nil
}

func (r *requiredAttrRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "required_attr_schema", r.cache(), nil
}

func (r *requiredAttrRegistry) GetSchemaByName(string) (int16, forma.JSONSchema, error) {
	return 500, forma.JSONSchema{}, nil
}

func (r *requiredAttrRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "required_attr_schema", forma.JSONSchema{}, nil
}

func (r *requiredAttrRegistry) ListSchemas() []string {
	return []string{"required_attr_schema"}
}

// TestCreateMissingRequiredAttributeIs400AndVerbatim is the Finding 2
// counterpart: removing forma.ErrInvalidInput from the *shared* converter
// (internal/transform/attribute_converter.go, reachable from the read path)
// must not cost the write path its 400.
//
// The error is the real one — produced by running transform's write path over a
// body that omits a required attribute — rather than a hand-written chain, so
// this fails if the write-only validator ever stops carrying the sentinel.
func TestCreateMissingRequiredAttributeIs400AndVerbatim(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	_, writeErr := transform.NewTransformer(&requiredAttrRegistry{}).ToAttributes(
		context.Background(), 500, uuid.Must(uuid.NewV7()), map[string]any{"name": "caller"})
	if writeErr == nil {
		t.Fatal("precondition failed: the write path accepted a body missing a required attribute")
	}
	if !strings.Contains(writeErr.Error(), "missing required attribute 'email'") {
		t.Fatalf("precondition failed: unexpected write-path error %v", writeErr)
	}

	manager := &mockEntityManager{
		batchCreateErr: forma.WrapPublicf(writeErr, "operation[0]"),
	}
	srv := NewServer(manager, Options{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/required_attr_schema", bytes.NewReader([]byte(`{"name":"caller"}`)))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for a body missing a required attribute, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if !strings.Contains(resp.Error, "missing required attribute 'email'") {
		t.Fatalf("expected the published message naming the attribute, got %q", resp.Error)
	}
	// The batch index is the one piece of the failure the caller cannot recover
	// from anywhere else in the response; losing it from the published body is
	// the silent regression this assertion exists to catch (#313).
	if !strings.Contains(resp.Error, "operation[0]") {
		t.Fatalf("expected the published message to keep the batch index, got %q", resp.Error)
	}
	if resp.ErrorClass != "" || resp.ErrorID != "" {
		t.Fatalf("a write-validation error took the redacted branch: error_class=%q error_id=%q",
			resp.ErrorClass, resp.ErrorID)
	}
}

// TestAdvancedQueryOperatorWhitelistIs400AndVerbatim is Finding 4's HTTP half.
//
// The condition DSL is entirely caller-supplied, so `starts_with` on a UUID
// column is a fixable client mistake. The earlier #301 sentinel sweep missed
// normalizePgEavPayload's operator whitelist, so it answered an opaque 500 with
// a redacted body while docs/error-handling.md claimed condition-DSL errors stay
// 400.
//
// The error is the real one — produced by running sqlgen over the rejected
// condition — rather than a hand-written chain, so this fails if the sentinel is
// removed again.
func TestAdvancedQueryOperatorWhitelistIs400AndVerbatim(t *testing.T) {
	restore := zap.ReplaceGlobals(zap.NewNop())
	defer restore()

	cache := forma.SchemaAttributeCache{
		"ref": {AttributeName: "ref", AttributeID: 1, ValueType: forma.ValueTypeUUID},
	}
	paramIndex := 0
	_, genErr := sqlgen.ToDualClauses(
		&forma.KvCondition{Attr: "ref", Value: "starts_with:0b"}, "eav_data", 7, cache, &paramIndex)
	if genErr == nil {
		t.Fatal("precondition failed: starts_with on a uuid attribute was accepted")
	}
	if !strings.Contains(genErr.Error(), "only supported for text attributes") {
		t.Fatalf("precondition failed: unexpected generator error %v", genErr)
	}

	manager := &mockEntityManager{advancedErr: fmt.Errorf("advanced query: %w", genErr)}
	srv := NewServer(manager, Options{})

	body := `{"schema_name":"lead",` +
		`"condition":{"l":"and","c":[{"a":"ref","v":"starts_with:0b"}]},` +
		`"page":1,"items_per_page":10}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/advanced_query", bytes.NewReader([]byte(body)))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for a rejected filter operator, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp APIResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if !strings.Contains(resp.Error, "only supported for text attributes") {
		t.Fatalf("expected the published message naming the operator constraint, got %q", resp.Error)
	}
	// The generator's internal phase wraps ("pg main generation",
	// "pg sql generation") are plain context, not publication — they must stay
	// out of the body now that only published text crosses (#313).
	if strings.Contains(resp.Error, "generation") {
		t.Fatalf("an internal phase name reached the 400 body: %q", resp.Error)
	}
	if resp.ErrorClass != "" || resp.ErrorID != "" {
		t.Fatalf("a condition-DSL error took the redacted branch: error_class=%q error_id=%q",
			resp.ErrorClass, resp.ErrorID)
	}
}

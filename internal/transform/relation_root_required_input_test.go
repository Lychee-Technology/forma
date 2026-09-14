package transform

// Relation-root carve-out for the write path's own required check (#389).
//
// ToAttributes runs two required-policy checks on every create and update:
// validateRequiredAttributesFromInput against the caller's input, and
// AttributeConverter.checkRequiredAttributes against the flattened records.
// #315 gave the second a relation-root carve-out; these tests pin that the
// first carves the same names out, on the same boundary. The end-to-end half —
// a create through the manager, with the strip in front — is
// TestCreateSucceedsWithRequiredAlwaysBeneathRelationRoot in package internal.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

func relationRootPolicyTransformer(t *testing.T, registry *stubSchemaRegistry) *transformer {
	t.Helper()
	tr := NewTransformer(registry)
	tr.SetRelationRoots(func(schemaName string) RelationRoots {
		if schemaName != "visit_like" {
			return nil
		}
		return RelationRoots{"contactSnapshot": struct{}{}}
	})
	return tr
}

// TestToAttributesSkipsRequiredAlwaysBeneathRelationRoot is the #389 red.
//
// contactSnapshot.tenantId is required_always and the input carries no
// contactSnapshot at all — which is what every payload looks like after
// StripComputedFields has removed the relation subtree. Before #389 the input
// check answered a 400 here that no caller could fix by sending the field.
func TestToAttributesSkipsRequiredAlwaysBeneathRelationRoot(t *testing.T) {
	tr := relationRootPolicyTransformer(t, relationRootPolicyRegistry())

	attrs, err := tr.ToAttributes(context.Background(), 500, uuid.Must(uuid.NewV7()),
		map[string]any{"id": "visit-1"})
	if err != nil {
		t.Fatalf("a required_always beneath a relation root must not be enforced on input (#389): %v", err)
	}
	if len(attrs) != 1 {
		t.Fatalf("got %d attributes, want 1: %+v", len(attrs), attrs)
	}
}

// TestToAttributesStillEnforcesRequiredAlwaysOutsideRelationRoot is the
// inverse pin: the carve-out must not widen into "required_always is not
// enforced on input". propertySnapshot is an ordinary object, not a relation
// root, and the rejection must keep the caller-facing sentinel.
func TestToAttributesStillEnforcesRequiredAlwaysOutsideRelationRoot(t *testing.T) {
	registry := relationRootPolicyRegistry()
	registry.cache["propertySnapshot.status"] = forma.AttributeMetadata{
		AttributeID:    6,
		ValueType:      forma.ValueTypeText,
		RequiredPolicy: forma.RequiredPolicyAlways,
	}
	tr := relationRootPolicyTransformer(t, registry)

	_, err := tr.ToAttributes(context.Background(), 500, uuid.Must(uuid.NewV7()),
		map[string]any{"id": "visit-1"})
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("a required_always outside every relation root must still answer ErrInvalidInput, got: %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "missing required attribute 'propertySnapshot.status'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestToAttributesEnforcesRequiredAlwaysOnRelationRootItself pins Covers'
// boundary on the input check: a name that *is* a relation root is not
// "beneath" one, so its own policy stays enforced — the same line
// TestFromEAVRecordsEnforcesRequiredPolicyOnRelationRootItself draws for the
// record check.
func TestToAttributesEnforcesRequiredAlwaysOnRelationRootItself(t *testing.T) {
	registry := relationRootPolicyRegistry()
	registry.cache["contactSnapshot"] = forma.AttributeMetadata{
		AttributeID:    7,
		ValueType:      forma.ValueTypeText,
		RequiredPolicy: forma.RequiredPolicyAlways,
	}
	tr := relationRootPolicyTransformer(t, registry)

	_, err := tr.ToAttributes(context.Background(), 500, uuid.Must(uuid.NewV7()),
		map[string]any{"id": "visit-1"})
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("the relation root's own required policy must stay enforced, got: %v", err)
	}
	if !strings.Contains(err.Error(), "missing required attribute 'contactSnapshot'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestToAttributesWithoutRelationRootsKeepsInputEnforcement pins the nil-lookup
// contract: a transformer nothing has installed roots on behaves exactly as it
// did, so the carve-out cannot loosen a deployment that never wired a relation
// index.
func TestToAttributesWithoutRelationRootsKeepsInputEnforcement(t *testing.T) {
	tr := NewTransformer(relationRootPolicyRegistry())

	_, err := tr.ToAttributes(context.Background(), 500, uuid.Must(uuid.NewV7()),
		map[string]any{"id": "visit-1"})
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("without relation roots the input check must enforce as before, got: %v", err)
	}
	if !strings.Contains(err.Error(), "missing required attribute 'contactSnapshot.tenantId'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

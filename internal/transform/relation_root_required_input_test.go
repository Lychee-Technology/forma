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

func relationRootPolicyTransformer(t *testing.T, registry forma.SchemaRegistry) *transformer {
	t.Helper()
	tr := NewTransformer(registry)
	tr.SetRelationRoots(visitLikeRelationRoots)
	return tr
}

func visitLikeRelationRoots(schemaName string) RelationRoots {
	if schemaName != "visit_like" {
		return nil
	}
	return RelationRoots{"contactSnapshot": struct{}{}}
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

// unstableSchemaRegistry answers GetSchemaByID normally once and then hands
// every later call to second. forma.SchemaRegistry promises nothing about
// repeated reads (SnapshotSchemaDocuments, package internal), so the write path
// must resolve the relation roots for one conversion exactly once and hand
// that snapshot to both required checks; a registry that changes or fails on
// the second read is how a second read would show.
type unstableSchemaRegistry struct {
	*stubSchemaRegistry
	byIDCalls int
	second    func() (string, forma.JSONSchema, error)
}

func (r *unstableSchemaRegistry) GetSchemaByID(id int16) (string, forma.JSONSchema, error) {
	r.byIDCalls++
	if r.byIDCalls > 1 {
		return r.second()
	}
	return r.stubSchemaRegistry.GetSchemaByID(id)
}

// TestToAttributesResolvesRelationRootsOnce pins the single resolution at the
// transform seam: the input-side check passes on the first read, and the
// record-side check must not go back to a registry that now fails.
func TestToAttributesResolvesRelationRootsOnce(t *testing.T) {
	registry := &unstableSchemaRegistry{
		stubSchemaRegistry: relationRootPolicyRegistry(),
		second: func() (string, forma.JSONSchema, error) {
			return "", forma.JSONSchema{}, errors.New("registry read a second time within one write")
		},
	}
	tr := relationRootPolicyTransformer(t, registry)

	attrs, err := tr.ToAttributes(context.Background(), 500, uuid.Must(uuid.NewV7()),
		map[string]any{"id": "visit-1"})
	if err != nil {
		t.Fatalf("one write must resolve the relation roots once; second read reached: %v", err)
	}
	if len(attrs) != 1 {
		t.Fatalf("got %d attributes, want 1: %+v", len(attrs), attrs)
	}
	if registry.byIDCalls != 1 {
		t.Fatalf("GetSchemaByID called %d times within one write, want 1", registry.byIDCalls)
	}
}

// TestToPersistentRecordResolvesRelationRootsOnce is the same pin through the
// persistent-record conversion the manager actually calls. Here the second
// read answers a different schema name: the lookup would return no roots for
// it, and the record-side check would enforce contactSnapshot.tenantId that
// the input-side check had just carved out.
func TestToPersistentRecordResolvesRelationRootsOnce(t *testing.T) {
	registry := &unstableSchemaRegistry{
		stubSchemaRegistry: relationRootPolicyRegistry(),
		second: func() (string, forma.JSONSchema, error) {
			return "renamed", forma.JSONSchema{ID: 500, Name: "renamed"}, nil
		},
	}
	tr := NewPersistentRecordTransformer(registry)
	tr.(RelationRootsAware).SetRelationRoots(visitLikeRelationRoots)

	_, err := tr.ToPersistentRecord(context.Background(), 500, uuid.Must(uuid.NewV7()),
		map[string]any{"id": "visit-1"})
	if err != nil {
		t.Fatalf("the two required checks on one write must read the same relation roots: %v", err)
	}
	if registry.byIDCalls != 1 {
		t.Fatalf("GetSchemaByID called %d times within one write, want 1", registry.byIDCalls)
	}
}

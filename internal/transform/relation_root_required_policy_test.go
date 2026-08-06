package transform

// Relation-root carve-out for required-policy enforcement (#314/#315).
//
// These tests drive AttributeConverter.FromEAVRecords directly, which is the
// seam where required policies are enforced against attribute metadata. The
// integration halves live in package internal
// (TestCreateAcceptsDottedKeyUnderRelationRoot and its "outside" twin).

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// relationRootPolicyRegistry mirrors the shape #315 created on visit.json:
// contactSnapshot is a relation root whose expanded children carry required
// policies, while propertySnapshot is an ordinary nested object whose children
// carry the same policies and must keep being enforced.
func relationRootPolicyRegistry() *stubSchemaRegistry {
	return &stubSchemaRegistry{
		schemaID:   500,
		schemaName: "visit_like",
		cache: forma.SchemaAttributeCache{
			"id": {AttributeID: 1, ValueType: forma.ValueTypeText, RequiredPolicy: forma.RequiredPolicyAlways},

			// Beneath a relation root: never enforced.
			"contactSnapshot.name":        {AttributeID: 2, ValueType: forma.ValueTypeText},
			"contactSnapshot.isAnonymous": {AttributeID: 3, ValueType: forma.ValueTypeBool, RequiredPolicy: forma.RequiredPolicyIfParentPresent},
			"contactSnapshot.tenantId":    {AttributeID: 4, ValueType: forma.ValueTypeText, RequiredPolicy: forma.RequiredPolicyAlways},

			// Outside every relation root: still enforced.
			"propertySnapshot.price":  {AttributeID: 5, ValueType: forma.ValueTypeNumeric},
			"propertySnapshot.status": {AttributeID: 6, ValueType: forma.ValueTypeText, RequiredPolicy: forma.RequiredPolicyIfParentPresent},
		},
	}
}

func relationRootPolicyConverter(t *testing.T) (*AttributeConverter, uuid.UUID) {
	t.Helper()
	converter := NewAttributeConverter(relationRootPolicyRegistry())
	converter.SetRelationRoots(func(schemaName string) RelationRoots {
		if schemaName != "visit_like" {
			return nil
		}
		return RelationRoots{"contactSnapshot": struct{}{}}
	})
	return converter, uuid.Must(uuid.NewV7())
}

// TestFromEAVRecordsSkipsRequiredPolicyBeneathRelationRoot is the #315 red.
//
// A sibling under the relation root is present, which is exactly what makes the
// parent "present" for required_if_parent_present — and required_always would
// fire regardless. Neither may be enforced beneath a relation root.
func TestFromEAVRecordsSkipsRequiredPolicyBeneathRelationRoot(t *testing.T) {
	converter, rowID := relationRootPolicyConverter(t)

	id := "visit-1"
	name := "Ada"
	records := []model.EAVRecord{
		{SchemaID: 500, RowID: rowID, AttrID: 1, ValueText: &id},
		{SchemaID: 500, RowID: rowID, AttrID: 2, ValueText: &name},
	}

	attrs, err := converter.FromEAVRecords(records)
	if err != nil {
		t.Fatalf("a required policy beneath a relation root must not be enforced (#314/#315): %v", err)
	}
	if len(attrs) != 2 {
		t.Fatalf("got %d attributes, want 2: %+v", len(attrs), attrs)
	}
}

// TestFromEAVRecordsStillEnforcesRequiredPolicyOutsideRelationRoot is the
// inverse pin: the carve-out must not widen into "nested required policies are
// not enforced". propertySnapshot is an ordinary object, not a relation root.
func TestFromEAVRecordsStillEnforcesRequiredPolicyOutsideRelationRoot(t *testing.T) {
	converter, rowID := relationRootPolicyConverter(t)

	id := "visit-1"
	price := 100.0
	records := []model.EAVRecord{
		{SchemaID: 500, RowID: rowID, AttrID: 1, ValueText: &id},
		{SchemaID: 500, RowID: rowID, AttrID: 5, ValueNumeric: &price},
	}

	_, err := converter.FromEAVRecords(records)
	if err == nil {
		t.Fatal("a required policy outside every relation root must still be enforced")
	}
	if !strings.Contains(err.Error(), "missing required attribute 'propertySnapshot.status'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestFromEAVRecordsEnforcesRequiredPolicyOnRelationRootItself pins Covers'
// documented boundary: a name that *is* a relation root is not "beneath" one,
// so the carve-out does not reach it.
func TestFromEAVRecordsEnforcesRequiredPolicyOnRelationRootItself(t *testing.T) {
	registry := relationRootPolicyRegistry()
	registry.cache["contactSnapshot"] = forma.AttributeMetadata{
		AttributeID:    7,
		ValueType:      forma.ValueTypeText,
		RequiredPolicy: forma.RequiredPolicyAlways,
	}
	converter := NewAttributeConverter(registry)
	converter.SetRelationRoots(func(string) RelationRoots {
		return RelationRoots{"contactSnapshot": struct{}{}}
	})

	rowID := uuid.Must(uuid.NewV7())
	id := "visit-1"
	_, err := converter.FromEAVRecords([]model.EAVRecord{
		{SchemaID: 500, RowID: rowID, AttrID: 1, ValueText: &id},
	})
	if err == nil {
		t.Fatal("a required policy on the relation root name itself is not covered by the carve-out")
	}
	if !strings.Contains(err.Error(), "missing required attribute 'contactSnapshot'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestFromEAVRecordsWithoutRelationRootsKeepsEnforcement pins that an
// uninstalled lookup changes nothing — the carve-out is opt-in per wiring.
func TestFromEAVRecordsWithoutRelationRootsKeepsEnforcement(t *testing.T) {
	converter := NewAttributeConverter(relationRootPolicyRegistry())
	rowID := uuid.Must(uuid.NewV7())

	id := "visit-1"
	name := "Ada"
	_, err := converter.FromEAVRecords([]model.EAVRecord{
		{SchemaID: 500, RowID: rowID, AttrID: 1, ValueText: &id},
		{SchemaID: 500, RowID: rowID, AttrID: 2, ValueText: &name},
	})
	if err == nil {
		t.Fatal("without a relation-roots lookup, enforcement must be unchanged")
	}
	if !strings.Contains(err.Error(), "missing required attribute 'contactSnapshot.") {
		t.Fatalf("unexpected error: %v", err)
	}
}

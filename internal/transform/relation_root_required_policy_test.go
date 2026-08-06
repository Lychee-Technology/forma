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

// TestFromEAVRecordsEnforcesRequiredPolicyOnFullVariant is the positive control
// for the carve-out's blast radius: it must not reach the _full variants.
//
// visit_full.json is the inlined artifact, and inlining resolves $ref and drops
// x-* — so it declares no x-relation and RelationIndex finds no relation roots
// for it. contactSnapshot is therefore an ordinary nested object there, and its
// children stay under required-policy enforcement even though the identical
// names are exempt on visit.
//
// The metadata below mirrors visit_full_attributes.json after the #315 repair
// (synthetic, to match this file's other fixtures): contactSnapshot.name (16)
// lost required_if_parent_present, contactSnapshot.nameKana (17) never had one,
// and contactSnapshot.isAnonymous (43) is new and carries it. That combination
// is a write-contract change on visit_full, and is pinned here deliberately.
func TestFromEAVRecordsEnforcesRequiredPolicyOnFullVariant(t *testing.T) {
	registry := &stubSchemaRegistry{
		schemaID:   501,
		schemaName: "visit_full_like",
		cache: forma.SchemaAttributeCache{
			"id":                          {AttributeID: 1, ValueType: forma.ValueTypeText, RequiredPolicy: forma.RequiredPolicyAlways},
			"contactSnapshot.name":        {AttributeID: 16, ValueType: forma.ValueTypeText},
			"contactSnapshot.nameKana":    {AttributeID: 17, ValueType: forma.ValueTypeText},
			"contactSnapshot.isAnonymous": {AttributeID: 43, ValueType: forma.ValueTypeBool, RequiredPolicy: forma.RequiredPolicyIfParentPresent},
		},
	}

	// The lookup is installed and answers "no roots" for this schema, exactly
	// as RelationIndex does for an inlined _full schema. The carve-out is wired
	// but must not fire.
	converter := NewAttributeConverter(registry)
	converter.SetRelationRoots(func(schemaName string) RelationRoots {
		if schemaName == "visit_like" {
			return RelationRoots{"contactSnapshot": struct{}{}}
		}
		return nil
	})

	rowID := uuid.Must(uuid.NewV7())
	id := "visit-1"
	nameKana := "Ada"
	withoutIsAnonymous := []model.EAVRecord{
		{SchemaID: 501, RowID: rowID, AttrID: 1, ValueText: &id},
		{SchemaID: 501, RowID: rowID, AttrID: 17, ValueText: &nameKana},
	}

	_, err := converter.FromEAVRecords(withoutIsAnonymous)
	if err == nil {
		t.Fatal("visit_full has no relation roots, so contactSnapshot children stay enforced")
	}
	if !strings.Contains(err.Error(), "missing required attribute 'contactSnapshot.isAnonymous'") {
		t.Fatalf("unexpected error: %v", err)
	}

	isAnonymous := 0.0
	withIsAnonymous := append(withoutIsAnonymous,
		model.EAVRecord{SchemaID: 501, RowID: rowID, AttrID: 43, ValueNumeric: &isAnonymous},
	)
	if _, err := converter.FromEAVRecords(withIsAnonymous); err != nil {
		t.Fatalf("supplying contactSnapshot.isAnonymous must satisfy the policy: %v", err)
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

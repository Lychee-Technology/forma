package transform

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransformer_ToAttributes_RequiredNestedFieldDependsOnParentPresence(t *testing.T) {
	ctx := context.Background()
	registry := &stubSchemaRegistry{
		schemaID:   302,
		schemaName: "lead_schema",
		cache: forma.SchemaAttributeCache{
			"id":                           {AttributeID: 1, ValueType: forma.ValueTypeText, Required: true},
			"propertyInterests.propertyId": {AttributeID: 2, ValueType: forma.ValueTypeText},
			"propertyInterests.status":     {AttributeID: 3, ValueType: forma.ValueTypeText, Required: true},
		},
	}

	transformer := NewTransformer(registry)
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("lead_schema")
	require.NoError(t, err)

	rowID := uuid.Must(uuid.NewV7())

	// Parent path does not exist: nested required field should not be enforced.
	_, err = transformer.ToAttributes(ctx, schemaID, rowID, map[string]any{
		"id": "lead-1",
	})
	require.NoError(t, err)

	// Parent path exists (array item exists): nested required field should be enforced.
	_, err = transformer.ToAttributes(ctx, schemaID, rowID, map[string]any{
		"id": "lead-2",
		"propertyInterests": []any{
			map[string]any{
				"propertyId": "p-1",
			},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required attribute 'propertyInterests.status'")

	// Nested required field present: should pass.
	_, err = transformer.ToAttributes(ctx, schemaID, rowID, map[string]any{
		"id": "lead-3",
		"propertyInterests": []any{
			map[string]any{
				"propertyId": "p-1",
				"status":     "viewed",
			},
		},
	})
	require.NoError(t, err)
}

func TestTransformer_ToAttributes_RequiredNestedFieldFailsForEmptyParentObject(t *testing.T) {
	ctx := context.Background()
	registry := &stubSchemaRegistry{
		schemaID:   303,
		schemaName: "contact_schema_required",
		cache: forma.SchemaAttributeCache{
			"id":            {AttributeID: 1, ValueType: forma.ValueTypeText, Required: true},
			"contact.email": {AttributeID: 2, ValueType: forma.ValueTypeText, Required: true},
		},
	}

	transformer := NewTransformer(registry)
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("contact_schema_required")
	require.NoError(t, err)

	rowID := uuid.Must(uuid.NewV7())

	// Optional parent absent: should pass.
	_, err = transformer.ToAttributes(ctx, schemaID, rowID, map[string]any{
		"id": "lead-1",
	})
	require.NoError(t, err)

	// Parent explicitly exists but child missing: should fail.
	_, err = transformer.ToAttributes(ctx, schemaID, rowID, map[string]any{
		"id":      "lead-2",
		"contact": map[string]any{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required attribute 'contact.email'")
}

func TestTransformer_ToAttributes_RequiredAlwaysEnforcedEvenWhenParentMissing(t *testing.T) {
	ctx := context.Background()
	registry := &stubSchemaRegistry{
		schemaID:   304,
		schemaName: "required_always_schema",
		cache: forma.SchemaAttributeCache{
			"id":            {AttributeID: 1, ValueType: forma.ValueTypeText, RequiredPolicy: forma.RequiredPolicyAlways},
			"contact.email": {AttributeID: 2, ValueType: forma.ValueTypeText, RequiredPolicy: forma.RequiredPolicyAlways},
			"contact.phone": {AttributeID: 3, ValueType: forma.ValueTypeText},
		},
	}

	transformer := NewTransformer(registry)
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("required_always_schema")
	require.NoError(t, err)

	rowID := uuid.Must(uuid.NewV7())
	_, err = transformer.ToAttributes(ctx, schemaID, rowID, map[string]any{
		"id": "lead-1",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required attribute 'contact.email'")
}

func TestTransformer_ToAttributes_UnknownLeafAttribute_ReturnsError(t *testing.T) {
	ctx := context.Background()
	reg := newStubSchemaRegistry()
	schemaID, _, err := reg.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)
	tr := NewTransformer(reg)

	_, err = tr.ToAttributes(ctx, schemaID, uuid.New(), map[string]any{
		"name":    "Alice",
		"unknown": "value",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown")
	assert.True(t, errors.Is(err, forma.ErrInvalidInput), "expected ErrInvalidInput, got: %v", err)
}

func TestTransformer_ToAttributes_InvalidOptionalValue_ReturnsError(t *testing.T) {
	ctx := context.Background()
	reg := newStubSchemaRegistry()
	schemaID, _, err := reg.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)
	tr := NewTransformer(reg)

	_, err = tr.ToAttributes(ctx, schemaID, uuid.New(), map[string]any{
		"name": "Alice",
		"age":  "not-a-number",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "age")
	assert.True(t, errors.Is(err, forma.ErrInvalidInput), "expected ErrInvalidInput, got: %v", err)
}

// F3: null-clear divergence tests

// Setting a known leaf attribute to null must return an error, not silently skip it.
func TestTransformer_ToAttributes_NullLeafAttribute_ReturnsError(t *testing.T) {
	ctx := context.Background()
	reg := newStubSchemaRegistry()
	schemaID, _, err := reg.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)
	tr := NewTransformer(reg)

	_, err = tr.ToAttributes(ctx, schemaID, uuid.New(), map[string]any{
		"name": nil,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name")
	assert.Contains(t, err.Error(), "null")
}

// Setting a known nested leaf attribute to null must return an error.
func TestTransformer_ToAttributes_NullNestedAttribute_ReturnsError(t *testing.T) {
	ctx := context.Background()
	reg := newStubSchemaRegistry()
	schemaID, _, err := reg.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)
	tr := NewTransformer(reg)

	_, err = tr.ToAttributes(ctx, schemaID, uuid.New(), map[string]any{
		"person": map[string]any{
			"name": nil,
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "person.name")
	assert.Contains(t, err.Error(), "null")
}

// Setting a parent object key to null (which would clear its entire subtree in EAV)
// must return an error because one or more child attributes are schema-defined.
func TestTransformer_ToAttributes_NullParentObject_ReturnsError(t *testing.T) {
	ctx := context.Background()
	reg := newStubSchemaRegistry()
	schemaID, _, err := reg.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)
	tr := NewTransformer(reg)

	_, err = tr.ToAttributes(ctx, schemaID, uuid.New(), map[string]any{
		"person": nil, // parent of person.name and person.age
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "person")
	assert.Contains(t, err.Error(), "null")
}

// A null value for a key that is not in the schema must be silently skipped.
func TestTransformer_ToAttributes_NullUnknownField_IsSilentlySkipped(t *testing.T) {
	ctx := context.Background()
	reg := newStubSchemaRegistry()
	schemaID, _, err := reg.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)
	tr := NewTransformer(reg)

	attrs, err := tr.ToAttributes(ctx, schemaID, uuid.New(), map[string]any{
		"name":          "Alice",
		"unknown_field": nil, // not in schema — should be skipped without error
	})
	require.NoError(t, err)
	require.Len(t, attrs, 1)
	assert.Equal(t, int16(1), attrs[0].AttrID) // name
}

// F3 null errors must be classified as ErrInvalidInput so callers get HTTP 400.

func TestTransformer_ToAttributes_NullLeafAttribute_WrapsErrInvalidInput(t *testing.T) {
	ctx := context.Background()
	reg := newStubSchemaRegistry()
	schemaID, _, err := reg.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)
	tr := NewTransformer(reg)

	_, err = tr.ToAttributes(ctx, schemaID, uuid.New(), map[string]any{"name": nil})
	require.Error(t, err)
	assert.True(t, errors.Is(err, forma.ErrInvalidInput), "expected ErrInvalidInput, got: %v", err)
}

func TestTransformer_ToAttributes_NullNestedAttribute_WrapsErrInvalidInput(t *testing.T) {
	ctx := context.Background()
	reg := newStubSchemaRegistry()
	schemaID, _, err := reg.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)
	tr := NewTransformer(reg)

	_, err = tr.ToAttributes(ctx, schemaID, uuid.New(), map[string]any{
		"person": map[string]any{"name": nil},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, forma.ErrInvalidInput), "expected ErrInvalidInput, got: %v", err)
}

func TestTransformer_ToAttributes_NullParentObject_WrapsErrInvalidInput(t *testing.T) {
	ctx := context.Background()
	reg := newStubSchemaRegistry()
	schemaID, _, err := reg.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)
	tr := NewTransformer(reg)

	_, err = tr.ToAttributes(ctx, schemaID, uuid.New(), map[string]any{"person": nil})
	require.Error(t, err)
	assert.True(t, errors.Is(err, forma.ErrInvalidInput), "expected ErrInvalidInput, got: %v", err)
}

// F2: null elements inside arrays must also be rejected.

// A null primitive inside a schema-defined array leaf must return an error.
func TestTransformer_ToAttributes_NullPrimitiveArrayItem_ReturnsError(t *testing.T) {
	ctx := context.Background()
	reg := newStubSchemaRegistry() // "items" is a schema-defined text leaf
	schemaID, _, err := reg.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)
	tr := NewTransformer(reg)

	_, err = tr.ToAttributes(ctx, schemaID, uuid.New(), map[string]any{
		"items": []any{"valid", nil}, // null at index 1
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "items")
	assert.Contains(t, err.Error(), "null")
	assert.True(t, errors.Is(err, forma.ErrInvalidInput), "expected ErrInvalidInput, got: %v", err)
}

// A null object inside a schema-defined array must return an error.
func TestTransformer_ToAttributes_NullObjectArrayItem_ReturnsError(t *testing.T) {
	ctx := context.Background()
	reg := &stubSchemaRegistry{
		schemaID:   305,
		schemaName: "contacts_schema",
		cache: forma.SchemaAttributeCache{
			"contacts.name": {AttributeID: 1, ValueType: forma.ValueTypeText},
		},
	}
	tr := NewTransformer(reg)
	schemaID, _, err := reg.GetSchemaAttributeCacheByName("contacts_schema")
	require.NoError(t, err)

	_, err = tr.ToAttributes(ctx, schemaID, uuid.New(), map[string]any{
		"contacts": []any{
			map[string]any{"name": "Alice"},
			nil, // null object at index 1
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "contacts")
	assert.Contains(t, err.Error(), "null")
	assert.True(t, errors.Is(err, forma.ErrInvalidInput), "expected ErrInvalidInput, got: %v", err)
}

// A null inside an array whose path is NOT in the schema must now be rejected.
func TestTransformer_ToAttributes_NullUnknownArrayItem_ReturnsError(t *testing.T) {
	ctx := context.Background()
	reg := newStubSchemaRegistry() // "unknown_list" is not in schema
	schemaID, _, err := reg.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)
	tr := NewTransformer(reg)

	_, err = tr.ToAttributes(ctx, schemaID, uuid.New(), map[string]any{
		"name":         "Alice",
		"unknown_list": []any{"x", nil},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown_list")
	assert.True(t, errors.Is(err, forma.ErrInvalidInput), "expected ErrInvalidInput, got: %v", err)
}

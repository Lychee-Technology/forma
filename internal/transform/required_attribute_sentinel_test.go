package transform

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// newRequiredAttributeRegistry declares one required attribute stored in EAV, so
// both the write payload and the persisted record can omit it.
func newRequiredAttributeRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{
		schemaID:   500,
		schemaName: "required_attr_schema",
		cache: forma.SchemaAttributeCache{
			"name":  {AttributeName: "name", AttributeID: 1, ValueType: forma.ValueTypeText},
			"email": {AttributeName: "email", AttributeID: 2, ValueType: forma.ValueTypeText, Required: true},
		},
	}
}

// TestPersistedRecordMissingRequiredAttributeIsNotClientInput is Finding 2.
//
// FromEAVRecords is shared: FromPersistentRecord rebuilds already-stored records
// through it on the *read* path. A persisted row missing a required EAV row is
// therefore metadata/state drift, not caller input, and must not carry
// forma.ErrInvalidInput — otherwise the HTTP boundary answers a verbatim 400 for
// a condition the caller cannot fix, inverting the split AGENTS.md draws.
func TestPersistedRecordMissingRequiredAttributeIsNotClientInput(t *testing.T) {
	registry := newRequiredAttributeRegistry()
	rowID := uuid.Must(uuid.NewV7())
	name := "stored"

	recordTransformer := NewPersistentRecordTransformer(registry)
	_, err := recordTransformer.FromPersistentRecord(context.Background(), &model.PersistentRecord{
		SchemaID: 500,
		RowID:    rowID,
		OtherAttributes: []model.EAVRecord{
			{SchemaID: 500, RowID: rowID, AttrID: 1, ValueText: &name},
		},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "missing required attribute 'email'")
	require.NotErrorIs(t, err, forma.ErrInvalidInput,
		"a persisted record missing a required row is read-path drift; the sentinel would make it a 400")
}

// TestSharedConverterMissingRequiredAttributeIsPlain pins the same contract at
// the shared converter itself, which is where the sentinel was wrongly added.
func TestSharedConverterMissingRequiredAttributeIsPlain(t *testing.T) {
	registry := newRequiredAttributeRegistry()
	rowID := uuid.Must(uuid.NewV7())
	name := "stored"

	_, err := NewAttributeConverter(registry).FromEAVRecords([]model.EAVRecord{
		{SchemaID: 500, RowID: rowID, AttrID: 1, ValueText: &name},
	})

	require.Error(t, err)
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
}

// TestWritePayloadMissingRequiredAttributeIsClientInput is the other half: the
// write path's 400 must survive the removal above. It does not depend on the
// shared converter at all — validateRequiredAttributesFromInput is the
// write-only validator, and it runs against the caller's input before flattening.
func TestWritePayloadMissingRequiredAttributeIsClientInput(t *testing.T) {
	registry := newRequiredAttributeRegistry()
	rowID := uuid.Must(uuid.NewV7())

	_, err := NewTransformer(registry).ToAttributes(context.Background(), 500, rowID, map[string]any{
		"name": "caller",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "missing required attribute 'email'")
	require.ErrorIs(t, err, forma.ErrInvalidInput,
		"a create/update body omitting a required attribute is caller fault and must stay a 400")
}

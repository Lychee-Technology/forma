package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/require"
)

// fidelityRegistry mirrors numericValidationRegistry but binds attributes to
// main columns so the #459 storage-fit rule has something to check. count and
// rank are declared one width wider than their column (bigint->integer_01,
// integer->smallint_01): a registrable narrowing where the declared-type check
// passes and only the bound-column check stands between the value and a
// wrapped row. The leadId text->uuid_02 binding deliberately bypasses
// schemameta registration (which now refuses it): this is the already-deployed
// shape the runtime rule must still refuse with a published 4xx, not a 500.
type fidelityRegistry struct{}

const fidelitySchemaJSON = `{
  "type": "object",
  "properties": {
    "name":   {"type": "string"},
    "count":  {"type": "integer"},
    "rank":   {"type": "integer"},
    "leadId": {"type": "string", "pattern": "^[a-zA-Z0-9_-]+$"}
  },
  "required": ["name"]
}`

func (fidelityRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	if name != "test" {
		return 0, nil, fmt.Errorf("schema %s not found", name)
	}
	return 100, forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText},
		"count": {AttributeID: 2, ValueType: forma.ValueTypeBigInt,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnInteger01}},
		"rank": {AttributeID: 3, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnSmallint01}},
		"leadId": {AttributeID: 4, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnUUID02}},
	}, nil
}

func (r fidelityRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	_, cache, err := r.GetSchemaAttributeCacheByName("test")
	return "test", cache, err
}

func (fidelityRegistry) ListSchemas() []string { return []string{"test"} }

func (fidelityRegistry) GetSchemaByName(string) (int16, forma.JSONSchema, error) {
	return 100, forma.JSONSchema{ID: 100, Name: "test", Schema: fidelitySchemaJSON}, nil
}

func (fidelityRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "test", forma.JSONSchema{ID: 100, Name: "test", Schema: fidelitySchemaJSON}, nil
}

// newFidelityManager builds a report-only (strict=false) manager so the update
// cases prove the storage-fit rule, not JSON Schema validation, is what refuses.
func newFidelityManager(t *testing.T) (forma.EntityManager, *mockPersistentRecordRepository) {
	t.Helper()
	registry := fidelityRegistry{}

	validator, err := schemavalidate.New(registry, t.TempDir())
	require.NoError(t, err)

	config := createTestConfig()
	config.Entity.ValidateUpdatesStrict = false

	transformer := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	manager := mustNewEntityManager(t, transformer, repo, nil, registry, config, validator)
	return manager, repo
}

// requirePublishedInvalidInput asserts the error is a published (not
// redacted) invalid-input carrier whose public message names the cause.
func requirePublishedInvalidInput(t *testing.T, err error, want string) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	msg, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok, "must publish, not redact: %v", err)
	require.Contains(t, msg, want)
}

// TestEntityManager_WriteFidelity is #459 at the manager seam: the three
// corruption shapes (integer wrap, smallint wrap, text into a uuid column) are
// published invalid input on create AND update. Update validation is
// report-only by default, so the storage-fit rule in transform, not JSON
// Schema, is what refuses them there.
func TestEntityManager_WriteFidelity(t *testing.T) {
	ctx := context.Background()
	em, repo := newFidelityManager(t)
	created, err := em.Create(ctx, createOp(map[string]any{"name": "a", "count": 1, "rank": 1}))
	require.NoError(t, err)

	cases := []struct {
		name string
		data map[string]any
		want string
	}{
		{"integer main column wraps no more",
			map[string]any{"name": "a", "count": float64(3e9)},
			"bound column integer_01 (integer)"},
		{"smallint main column wraps no more",
			map[string]any{"name": "a", "rank": 40000},
			"bound column smallint_01 (smallint)"},
		{"text to uuid column is 4xx not 500",
			map[string]any{"name": "a", "leadId": "abc"},
			`text value "abc" is not a UUID`},
	}
	for _, tc := range cases {
		t.Run("create/"+tc.name, func(t *testing.T) {
			_, err := em.Create(ctx, createOp(tc.data))
			requirePublishedInvalidInput(t, err, tc.want)
		})
		t.Run("update/"+tc.name, func(t *testing.T) {
			_, err := em.Update(ctx, updateOp(created.RowID, tc.data))
			requirePublishedInvalidInput(t, err, tc.want)
		})
	}

	// Nothing wrapped or mismatched reached storage.
	stored := repo.records[100][created.RowID]
	require.NotNil(t, stored)
	require.Equal(t, int32(1), stored.Int32Items["integer_01"])
	require.Equal(t, int16(1), stored.Int16Items["smallint_01"])
	require.NotContains(t, stored.UUIDItems, "uuid_02")
}

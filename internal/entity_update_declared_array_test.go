package internal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
)

// A schema whose document declares tags a primitive array while its
// attributes file still records the element type (pre-#204) is admitted by
// the registry, and a strict create writes the array. A strict update writes
// one too: the document positively declares that array, so the shape rule
// takes it as the attribute's value (#696). note, which the document declares
// a single value, still refuses an array.
func TestStrictUpdateWritesAnArrayTheSchemaDeclaresUnderAnElementTypedLedger(t *testing.T) {
	dir := t.TempDir()
	for name, content := range map[string]string{
		"legacy.json": `{"type":"object","properties":{"note":{"type":"string"},` +
			`"tags":{"type":"array","items":{"type":"string"}}}}`,
		"legacy_attributes.json": `{"note":{"attributeID":1,"valueType":"text"},"tags":{"attributeID":2,"valueType":"text"}}`,
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
	}
	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)
	validator, err := schemavalidate.New(SnapshotSchemaDocuments(registry), dir)
	require.NoError(t, err)
	config := createTestConfig()
	config.Entity.SchemaDirectory = dir
	config.Entity.ValidateUpdatesStrict = true
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), newMockPersistentRecordRepository(),
		nil, registry, config, validator)

	ctx := context.Background()
	created, err := em.Create(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "legacy"},
		Type:             forma.OperationCreate,
		Data:             map[string]any{"note": "n", "tags": []any{"a"}},
	})
	require.NoError(t, err)
	require.Equal(t, []any{"a"}, created.Attributes["tags"])

	update := func(updates map[string]any) (*forma.DataRecord, error) {
		return em.Update(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "legacy", RowID: created.RowID},
			Type:             forma.OperationUpdate,
			Updates:          updates,
		})
	}
	updated, err := update(map[string]any{"tags": []any{"b", "c"}})
	require.NoError(t, err)
	require.Equal(t, []any{"b", "c"}, updated.Attributes["tags"])

	_, err = update(map[string]any{"note": []any{"x"}})
	assertShapeRefused(t, err, "note", "declared type text takes a single value, got an array")
}

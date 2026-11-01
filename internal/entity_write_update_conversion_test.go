package internal

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateConversionRefusalIsRefusedBeforeTheRepository pins where an
// update's unconvertible value is refused (#772): at intake, before Update or
// an atomic BatchUpdate reaches the repository, so no row is locked or read.
// Before #772 the merged write refused it inside the write transaction,
// which poisons a Tx.
func TestUpdateConversionRefusalIsRefusedBeforeTheRepository(t *testing.T) {
	ctx := context.Background()
	registry := newStubSchemaRegistry()
	transformer := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)
	rowID, other := uuid.New(), uuid.New()
	repo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowID, map[string]any{"name": "Alice"}))
	repo.storeRecord(buildPersistentRecord(t, transformer, schemaID, other, map[string]any{"name": "Bob"}))
	em := mustNewEntityManager(t, transformer, repo, nil, registry, createTestConfig(), nil)

	for _, tc := range []struct {
		name    string
		updates map[string]any
		want    string // the published message's prefix
	}{
		{"numeric, not a number", map[string]any{"person": map[string]any{"age": "abc"}}, "invalid value for attribute 'person.age' (attrID=4): "},
		{"date, unparseable", map[string]any{"metadata": map[string]any{"createdAt": "yesterday"}}, "invalid value for attribute 'metadata.createdAt' (attrID=6): "},
		{"bool, not 0 or 1", map[string]any{"metadata.active": 2}, "invalid value for attribute 'metadata.active' (attrID=7): "},
	} {
		_, err = em.Update(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "test", RowID: rowID},
			Type:             forma.OperationUpdate,
			Updates:          tc.updates,
		})
		require.ErrorIs(t, err, forma.ErrInvalidInput, tc.name)
		message, _ := forma.ResolvePublicMessage(err)
		assert.Contains(t, message, tc.want, tc.name)
		assert.Zero(t, repo.mergeCalls, "%s: Update reached the repository", tc.name)

		_, err = em.BatchUpdate(ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{
			atomicUpdateOp(other, map[string]any{"name": "fine"}), atomicUpdateOp(rowID, tc.updates),
		}})
		require.ErrorIs(t, err, forma.ErrInvalidInput, tc.name)
		message, _ = forma.ResolvePublicMessage(err)
		assert.Contains(t, message, "operation[1]: "+tc.want, tc.name)
		assert.Zero(t, repo.batchUpdateCalls, "%s: the atomic BatchUpdate reached the repository", tc.name)
	}
}

// TestUpdateConversionRefusalPrecedesAMissingRequiredAttribute: the merged
// write judges the required policy before it converts (ToAttributes). An
// update that sends a parent the row lacks, without the attribute required
// beneath it, used to report the missing attribute first, even when a value
// it sent beside it could not be converted. The required policy needs the
// row; the conversion does not, so intake now reports the conversion first.
// An update with nothing to refuse at intake still reaches the row and
// reports the missing attribute.
func TestUpdateConversionRefusalPrecedesAMissingRequiredAttribute(t *testing.T) {
	ctx := context.Background()
	registry := newStubSchemaRegistry().(*stubSchemaRegistry)
	nameMeta := registry.cache["person.name"]
	nameMeta.RequiredPolicy = forma.RequiredPolicyIfParentPresent
	registry.cache["person.name"] = nameMeta
	transformer := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	rowID := uuid.New()
	repo.storeRecord(buildPersistentRecord(t, transformer, registry.schemaID, rowID, map[string]any{"name": "Alice"}))
	em := mustNewEntityManager(t, transformer, repo, nil, registry, createTestConfig(), nil)
	update := func(updates map[string]any) error {
		_, err := em.Update(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "test", RowID: rowID},
			Type:             forma.OperationUpdate,
			Updates:          updates,
		})
		return err
	}

	err := update(map[string]any{"person": map[string]any{"age": "abc"}})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, _ := forma.ResolvePublicMessage(err)
	assert.Contains(t, message, "invalid value for attribute 'person.age' (attrID=4): ")
	assert.Zero(t, repo.mergeCalls, "refused before the row is read")

	err = update(map[string]any{"person": map[string]any{"age": 4}})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, _ = forma.ResolvePublicMessage(err)
	assert.Contains(t, message, "missing required attribute 'person.name'")
	assert.Equal(t, 1, repo.mergeCalls, "judged on the read row")
}

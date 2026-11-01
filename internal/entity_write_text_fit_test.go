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

// TestUnstorableTextUpdateIsRefusedBeforeTheRepository pins where an
// update's unstorable text is refused (#679): at intake, before Update or an
// atomic BatchUpdate reaches the repository, so no row is locked or read.
// The merged write would refuse it too, but only inside the write
// transaction, which poisons a Tx.
func TestUnstorableTextUpdateIsRefusedBeforeTheRepository(t *testing.T) {
	ctx := context.Background()
	registry := newStubSchemaRegistry()
	transformer := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)
	rowID := uuid.New()
	repo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowID, map[string]any{"name": "Alice"}))
	em := mustNewEntityManager(t, transformer, repo, nil, registry, createTestConfig(), nil)

	const want = "invalid value for attribute 'person.name' (attrID=3): text cannot contain a NUL character (U+0000)"
	updates := map[string]any{"person": map[string]any{"name": "a\x00b"}}

	_, err = em.Update(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "test", RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          updates,
	})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, _ := forma.ResolvePublicMessage(err)
	assert.Equal(t, want, message)
	assert.Zero(t, repo.mergeCalls, "Update reached the repository")

	_, err = em.BatchUpdate(ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{
		atomicUpdateOp(rowID, updates),
	}})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, _ = forma.ResolvePublicMessage(err)
	assert.Equal(t, "operation[0]: "+want, message)
	assert.Zero(t, repo.batchUpdateCalls, "the atomic BatchUpdate reached the repository")
}

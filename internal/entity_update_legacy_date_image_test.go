package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
)

// legacyDateImageFixture seeds a visit row whose unbound actualEndAt
// (attr_id 1, date) carries the given float64 image in eav_data, the way a
// row written before #582 reaches the transformer: float slot only, no
// sidecar. It returns the manager and the row.
func legacyDateImageFixture(t *testing.T, image float64) (forma.EntityManager, uuid.UUID) {
	t.Helper()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	require.NoError(t, err)
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	require.NoError(t, err)

	rowID := uuid.New()
	record := buildPersistentRecord(t, transformer, schemaID, rowID, visitPayload("visit-legacy-date"))
	stored := image
	record.OtherAttributes = append(record.OtherAttributes, model.EAVRecord{
		SchemaID: schemaID, RowID: rowID, AttrID: 1, ValueNumeric: &stored,
	})
	mockRepo.storeRecord(record)
	return mustNewEntityManager(t, transformer, mockRepo, nil, registry, createTestConfig(), nil), rowID
}

const legacyDateImageRule = "outside the epoch milliseconds a float64 image keeps exactly (up to 9007199254740992, 2^53)"

// A legacy unbound date image past 2^53 (#587 review): before this fix the
// read accepted it while the write refused it, so an update that never
// mentioned the attribute failed as the caller's invalid input once the
// merged document re-entered ToPersistentRecord. The read now applies the
// same rule as the write, so the row is a read-side consistency error from
// the first GET, naming the attribute and the rule, never a 4xx blaming an
// unrelated update; the stored row is left untouched for the operator's
// migration (docs/schema-consistency-migration.md).
func TestEntityManager_LegacyDateImagePast2p53IsAConsistencyErrorNotInvalidInput(t *testing.T) {
	ctx := context.Background()
	em, rowID := legacyDateImageFixture(t, 9007199254740994)

	assertConsistencyError := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		require.Contains(t, err.Error(), "attrID=1")
		require.Contains(t, err.Error(), legacyDateImageRule)
		require.False(t, errors.Is(err, forma.ErrInvalidInput), "a stored image is the operator's, not the caller's: %v", err)
	}

	t.Run("get", func(t *testing.T) {
		_, err := em.Get(ctx, &forma.QueryRequest{SchemaName: "visit", RowID: &rowID})
		assertConsistencyError(t, err)
	})

	unrelated := forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          map[string]any{"status": "visited"},
	}
	t.Run("update", func(t *testing.T) {
		_, err := em.Update(ctx, &unrelated)
		assertConsistencyError(t, err)
	})

	t.Run("batch update", func(t *testing.T) {
		result, err := em.BatchUpdate(ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{unrelated}})
		require.NoError(t, err)
		require.Empty(t, result.Successful)
		require.Len(t, result.Failed, 1)
		require.Equal(t, "UPDATE_FAILED", result.Failed[0].Code)
		// The published body is the redacted one an operator error gets
		// (docs/error-handling.md); the full message is in the log only.
		require.Equal(t, undisclosedBatchError, result.Failed[0].Error)
	})
}

// The last image the destination keeps exactly is readable and rewritable:
// an unrelated update carries it through the merged document unchanged.
func TestEntityManager_DateImageAt2p53SurvivesAnUnrelatedUpdate(t *testing.T) {
	ctx := context.Background()
	em, rowID := legacyDateImageFixture(t, 9007199254740992)

	got, err := em.Get(ctx, &forma.QueryRequest{SchemaName: "visit", RowID: &rowID})
	require.NoError(t, err)
	require.NotNil(t, got.Attributes["actualEndAt"])

	updated, err := em.Update(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          map[string]any{"status": "visited"},
	})
	require.NoError(t, err)
	require.Equal(t, "visited", updated.Attributes["status"])
	require.Equal(t, got.Attributes["actualEndAt"], updated.Attributes["actualEndAt"])
}

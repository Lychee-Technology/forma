package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/redact"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// insertFailingRepository fails every insert with one storage error.
//
// It embeds the shared mock rather than reimplementing the interface, so the
// manager still type-asserts it to the same optional interfaces (the atomic
// batch repository among them) as any other test repository.
type insertFailingRepository struct {
	*mockPersistentRecordRepository
	insertErr error
}

func (r *insertFailingRepository) InsertPersistentRecord(
	ctx context.Context, tables model.StorageTables, record *model.PersistentRecord,
) error {
	return r.insertErr
}

// newBatchErrorManager builds a manager over the shipped schemas and the given
// repository. The validator is nil, so nothing here depends on JSON Schema
// enforcement: the failures under test come from the storage call and from the
// registry lookup.
func newBatchErrorManager(t *testing.T, repository model.PersistentRecordRepository) forma.EntityManager {
	t.Helper()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	require.NoError(t, err)
	return NewEntityManager(
		transform.NewPersistentRecordTransformer(registry),
		repository, nil, registry, createTestConfig(), nil)
}

func batchCreateOneVisit(t *testing.T, manager forma.EntityManager, schemaName string) forma.OperationError {
	t.Helper()
	result, err := manager.BatchCreate(context.Background(), &forma.BatchOperation{
		Operations: []forma.EntityOperation{{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: schemaName},
			Type:             forma.OperationCreate,
			Data:             visitPayload("visit-batch-error-1"),
		}},
	})
	require.NoError(t, err, "a best-effort batch reports per-operation failures in its result")
	require.Len(t, result.Failed, 1)
	return result.Failed[0]
}

// TestBatchResultWithholdsAnUnpublishedFailure is the leak guard for the
// fallback branch.
//
// forma.OperationError.Error is exported and JSON-serialised (types.go), and a
// storage failure publishes nothing: the repository returns a driver error and
// the CRUD path wraps it with fmt.Errorf, so no forma.PublicError carrier is in
// the chain. Rendering err.Error() there would put the driver's own prose —
// here a libpq connection string, password included — into a result the caller
// reads.
//
// Driven through a real BatchCreate with a real repository failure rather than
// a hand-built string, so it exercises the same wrap chain production does.
func TestBatchResultWithholdsAnUnpublishedFailure(t *testing.T) {
	repository := &insertFailingRepository{
		mockPersistentRecordRepository: newMockPersistentRecordRepository(),
		insertErr: errors.New(
			`failed to connect to "host=db.internal port=5432 user=forma password=s3cr3t dbname=forma"`),
	}

	failure := batchCreateOneVisit(t, newBatchErrorManager(t, repository), "visit")

	require.NotContains(t, failure.Error, "s3cr3t", "a credential must never reach an exported result field")
	require.NotContains(t, failure.Error, "db.internal")
	require.NotContains(t, failure.Error, "failed to insert persistent record",
		"internal phase context must not reach an exported result field")
	require.Equal(t, undisclosedBatchError, failure.Error)
	require.Equal(t, "CREATE_FAILED", failure.Code,
		"the machine-readable classification is what the caller keys on instead")
}

// TestBatchResultScrubsCredentialsFromAPublishedMessage covers the branch that
// does publish.
//
// The schema name is caller-supplied and the registry's not-found carrier
// publishes it verbatim (schemameta/file_registry.go), so a caller can put any
// text at all into a published batch message — a DSN included. internal/httpapi
// scrubs every string it writes for exactly this reason
// (respondErrorWithStatus); the batch surface has no boundary in front of it and
// must do the same itself.
func TestBatchResultScrubsCredentialsFromAPublishedMessage(t *testing.T) {
	name := "host=db.internal password=s3cr3t dbname=forma"

	failure := batchCreateOneVisit(t, newBatchErrorManager(t, newMockPersistentRecordRepository()), name)

	require.Contains(t, failure.Error, "schema not found", "the caller keeps the message authored for them")
	require.NotContains(t, failure.Error, "s3cr3t")
	require.Contains(t, failure.Error, redact.Placeholder)
}

package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/redact"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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
	return mustNewEntityManager(t,
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

// observeGlobalLog installs an observer as the process-global logger for the
// test, which is where the batch service writes its failure lines.
func observeGlobalLog(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zap.InfoLevel)
	t.Cleanup(zap.ReplaceGlobals(zap.New(core)))
	return logs
}

// requireJoinedLine is the #398 join: exactly one written line carries id as
// its error_id, and it is the Warn failure line. It returns that line's full
// error text.
func requireJoinedLine(t *testing.T, logs *observer.ObservedLogs, id string) string {
	t.Helper()
	_, err := uuid.Parse(id)
	require.NoError(t, err, "the id must be a UUID, the same shape as the HTTP error_id")
	lines := logs.FilterField(zap.String("error_id", id)).All()
	require.Len(t, lines, 1, "the id must identify exactly one log line")
	require.Equal(t, zap.WarnLevel, lines[0].Level)
	require.Equal(t, "BatchCreate operation failed", lines[0].Message)
	return fmt.Sprint(lines[0].ContextMap()["error"])
}

// TestBatchResultWithheldFailureCarriesACorrelationID is the #398 acceptance
// case: the caller reads only "internal error", so the id is their one way to
// reach the full error, and it has to be on the line that holds it.
func TestBatchResultWithheldFailureCarriesACorrelationID(t *testing.T) {
	logs := observeGlobalLog(t)
	repository := &insertFailingRepository{
		mockPersistentRecordRepository: newMockPersistentRecordRepository(),
		insertErr:                      errors.New("driver: connection reset by peer"),
	}

	failure := batchCreateOneVisit(t, newBatchErrorManager(t, repository), "visit")

	require.Equal(t, undisclosedBatchError, failure.Error)
	require.Equal(t, "CREATE_FAILED", failure.Code, "the id sits beside Code, it does not replace it")
	require.Contains(t, requireJoinedLine(t, logs, failure.ErrorID), "connection reset by peer",
		"the joined line must hold the error the result withheld")
}

// TestBatchResultPublishedFailureCarriesACorrelationID pins the decision #398
// asked to have stated: a published failure carries an id too. It logs the
// same Warn line with the full error, which can hold operator detail its
// published message leaves out (#318), so the join is just as useful.
func TestBatchResultPublishedFailureCarriesACorrelationID(t *testing.T) {
	logs := observeGlobalLog(t)

	failure := batchCreateOneVisit(t, newBatchErrorManager(t, newMockPersistentRecordRepository()), "no_such_schema")

	require.Contains(t, failure.Error, "schema not found", "the published text is unchanged")
	require.Contains(t, requireJoinedLine(t, logs, failure.ErrorID), "no_such_schema")
}

// TestBatchResultIdenticalFailuresCarryDistinctIDs: operations that are byte
// for byte the same, failing the same way, still produce one id per failure,
// each joining its own line, so no handle is ambiguous.
func TestBatchResultIdenticalFailuresCarryDistinctIDs(t *testing.T) {
	logs := observeGlobalLog(t)
	repository := &insertFailingRepository{
		mockPersistentRecordRepository: newMockPersistentRecordRepository(),
		insertErr:                      errors.New("driver: connection reset by peer"),
	}
	op := forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
		Type:             forma.OperationCreate,
		Data:             visitPayload("visit-batch-error-1"),
	}

	result, err := newBatchErrorManager(t, repository).BatchCreate(context.Background(), &forma.BatchOperation{
		Operations: []forma.EntityOperation{op, op, op},
	})
	require.NoError(t, err)
	require.Len(t, result.Failed, 3)

	seen := map[string]bool{}
	for _, failure := range result.Failed {
		requireJoinedLine(t, logs, failure.ErrorID)
		require.False(t, seen[failure.ErrorID], "two failures must never share an id")
		seen[failure.ErrorID] = true
	}
}

// TestBatchResultIssuesAnIDWhateverTheLogger pins where the contract ends.
// Forma puts the id on the result and on the line it writes. Whether that line
// is kept is decided by the embedder's logger, not by Forma, so a logger that
// keeps nothing still leaves every failure with its id.
func TestBatchResultIssuesAnIDWhateverTheLogger(t *testing.T) {
	t.Cleanup(zap.ReplaceGlobals(zap.NewNop()))
	repository := &insertFailingRepository{
		mockPersistentRecordRepository: newMockPersistentRecordRepository(),
		insertErr:                      errors.New("driver: connection reset by peer"),
	}

	failure := batchCreateOneVisit(t, newBatchErrorManager(t, repository), "visit")

	_, err := uuid.Parse(failure.ErrorID)
	require.NoError(t, err)
}

package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma/internal/errorid/erroridtest"
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
// forma.OperationError.Error is exported and JSON-serialised (types_batch.go), and a
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

// observeBatchFailureLog captures the Warn-level failure line
// executeBestEffortBatch writes, so a test can join a result entry to it the
// way an operator would.
func observeBatchFailureLog(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zap.WarnLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	t.Cleanup(restore)
	return logs
}

// requireFailureJoinsItsLogLine is the correlation contract (#398): the entry's
// ErrorID is a UUID, and exactly one failure line carries it verbatim as
// error_id next to the full error — the text the result deliberately does not
// carry.
func requireFailureJoinsItsLogLine(
	t *testing.T, failure forma.OperationError, logs *observer.ObservedLogs, wantInFullError string,
) {
	t.Helper()
	_, err := uuid.Parse(failure.ErrorID)
	require.NoError(t, err, "a failed best-effort operation must carry a UUID correlation id")

	entries := logs.FilterMessage("BatchCreate operation failed").All()
	require.Len(t, entries, 1, "one failed operation, one failure line")
	fields := entries[0].ContextMap()
	require.Equal(t, failure.ErrorID, fields["error_id"],
		"the id in the result must appear verbatim on the line carrying the full error")
	require.Contains(t, fmt.Sprint(fields["error"]), wantInFullError,
		"the line the id joins to must be the one holding the full error")
}

// TestBatchResultWithheldFailureCarriesACorrelationID is the case #398 was
// filed for: the result says only "internal error", so the id is all a caller
// has to quote to an operator, and the operator needs it to land on the line
// that holds the driver text.
func TestBatchResultWithheldFailureCarriesACorrelationID(t *testing.T) {
	logs := observeBatchFailureLog(t)
	repository := &insertFailingRepository{
		mockPersistentRecordRepository: newMockPersistentRecordRepository(),
		insertErr:                      errors.New("storage unavailable: disk quota exceeded on node-7"),
	}

	failure := batchCreateOneVisit(t, newBatchErrorManager(t, repository), "visit")

	require.Equal(t, undisclosedBatchError, failure.Error)
	requireFailureJoinsItsLogLine(t, failure, logs, "disk quota exceeded on node-7")
}

// TestBatchResultPublishedFailureCarriesACorrelationID pins the decision that
// the id is unconditional rather than reserved for withheld messages. Every
// best-effort failure logs at Warn with the full error — there is no
// Debug-level branch here for an id to correlate to nothing, which is the
// reason internal/httpapi leaves a detail-less disclosed 4xx id-free — so a
// published failure joins its line the same way. The published text is still
// the caller's own: the id sits beside it, it does not replace it.
func TestBatchResultPublishedFailureCarriesACorrelationID(t *testing.T) {
	logs := observeBatchFailureLog(t)

	failure := batchCreateOneVisit(t, newBatchErrorManager(t, newMockPersistentRecordRepository()), "nosuchschema")

	require.Contains(t, failure.Error, "schema not found")
	requireFailureJoinsItsLogLine(t, failure, logs, "nosuchschema")
}

// TestBatchResultFailuresCarryDistinctIDs: two failures in one batch must not
// share a handle, or the operator's grep lands on both lines.
func TestBatchResultFailuresCarryDistinctIDs(t *testing.T) {
	repository := &insertFailingRepository{
		mockPersistentRecordRepository: newMockPersistentRecordRepository(),
		insertErr:                      errors.New("storage unavailable"),
	}
	op := forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
		Type:             forma.OperationCreate,
		Data:             visitPayload("visit-batch-error-2"),
	}

	result, err := newBatchErrorManager(t, repository).BatchCreate(context.Background(), &forma.BatchOperation{
		Operations: []forma.EntityOperation{op, op},
	})

	require.NoError(t, err)
	require.Len(t, result.Failed, 2)
	require.NotEqual(t, result.Failed[0].ErrorID, result.Failed[1].ErrorID)
}

// TestBatchResultEveryFailureSurvivesProductionSampling is the regression for
// the review finding on #398: the production logger samples by level and
// message, the failure line's message is constant, so without an exemption
// the 101st identical failure inside one second would return an id whose line
// was never written — reachable within one best-effort batch, whose default
// MaxBatchSize is 1000. The logger here is an observer under the production
// sampler as every production logger installs it, on a frozen clock so all
// 150 lines land in one sampler tick; 150 failures must yield 150 ids, each
// joining exactly one line that holds the withheld driver text.
func TestBatchResultEveryFailureSurvivesProductionSampling(t *testing.T) {
	logs := erroridtest.ObserveUnderProductionSampler(t, zap.WarnLevel)
	repository := &insertFailingRepository{
		mockPersistentRecordRepository: newMockPersistentRecordRepository(),
		insertErr:                      errors.New("storage unavailable: disk quota exceeded on node-7"),
	}
	const failures = 150
	operations := make([]forma.EntityOperation, failures)
	for i := range operations {
		operations[i] = forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
			Type:             forma.OperationCreate,
			Data:             visitPayload(fmt.Sprintf("visit-batch-error-%d", i)),
		}
	}

	result, err := newBatchErrorManager(t, repository).BatchCreate(context.Background(),
		&forma.BatchOperation{Operations: operations})

	require.NoError(t, err)
	require.Len(t, result.Failed, failures)
	linesByID := map[string]int{}
	for _, entry := range logs.FilterMessage("BatchCreate operation failed").All() {
		fields := entry.ContextMap()
		require.Contains(t, fmt.Sprint(fields["error"]), "disk quota exceeded on node-7")
		linesByID[fmt.Sprint(fields["error_id"])]++
	}
	for _, failure := range result.Failed {
		require.Equal(t, undisclosedBatchError, failure.Error)
		require.Equal(t, 1, linesByID[failure.ErrorID],
			"id %s must join exactly one full-error line; the sampler must not have dropped it", failure.ErrorID)
	}
}

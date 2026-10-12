package internal

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBestEffortBatchReportsStorageOutcomeCodes pins #645's batch half: a
// best-effort operation whose cause is one of the four storage outcomes reports
// that outcome's code, so the caller can tell a safe retry from an unknown
// commit; any other failure keeps its per-operation code. The causes are the
// real classifier outputs the repository returns.
func TestBestEffortBatchReportsStorageOutcomeCodes(t *testing.T) {
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	require.NoError(t, err)
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	causes := []struct {
		err         error
		wantCode    string
		wantMessage string
	}{
		{classifyStatementError(pgError("23505", "duplicate key value")), "UNIQUE_CONFLICT",
			"the write conflicts with a row that already exists"},
		{forma.PreconditionFailedf("row changed since it was read"), "PRECONDITION_FAILED",
			"row changed since it was read"},
		{classifyStatementError(pgError("40P01", "deadlock detected")), "RETRYABLE_CONFLICT", "internal error"},
		{classifyCommitError(io.ErrUnexpectedEOF), "COMMIT_OUTCOME_UNKNOWN", "internal error"},
		{errors.New("connection refused"), "CREATE_FAILED", "internal error"},
	}
	calls := 0
	mockRepo.insertFunc = func(context.Context, *model.PersistentRecord) error {
		cause := causes[calls].err
		calls++
		return cause
	}

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, createTestConfig(), nil)
	ops := make([]forma.EntityOperation, len(causes))
	for i := range ops {
		ops[i] = forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
			Type:             forma.OperationCreate,
			Data:             visitPayload("visit-outcome"),
		}
	}

	result, err := em.BatchCreate(context.Background(), &forma.BatchOperation{Operations: ops})
	require.NoError(t, err)
	require.Len(t, result.Failed, len(causes))
	for i, cause := range causes {
		failed := result.Failed[i]
		assert.Equal(t, cause.wantCode, failed.Code, "operation %d", i)
		assert.Equal(t, cause.wantMessage, failed.Error, "operation %d", i)
		assert.NotEmpty(t, failed.ErrorID, "operation %d", i)
	}
}

// TestBatchErrorCodePrecedence pins that an unknown commit is never reported
// as a retryable conflict, even in a chain that carries both.
func TestBatchErrorCodePrecedence(t *testing.T) {
	both := errors.Join(forma.ErrRetryableConflict, forma.ErrCommitOutcomeUnknown)
	assert.Equal(t, "COMMIT_OUTCOME_UNKNOWN", batchErrorCode(both, "UPDATE_FAILED"))
	assert.Equal(t, "UPDATE_FAILED", batchErrorCode(forma.Conflictf("generic"), "UPDATE_FAILED"),
		"a generic conflict keeps the per-operation code")
}

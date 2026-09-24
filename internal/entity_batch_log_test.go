package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// payloadCanary is a value only a caller's document holds. If it shows up in
// any field of a log line, the document reached the log.
const payloadCanary = "canary-caller-document-content"

// TestBestEffortBatchFailureLogOmitsThePayload is the #396 acceptance case.
//
// Data and Updates are caller content and may be sensitive, so the Warn line a
// failed best-effort operation writes names the operation — schema, row id,
// type and its place in the batch — and the error, never the document. Both
// payload-carrying operation types are driven, each through a real failure.
func TestBestEffortBatchFailureLogOmitsThePayload(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	cases := []struct {
		name       string
		repository *insertFailingRepository
		run        func(forma.EntityManager, context.Context, *forma.BatchOperation) (*forma.BatchResult, error)
		op         forma.EntityOperation
		message    string
	}{
		{
			name: "create",
			repository: &insertFailingRepository{
				mockPersistentRecordRepository: newMockPersistentRecordRepository(),
				insertErr:                      errors.New("driver: connection reset by peer"),
			},
			run: forma.EntityManager.BatchCreate,
			op: forma.EntityOperation{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
				Type:             forma.OperationCreate,
				Data:             withCanary(visitPayload("visit-batch-log-1")),
			},
			message: "BatchCreate operation failed",
		},
		{
			// The mock holds no rows, so the update fails its read of rowID.
			name:       "update",
			repository: &insertFailingRepository{mockPersistentRecordRepository: newMockPersistentRecordRepository()},
			run:        forma.EntityManager.BatchUpdate,
			op: forma.EntityOperation{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: rowID},
				Type:             forma.OperationUpdate,
				Updates:          map[string]any{"status": payloadCanary},
			},
			message: "BatchUpdate operation failed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := observeGlobalLog(t)
			other := forma.EntityOperation{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "no_such_schema"},
				Type:             tc.op.Type,
			}

			result, err := tc.run(newBatchErrorManager(t, tc.repository), context.Background(), &forma.BatchOperation{
				Operations: []forma.EntityOperation{other, tc.op},
			})
			require.NoError(t, err)
			require.Len(t, result.Failed, 2)

			failure := result.Failed[1]
			lines := logs.FilterField(zap.String("error_id", failure.ErrorID)).All()
			require.Len(t, lines, 1)
			line := lines[0]
			require.Equal(t, zap.WarnLevel, line.Level)
			require.Equal(t, tc.message, line.Message)

			fields := line.ContextMap()
			require.Equal(t, "visit", fields["schema_name"])
			require.Equal(t, tc.op.RowID.String(), fields["row_id"])
			require.Equal(t, string(tc.op.Type), fields["operation_type"])
			require.EqualValues(t, 1, fields["operation_index"],
				"the index is the position in the request, not in Failed")
			require.NotEmpty(t, fields["error"])
			require.NotContains(t, fields, "operation", "the whole operation carries the document")

			for key, value := range fields {
				require.NotContains(t, fmt.Sprint(value), payloadCanary,
					"field %q must not carry caller document content", key)
			}
		})
	}
}

func withCanary(data map[string]any) map[string]any {
	data["status"] = payloadCanary
	return data
}

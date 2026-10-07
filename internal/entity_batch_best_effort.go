package internal

import (
	"context"
	"time"

	"github.com/lychee-technology/forma/internal/errorid"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

func (s *entityBatchService) executeBestEffortBatch(
	ctx context.Context,
	req *forma.BatchOperation,
	operationName string,
	errorCode string,
	executor batchOperationExecutor,
) (*forma.BatchResult, error) {
	startTime := time.Now()

	successful := make([]*forma.DataRecord, 0, len(req.Operations))
	failed := make([]forma.OperationError, 0)

	for index, operation := range req.Operations {
		op := operation
		record, err := executor(ctx, &op)
		if err != nil {
			// The id joins this entry to this line (#398): Error may be only
			// "internal error", and the line is where the full error lives.
			errorID := errorid.New()
			// Name the operation, never log it whole: Data and Updates are
			// caller content and may be sensitive (#396). A create's row_id is
			// the caller's, normally nil, since Create mints the real one.
			zap.S().Warnw(operationName+" operation failed",
				"schema_name", op.SchemaName, "row_id", op.RowID.String(),
				"operation_type", string(op.Type), "operation_index", index,
				"error_id", errorID, "error", err)
			failed = append(failed, forma.OperationError{
				Operation: op,
				Error:     resolveBatchErrorMessage(err),
				Code:      errorCode,
				ErrorID:   errorID,
			})
			continue
		}
		successful = append(successful, record)
	}

	duration := time.Since(startTime).Microseconds()
	zap.S().Debugw(operationName+" completed", "successfulCount", len(successful), "failedCount", len(failed), "durationMicroseconds", duration)
	return &forma.BatchResult{
		Successful: successful,
		Failed:     failed,
		TotalCount: len(req.Operations),
		Duration:   duration,
	}, nil
}

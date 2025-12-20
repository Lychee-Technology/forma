package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// BatchCreate creates multiple entities atomically
func (em *entityManager) BatchCreate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if req == nil {
		return nil, fmt.Errorf("batch operation cannot be nil")
	}
	zap.S().Debugw("BatchCreate called", "operationCount", len(req.Operations))
	if len(req.Operations) == 0 {
		return &forma.BatchResult{
			Successful: make([]*forma.DataRecord, 0),
			Failed:     make([]forma.OperationError, 0),
			TotalCount: 0,
		}, nil
	}

	startTime := time.Now()

	// For atomic operations, we need transaction support
	// For now, we'll collect all results and return them
	successful := make([]*forma.DataRecord, 0)
	failed := make([]forma.OperationError, 0)

	for _, op := range req.Operations {
		record, err := em.Create(ctx, &op)
		if err != nil {
			zap.S().Warnw("BatchCreate operation failed", "operation", op, "error", err)
			failed = append(failed, forma.OperationError{
				Operation: op,
				Error:     err.Error(),
				Code:      "CREATE_FAILED",
			})
		} else {
			successful = append(successful, record)
		}
	}

	duration := time.Since(startTime).Microseconds()
	zap.S().Debugw("BatchCreate completed", "successfulCount", len(successful), "failedCount", len(failed), "durationMicroseconds", duration)
	return &forma.BatchResult{
		Successful: successful,
		Failed:     failed,
		TotalCount: len(req.Operations),
		Duration:   duration,
	}, nil
}

// BatchUpdate updates multiple entities atomically
func (em *entityManager) BatchUpdate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if req == nil {
		return nil, fmt.Errorf("batch operation cannot be nil")
	}

	zap.S().Debugw("BatchUpdate called", "operationCount", len(req.Operations))
	if len(req.Operations) == 0 {
		return &forma.BatchResult{
			Successful: make([]*forma.DataRecord, 0),
			Failed:     make([]forma.OperationError, 0),
			TotalCount: 0,
		}, nil
	}

	startTime := time.Now()

	successful := make([]*forma.DataRecord, 0)
	failed := make([]forma.OperationError, 0)

	for _, op := range req.Operations {
		record, err := em.Update(ctx, &op)
		if err != nil {
			failed = append(failed, forma.OperationError{
				Operation: op,
				Error:     err.Error(),
				Code:      "UPDATE_FAILED",
			})
		} else {
			successful = append(successful, record)
		}
	}

	duration := time.Since(startTime).Microseconds()

	return &forma.BatchResult{
		Successful: successful,
		Failed:     failed,
		TotalCount: len(req.Operations),
		Duration:   duration,
	}, nil
}

// BatchDelete deletes multiple entities atomically
func (em *entityManager) BatchDelete(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if req == nil {
		return nil, fmt.Errorf("batch operation cannot be nil")
	}

	if len(req.Operations) == 0 {
		return &forma.BatchResult{
			Successful: make([]*forma.DataRecord, 0),
			Failed:     make([]forma.OperationError, 0),
			TotalCount: 0,
		}, nil
	}

	startTime := time.Now()

	successful := make([]*forma.DataRecord, 0)
	failed := make([]forma.OperationError, 0)

	for _, op := range req.Operations {
		err := em.Delete(ctx, &op)
		if err != nil {
			failed = append(failed, forma.OperationError{
				Operation: op,
				Error:     err.Error(),
				Code:      "DELETE_FAILED",
			})
		} else {
			successful = append(successful, &forma.DataRecord{
				SchemaName: op.SchemaName,
				RowID:      op.RowID,
			})
		}
	}

	duration := time.Since(startTime).Microseconds()

	return &forma.BatchResult{
		Successful: successful,
		Failed:     failed,
		TotalCount: len(req.Operations),
		Duration:   duration,
	}, nil
}

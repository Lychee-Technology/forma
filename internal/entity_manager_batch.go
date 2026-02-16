package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

func validateBatchOperation(req *forma.BatchOperation) error {
	if req == nil {
		return fmt.Errorf("batch operation cannot be nil")
	}
	return nil
}

func emptyBatchResult() *forma.BatchResult {
	return &forma.BatchResult{
		Successful: make([]*forma.DataRecord, 0),
		Failed:     make([]forma.OperationError, 0),
		TotalCount: 0,
	}
}

type batchOperationExecutor func(context.Context, *forma.EntityOperation) (*forma.DataRecord, error)

func (em *entityManager) executeBestEffortBatch(
	ctx context.Context,
	req *forma.BatchOperation,
	operationName string,
	errorCode string,
	executor batchOperationExecutor,
) (*forma.BatchResult, error) {
	startTime := time.Now()

	successful := make([]*forma.DataRecord, 0, len(req.Operations))
	failed := make([]forma.OperationError, 0)

	for _, operation := range req.Operations {
		op := operation
		record, err := executor(ctx, &op)
		if err != nil {
			zap.S().Warnw(operationName+" operation failed", "operation", op, "error", err)
			failed = append(failed, forma.OperationError{
				Operation: op,
				Error:     err.Error(),
				Code:      errorCode,
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

// BatchCreate creates multiple entities.
func (em *entityManager) BatchCreate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if err := validateBatchOperation(req); err != nil {
		return nil, err
	}
	zap.S().Debugw("BatchCreate called", "operationCount", len(req.Operations))
	if len(req.Operations) == 0 {
		return emptyBatchResult(), nil
	}
	if req.Atomic {
		return em.batchCreateAtomic(ctx, req)
	}

	return em.executeBestEffortBatch(ctx, req, "BatchCreate", "CREATE_FAILED", em.Create)
}

// BatchUpdate updates multiple entities.
func (em *entityManager) BatchUpdate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if err := validateBatchOperation(req); err != nil {
		return nil, err
	}

	zap.S().Debugw("BatchUpdate called", "operationCount", len(req.Operations))
	if len(req.Operations) == 0 {
		return emptyBatchResult(), nil
	}
	if req.Atomic {
		return em.batchUpdateAtomic(ctx, req)
	}

	return em.executeBestEffortBatch(ctx, req, "BatchUpdate", "UPDATE_FAILED", em.Update)
}

// BatchDelete deletes multiple entities.
func (em *entityManager) BatchDelete(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if err := validateBatchOperation(req); err != nil {
		return nil, err
	}
	zap.S().Debugw("BatchDelete called", "operationCount", len(req.Operations))

	if len(req.Operations) == 0 {
		return emptyBatchResult(), nil
	}
	if req.Atomic {
		return em.batchDeleteAtomic(ctx, req)
	}

	return em.executeBestEffortBatch(ctx, req, "BatchDelete", "DELETE_FAILED", func(execCtx context.Context, op *forma.EntityOperation) (*forma.DataRecord, error) {
		if err := em.Delete(execCtx, op); err != nil {
			return nil, err
		}
		return &forma.DataRecord{
			SchemaName: op.SchemaName,
			RowID:      op.RowID,
		}, nil
	})
}

func (em *entityManager) atomicRepository() (AtomicBatchPersistentRecordRepository, error) {
	repo, ok := em.repository.(AtomicBatchPersistentRecordRepository)
	if !ok {
		return nil, fmt.Errorf("repository does not support atomic batch operations")
	}
	return repo, nil
}

func (em *entityManager) batchCreateAtomic(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	atomicRepo, err := em.atomicRepository()
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	tables := em.storageTables()
	persistentRecords := make([]*PersistentRecord, len(req.Operations))
	successful := make([]*forma.DataRecord, len(req.Operations))

	for i, op := range req.Operations {
		if op.SchemaName == "" {
			return nil, fmt.Errorf("operation[%d]: schema name is required", i)
		}
		if op.Data == nil {
			return nil, fmt.Errorf("operation[%d]: data is required for create operation", i)
		}

		schemaID, _, err := em.registry.GetSchemaAttributeCacheByName(op.SchemaName)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to get schema: %w", i, err)
		}

		rowID := uuid.Must(uuid.NewV7())
		inputData := op.Data
		if em.relations != nil {
			inputData = em.relations.StripComputedFields(op.SchemaName, op.Data)
		}

		record, err := em.transformer.ToPersistentRecord(ctx, schemaID, rowID, inputData)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to transform data to persistent record: %w", i, err)
		}
		attributes, err := em.transformer.FromPersistentRecord(ctx, record)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to transform persistent record to json: %w", i, err)
		}

		persistentRecords[i] = record
		successful[i] = &forma.DataRecord{
			SchemaName: op.SchemaName,
			RowID:      rowID,
			Attributes: attributes,
		}
	}

	if err := atomicRepo.BatchInsertPersistentRecords(ctx, tables, persistentRecords); err != nil {
		return nil, fmt.Errorf("atomic batch create failed: %w", err)
	}

	duration := time.Since(startTime).Microseconds()
	return &forma.BatchResult{
		Successful: successful,
		Failed:     make([]forma.OperationError, 0),
		TotalCount: len(req.Operations),
		Duration:   duration,
	}, nil
}

func (em *entityManager) batchUpdateAtomic(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	atomicRepo, err := em.atomicRepository()
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	tables := em.storageTables()
	persistentRecords := make([]*PersistentRecord, len(req.Operations))
	successful := make([]*forma.DataRecord, len(req.Operations))

	for i, op := range req.Operations {
		if op.SchemaName == "" {
			return nil, fmt.Errorf("operation[%d]: schema name is required", i)
		}
		if op.RowID == (uuid.UUID{}) {
			return nil, fmt.Errorf("operation[%d]: row id is required for update operation", i)
		}
		if op.Updates == nil {
			return nil, fmt.Errorf("operation[%d]: updates are required for update operation", i)
		}

		schemaID, _, err := em.registry.GetSchemaAttributeCacheByName(op.SchemaName)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to get schema: %w", i, err)
		}

		existingRecord, err := em.repository.GetPersistentRecord(ctx, tables, schemaID, op.RowID)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to load existing record: %w", i, err)
		}
		if existingRecord == nil {
			return nil, fmt.Errorf("operation[%d]: entity not found: %s/%s", i, op.SchemaName, op.RowID)
		}

		existingData, err := em.transformer.FromPersistentRecord(ctx, existingRecord)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to transform existing record: %w", i, err)
		}

		mergedData := mergeMaps(existingData, op.Updates)
		if em.relations != nil {
			mergedData = em.relations.StripComputedFields(op.SchemaName, mergedData)
		}

		updatedRecord, err := em.transformer.ToPersistentRecord(ctx, schemaID, op.RowID, mergedData)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to transform merged data: %w", i, err)
		}
		updatedRecord.CreatedAt = existingRecord.CreatedAt
		updatedRecord.DeletedAt = existingRecord.DeletedAt

		persistentRecords[i] = updatedRecord
		successful[i] = &forma.DataRecord{
			SchemaName: op.SchemaName,
			RowID:      op.RowID,
			Attributes: mergedData,
		}
	}

	if err := atomicRepo.BatchUpdatePersistentRecords(ctx, tables, persistentRecords); err != nil {
		return nil, fmt.Errorf("atomic batch update failed: %w", err)
	}

	duration := time.Since(startTime).Microseconds()
	return &forma.BatchResult{
		Successful: successful,
		Failed:     make([]forma.OperationError, 0),
		TotalCount: len(req.Operations),
		Duration:   duration,
	}, nil
}

func (em *entityManager) batchDeleteAtomic(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	atomicRepo, err := em.atomicRepository()
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	tables := em.storageTables()
	keys := make([]PersistentRecordKey, len(req.Operations))
	successful := make([]*forma.DataRecord, len(req.Operations))

	for i, op := range req.Operations {
		if op.SchemaName == "" {
			return nil, fmt.Errorf("operation[%d]: schema name is required", i)
		}
		if op.RowID == (uuid.UUID{}) {
			return nil, fmt.Errorf("operation[%d]: row id is required for delete operation", i)
		}

		schemaID, _, err := em.registry.GetSchemaAttributeCacheByName(op.SchemaName)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to get schema: %w", i, err)
		}

		keys[i] = PersistentRecordKey{SchemaID: schemaID, RowID: op.RowID}
		successful[i] = &forma.DataRecord{
			SchemaName: op.SchemaName,
			RowID:      op.RowID,
		}
	}

	if err := atomicRepo.BatchDeletePersistentRecords(ctx, tables, keys); err != nil {
		return nil, fmt.Errorf("atomic batch delete failed: %w", err)
	}

	duration := time.Since(startTime).Microseconds()
	return &forma.BatchResult{
		Successful: successful,
		Failed:     make([]forma.OperationError, 0),
		TotalCount: len(req.Operations),
		Duration:   duration,
	}, nil
}

package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

type entityBatchService struct {
	repository    model.PersistentRecordRepository
	transformer   model.PersistentRecordTransformer
	registry      forma.SchemaRegistry
	relations     *RelationIndex
	storageTables storageTablesResolver
	createOp      func(context.Context, *forma.EntityOperation) (*forma.DataRecord, error)
	updateOp      func(context.Context, *forma.EntityOperation) (*forma.DataRecord, error)
	deleteOp      func(context.Context, *forma.EntityOperation) error
}

// newEntityBatchService takes the CRUD service as an explicit parameter so the
// batch service's dependency on it is visible at the construction site instead
// of relying on em.crud having been assigned first.
func newEntityBatchService(em *entityManager, crud *entityCRUDService) *entityBatchService {
	if em == nil {
		return &entityBatchService{}
	}
	var createOp func(context.Context, *forma.EntityOperation) (*forma.DataRecord, error)
	var updateOp func(context.Context, *forma.EntityOperation) (*forma.DataRecord, error)
	var deleteOp func(context.Context, *forma.EntityOperation) error
	if crud != nil {
		createOp = crud.Create
		updateOp = crud.Update
		deleteOp = crud.Delete
	}
	return &entityBatchService{
		repository:    em.repository,
		transformer:   em.transformer,
		registry:      em.registry,
		relations:     em.relations,
		storageTables: em.storageTables,
		createOp:      createOp,
		updateOp:      updateOp,
		deleteOp:      deleteOp,
	}
}

func validateBatchOperation(req *forma.BatchOperation) error {
	if req == nil {
		return fmt.Errorf("batch operation cannot be nil: %w", forma.ErrInvalidInput)
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

func (s *entityBatchService) BatchCreate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if err := s.validateDependencies(); err != nil {
		return nil, err
	}
	if err := validateBatchOperation(req); err != nil {
		return nil, err
	}
	zap.S().Debugw("BatchCreate called", "operationCount", len(req.Operations))
	if len(req.Operations) == 0 {
		return emptyBatchResult(), nil
	}
	if req.Atomic {
		return s.batchCreateAtomic(ctx, req)
	}

	return s.executeBestEffortBatch(ctx, req, "BatchCreate", "CREATE_FAILED", s.createOp)
}

func (s *entityBatchService) BatchUpdate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if err := s.validateDependencies(); err != nil {
		return nil, err
	}
	if err := validateBatchOperation(req); err != nil {
		return nil, err
	}

	zap.S().Debugw("BatchUpdate called", "operationCount", len(req.Operations))
	if len(req.Operations) == 0 {
		return emptyBatchResult(), nil
	}
	if req.Atomic {
		return s.batchUpdateAtomic(ctx, req)
	}

	return s.executeBestEffortBatch(ctx, req, "BatchUpdate", "UPDATE_FAILED", s.updateOp)
}

func (s *entityBatchService) BatchDelete(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if err := s.validateDependencies(); err != nil {
		return nil, err
	}
	if err := validateBatchOperation(req); err != nil {
		return nil, err
	}
	zap.S().Debugw("BatchDelete called", "operationCount", len(req.Operations))

	if len(req.Operations) == 0 {
		return emptyBatchResult(), nil
	}
	if req.Atomic {
		return s.batchDeleteAtomic(ctx, req)
	}

	return s.executeBestEffortBatch(ctx, req, "BatchDelete", "DELETE_FAILED", func(execCtx context.Context, op *forma.EntityOperation) (*forma.DataRecord, error) {
		if err := s.deleteOp(execCtx, op); err != nil {
			return nil, err
		}
		return &forma.DataRecord{
			SchemaName: op.SchemaName,
			RowID:      op.RowID,
		}, nil
	})
}

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

func (s *entityBatchService) atomicRepository() (model.AtomicBatchPersistentRecordRepository, error) {
	repo, ok := s.repository.(model.AtomicBatchPersistentRecordRepository)
	if !ok {
		return nil, fmt.Errorf("repository does not support atomic batch operations")
	}
	return repo, nil
}

func (s *entityBatchService) batchCreateAtomic(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	atomicRepo, err := s.atomicRepository()
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	tables := s.resolveTables()
	persistentRecords := make([]*model.PersistentRecord, len(req.Operations))
	successful := make([]*forma.DataRecord, len(req.Operations))

	for i, op := range req.Operations {
		if op.SchemaName == "" {
			return nil, fmt.Errorf("operation[%d]: schema name is required: %w", i, forma.ErrInvalidInput)
		}
		if op.Data == nil {
			return nil, fmt.Errorf("operation[%d]: data is required for create operation: %w", i, forma.ErrInvalidInput)
		}

		schemaID, _, err := s.registry.GetSchemaAttributeCacheByName(op.SchemaName)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to get schema: %w", i, err)
		}

		rowID := uuid.Must(uuid.NewV7())
		inputData := op.Data
		if s.relations != nil {
			inputData = s.relations.StripComputedFields(op.SchemaName, op.Data)
		}

		record, err := s.transformer.ToPersistentRecord(ctx, schemaID, rowID, inputData)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to transform data to persistent record: %w", i, err)
		}
		attributes, err := s.transformer.FromPersistentRecord(ctx, record)
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

func (s *entityBatchService) batchUpdateAtomic(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	atomicRepo, err := s.atomicRepository()
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	tables := s.resolveTables()
	persistentRecords := make([]*model.PersistentRecord, len(req.Operations))
	successful := make([]*forma.DataRecord, len(req.Operations))

	for i, op := range req.Operations {
		if op.SchemaName == "" {
			return nil, fmt.Errorf("operation[%d]: schema name is required: %w", i, forma.ErrInvalidInput)
		}
		if op.RowID == (uuid.UUID{}) {
			return nil, fmt.Errorf("operation[%d]: row id is required for update operation: %w", i, forma.ErrInvalidInput)
		}
		if op.Updates == nil {
			return nil, fmt.Errorf("operation[%d]: updates are required for update operation: %w", i, forma.ErrInvalidInput)
		}

		schemaID, _, err := s.registry.GetSchemaAttributeCacheByName(op.SchemaName)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to get schema: %w", i, err)
		}

		existingRecord, err := s.repository.GetPersistentRecord(ctx, tables, schemaID, op.RowID)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to load existing record: %w", i, err)
		}
		if existingRecord == nil {
			return nil, fmt.Errorf("operation[%d]: entity not found: %s/%s: %w", i, op.SchemaName, op.RowID, forma.ErrNotFound)
		}

		existingData, err := s.transformer.FromPersistentRecord(ctx, existingRecord)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to transform existing record: %w", i, err)
		}

		mergedData := mergeMaps(existingData, op.Updates)
		if s.relations != nil {
			mergedData = s.relations.StripComputedFields(op.SchemaName, mergedData)
		}

		updatedRecord, err := s.transformer.ToPersistentRecord(ctx, schemaID, op.RowID, mergedData)
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

func (s *entityBatchService) batchDeleteAtomic(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	atomicRepo, err := s.atomicRepository()
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	tables := s.resolveTables()
	keys := make([]model.PersistentRecordKey, len(req.Operations))
	successful := make([]*forma.DataRecord, len(req.Operations))

	for i, op := range req.Operations {
		if op.SchemaName == "" {
			return nil, fmt.Errorf("operation[%d]: schema name is required: %w", i, forma.ErrInvalidInput)
		}
		if op.RowID == (uuid.UUID{}) {
			return nil, fmt.Errorf("operation[%d]: row id is required for delete operation: %w", i, forma.ErrInvalidInput)
		}

		schemaID, _, err := s.registry.GetSchemaAttributeCacheByName(op.SchemaName)
		if err != nil {
			return nil, fmt.Errorf("operation[%d]: failed to get schema: %w", i, err)
		}

		keys[i] = model.PersistentRecordKey{SchemaID: schemaID, RowID: op.RowID}
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

func (s *entityBatchService) validateDependencies() error {
	if s.repository == nil || s.transformer == nil || s.registry == nil || s.createOp == nil || s.updateOp == nil || s.deleteOp == nil {
		return fmt.Errorf("entity batch service is not initialized")
	}
	return nil
}

func (s *entityBatchService) resolveTables() model.StorageTables {
	if s.storageTables == nil {
		return model.StorageTables{}
	}
	return s.storageTables()
}

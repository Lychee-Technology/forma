package internal

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/telemetry"

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

	// validator is nil when schema validation is unconfigured. Callers must skip
	// validation entirely in that case: Validate on a nil validator returns an
	// error, not a no-op.
	validator             *schemavalidate.Validator
	validateUpdatesStrict bool
	reportOnlyStats       *reportOnlyStats
	metrics               *telemetry.Sink
	// budgets carries the batch cap and the per-transaction write budget
	// (#465). Only the atomic paths apply the write budget here: a
	// best-effort batch runs one transaction per operation through createOp/
	// updateOp/deleteOp, each of which is bounded by the CRUD service.
	budgets requestBudgets
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

		validator:             em.validator,
		validateUpdatesStrict: em.validateUpdatesStrict,
		reportOnlyStats:       em.reportOnlyStats,
		metrics:               em.metrics,
		budgets:               budgetsFromConfig(em.config),
	}
}

type batchOperationExecutor func(context.Context, *forma.EntityOperation) (*forma.DataRecord, error)

func (s *entityBatchService) BatchCreate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if err := s.validateDependencies(); err != nil {
		return nil, err
	}
	if err := validateBatchOperation(req, s.budgets.maxBatchSize); err != nil {
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
	if err := validateBatchOperation(req, s.budgets.maxBatchSize); err != nil {
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
	if err := validateBatchOperation(req, s.budgets.maxBatchSize); err != nil {
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

func (s *entityBatchService) atomicRepository() (model.AtomicBatchPersistentRecordRepository, error) {
	repo, ok := s.repository.(model.AtomicBatchPersistentRecordRepository)
	if !ok {
		return nil, fmt.Errorf("repository does not support atomic batch operations")
	}
	return repo, nil
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

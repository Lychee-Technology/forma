package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

func (s *entityBatchService) batchDeleteAtomic(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	atomicRepo, err := s.atomicRepository()
	if err != nil {
		return nil, err
	}
	ctx, cancel := withBudget(ctx, s.budgets.write)
	defer cancel()

	startTime := time.Now()
	tables := s.resolveTables()
	keys := make([]model.PersistentRecordKey, len(req.Operations))
	successful := make([]*forma.DataRecord, len(req.Operations))

	for i, op := range req.Operations {
		if op.SchemaName == "" {
			return nil, forma.InvalidInputf("operation[%d]: schema name is required", i)
		}
		if op.RowID == (uuid.UUID{}) {
			return nil, forma.InvalidInputf("operation[%d]: row id is required for delete operation", i)
		}

		schemaID, _, err := s.registry.GetSchemaAttributeCacheByName(op.SchemaName)
		if err != nil {
			return nil, forma.WrapPublicf(fmt.Errorf("failed to get schema: %w", err), "operation[%d]", i)
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

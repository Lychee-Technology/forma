package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

func (s *entityBatchService) batchCreateAtomic(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	atomicRepo, err := s.atomicRepository()
	if err != nil {
		return nil, err
	}
	ctx, cancel := withBudget(ctx, s.budgets.write)
	defer cancel()

	startTime := time.Now()
	tables := s.resolveTables()
	persistentRecords := make([]*model.PersistentRecord, len(req.Operations))
	successful := make([]*forma.DataRecord, len(req.Operations))

	relationRoots := newRelationRootMemo(s.relations)
	for i, op := range req.Operations {
		if op.SchemaName == "" {
			return nil, forma.InvalidInputf("operation[%d]: schema name is required", i)
		}
		if op.Data == nil {
			return nil, forma.InvalidInputf("operation[%d]: data is required for create operation", i)
		}

		schemaID, schemaCache, err := s.registry.GetSchemaAttributeCacheByName(op.SchemaName)
		if err != nil {
			return nil, forma.WrapPublicf(fmt.Errorf("failed to get schema: %w", err), "operation[%d]", i)
		}

		rowID := uuid.Must(uuid.NewV7())
		// No nil guard: both RelationIndex methods are nil-receiver-safe, and
		// StripComputedFields answers the very map it was handed, so a manager
		// without an index writes exactly what the caller sent.
		inputData := s.relations.StripComputedFields(op.SchemaName, op.Data)

		// Creates always enforce, and this batch is atomic: one violation fails
		// the whole request before anything is written.
		if err := validateWritePayload(ctx, writeValidation{
			validator:     s.validator,
			schemaID:      schemaID,
			schemaName:    op.SchemaName,
			rowID:         rowID,
			cache:         schemaCache,
			data:          inputData,
			enforce:       true,
			relationRoots: relationRoots.resolve(op.SchemaName),
			stats:         s.reportOnlyStats,
			metrics:       s.metrics,
		}); err != nil {
			return nil, forma.WrapPublicf(err, "operation[%d]", i)
		}

		record, err := s.transformer.ToPersistentRecord(ctx, schemaID, rowID, inputData)
		if err != nil {
			return nil, forma.WrapPublicf(fmt.Errorf("failed to transform data to persistent record: %w", err), "operation[%d]", i)
		}
		attributes, err := s.transformer.FromPersistentRecord(ctx, record)
		if err != nil {
			return nil, forma.WrapPublicf(fmt.Errorf("failed to transform persistent record to json: %w", err), "operation[%d]", i)
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

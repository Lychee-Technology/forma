package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/lychee-technology/forma/internal/model"

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

		rowID := assignedRowID(op.RowID)
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
		persistentRecords[i] = record
	}

	if err := atomicRepo.BatchInsertPersistentRecords(ctx, tables, persistentRecords); err != nil {
		return nil, fmt.Errorf("atomic batch create failed: %w", err)
	}

	// The insert stamps each record's created_at and updated_at from the
	// repository's clock, so the response is built only after it, from the
	// records as stored, as single Create does (#701).
	successful, err := s.storedCreateResponses(ctx, req.Operations, persistentRecords)
	if err != nil {
		return nil, err
	}

	duration := time.Since(startTime).Microseconds()
	return &forma.BatchResult{
		Successful: successful,
		Failed:     make([]forma.OperationError, 0),
		TotalCount: len(req.Operations),
		Duration:   duration,
	}, nil
}

// storedCreateResponses answers each operation of an atomic create with its
// record as the batch insert stored it. The rows are already committed, so a
// failure here is the response's, not the write's: it is reported as a plain
// error, as single Create reports one.
func (s *entityBatchService) storedCreateResponses(ctx context.Context, ops []forma.EntityOperation, records []*model.PersistentRecord) ([]*forma.DataRecord, error) {
	successful := make([]*forma.DataRecord, len(records))
	for i, record := range records {
		attributes, err := s.transformer.FromPersistentRecord(ctx, record)
		if err != nil {
			return nil, fmt.Errorf("failed to transform persistent record of operation[%d] to JSON: %w", i, err)
		}
		successful[i] = &forma.DataRecord{
			SchemaName: ops[i].SchemaName,
			RowID:      record.RowID,
			Attributes: attributes,
		}
	}
	return successful, nil
}

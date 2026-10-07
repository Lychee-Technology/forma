package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
)

func (s *entityBatchService) batchUpdateAtomic(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	atomicRepo, err := s.atomicRepository()
	if err != nil {
		return nil, err
	}
	ctx, cancel := withBudget(ctx, s.budgets.write)
	defer cancel()
	if err := validateAtomicUpdateOperations(req.Operations); err != nil {
		return nil, err
	}

	startTime := time.Now()
	keys := make([]model.PersistentRecordKey, len(req.Operations))
	caches := make([]forma.SchemaAttributeCache, len(req.Operations))
	for i, op := range req.Operations {
		schemaID, schemaCache, err := s.registry.GetSchemaAttributeCacheByName(op.SchemaName)
		if err != nil {
			return nil, forma.WrapPublicf(fmt.Errorf("failed to get schema: %w", err), "operation[%d]", i)
		}
		keys[i] = model.PersistentRecordKey{SchemaID: schemaID, RowID: op.RowID}
		caches[i] = schemaCache
	}

	// The whole read-modify-write of every operation happens inside the
	// repository's one write transaction, after every row's advisory lock
	// has been granted (#554, the batch analogue of #457): each merge base
	// is read there, so an overlapping batch or single-row Update cannot be
	// clobbered by a merge onto a pre-write snapshot.
	relationRoots := newRelationRootMemo(s.relations)
	stored, err := atomicRepo.BatchMergePersistentRecords(ctx, s.resolveTables(), keys,
		func(ctx context.Context, i int, existing *model.PersistentRecord) (*model.PersistentRecord, error) {
			return s.mergeBatchUpdateRecord(ctx, &req.Operations[i], i, keys[i].SchemaID, caches[i], relationRoots, existing)
		})
	if err != nil {
		return nil, fmt.Errorf("atomic batch update failed: %w", err)
	}

	// The response is the storage round-trip, as Update's and Create's are:
	// echoing the pre-write merge answered a version storage may not have
	// stamped (#554).
	successful := make([]*forma.DataRecord, len(req.Operations))
	for i, op := range req.Operations {
		attributes, err := s.transformer.FromPersistentRecord(ctx, stored[i])
		if err != nil {
			return nil, forma.WrapPublicf(fmt.Errorf("failed to transform stored record: %w", err), "operation[%d]", i)
		}
		successful[i] = &forma.DataRecord{
			SchemaName: op.SchemaName,
			RowID:      op.RowID,
			Attributes: attributes,
		}
	}

	duration := time.Since(startTime).Microseconds()
	return &forma.BatchResult{
		Successful: successful,
		Failed:     make([]forma.OperationError, 0),
		TotalCount: len(req.Operations),
		Duration:   duration,
	}, nil
}

// mergeBatchUpdateRecord is one operation's merge body, run by the
// repository inside the locked write transaction with the row as it exists
// there. existing is nil when the row is absent: the 404 is authored here
// because it names the caller's schema, row and operation index.
func (s *entityBatchService) mergeBatchUpdateRecord(
	ctx context.Context,
	op *forma.EntityOperation,
	i int,
	schemaID int16,
	schemaCache forma.SchemaAttributeCache,
	relationRoots *relationRootMemo,
	existing *model.PersistentRecord,
) (*model.PersistentRecord, error) {
	if existing == nil {
		return nil, forma.NotFoundf("operation[%d]: entity not found: %s/%s", i, op.SchemaName, op.RowID)
	}

	mergedData, err := mergeUpdateDocument(ctx, s.transformer, s.relations, op.SchemaName, existing, op.Updates)
	if err != nil {
		return nil, forma.WrapPublicf(fmt.Errorf("failed to transform existing record: %w", err), "operation[%d]", i)
	}

	// The *merged* document is what gets validated, so a partial update that
	// does not mention a required attribute still succeeds.
	if err := validateWritePayload(ctx, writeValidation{
		validator:     s.validator,
		schemaID:      schemaID,
		schemaName:    op.SchemaName,
		rowID:         op.RowID,
		cache:         schemaCache,
		data:          mergedData,
		enforce:       s.validateUpdatesStrict,
		relationRoots: relationRoots.resolve(op.SchemaName),
		stats:         s.reportOnlyStats,
		metrics:       s.metrics,
	}); err != nil {
		return nil, forma.WrapPublicf(err, "operation[%d]", i)
	}

	updatedRecord, err := s.transformer.ToPersistentRecord(ctx, schemaID, op.RowID, mergedData)
	if err != nil {
		return nil, forma.WrapPublicf(fmt.Errorf("failed to transform merged data: %w", err), "operation[%d]", i)
	}
	updatedRecord.CreatedAt = existing.CreatedAt
	updatedRecord.DeletedAt = existing.DeletedAt
	return updatedRecord, nil
}

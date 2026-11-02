package benchmark

import (
	"context"
	"fmt"

	forma "github.com/lychee-technology/forma"
	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
)

func (r *Runner) executeKeysetServiceQuery(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition) (*forma.QueryResult, []*model.PersistentRecord, *model.ExecutionPlan, error) {
	pool, err := openBenchmarkPool(ctx, h)
	if err != nil {
		return nil, nil, nil, err
	}
	defer pool.Close()

	if err := RegisterFixtureSchemas(h); err != nil {
		return nil, nil, nil, fmt.Errorf("register fixture schemas: %w", err)
	}
	stack, err := newBenchmarkQueryStack(ctx, pool, h, r.planCache)
	if err != nil {
		return nil, nil, nil, err
	}
	schemaID, err := stack.resolveSchemaID(workload.TargetSchema)
	if err != nil {
		return nil, nil, nil, err
	}

	pageSize := workload.PageSize
	if pageSize <= 0 {
		pageSize = r.config.PageSize
	}
	pageNumber := workload.PageNumber
	if pageNumber < 1 {
		pageNumber = 1
	}
	tables := benchmarkStorageTables(h)
	fq := keysetWorkloadQuery(h, workload, schemaID, pageSize)

	if pageNumber == 1 {
		// Page 1: keyset with no cursor is equivalent to offset 0.
		// For page 1 of a keyset workload, use a nil cursor.
		recs, total, err := stack.engine.ExecuteDuckDBFederatedQuery(ctx, tables, fq, pageSize, 0, nil, &model.FederatedQueryOptions{
			IncludeExecutionPlan: true,
		})
		if err != nil {
			return nil, nil, nil, fmt.Errorf("keyset page 1: %w", err)
		}
		result := &forma.QueryResult{TotalRecords: int(total), ItemsPerPage: pageSize, CurrentPage: 1}
		return result, recs, nil, nil
	}
	return executeKeysetPageAfterCursor(ctx, stack.engine, tables, fq, pageSize, pageNumber)
}

// keysetWorkloadQuery builds the federated query of a keyset workload's page,
// with no cursor yet.
//
// No AttributeOrders here, unlike the offset path: a keyset page is
// ordered by its cursor columns, and this workload builds its cursor
// (keysetCursorAfter) on created_at DESC, row_id ASC — the default order — so
// a keyset page is never in the tradeTime order the offset workloads use, and
// sortExpectedRecordsForWorkload matches the cursor instead. The cursor
// used to have no other choice: the retired federated allowlist accepted
// system columns only. #381 replaced it with validateKeysetCursor, which
// admits attribute columns too, but this workload's expectations are built
// around the default order and stay there.
//
// This code used to build a tradeTime order and discard it, which read as
// an oversight but was in fact unobservable: tradeTime is generated FROM
// changed_at and the reader aliased changed_at into created_at, so both
// orders were the same sequence until #460 separated them.
func keysetWorkloadQuery(h *federated.FederatedTestHarness, workload WorkloadDefinition, schemaID int16, pageSize int) *model.FederatedAttributeQuery {
	fq := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			SchemaID: schemaID,
			Limit:    pageSize,
			Offset:   0,
		},
	}
	if conditions := workload.ResolvedFilterConditions(); len(conditions) > 0 {
		fq.Condition = conditionForWorkload(workload)
	}
	if h.Duck != nil {
		fq.DuckDBHints = &model.DuckDBRenderHints{
			S3ParquetPathTemplate: benchmarkS3ParquetPathTemplate(h),
		}
	}
	return fq
}

// executeKeysetPageAfterCursor serves page N > 1 of a keyset workload: it
// fetches the cursor row (page N-1's last row) via offset, then uses keyset
// pagination after it to fetch page N.
func executeKeysetPageAfterCursor(ctx context.Context, engine *fedengine.DBFederatedQueryEngine, tables model.StorageTables, fq *model.FederatedAttributeQuery, pageSize, pageNumber int) (*forma.QueryResult, []*model.PersistentRecord, *model.ExecutionPlan, error) {
	cursorOffset := (pageNumber-1)*pageSize - 1
	cursorFq := *fq
	cursorFq.Limit = 1
	cursorFq.Offset = cursorOffset
	cursorRecs, _, err := engine.ExecuteDuckDBFederatedQuery(ctx, tables, &cursorFq, 1, cursorOffset, nil, &model.FederatedQueryOptions{MaxRows: 1})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("fetch cursor row: %w", err)
	}
	if len(cursorRecs) == 0 {
		result := &forma.QueryResult{TotalRecords: 0, ItemsPerPage: pageSize, CurrentPage: pageNumber}
		return result, nil, nil, nil
	}

	fq.KeysetCursor = keysetCursorAfter(cursorRecs[0])
	opts := &model.FederatedQueryOptions{
		IncludeExecutionPlan: true,
	}
	recs, total, err := engine.ExecuteFederatedKeysetQuery(ctx, tables, fq, pageSize, nil, opts)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("keyset query page %d: %w", pageNumber, err)
	}

	plan := opts.ExecutionPlan
	if plan == nil {
		plan = &model.ExecutionPlan{Notes: []string{"keyset_pagination"}, Timings: map[string]int64{}}
	} else {
		plan.Notes = append(plan.Notes, "keyset_pagination")
	}
	result := &forma.QueryResult{
		TotalRecords: int(total),
		ItemsPerPage: pageSize,
		CurrentPage:  pageNumber,
	}
	return result, recs, plan, nil
}

// keysetCursorAfter builds the keyset cursor that resumes after rec:
// created_at DESC with the mandatory row_id ASC tiebreak (#183). created_at is
// the row's creation stamp, version-invariant since #460 — NOT the trade time,
// which the offset workloads sort by and a cursor cannot address.
func keysetCursorAfter(rec *model.PersistentRecord) *model.KeysetCursor {
	return &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderDesc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{
			rec.CreatedAt,
			rec.RowID.String(),
		},
		Mode: model.KeysetCursorModeAfter,
	}
}

package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// Query queries entities with filters and pagination
func (em *entityManager) Query(ctx context.Context, req *forma.QueryRequest) (*forma.QueryResult, error) {
	if req == nil {
		return nil, fmt.Errorf("query request cannot be nil")
	}

	if req.SchemaName == "" {
		return nil, fmt.Errorf("schema name is required")
	}

	if req.Page < 1 {
		req.Page = 1
	}

	if req.ItemsPerPage < 1 {
		req.ItemsPerPage = em.config.Query.DefaultPageSize
	}

	if req.ItemsPerPage > em.config.Query.MaxPageSize {
		req.ItemsPerPage = em.config.Query.MaxPageSize
	}

	// Verify schema exists and get attribute metadata
	schemaId, schemaCache, err := em.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	sortOrder := req.SortOrder
	if sortOrder == "" {
		sortOrder = forma.SortOrderAsc
	}

	attributeOrders := make([]AttributeOrder, 0, len(req.SortBy))
	for _, sortAttr := range req.SortBy {
		meta, ok := schemaCache[sortAttr]
		if !ok {
			return nil, fmt.Errorf("cannot sort by unknown attribute '%s' in schema '%s'", sortAttr, req.SchemaName)
		}
		order := AttributeOrder{
			AttrID:    meta.AttributeID,
			ValueType: meta.ValueType,
			SortOrder: sortOrder,
		}
		// Check if attribute has column_binding to main table
		if meta.ColumnBinding != nil {
			order.StorageLocation = forma.AttributeStorageLocationMain
			order.ColumnName = string(meta.ColumnBinding.ColumnName)
		} else {
			order.StorageLocation = forma.AttributeStorageLocationEAV
		}
		attributeOrders = append(attributeOrders, order)
	}

	tables := em.storageTables()
	query := &PersistentRecordQuery{
		Tables:          tables,
		SchemaID:        schemaId,
		Condition:       req.Condition,
		AttributeOrders: attributeOrders,
		Limit:           req.ItemsPerPage,
		Offset:          (req.Page - 1) * req.ItemsPerPage,
	}

	startTime := time.Now()
	page, err := em.repository.QueryPersistentRecords(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query persistent records: %w", err)
	}

	records := make([]*forma.DataRecord, 0, len(page.Records))
	for _, record := range page.Records {
		dataRecord, err := em.toDataRecord(ctx, req.SchemaName, record)
		if err != nil {
			return nil, err
		}
		records = append(records, dataRecord)
	}

	if err := em.enrichDataRecords(ctx, req.SchemaName, req.Attrs, records...); err != nil {
		return nil, err
	}

	applyProjection(records, req.Attrs)

	totalPages := page.TotalPages
	if totalPages == 0 && page.TotalRecords > 0 && req.ItemsPerPage > 0 {
		totalPages = int((page.TotalRecords + int64(req.ItemsPerPage) - 1) / int64(req.ItemsPerPage))
	}

	zap.S().Infow("query results", "records", len(records), "totalPages", totalPages)

	return &forma.QueryResult{
		Data:          records,
		TotalRecords:  int(page.TotalRecords),
		TotalPages:    totalPages,
		CurrentPage:   req.Page,
		ItemsPerPage:  req.ItemsPerPage,
		HasNext:       req.Page < totalPages,
		HasPrevious:   req.Page > 1,
		ExecutionTime: time.Since(startTime),
	}, nil
}

// CrossSchemaSearch searches across multiple schemas using a single optimized query
func (em *entityManager) CrossSchemaSearch(ctx context.Context, req *forma.CrossSchemaRequest) (*forma.QueryResult, error) {
	if err := em.validateCrossSchemaRequest(req); err != nil {
		return nil, err
	}

	startTime := time.Now()
	tables := em.storageTables()

	// Build schema contexts with conditions
	schemaContexts, err := em.buildSchemaContexts(req.SchemaNames, req.Condition)
	if err != nil {
		return nil, err
	}

	// Count records per schema
	schemaTotals, err := em.countSchemaRecords(ctx, tables, schemaContexts)
	if err != nil {
		return nil, err
	}

	// Calculate total
	var totalRecords int64
	for _, count := range schemaTotals {
		totalRecords += count
	}

	// Return empty result if no records
	if totalRecords == 0 {
		return em.emptyQueryResult(req.Page, req.ItemsPerPage, startTime), nil
	}

	// Fetch paginated results across schemas
	results, err := em.fetchCrossSchemaResults(ctx, tables, schemaContexts, schemaTotals, req)
	if err != nil {
		return nil, err
	}

	totalPages := int((totalRecords + int64(req.ItemsPerPage) - 1) / int64(req.ItemsPerPage))
	return &forma.QueryResult{
		Data:          results,
		TotalRecords:  int(totalRecords),
		TotalPages:    totalPages,
		CurrentPage:   req.Page,
		ItemsPerPage:  req.ItemsPerPage,
		HasNext:       req.Page < totalPages,
		HasPrevious:   req.Page > 1,
		ExecutionTime: time.Since(startTime),
	}, nil
}

// validateCrossSchemaRequest validates the cross schema search request parameters.
func (em *entityManager) validateCrossSchemaRequest(req *forma.CrossSchemaRequest) error {
	if req == nil {
		return fmt.Errorf("cross schema request cannot be nil")
	}
	if len(req.SchemaNames) == 0 {
		return fmt.Errorf("schema names are required")
	}
	if req.SearchTerm == "" {
		return fmt.Errorf("search term is required")
	}
	if req.Page < 1 {
		req.Page = 1
	}
	if req.ItemsPerPage < 1 {
		req.ItemsPerPage = em.config.Query.DefaultPageSize
	}
	if req.ItemsPerPage > em.config.Query.MaxPageSize {
		req.ItemsPerPage = em.config.Query.MaxPageSize
	}
	return nil
}

// schemaContext holds schema information for cross-schema queries.
type schemaContext struct {
	name      string
	id        int16
	condition forma.Condition
}

// buildSchemaContexts builds schema contexts from schema names.
func (em *entityManager) buildSchemaContexts(schemaNames []string, condition forma.Condition) ([]schemaContext, error) {
	searchCondition := condition
	if searchCondition == nil {
		searchCondition = &forma.CompositeCondition{
			Logic:      forma.LogicAnd,
			Conditions: []forma.Condition{},
		}
	}

	contexts := make([]schemaContext, 0, len(schemaNames))
	for _, schemaName := range schemaNames {
		schemaID, _, err := em.registry.GetSchemaAttributeCacheByName(schemaName)
		if err != nil {
			return nil, fmt.Errorf("failed to get schema %s: %w", schemaName, err)
		}
		contexts = append(contexts, schemaContext{
			name:      schemaName,
			id:        schemaID,
			condition: searchCondition,
		})
	}
	return contexts, nil
}

// countSchemaRecords counts total records for each schema.
func (em *entityManager) countSchemaRecords(ctx context.Context, tables StorageTables, contexts []schemaContext) ([]int64, error) {
	totals := make([]int64, len(contexts))
	for idx, schemaCtx := range contexts {
		page, err := em.repository.QueryPersistentRecords(ctx, &PersistentRecordQuery{
			Tables:    tables,
			SchemaID:  schemaCtx.id,
			Condition: schemaCtx.condition,
			Limit:     1,
			Offset:    0,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to count records for schema %s: %w", schemaCtx.name, err)
		}
		totals[idx] = page.TotalRecords
	}
	return totals, nil
}

// emptyQueryResult returns an empty query result.
func (em *entityManager) emptyQueryResult(page, itemsPerPage int, startTime time.Time) *forma.QueryResult {
	return &forma.QueryResult{
		Data:          []*forma.DataRecord{},
		TotalRecords:  0,
		TotalPages:    0,
		CurrentPage:   page,
		ItemsPerPage:  itemsPerPage,
		HasNext:       false,
		HasPrevious:   page > 1,
		ExecutionTime: time.Since(startTime),
	}
}

// fetchCrossSchemaResults fetches paginated results across multiple schemas.
func (em *entityManager) fetchCrossSchemaResults(
	ctx context.Context,
	tables StorageTables,
	contexts []schemaContext,
	schemaTotals []int64,
	req *forma.CrossSchemaRequest,
) ([]*forma.DataRecord, error) {
	offset := (req.Page - 1) * req.ItemsPerPage
	remaining := req.ItemsPerPage
	results := make([]*forma.DataRecord, 0, req.ItemsPerPage)
	skip := offset

	for idx, schemaCtx := range contexts {
		count := int(schemaTotals[idx])
		if skip >= count {
			skip -= count
			continue
		}

		schemaOffset := skip
		skip = 0
		avail := count - schemaOffset
		schemaLimit := remaining
		if avail < schemaLimit {
			schemaLimit = avail
		}
		if schemaLimit <= 0 {
			continue
		}

		batchRecords, err := em.fetchSchemaBatch(ctx, tables, schemaCtx, schemaOffset, schemaLimit, req.Attrs)
		if err != nil {
			return nil, err
		}

		results = append(results, batchRecords...)
		remaining -= len(batchRecords)
		if remaining <= 0 {
			break
		}
	}

	return results, nil
}

// fetchSchemaBatch fetches a batch of records from a single schema.
func (em *entityManager) fetchSchemaBatch(
	ctx context.Context,
	tables StorageTables,
	schemaCtx schemaContext,
	offset, limit int,
	attrs []string,
) ([]*forma.DataRecord, error) {
	page, err := em.repository.QueryPersistentRecords(ctx, &PersistentRecordQuery{
		Tables:    tables,
		SchemaID:  schemaCtx.id,
		Condition: schemaCtx.condition,
		Limit:     limit,
		Offset:    offset,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch records for schema %s: %w", schemaCtx.name, err)
	}

	batchRecords := make([]*forma.DataRecord, 0, len(page.Records))
	for _, record := range page.Records {
		dataRecord, err := em.toDataRecord(ctx, schemaCtx.name, record)
		if err != nil {
			return nil, err
		}
		batchRecords = append(batchRecords, dataRecord)
	}

	if err := em.enrichDataRecords(ctx, schemaCtx.name, attrs, batchRecords...); err != nil {
		return nil, err
	}

	applyProjection(batchRecords, attrs)
	return batchRecords, nil
}

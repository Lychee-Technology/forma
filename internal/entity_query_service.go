package internal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

var defaultFederatedPreferredTiers = []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold}

type dataRecordConverter func(context.Context, string, *model.PersistentRecord) (*forma.DataRecord, error)
type dataRecordEnricher func(context.Context, string, []string, ...*forma.DataRecord) error
type storageTablesResolver func() model.StorageTables

type entityQueryService struct {
	repository           model.PersistentRecordRepository
	federatedQueryEngine model.FederatedQueryEngine
	registry             forma.SchemaRegistry
	config               *forma.Config
	toDataRecord         dataRecordConverter
	enrichDataRecords    dataRecordEnricher
	storageTables        storageTablesResolver
}

func newEntityQueryService(em *entityManager) *entityQueryService {
	if em == nil {
		return &entityQueryService{}
	}
	return &entityQueryService{
		repository:           em.repository,
		federatedQueryEngine: em.federatedQueryEngine,
		registry:             em.registry,
		config:               em.config,
		toDataRecord:         em.toDataRecord,
		enrichDataRecords:    em.enrichDataRecords,
		storageTables:        em.storageTables,
	}
}

func (s *entityQueryService) Query(ctx context.Context, req *forma.QueryRequest) (*forma.QueryResult, error) {
	if s.config == nil {
		return nil, fmt.Errorf("entity manager config is required: %w", forma.ErrInvalidInput)
	}
	if s.registry == nil || s.repository == nil || s.toDataRecord == nil || s.enrichDataRecords == nil {
		return nil, fmt.Errorf("entity query service is not initialized: %w", forma.ErrInvalidInput)
	}

	if req == nil {
		return nil, fmt.Errorf("query request cannot be nil: %w", forma.ErrInvalidInput)
	}

	if req.SchemaName == "" {
		return nil, fmt.Errorf("schema name is required: %w", forma.ErrInvalidInput)
	}

	if req.Page < 1 {
		req.Page = 1
	}

	if req.ItemsPerPage < 1 {
		req.ItemsPerPage = s.config.Query.DefaultPageSize
	}

	if req.ItemsPerPage > s.config.Query.MaxPageSize {
		req.ItemsPerPage = s.config.Query.MaxPageSize
	}

	// Verify schema exists and get attribute metadata.
	schemaID, schemaCache, err := s.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	attributeOrders, err := buildAttributeOrders(req, schemaCache)
	if err != nil {
		return nil, err
	}

	query := &model.PersistentRecordQuery{
		Tables:          s.resolveTables(),
		SchemaID:        schemaID,
		Condition:       req.Condition,
		AttributeOrders: attributeOrders,
		Limit:           req.ItemsPerPage,
		Offset:          (req.Page - 1) * req.ItemsPerPage,
	}

	startTime := time.Now()
	page, err := s.queryRecords(ctx, query, req)
	if err != nil {
		return nil, fmt.Errorf("failed to query persistent records: %w", err)
	}

	records := make([]*forma.DataRecord, 0, len(page.Records))
	for _, record := range page.Records {
		dataRecord, err := s.toDataRecord(ctx, req.SchemaName, record)
		if err != nil {
			return nil, err
		}
		records = append(records, dataRecord)
	}

	if err := s.enrichDataRecords(ctx, req.SchemaName, req.Attrs, records...); err != nil {
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
		ExecutionPlan: toExecutionPlan(page.ExecutionPlan),
	}, nil
}

// sortKey is one normalized (attribute, direction) pair extracted from the
// request's sort surface — either the structured Sort field or legacy
// SortBy + shared SortOrder.
type sortKey struct {
	attr  string
	order forma.SortOrder
}

// resolveSortKeys normalizes the request's two sort surfaces into ordered
// sortKey pairs. The structured Sort field carries per-key directions (#240);
// the legacy SortBy list shares the single SortOrder. The two surfaces are
// mutually exclusive so a caller bug cannot be silently half-applied.
func resolveSortKeys(req *forma.QueryRequest) ([]sortKey, error) {
	if len(req.Sort) == 0 {
		sortOrder := req.SortOrder
		if sortOrder == "" {
			sortOrder = forma.SortOrderAsc
		}
		keys := make([]sortKey, 0, len(req.SortBy))
		for _, attr := range req.SortBy {
			keys = append(keys, sortKey{attr: attr, order: sortOrder})
		}
		return keys, nil
	}

	if len(req.SortBy) > 0 || req.SortOrder != "" {
		return nil, fmt.Errorf(
			"sort cannot be combined with sort_by/sort_order in schema '%s': use sort alone for per-key directions: %w",
			req.SchemaName, forma.ErrInvalidInput)
	}

	keys := make([]sortKey, 0, len(req.Sort))
	for i, entry := range req.Sort {
		attr := strings.TrimSpace(entry.Attribute)
		if attr == "" {
			return nil, fmt.Errorf("sort entry %d in schema '%s' has an empty attribute: %w",
				i, req.SchemaName, forma.ErrInvalidInput)
		}
		order, err := normalizeSortOrder(entry.SortOrder)
		if err != nil {
			return nil, fmt.Errorf("sort entry for attribute '%s' in schema '%s': %w", attr, req.SchemaName, err)
		}
		keys = append(keys, sortKey{attr: attr, order: order})
	}
	return keys, nil
}

// normalizeSortOrder folds a Sort entry's direction case-insensitively:
// empty defaults to asc, anything other than asc/desc is invalid input.
func normalizeSortOrder(raw forma.SortOrder) (forma.SortOrder, error) {
	switch forma.SortOrder(strings.ToLower(string(raw))) {
	case "", forma.SortOrderAsc:
		return forma.SortOrderAsc, nil
	case forma.SortOrderDesc:
		return forma.SortOrderDesc, nil
	default:
		return "", fmt.Errorf("invalid sort_order '%s': expected 'asc' or 'desc': %w", raw, forma.ErrInvalidInput)
	}
}

// buildAttributeOrders resolves the request's sort keys against the schema
// cache into typed AttributeOrders, tagging each with its storage location
// (main column vs EAV) and its own direction. It errors on an unknown sort
// attribute.
func buildAttributeOrders(req *forma.QueryRequest, schemaCache forma.SchemaAttributeCache) ([]model.AttributeOrder, error) {
	keys, err := resolveSortKeys(req)
	if err != nil {
		return nil, err
	}

	orders := make([]model.AttributeOrder, 0, len(keys))
	for _, key := range keys {
		meta, ok := schemaCache[key.attr]
		if !ok {
			return nil, fmt.Errorf("cannot sort by unknown attribute '%s' in schema '%s'", key.attr, req.SchemaName)
		}
		order := model.AttributeOrder{
			AttrID:    meta.AttributeID,
			ValueType: meta.ValueType,
			SortOrder: key.order,
			AttrName:  key.attr,
		}
		if meta.ColumnBinding != nil {
			order.StorageLocation = forma.AttributeStorageLocationMain
			order.ColumnName = string(meta.ColumnBinding.ColumnName)
		} else {
			order.StorageLocation = forma.AttributeStorageLocationEAV
		}
		orders = append(orders, order)
	}
	return orders, nil
}

// toExecutionPlan converts the engine's internal execution plan into the
// public forma.ExecutionPlan projection surfaced on QueryResult. It returns nil
// when no plan was recorded (non-federated requests, or federated requests that
// did not ask for the plan), so QueryResult.ExecutionPlan stays omitted.
func toExecutionPlan(plan *model.ExecutionPlan) *forma.ExecutionPlan {
	if plan == nil {
		return nil
	}

	// SECURITY: do not project src.SQL / src.Params or plan.Notes / merge.Notes.
	// The DuckDB source SQL embeds the postgres_scan connection string (with the
	// DB password) and notes can echo raw engine errors — surfacing them on the
	// HTTP response would leak credentials to any advanced_query caller. Only
	// safe routing/tier metadata crosses into the public projection.
	out := &forma.ExecutionPlan{
		Routing: forma.ExecutionRouting{
			UsedDuckDB: plan.Routing.UseDuckDB,
			Tiers:      tiersToStrings(plan.Routing.Tiers),
			Reason:     plan.Routing.Reason,
		},
		Timings: plan.Timings,
	}

	if len(plan.Sources) > 0 {
		out.Sources = make([]forma.ExecutionSource, 0, len(plan.Sources))
		for _, src := range plan.Sources {
			out.Sources = append(out.Sources, forma.ExecutionSource{
				Tier:              string(src.Tier),
				Engine:            src.Engine,
				RowEstimate:       src.RowEstimate,
				ActualRows:        src.ActualRows,
				PredicatePushdown: src.PredicatePushdown,
				DurationMs:        src.DurationMs,
				Reason:            src.Reason,
			})
		}
	}

	if plan.Merge.Strategy != "" || plan.Merge.PreferHot || len(plan.Merge.DedupKeys) > 0 {
		out.Merge = &forma.ExecutionMerge{
			Strategy:   string(plan.Merge.Strategy),
			PreferHot:  plan.Merge.PreferHot,
			DedupKeys:  plan.Merge.DedupKeys,
			DurationMs: plan.Merge.DurationMs,
		}
	}

	return out
}

func tiersToStrings(tiers []model.DataTier) []string {
	if len(tiers) == 0 {
		return nil
	}
	out := make([]string, 0, len(tiers))
	for _, t := range tiers {
		out = append(out, string(t))
	}
	return out
}

func (s *entityQueryService) queryRecords(ctx context.Context, query *model.PersistentRecordQuery, req *forma.QueryRequest) (*model.PersistentRecordPage, error) {
	if query == nil {
		return nil, fmt.Errorf("query cannot be nil")
	}
	if req == nil || req.Federated == nil || !req.Federated.Enabled {
		return s.repository.QueryPersistentRecords(ctx, query)
	}
	if s.federatedQueryEngine == nil {
		return nil, fmt.Errorf("federated query engine is not initialized")
	}

	fq := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			SchemaID:        query.SchemaID,
			Condition:       query.Condition,
			AttributeOrders: query.AttributeOrders,
			Limit:           query.Limit,
			Offset:          query.Offset,
		},
		PreferredTiers:  federatedPreferredTiers(req.Federated.PreferredTiers),
		PreferHot:       req.Federated.PreferHot,
		UseMainAsAnchor: req.Federated.UseMainAsAnchor,
	}
	if req.Federated.S3ParquetPathTemplate != "" {
		fq.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: req.Federated.S3ParquetPathTemplate}
	}

	return s.federatedQueryEngine.Query(ctx, query.Tables, fq, &model.FederatedQueryOptions{
		AllowPartialDegradedMode: req.Federated.AllowPartialDegradedMode,
		IncludeExecutionPlan:     req.Federated.IncludeExecutionPlan,
		ConsistencyMode:          model.ConsistencyMode(req.Federated.ConsistencyMode),
	})
}

func federatedPreferredTiers(tiers []string) []model.DataTier {
	if len(tiers) == 0 {
		return append([]model.DataTier(nil), defaultFederatedPreferredTiers...)
	}
	out := make([]model.DataTier, 0, len(tiers))
	for _, tier := range tiers {
		switch model.DataTier(tier) {
		case model.DataTierHot, model.DataTierWarm, model.DataTierCold:
			out = append(out, model.DataTier(tier))
		}
	}
	if len(out) == 0 {
		return append([]model.DataTier(nil), defaultFederatedPreferredTiers...)
	}
	return out
}

func (s *entityQueryService) CrossSchemaSearch(ctx context.Context, req *forma.CrossSchemaRequest) (*forma.QueryResult, error) {
	if s.config == nil {
		return nil, fmt.Errorf("entity manager config is required: %w", forma.ErrInvalidInput)
	}
	if s.registry == nil || s.repository == nil || s.toDataRecord == nil || s.enrichDataRecords == nil {
		return nil, fmt.Errorf("entity query service is not initialized: %w", forma.ErrInvalidInput)
	}

	if err := s.validateCrossSchemaRequest(req); err != nil {
		return nil, err
	}

	startTime := time.Now()
	tables := s.resolveTables()

	// Build schema contexts with conditions.
	schemaContexts, err := s.buildSchemaContexts(req.SchemaNames, req.Condition)
	if err != nil {
		return nil, err
	}

	// Count records per schema.
	schemaTotals, err := s.countSchemaRecords(ctx, tables, schemaContexts)
	if err != nil {
		return nil, err
	}

	// Calculate total.
	var totalRecords int64
	for _, count := range schemaTotals {
		totalRecords += count
	}

	// Return empty result if no records.
	if totalRecords == 0 {
		return s.emptyQueryResult(req.Page, req.ItemsPerPage, startTime), nil
	}

	// Fetch paginated results across schemas.
	results, err := s.fetchCrossSchemaResults(ctx, tables, schemaContexts, schemaTotals, req)
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
func (s *entityQueryService) validateCrossSchemaRequest(req *forma.CrossSchemaRequest) error {
	if req == nil {
		return fmt.Errorf("cross schema request cannot be nil: %w", forma.ErrInvalidInput)
	}
	if len(req.SchemaNames) == 0 {
		return fmt.Errorf("schema names are required: %w", forma.ErrInvalidInput)
	}
	if req.SearchTerm == "" {
		return fmt.Errorf("search term is required: %w", forma.ErrInvalidInput)
	}
	if req.Page < 1 {
		req.Page = 1
	}
	if req.ItemsPerPage < 1 {
		req.ItemsPerPage = s.config.Query.DefaultPageSize
	}
	if req.ItemsPerPage > s.config.Query.MaxPageSize {
		req.ItemsPerPage = s.config.Query.MaxPageSize
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
func (s *entityQueryService) buildSchemaContexts(schemaNames []string, condition forma.Condition) ([]schemaContext, error) {
	searchCondition := condition
	if searchCondition == nil {
		searchCondition = &forma.CompositeCondition{
			Logic:      forma.LogicAnd,
			Conditions: []forma.Condition{},
		}
	}

	contexts := make([]schemaContext, 0, len(schemaNames))
	for _, schemaName := range schemaNames {
		schemaID, _, err := s.registry.GetSchemaAttributeCacheByName(schemaName)
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
func (s *entityQueryService) countSchemaRecords(ctx context.Context, tables model.StorageTables, contexts []schemaContext) ([]int64, error) {
	totals := make([]int64, len(contexts))
	for idx, schemaCtx := range contexts {
		page, err := s.repository.QueryPersistentRecords(ctx, &model.PersistentRecordQuery{
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
func (s *entityQueryService) emptyQueryResult(page, itemsPerPage int, startTime time.Time) *forma.QueryResult {
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
func (s *entityQueryService) fetchCrossSchemaResults(
	ctx context.Context,
	tables model.StorageTables,
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
		schemaLimit := min(avail, remaining)
		if schemaLimit <= 0 {
			continue
		}

		batchRecords, err := s.fetchSchemaBatch(ctx, tables, schemaCtx, schemaOffset, schemaLimit, req.Attrs)
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
func (s *entityQueryService) fetchSchemaBatch(
	ctx context.Context,
	tables model.StorageTables,
	schemaCtx schemaContext,
	offset, limit int,
	attrs []string,
) ([]*forma.DataRecord, error) {
	page, err := s.repository.QueryPersistentRecords(ctx, &model.PersistentRecordQuery{
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
		dataRecord, err := s.toDataRecord(ctx, schemaCtx.name, record)
		if err != nil {
			return nil, err
		}
		batchRecords = append(batchRecords, dataRecord)
	}

	if err := s.enrichDataRecords(ctx, schemaCtx.name, attrs, batchRecords...); err != nil {
		return nil, err
	}

	applyProjection(batchRecords, attrs)
	return batchRecords, nil
}

func (s *entityQueryService) resolveTables() model.StorageTables {
	if s.storageTables == nil {
		return model.StorageTables{}
	}
	return s.storageTables()
}

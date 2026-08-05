package internal

import (
	"context"
	"errors"
	"fmt"
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
		return nil, errors.New("entity manager config is required")
	}
	if s.registry == nil || s.repository == nil || s.toDataRecord == nil || s.enrichDataRecords == nil {
		return nil, errors.New("entity query service is not initialized")
	}

	if req == nil {
		return nil, forma.InvalidInputf("query request cannot be nil")
	}

	if req.SchemaName == "" {
		return nil, forma.InvalidInputf("schema name is required")
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

// toExecutionPlan converts the engine's internal execution plan into the
// public forma.ExecutionPlan projection surfaced on QueryResult. It returns nil
// when no plan was recorded (non-federated requests, or federated requests that
// did not ask for the plan), so QueryResult.ExecutionPlan stays omitted.
func toExecutionPlan(plan *model.ExecutionPlan) *forma.ExecutionPlan {
	if plan == nil {
		return nil
	}

	// SECURITY: do not project src.SQL / src.Params or plan.Notes / merge.Notes.
	// Since #306, the database password is redacted at the source (plan SQL and
	// failure notes carry password=***REDACTED***), but the rendered DuckDB SQL
	// still embeds host/user/dbname, table internals; Params carry query arguments;
	// notes carry storage keys and engine internals — surfacing them on the HTTP
	// response would leak internals to any advanced_query caller. Only safe
	// routing/tier metadata crosses into the public projection.
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
		return nil, errors.New("entity manager config is required")
	}
	if s.registry == nil || s.repository == nil || s.toDataRecord == nil || s.enrichDataRecords == nil {
		return nil, errors.New("entity query service is not initialized")
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
		return forma.InvalidInputf("cross schema request cannot be nil")
	}
	if len(req.SchemaNames) == 0 {
		return forma.InvalidInputf("schema names are required")
	}
	if req.SearchTerm == "" {
		return forma.InvalidInputf("search term is required")
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

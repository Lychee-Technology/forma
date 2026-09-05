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
		Partial:       toPartialResult(page.Partial),
	}, nil
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

// federatedPreferredTiers forwards the caller's tier declaration to the
// engine, keeping only known tier names. An omitted list — or one naming no
// known tier — is forwarded as nil, NOT filled with the default three: the
// engine, the DuckDB template and the plan-cache shape all read an empty
// list as the default all-tier form (#184), and a non-empty list is what the
// routing policy treats as an explicit coverage declaration that overrides
// the cost heuristics (#468). Filling the default here would make every
// request look explicit.
func federatedPreferredTiers(tiers []string) []model.DataTier {
	var out []model.DataTier
	for _, tier := range tiers {
		switch model.DataTier(tier) {
		case model.DataTierHot, model.DataTierWarm, model.DataTierCold:
			out = append(out, model.DataTier(tier))
		}
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

package internal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen"

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
	// readBudget bounds each Query and CrossSchemaSearch call
	// (QueryConfig.DefaultTimeout, #465); zero means none.
	readBudget time.Duration
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
		readBudget:           budgetsFromConfig(em.config).read,
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

	s.applyPageDefaults(req)

	// Verify schema exists and get attribute metadata.
	schemaID, schemaCache, err := s.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	attributeOrders, err := buildAttributeOrders(req, schemaCache)
	if err != nil {
		return nil, err
	}

	federated := s.federatedRoute()
	if federated {
		if err := validateFederatedSort(req, attributeOrders); err != nil {
			return nil, err
		}
	}

	offset, err := pageOffset(req.Page, req.ItemsPerPage, s.config.Query.MaxRows)
	if err != nil {
		return nil, err
	}

	query := &model.PersistentRecordQuery{
		Tables:          s.resolveTables(),
		SchemaID:        schemaID,
		Condition:       req.Condition,
		AttributeOrders: attributeOrders,
		Limit:           req.ItemsPerPage,
		Offset:          offset,
	}

	ctx, cancel := withBudget(ctx, s.readBudget)
	defer cancel()

	startTime := time.Now()
	var page *model.PersistentRecordPage
	if federated {
		page, err = s.queryFederated(ctx, query, req, schemaCache)
	} else {
		page, err = s.repository.QueryPersistentRecords(ctx, query)
	}
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

// applyPageDefaults clamps the request's page to at least 1 and its page
// size into [1, MaxPageSize], defaulting an unset size to DefaultPageSize.
func (s *entityQueryService) applyPageDefaults(req *forma.QueryRequest) {
	if req.Page < 1 {
		req.Page = 1
	}
	if req.ItemsPerPage < 1 {
		req.ItemsPerPage = s.config.Query.DefaultPageSize
	}
	if req.ItemsPerPage > s.config.Query.MaxPageSize {
		req.ItemsPerPage = s.config.Query.MaxPageSize
	}
}

// federatedRoute reports whether Query runs through the federated engine.
// That is the deployment's choice, never the request's (#715): with DuckDB
// enabled every non-transactional Query enters the engine, whose routing
// policy may still answer it from PostgreSQL alone; without it every Query
// runs on PostgreSQL. A Tx session's service has no engine (boundTo), so
// its reads stay on PostgreSQL either way.
func (s *entityQueryService) federatedRoute() bool {
	return s.federatedQueryEngine != nil && s.config != nil && s.config.DuckDB.Enabled
}

// queryFederated serves query through the federated engine. A $now leaf is
// pinned first to one instant read from PostgreSQL, so every leg compares
// against the same value (#715). The query carries why the parquet tiers
// cannot hold the schema, if they cannot (#704): only the registry's cache,
// schemaCache, derives the array ancestry that decides it.
func (s *entityQueryService) queryFederated(ctx context.Context, query *model.PersistentRecordQuery, req *forma.QueryRequest, schemaCache forma.SchemaAttributeCache) (*model.PersistentRecordPage, error) {
	condition, err := s.resolveServerTime(ctx, query.Condition, schemaCache)
	if err != nil {
		return nil, err
	}
	fq := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			SchemaID:        query.SchemaID,
			Condition:       condition,
			AttributeOrders: query.AttributeOrders,
			Limit:           query.Limit,
			Offset:          query.Offset,
		},
		ParquetUnrepresentable: sqlgen.ParquetUnrepresentableReason(schemaCache),
	}
	return s.federatedQueryEngine.Query(ctx, query.Tables, fq, &model.FederatedQueryOptions{
		AllowPartialDegradedMode: s.config.DuckDB.AllowPartialDegradedMode,
		IncludeExecutionPlan:     req.IncludeExecutionPlan,
	})
}

// resolveServerTime pins cond's $now leaves to one PostgreSQL instant
// (sqlgen.ResolveServerTime). DuckDB cannot read the PostgreSQL clock, so
// the instant is read once, before routing, and only for a condition that
// holds a $now leaf. It is PostgreSQL's clock, not this process's: $now and
// server_time read one clock (docs/claims.md), and comparing values written
// with it against another would turn clock skew into lease errors. If
// PostgreSQL cannot answer, neither can the query.
func (s *entityQueryService) resolveServerTime(ctx context.Context, cond forma.Condition, cache forma.SchemaAttributeCache) (forma.Condition, error) {
	if !sqlgen.HasServerTime(cond, cache) {
		return cond, nil
	}
	clock, ok := s.repository.(model.ServerClock)
	if !ok {
		return nil, fmt.Errorf("resolve $now for a federated query: repository %T cannot read the postgresql clock", s.repository)
	}
	nowMs, err := clock.ServerTimeMillis(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve $now for a federated query: %w", err)
	}
	return sqlgen.ResolveServerTime(cond, cache, nowMs), nil
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

	ctx, cancel := withBudget(ctx, s.readBudget)
	defer cancel()

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
	if _, err := pageOffset(req.Page, req.ItemsPerPage, s.config.Query.MaxRows); err != nil {
		return err
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
	// validateCrossSchemaRequest already refused an overflowing or too-deep
	// page; the error branch here is unreachable but keeps the arithmetic in
	// one place.
	offset, err := pageOffset(req.Page, req.ItemsPerPage, s.config.Query.MaxRows)
	if err != nil {
		return nil, err
	}
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

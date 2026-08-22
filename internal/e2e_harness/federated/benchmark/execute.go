package benchmark

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/transform"
)

func extractRouteInfo(plan *model.ExecutionPlan) (engine, reason string) {
	if plan == nil {
		return "", ""
	}
	if plan.Routing.UseDuckDB {
		engine = "duckdb"
	} else if len(plan.Routing.Reason) > 0 {
		engine = "postgres"
	}
	reason = plan.Routing.Reason
	return
}

func routeInfoFromPlanNotes(notes []string) (engine, reason string) {
	for _, note := range notes {
		switch note {
		case "postgres_only_execution", "prefer_hot_override":
			engine = "postgres"
			reason = note
		case "dirty_ids_excluded":
			if engine == "" {
				engine = "duckdb"
				reason = "federated"
			}
		}
	}
	if engine == "" && len(notes) > 0 {
		engine = "duckdb"
		reason = "federated"
	}
	return
}

func (r *Runner) executeServiceWorkload(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition) (WorkloadRunResult, []*model.PersistentRecord, error) {
	req, pageSize := queryRequestForWorkload(workload, r.config.PageSize)
	start := time.Now()
	capturePlan := workload.Category == WorkloadCategoryPushdown || workload.Category == WorkloadCategoryTierMix

	var result *forma.QueryResult
	var records []*model.PersistentRecord
	var plan *model.ExecutionPlan
	var err error

	if workload.UseKeysetPagination {
		result, records, plan, err = r.executeKeysetServiceQuery(ctx, h, workload)
	} else {
		result, records, plan, err = executeServiceQueryWithPlan(ctx, h, req, r.config.PageSize, capturePlan, r.planCache)
	}
	if err != nil {
		return WorkloadRunResult{}, nil, err
	}
	run := WorkloadRunResult{
		Name:         workload.Name,
		Category:     string(workload.Category),
		Distribution: r.genConfig.Distribution,
		PageSize:     pageSize,
		PageNumber:   workload.PageNumber,
		Offset:       workload.DerivedOffset(r.config.PageSize),
		PreferHot:    workload.PreferHot,
		ResultCount:  len(records),
		TotalRecords: int64(result.TotalRecords),
		Duration:     time.Since(start),
		Passed:       true,
		RowIDs:       persistentRecordIDs(records),
		PlanNotes:    []string{"entity_manager_query_service"},
		PerTier:      extractPerTierMetrics(plan),
	}
	if workload.UseKeysetPagination {
		run.PaginationMode = "keyset"
		run.PlanNotes = append(run.PlanNotes, "keyset_pagination")
	}
	if engine, reason := extractRouteInfo(plan); engine != "" {
		run.RouteEngine = engine
		run.RouteReason = reason
	}
	return run, records, nil
}

func (r *Runner) executeKeysetServiceQuery(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition) (*forma.QueryResult, []*model.PersistentRecord, *model.ExecutionPlan, error) { //nolint:funlen // #437: benchmark harness split
	if h == nil || h.PGDSN == "" {
		return nil, nil, nil, fmt.Errorf("benchmark harness postgres DSN is required")
	}
	pool, err := pgxpool.New(ctx, h.PGDSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("connect benchmark pgx pool: %w", err)
	}
	defer pool.Close()

	if err := RegisterFixtureSchemas(h); err != nil {
		return nil, nil, nil, fmt.Errorf("register fixture schemas: %w", err)
	}
	schemaTable, err := ensureBenchmarkSchemaRegistry(ctx, pool)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("prepare benchmark schema registry: %w", err)
	}

	registry, err := schemameta.NewFileSchemaRegistry(pool, schemaTable, FixturesDir())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build benchmark schema registry: %w", err)
	}
	metadata, err := schemameta.NewMetadataLoader(pool, schemaTable, FixturesDir()).LoadMetadata(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load benchmark metadata: %w", err)
	}

	duckCfg := engineDuckDBConfig(h)
	repo := internal.NewDBPersistentRecordRepository(pool, metadata, internal.WithPlanCache(r.planCache))
	engine := fedengine.NewDBFederatedQueryEngine(
		repo,
		fedengine.NewPostgresDirtyIDFetcher(pool),
		fedengine.NewDuckDBClientQueryExecutor(h.Duck),
		nil,
		duckCfg,
		metadata,
		fedengine.DuckDBPostgresConnStringFromPool(pool),
		fedengine.WithPlanCache(r.planCache),
	)

	schemaName := workload.TargetSchema
	if schemaName == "" {
		schemaName = "trade"
	}
	schemaID, _, err := registry.GetSchemaAttributeCacheByName(schemaName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("resolve schema %s: %w", schemaName, err)
	}

	pageSize := workload.PageSize
	if pageSize <= 0 {
		pageSize = r.config.PageSize
	}
	pageNumber := workload.PageNumber
	if pageNumber < 1 {
		pageNumber = 1
	}

	tables := model.StorageTables{
		EAVData:    h.CDCConfig.EAVDataTable,
		EntityMain: h.CDCConfig.EntityMainTable,
		ChangeLog:  h.CDCConfig.ChangeLogTable,
	}

	// Build attribute orders for sort
	attrOrders := []model.AttributeOrder{}
	if cache, ok := metadata.GetSchemaCacheByID(schemaID); ok {
		if meta, found := cache["tradeTime"]; found {
			attrOrders = append(attrOrders, model.AttributeOrder{
				AttrID:    meta.AttributeID,
				ValueType: meta.ValueType,
				SortOrder: forma.SortOrderDesc,
			})
		}
	}
	_ = attrOrders

	// Build the federated query with filter conditions
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

	start := time.Now()
	var totalRecords int64

	if pageNumber == 1 {
		// Page 1: keyset with no cursor is equivalent to offset 0.
		// For page 1 of a keyset workload, use a nil cursor.
		recs, total, err := engine.ExecuteDuckDBFederatedQuery(ctx, tables, fq, pageSize, 0, nil, &model.FederatedQueryOptions{
			IncludeExecutionPlan: true,
		})
		if err != nil {
			return nil, nil, nil, fmt.Errorf("keyset page 1: %w", err)
		}
		totalRecords = total
		result := &forma.QueryResult{TotalRecords: int(totalRecords), ItemsPerPage: pageSize, CurrentPage: 1}
		return result, recs, nil, nil
	}

	// For page N > 1: fetch the cursor row (page N-1's last row) via offset,
	// then use keyset to fetch page N.
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

	// Build the keyset cursor from the cursor row.
	// Benchmark schema sorts by created_at (trade_time) DESC + row_id ASC tiebreaker.
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderDesc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{
			cursorRecs[0].CreatedAt,
			cursorRecs[0].RowID.String(),
		},
		Mode: model.KeysetCursorModeAfter,
	}

	fq.KeysetCursor = cursor
	opts := &model.FederatedQueryOptions{
		KeysetEnabled:        true,
		IncludeExecutionPlan: true,
	}

	recs, total, err := engine.ExecuteFederatedPaginatedQuery(ctx, tables, fq, pageSize, 0, nil, opts)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("keyset query page %d: %w", pageNumber, err)
	}
	totalRecords = total

	plan := opts.ExecutionPlan
	if plan == nil {
		plan = &model.ExecutionPlan{Notes: []string{"keyset_pagination"}, Timings: map[string]int64{}}
	} else {
		plan.Notes = append(plan.Notes, "keyset_pagination")
	}
	_ = start

	result := &forma.QueryResult{
		TotalRecords: int(totalRecords),
		ItemsPerPage: pageSize,
		CurrentPage:  pageNumber,
	}
	return result, recs, plan, nil
}

// engineDuckDBConfig returns the engine-level DuckDB config for a harness:
// the exact configuration the harness started DuckDB with (single source of
// truth for resource settings), or a zero config when DuckDB is not running.
func engineDuckDBConfig(h *federated.FederatedTestHarness) forma.DuckDBConfig {
	if h.Duck == nil {
		return forma.DuckDBConfig{}
	}
	return h.DuckCfg
}

func executeServiceQuery(ctx context.Context, h *federated.FederatedTestHarness, req *forma.QueryRequest, defaultPageSize int, planCache *queryplan.Cache) (*forma.QueryResult, []*model.PersistentRecord, error) {
	if h == nil || h.PGDSN == "" {
		return nil, nil, fmt.Errorf("benchmark harness postgres DSN is required")
	}
	pool, err := pgxpool.New(ctx, h.PGDSN)
	if err != nil {
		return nil, nil, fmt.Errorf("connect benchmark pgx pool: %w", err)
	}
	defer pool.Close()

	if err := RegisterFixtureSchemas(h); err != nil {
		return nil, nil, fmt.Errorf("register fixture schemas: %w", err)
	}
	schemaTable, err := ensureBenchmarkSchemaRegistry(ctx, pool)
	if err != nil {
		return nil, nil, fmt.Errorf("prepare benchmark schema registry: %w", err)
	}

	registry, err := schemameta.NewFileSchemaRegistry(pool, schemaTable, FixturesDir())
	if err != nil {
		return nil, nil, fmt.Errorf("build benchmark schema registry: %w", err)
	}
	metadata, err := schemameta.NewMetadataLoader(pool, schemaTable, FixturesDir()).LoadMetadata(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("load benchmark metadata: %w", err)
	}

	duckCfg := engineDuckDBConfig(h)

	repo := internal.NewDBPersistentRecordRepository(pool, metadata, internal.WithPlanCache(planCache))
	engine := fedengine.NewDBFederatedQueryEngine(
		repo,
		fedengine.NewPostgresDirtyIDFetcher(pool),
		fedengine.NewDuckDBClientQueryExecutor(h.Duck),
		nil,
		duckCfg,
		metadata,
		fedengine.DuckDBPostgresConnStringFromPool(pool),
		fedengine.WithPlanCache(planCache),
	)
	config := &forma.Config{
		Database: forma.DatabaseConfig{
			TableNames: forma.TableNames{
				SchemaRegistry: schemaTable,
				EntityMain:     h.CDCConfig.EntityMainTable,
				EAVData:        h.CDCConfig.EAVDataTable,
				ChangeLog:      h.CDCConfig.ChangeLogTable,
			},
		},
		Query: forma.QueryConfig{
			DefaultPageSize: benchmarkDefaultPageSize(defaultPageSize),
			MaxPageSize:     maxInt(defaultPageSize, 1000),
		},
		Entity: forma.EntityConfig{
			SchemaDirectory: FixturesDir(),
		},
		DuckDB: duckCfg,
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	// No WithRelationIndex here, so this is the manager's own relation-index load,
	// and it now fails closed (#388). Reported as an error rather than absorbed:
	// a benchmark that ran on with stripping disabled would be measuring a write
	// and read path production does not have.
	manager, err := internal.NewEntityManager(transformer, repo, engine, registry, config, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build benchmark entity manager: %w", err)
	}
	if req != nil && req.Federated != nil && req.Federated.Enabled {
		req.Federated.S3ParquetPathTemplate = benchmarkS3ParquetPathTemplate(h)
	}
	result, err := manager.Query(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("execute service query: %w", err)
	}
	records, err := persistentRecordsForQueryResult(ctx, result, registry)
	if err != nil {
		return nil, nil, err
	}
	return result, records, nil
}

func executeServiceQueryWithPlan(ctx context.Context, h *federated.FederatedTestHarness, req *forma.QueryRequest, defaultPageSize int, capturePlan bool, planCache *queryplan.Cache) (*forma.QueryResult, []*model.PersistentRecord, *model.ExecutionPlan, error) { //nolint:funlen // #437: benchmark harness split
	if h == nil || h.PGDSN == "" {
		return nil, nil, nil, fmt.Errorf("benchmark harness postgres DSN is required")
	}
	pool, err := pgxpool.New(ctx, h.PGDSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("connect benchmark pgx pool: %w", err)
	}
	defer pool.Close()

	result, records, err := executeServiceQuery(ctx, h, req, defaultPageSize, planCache)
	if err != nil {
		return nil, nil, nil, err
	}

	if !capturePlan || req == nil || req.Federated == nil {
		return result, records, nil, nil
	}

	schemaTable, err := ensureBenchmarkSchemaRegistry(ctx, pool)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("prepare benchmark schema registry: %w", err)
	}
	registry, err := schemameta.NewFileSchemaRegistry(pool, schemaTable, FixturesDir())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build benchmark schema registry: %w", err)
	}
	metadata, err := schemameta.NewMetadataLoader(pool, schemaTable, FixturesDir()).LoadMetadata(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load benchmark metadata: %w", err)
	}

	schemaName := req.SchemaName
	if schemaName == "" {
		schemaName = "trade"
	}
	schemaID, _, err := registry.GetSchemaAttributeCacheByName(schemaName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("resolve schema %s: %w", schemaName, err)
	}

	duckCfg := engineDuckDBConfig(h)
	repo := internal.NewDBPersistentRecordRepository(pool, metadata, internal.WithPlanCache(planCache))
	engine := fedengine.NewDBFederatedQueryEngine(
		repo,
		fedengine.NewPostgresDirtyIDFetcher(pool),
		fedengine.NewDuckDBClientQueryExecutor(h.Duck),
		nil,
		duckCfg,
		metadata,
		fedengine.DuckDBPostgresConnStringFromPool(pool),
		fedengine.WithPlanCache(planCache),
	)

	pageSize := benchmarkDefaultPageSize(defaultPageSize)
	limit := pageSize
	offset := (maxInt(req.Page, 1) - 1) * pageSize

	sortOrder := forma.SortOrderDesc
	if req.SortOrder == forma.SortOrderAsc {
		sortOrder = forma.SortOrderAsc
	}

	var attrOrders []model.AttributeOrder
	if cache, ok := metadata.GetSchemaCacheByID(schemaID); ok {
		for _, sortAttr := range req.SortBy {
			meta, found := cache[sortAttr]
			if !found {
				continue
			}
			order := model.AttributeOrder{
				AttrID:    meta.AttributeID,
				ValueType: meta.ValueType,
				SortOrder: sortOrder,
			}
			if meta.ColumnBinding != nil {
				order.StorageLocation = forma.AttributeStorageLocationMain
				order.ColumnName = string(meta.ColumnBinding.ColumnName)
			} else {
				order.StorageLocation = forma.AttributeStorageLocationEAV
			}
			attrOrders = append(attrOrders, order)
		}
	}

	plan := &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
	fqOpts := &model.FederatedQueryOptions{
		IncludeExecutionPlan: true,
		ExecutionPlan:        plan,
	}

	tables := model.StorageTables{
		EntityMain: h.CDCConfig.EntityMainTable,
		EAVData:    h.CDCConfig.EAVDataTable,
		ChangeLog:  h.CDCConfig.ChangeLogTable,
	}

	fq := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			SchemaID:        schemaID,
			Condition:       req.Condition,
			AttributeOrders: attrOrders,
			Limit:           limit,
			Offset:          offset,
		},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
	}
	if req.Federated.S3ParquetPathTemplate != "" {
		fq.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: req.Federated.S3ParquetPathTemplate}
	}
	if req.Federated.PreferHot {
		fq.PreferHot = true
	}

	_, err = engine.Query(ctx, tables, fq, fqOpts)
	if err != nil {
		return result, records, plan, nil
	}
	if fqOpts.ExecutionPlan == nil || !fqOpts.ExecutionPlan.Routing.UseDuckDB {
		return result, records, plan, fmt.Errorf("federated benchmark query did not route to duckdb")
	}

	return result, records, plan, nil
}

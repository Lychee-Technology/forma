package benchmark

import (
	"context"
	"fmt"
	"time"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
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

func executeServiceQuery(ctx context.Context, h *federated.FederatedTestHarness, req *forma.QueryRequest, defaultPageSize int, planCache *queryplan.Cache) (*forma.QueryResult, []*model.PersistentRecord, error) {
	pool, err := openBenchmarkPool(ctx, h)
	if err != nil {
		return nil, nil, err
	}
	defer pool.Close()

	if err := RegisterFixtureSchemas(h); err != nil {
		return nil, nil, fmt.Errorf("register fixture schemas: %w", err)
	}
	stack, err := newBenchmarkQueryStack(ctx, pool, h, planCache)
	if err != nil {
		return nil, nil, err
	}
	transformer := transform.NewPersistentRecordTransformer(stack.registry)
	// No WithRelationIndex here, so this is the manager's own relation-index load,
	// and it now fails closed (#388). Reported as an error rather than absorbed:
	// a benchmark that ran on with stripping disabled would be measuring a write
	// and read path production does not have.
	manager, err := internal.NewEntityManager(transformer, stack.repo, stack.engine, stack.registry,
		benchmarkServiceConfig(h, stack, defaultPageSize), nil)
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
	records, err := persistentRecordsForQueryResult(ctx, result, stack.registry)
	if err != nil {
		return nil, nil, err
	}
	return result, records, nil
}

// benchmarkServiceConfig is the forma config of the benchmark's entity
// manager: the harness's tables and DuckDB config, the stack's schema registry
// table, and the benchmark's page size.
func benchmarkServiceConfig(h *federated.FederatedTestHarness, stack *benchmarkQueryStack, defaultPageSize int) *forma.Config {
	return &forma.Config{
		Database: forma.DatabaseConfig{
			TableNames: forma.TableNames{
				SchemaRegistry: stack.schemaTable,
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
		DuckDB: stack.duckCfg,
	}
}

// executeServiceQueryWithPlan runs req through the entity manager and, when
// capturePlan is set on a federated request, replays it directly against the
// federated engine to capture the execution plan the service path does not
// expose. The replay's records are discarded: the measured result is the
// service query's.
func executeServiceQueryWithPlan(ctx context.Context, h *federated.FederatedTestHarness, req *forma.QueryRequest, defaultPageSize int, capturePlan bool, planCache *queryplan.Cache) (*forma.QueryResult, []*model.PersistentRecord, *model.ExecutionPlan, error) {
	pool, err := openBenchmarkPool(ctx, h)
	if err != nil {
		return nil, nil, nil, err
	}
	defer pool.Close()

	result, records, err := executeServiceQuery(ctx, h, req, defaultPageSize, planCache)
	if err != nil {
		return nil, nil, nil, err
	}

	if !capturePlan || req == nil || req.Federated == nil {
		return result, records, nil, nil
	}

	stack, err := newBenchmarkQueryStack(ctx, pool, h, planCache)
	if err != nil {
		return nil, nil, nil, err
	}
	schemaID, err := stack.resolveSchemaID(req.SchemaName)
	if err != nil {
		return nil, nil, nil, err
	}

	plan := &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
	fqOpts := &model.FederatedQueryOptions{
		IncludeExecutionPlan: true,
		ExecutionPlan:        plan,
	}
	fq := planCaptureQuery(req, schemaID, attributeOrdersForRequest(stack.metadata, schemaID, req), defaultPageSize)
	_, err = stack.engine.Query(ctx, benchmarkStorageTables(h), fq, fqOpts)
	if err != nil {
		return result, records, plan, nil
	}
	if fqOpts.ExecutionPlan == nil || !fqOpts.ExecutionPlan.Routing.UseDuckDB {
		return result, records, plan, fmt.Errorf("federated benchmark query did not route to duckdb")
	}

	return result, records, plan, nil
}

// planCaptureQuery mirrors a federated service request as a direct federated
// engine query over all three tiers, paged by the benchmark page size.
func planCaptureQuery(req *forma.QueryRequest, schemaID int16, attrOrders []model.AttributeOrder, defaultPageSize int) *model.FederatedAttributeQuery {
	pageSize := benchmarkDefaultPageSize(defaultPageSize)
	fq := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			SchemaID:        schemaID,
			Condition:       req.Condition,
			AttributeOrders: attrOrders,
			Limit:           pageSize,
			Offset:          (maxInt(req.Page, 1) - 1) * pageSize,
		},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
	}
	if req.Federated.S3ParquetPathTemplate != "" {
		fq.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: req.Federated.S3ParquetPathTemplate}
	}
	if req.Federated.PreferHot {
		fq.PreferHot = true
	}
	return fq
}

// attributeOrdersForRequest resolves req's SortBy attributes against the
// schema's metadata, in req's sort direction (descending unless ascending is
// asked for). Attributes the schema does not know are skipped.
func attributeOrdersForRequest(metadata *schemameta.MetadataCache, schemaID int16, req *forma.QueryRequest) []model.AttributeOrder {
	cache, ok := metadata.GetSchemaCacheByID(schemaID)
	if !ok {
		return nil
	}
	sortOrder := forma.SortOrderDesc
	if req.SortOrder == forma.SortOrderAsc {
		sortOrder = forma.SortOrderAsc
	}
	var attrOrders []model.AttributeOrder
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
	return attrOrders
}

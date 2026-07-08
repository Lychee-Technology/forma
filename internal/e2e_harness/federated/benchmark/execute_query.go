package benchmark

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	forma "github.com/lychee-technology/forma"
	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
)

func ensureBenchmarkSchemaRegistry(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	const tableName = "benchmark_schema_registry"
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS benchmark_schema_registry (
			schema_id SMALLINT PRIMARY KEY,
			schema_name TEXT NOT NULL UNIQUE,
			created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
		)`); err != nil {
		return "", err
	}
	if _, err := pool.Exec(ctx, `DELETE FROM benchmark_schema_registry`); err != nil {
		return "", err
	}
	for _, fixture := range DefaultSchemaFixtures() {
		if _, err := pool.Exec(ctx, `INSERT INTO benchmark_schema_registry (schema_id, schema_name) VALUES ($1, $2)`, fixture.ID, fixture.Name); err != nil {
			return "", err
		}
	}
	return tableName, nil
}

func queryRequestForWorkload(workload WorkloadDefinition, defaultPageSize int) (*forma.QueryRequest, int) {
	pageSize := workload.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	req := &forma.QueryRequest{
		SchemaName:   workload.TargetSchema,
		Page:         maxInt(workload.PageNumber, 1),
		ItemsPerPage: pageSize,
	}
	if workload.ExecutionSource == "service" {
		req.Federated = &forma.FederatedQueryRequest{
			Enabled:              true,
			PreferredTiers:       []string{"hot", "warm", "cold"},
			IncludeExecutionPlan: workload.Category == WorkloadCategoryPushdown || workload.Category == WorkloadCategoryTierMix,
		}
	}
	if workload.TargetSchema == "trade" {
		req.SortBy = []string{"tradeTime"}
		req.SortOrder = forma.SortOrderDesc
	}
	if cond := conditionForWorkload(workload); cond != nil {
		req.Condition = cond
	}
	return req, pageSize
}

func benchmarkS3ParquetPathTemplate(h *federated.FederatedTestHarness) string {
	if h == nil {
		return ""
	}
	return fmt.Sprintf("s3://%s/%s/{{.SchemaID}}/base/*.parquet, s3://%s/%s/{{.SchemaID}}/delta/*.parquet", h.S3Bucket, h.S3Prefix, h.S3Bucket, h.S3Prefix)
}

func conditionForWorkload(workload WorkloadDefinition) forma.Condition {
	conditions := make([]forma.Condition, 0, len(workload.ResolvedFilterConditions())+1)
	for key, value := range workload.ResolvedFilterConditions() {
		conditions = append(conditions, &forma.KvCondition{Attr: key, Value: fmt.Sprintf("equals:%v", value)})
	}
	if len(conditions) == 0 {
		return nil
	}
	if len(conditions) == 1 {
		return conditions[0]
	}
	return &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: conditions}
}

func persistentRecordsForQueryResult(ctx context.Context, result *forma.QueryResult, registry forma.SchemaRegistry) ([]*model.PersistentRecord, error) {
	if result == nil {
		return nil, nil
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	records := make([]*model.PersistentRecord, 0, len(result.Data))
	for _, data := range result.Data {
		if data == nil {
			continue
		}
		schemaID, _, err := registry.GetSchemaAttributeCacheByName(data.SchemaName)
		if err != nil {
			return nil, fmt.Errorf("resolve schema %s: %w", data.SchemaName, err)
		}
		record, err := transformer.ToPersistentRecord(ctx, schemaID, data.RowID, data.Attributes)
		if err != nil {
			return nil, fmt.Errorf("rebuild persistent record %s: %w", data.RowID, err)
		}
		if record != nil {
			records = append(records, record)
		}
	}
	return records, nil
}

func benchmarkDefaultPageSize(pageSize int) int {
	if pageSize > 0 {
		return pageSize
	}
	return 20
}

func failedWorkloadRunResult(workload WorkloadDefinition, distribution Distribution, defaultPageSize int, infraError string) WorkloadRunResult {
	pageSize := workload.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	return WorkloadRunResult{
		Name:         workload.Name,
		Category:     string(workload.Category),
		Distribution: distribution,
		PageSize:     pageSize,
		PageNumber:   workload.PageNumber,
		Offset:       workload.DerivedOffset(defaultPageSize),
		PreferHot:    workload.PreferHot,
		Passed:       false,
		FailureKind:  FailureKindInfra,
		FailureCount: 1,
		InfraError:   infraError,
	}
}

func queryOptionsForWorkload(workload WorkloadDefinition, defaultPageSize int) *federated.QueryOptions {
	return queryOptionsForWorkloadWithConfig(workload, defaultPageSize, DefaultGeneratorConfig())
}

func queryOptionsForWorkloadWithConfig(workload WorkloadDefinition, defaultPageSize int, genCfg GeneratorConfig) *federated.QueryOptions {
	pageSize := workload.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	opts := &federated.QueryOptions{Limit: pageSize, Offset: workload.DerivedOffset(defaultPageSize)}
	if workload.TargetSchema == "trade" {
		opts.SortBy = "tradeTime"
		opts.SortDesc = true
	}
	semantics := semanticsForWorkload(workload, genCfg)
	opts.TradeTimeStart = semantics.TradeTimeStart
	opts.TradeTimeEnd = semantics.TradeTimeEnd
	return opts
}

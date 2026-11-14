package federated

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// The parquet leg of a federated pass (design.md §5, plan step 3, #854):
// sqlgen.ParquetOnlyQueryTemplateDuckDB, rendered through the compiled plan
// cache. Since #858 it is the only DuckDB statement a pass runs.

// parquetOnlyPlanKind keys the parquet-only skeletons in the plan cache the
// engine shares with the PostgreSQL route.
const parquetOnlyPlanKind = "duckdb_federated_parquet_only"

// RenderParquetOnlyStatement resolves q's parquet scan set as a pass does
// and renders the parquet-only statement for it, with window bound. q's own
// Limit, Offset and AttributeOrders are the page's. The caller stages the
// dirty set with StageDirtySet on the DuckDB connection it runs the statement
// on; the statement reads nothing else of PostgreSQL's.
//
// A pass renders it internally; the production harness calls this to time
// the parquet leg on its own (#851, TestHotLegLatencyBaseline).
func (e *DBFederatedQueryEngine) RenderParquetOnlyStatement(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	window sqlgen.ParquetWindow,
) (string, []any, error) {
	if q == nil {
		return "", nil, fmt.Errorf("render parquet-only statement: the query is nil")
	}
	q, err := e.keysetQuery(q, q.AttributeOrders)
	if err != nil {
		return "", nil, fmt.Errorf("render parquet-only statement: validate keyset cursor: %w", err)
	}
	src, err := e.resolvePgxScanSources(ctx, tables, q)
	if err != nil {
		return "", nil, fmt.Errorf("render parquet-only statement for schema %d: %w", q.SchemaID, err)
	}
	return e.buildParquetOnlyQuery(ctx, tables, q, q.AttributeOrders, q.Limit, q.Offset, window, src.paths, src.cold)
}

// buildParquetOnlyQuery renders the parquet leg for one dispatched request
// and binds window. The caller has staged the dirty set on the connection the
// statement will run on (StageDirtySet). Neither the set nor the hot match
// count reaches the plan-cache key, so a shape compiles once whatever either
// holds.
func (e *DBFederatedQueryEngine) buildParquetOnlyQuery(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	attributeOrders []model.AttributeOrder,
	limit, offset int,
	window sqlgen.ParquetWindow,
	parquetPaths []string,
	cold coldScanSet,
) (string, []any, error) {
	sqlStr, args, _, err := e.buildParquetOnlyQueryCached(ctx, tables, q, attributeOrders, limit, offset, window, parquetPaths, cold)
	return sqlStr, args, err
}

// buildParquetOnlyQueryCached is buildParquetOnlyQuery that also reports
// whether the compiled plan came from the plan cache, for the execution plan.
func (e *DBFederatedQueryEngine) buildParquetOnlyQueryCached(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	attributeOrders []model.AttributeOrder,
	limit, offset int,
	window sqlgen.ParquetWindow,
	parquetPaths []string,
	cold coldScanSet,
) (string, []any, *bool, error) {
	if q == nil {
		return "", nil, nil, fmt.Errorf("build parquet-only query: the query is nil")
	}
	q = dispatchedQuery(q, limit, offset, attributeOrders)
	sqlParams := map[string]any{"SchemaID": q.SchemaID}
	applyParquetScanParams(sqlParams, parquetPaths, cold)

	cache, _ := e.schemaCacheByID(q.SchemaID)
	dc, err := e.translateDuckDBQuery(ctx, tables.EAVData, q, cache, sqlParams)
	if err != nil {
		return "", nil, nil, fmt.Errorf("build parquet-only query for schema %d: %w", q.SchemaID, err)
	}
	compile := func() (any, error) {
		return sqlgen.CompileParquetOnlyQuery(sqlParams, q, &dc)
	}

	var artifact any
	var hit *bool
	if e.planCache == nil {
		artifact, err = compile()
	} else {
		key, keyErr := e.parquetOnlyPlanCacheKey(tables, q, parquetPaths, attributeOrders, cold)
		if keyErr != nil {
			return "", nil, nil, fmt.Errorf("build parquet-only query: %w", keyErr)
		}
		var cached bool
		artifact, cached, err = e.planCache.GetOrBuild(ctx, key, compile)
		hit = &cached
	}
	if err != nil {
		return "", nil, nil, fmt.Errorf("compile parquet-only query for schema %d: %w", q.SchemaID, err)
	}
	compiled, ok := artifact.(*sqlgen.ParquetOnlyCompiledQuery)
	if !ok || compiled == nil {
		return "", nil, nil, fmt.Errorf("compile parquet-only query for schema %d: the plan cache holds %T under its key", q.SchemaID, artifact)
	}
	sqlStr, args, err := compiled.Bind(q, dc, window)
	if err != nil {
		return "", nil, nil, fmt.Errorf("build parquet-only query for schema %d: %w", q.SchemaID, err)
	}
	return sqlStr, args, hit, nil
}

// parquetOnlyPlanCacheKey derives the plan-cache key of a parquet-only
// skeleton: the schema fingerprint, the query shape and parquetOnlyScopeParts.
func (e *DBFederatedQueryEngine) parquetOnlyPlanCacheKey(
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	parquetPaths []string,
	attributeOrders []model.AttributeOrder,
	cold coldScanSet,
) (queryplan.Key, error) {
	shapeHash, err := queryplan.HashFederatedQueryShape(q)
	if err != nil {
		return queryplan.Key{}, fmt.Errorf("hash federated query shape for schema %d: %w", q.SchemaID, err)
	}
	fingerprint := "no-fingerprint"
	if e.metadataCache != nil {
		if fp, ok := e.metadataCache.SchemaFingerprint(q.SchemaID); ok {
			fingerprint = fp
		}
	}
	return queryplan.Key{
		Kind:          parquetOnlyPlanKind,
		SchemaVersion: fingerprint,
		SchemaID:      q.SchemaID,
		ShapeHash:     shapeHash,
		ScopeHash:     queryplan.HashScopeParts(parquetOnlyScopeParts(tables, parquetPaths, attributeOrders, cold)...),
	}, nil
}

// parquetOnlyScopeParts is the retired attach template's scope without what
// the parquet-only skeleton does not render: the connection string (no
// catalog reference), the dirty-set flag (the set is a staged table whatever
// its size), and the limit and offset (the window is bound). The EAV table stays, because the
// DuckDB clause is built against it.
func parquetOnlyScopeParts(
	tables model.StorageTables,
	parquetPaths []string,
	attributeOrders []model.AttributeOrder,
	cold coldScanSet,
) []string {
	parts := []string{"parquet-only-scope-v1", tables.EAVData}
	return append(parts, parquetScanScopeParts(parquetPaths, attributeOrders, cold)...)
}

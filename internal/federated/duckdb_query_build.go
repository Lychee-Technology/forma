package federated

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/lychee-technology/forma/internal/telemetry"
)

// SQL construction for the DuckDB federated path: template-parameter
// assembly, the compiled-plan cache fast path, and parquet path resolution.
// Pure move from duckdb_query.go (#184 review round; split seam per #220).

// buildDuckDBQueryWithPlan builds the DuckDB query with execution plan recording.
func (e *DBFederatedQueryEngine) buildDuckDBQueryWithPlan(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	dirtyIDs []uuid.UUID,
	attributeOrders []model.AttributeOrder,
	limit, offset int,
	planCtx *duckDBExecutionPlanContext,
) (string, []any, int64, error) {
	sqlParams := e.buildDuckDBTemplateBaseParams(tables, q, attributeOrders, limit, offset)

	startTranslate := time.Now()

	// Build dual clauses (PG pushdown + DuckDB logical) if metadata cache available
	var cache forma.SchemaAttributeCache
	if e.metadataCache != nil {
		if c, ok := e.metadataCache.GetSchemaCacheByID(q.SchemaID); ok {
			cache = c
		}
	}
	paramIndex := 0
	dc, err := sqlgen.ToDualClauses(q.Condition, sqlutil.SanitizeIdentifier(tables.EAVData), q.SchemaID, cache, &paramIndex)
	if err != nil {
		return "", nil, 0, fmt.Errorf("to dual clauses: %w", err)
	}

	// Compute schema-driven projections for the template
	projectionCacheHit, err := e.injectSchemaProjections(sqlParams, q.SchemaID, cache)
	if err != nil {
		return "", nil, 0, err
	}
	planCtx.recordProjectionCache(projectionCacheHit)

	// Determine if the EAV pivot can be skipped entirely (all filter/sort
	// attributes are column-bound, no EAV-only attributes needed).
	// Skip this optimization for benchmark schemas because their output schema
	// includes ALL attributes (both column-bound and EAV-only) unlike production
	// which outputs a fixed entity_main layout.
	// The shortcut is additionally gated on the schema having no EAV-only
	// attributes: BuildPGSelectNoEAV drops EAV attribute columns from
	// pg_source while s3_source keeps the full projection, so for schemas
	// with EAV-only attributes the shortcut rendered a UNION ALL with
	// mismatched column counts (invalid SQL) and would silently drop hot
	// rows' EAV values (#173).
	if !isBenchmarkSchemaID(q.SchemaID) && !needsEAVJoin(q, cache) {
		sp, _, _ := e.schemaProjection(q.SchemaID, cache)
		if sp != nil && !sp.HasEAVAttrs {
			sqlParams["HasEAVPivot"] = false
			sqlParams["PGSourceSelect"] = sp.BuildPGSelectNoEAV()
			sqlParams["PGGroupBy"] = sp.BuildPGGroupByNoEAV()
		}
	}

	// Record Postgres pushdown fragment — only when the tier form renders
	// pg_source; a hot-excluded query has no hot data scan to push into (#184).
	if sqlgen.FederatedQueryHasHot(q) {
		planCtx.recordPushdownFragment(dc.PgMainClause)
	}

	// The DuckDB logical WHERE clause already references attribute names, which
	// match the attribute-aliased columns projected by both the benchmark and
	// production DuckDB CTEs (see resolveDuckDBColumn). No per-schema column-name
	// translation is needed (#167).

	// Compiled-plan cache (#142): skeleton + template args are reused per
	// (fingerprint, shape, scope); condition/keyset/dirty operands bind per
	// request. Test hooks and non-advanced templates bypass the cache.
	if sqlStr, args, ok := e.serveFromPlanCache(tables, q, dirtyIDs, attributeOrders, limit, offset, sqlParams, &dc, cache, planCtx); ok {
		translateMs := time.Since(startTranslate).Milliseconds()
		telemetry.EmitLatency(ctx, "translation", translateMs)
		return sqlStr, args, translateMs, nil
	}

	sqlStr, args, err := e.getDuckDBQueryBuilder()(e.getDuckDBTemplate(), sqlParams, q, dirtyIDs, &dc)
	translateMs := time.Since(startTranslate).Milliseconds()
	telemetry.EmitLatency(ctx, "translation", translateMs)
	if err != nil {
		return "", nil, 0, fmt.Errorf("build duckdb query: %w", err)
	}

	return sqlStr, args, translateMs, nil
}

// buildDuckDBTemplateBaseParams assembles the static template parameter map:
// scan locations, projections defaults, pagination, and the parquet path list
// from the query's render hints.
func (e *DBFederatedQueryEngine) buildDuckDBTemplateBaseParams(
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	attributeOrders []model.AttributeOrder,
	limit, offset int,
) map[string]any {
	changeLogSchema, changeLogScanTable := duckDBPostgresScanLocation(tables.ChangeLog)
	mainSchema, mainScanTable := duckDBPostgresScanLocation(tables.EntityMain)
	eavSchema, eavScanTable := duckDBPostgresScanLocation(tables.EAVData)
	sqlParams := map[string]any{
		"EAVTable":             sqlutil.SanitizeIdentifier(tables.EAVData),
		"MainTable":            sqlutil.SanitizeIdentifier(tables.EntityMain),
		"ChangeLogTable":       sqlutil.SanitizeIdentifier(tables.ChangeLog),
		"ChangeLogSchema":      changeLogSchema,
		"ChangeLogScanTable":   changeLogScanTable,
		"MainSchema":           mainSchema,
		"MainScanTable":        mainScanTable,
		"EAVSchema":            eavSchema,
		"EAVScanTable":         eavScanTable,
		"MainProjection":       model.EntityMainProjection,
		"SchemaID":             q.SchemaID,
		"UseMainTableAsAnchor": q.UseMainAsAnchor,
		"Anchor": map[string]any{
			"Condition": "1=1", // BuildDuckDBQuery will overwrite with actual where clause
		},
		"SortKeys": attributeOrders,
		"Limit":    limit,
		"Offset":   offset,
		"PageSize": limit,
	}

	if connStr := e.pgConnString; connStr != "" {
		sqlParams["DuckDBPGConnString"] = connStr
	}
	if paths := duckDBParquetPathsForQuery(q); len(paths) > 0 {
		sqlParams["DuckDBS3Paths"] = paths
	}
	return sqlParams
}

// duckCompiledEntry is the artifact stored in the shared plan cache: the
// rendered skeleton plus the dual-clause plan used to bind condition args.
type duckCompiledEntry struct {
	compiled *sqlgen.DuckDBCompiledQuery
	dualPlan *sqlgen.DualClausePlan
}

// serveFromPlanCache attempts the compiled-plan path. It returns ok=false
// when the request is not cacheable (no cache injected, test hook installed,
// non-advanced template, or shape hashing failed) — the caller then uses the
// direct builder. On a miss the compile result serves the request too, so
// rendering happens at most once per shape.
func (e *DBFederatedQueryEngine) serveFromPlanCache(
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	dirtyIDs []uuid.UUID,
	attributeOrders []model.AttributeOrder,
	limit, offset int,
	sqlParams map[string]any,
	dc *sqlgen.DualClauses,
	cache forma.SchemaAttributeCache,
	planCtx *duckDBExecutionPlanContext,
) (string, []any, bool) {
	if e.planCache == nil || e.buildDuckSQL != nil || e.getDuckDBTemplate() != sqlgen.AdvancedQueryTemplateDuckDB {
		return "", nil, false
	}

	shapeHash, err := queryplan.HashFederatedQueryShape(q)
	if err != nil {
		return "", nil, false
	}
	fingerprint := "no-fingerprint"
	if e.metadataCache != nil {
		if fp, ok := e.metadataCache.SchemaFingerprint(q.SchemaID); ok {
			fingerprint = fp
		}
	}
	scopeParts := []string{
		"scope-v1",
		tables.EAVData, tables.EntityMain, tables.ChangeLog,
		e.pgConnString,
		strconv.Itoa(limit), strconv.Itoa(offset),
		strconv.FormatBool(len(dirtyIDs) > 0),
	}
	scopeParts = append(scopeParts, duckDBParquetPathsForQuery(q)...)
	for _, o := range attributeOrders {
		scopeParts = append(scopeParts, strconv.Itoa(int(o.AttrID)), o.AttrName, o.ColumnName,
			string(o.StorageLocation), string(o.SortOrder))
	}
	key := queryplan.Key{
		Kind:          "duckdb_federated",
		SchemaVersion: fingerprint,
		SchemaID:      q.SchemaID,
		ShapeHash:     shapeHash,
		ScopeHash:     queryplan.HashScopeParts(scopeParts...),
	}

	entryAny, hit, err := e.planCache.GetOrBuild(key, func() (any, error) {
		compiled, err := sqlgen.CompileDuckDBQuery(sqlgen.AdvancedQueryTemplateDuckDB, sqlParams, q, dc, len(dirtyIDs) > 0)
		if err != nil || compiled == nil {
			return nil, err
		}
		dualPlan := &sqlgen.DualClausePlan{
			PgClause:     dc.PgClause,
			PgMainClause: dc.PgMainClause,
			DuckClause:   dc.DuckClause,
		}
		return &duckCompiledEntry{compiled: compiled, dualPlan: dualPlan}, nil
	})
	if err != nil || entryAny == nil {
		return "", nil, false
	}
	entry := entryAny.(*duckCompiledEntry)
	planCtx.recordPlanCache(hit)

	bound := *dc
	if hit {
		// The request's own dual clauses were already built this call; args
		// come from them, clauses from the cached plan (identical by key).
		bound.PgMainClause = entry.dualPlan.PgMainClause
		bound.DuckClause = entry.dualPlan.DuckClause
	}
	sqlStr, args := entry.compiled.Bind(q, bound, dirtyIDs)
	return sqlStr, args, true
}

func duckDBParquetPathsForQuery(q *model.FederatedAttributeQuery) []string {
	if q == nil || q.DuckDBHints == nil || q.DuckDBHints.S3ParquetPathTemplate == "" {
		return nil
	}
	rendered, err := sqlgen.RenderS3ParquetPath(q.DuckDBHints.S3ParquetPathTemplate, q.SchemaID)
	if err != nil {
		return nil
	}
	parts := strings.Split(rendered, ",")
	paths := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			paths = append(paths, trimmed)
		}
	}
	return paths
}

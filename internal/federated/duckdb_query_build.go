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
	parquetPaths []string,
	graceCutoffMs int64,
	coldMissing []sqlgen.NullScanColumn,
	planCtx *duckDBExecutionPlanContext,
) (string, []any, int64, error) {
	sqlParams := e.buildDuckDBTemplateBaseParams(tables, q, attributeOrders, limit, offset, parquetPaths, graceCutoffMs, coldMissing)

	startTranslate := time.Now()

	// Build dual clauses (PG pushdown + DuckDB logical) if metadata cache available
	cache, _ := e.schemaCacheByID(q.SchemaID)
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
	if sqlStr, args, ok := e.serveFromPlanCache(tables, q, dirtyIDs, attributeOrders, limit, offset, parquetPaths, graceCutoffMs, coldMissing, sqlParams, &dc, cache, planCtx); ok {
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
// scan locations, projections defaults, pagination, and the resolved parquet
// path list (render hints or manifest source, resolved once by the caller).
func (e *DBFederatedQueryEngine) buildDuckDBTemplateBaseParams(
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	attributeOrders []model.AttributeOrder,
	limit, offset int,
	parquetPaths []string,
	graceCutoffMs int64,
	missing []sqlgen.NullScanColumn,
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
	if len(parquetPaths) > 0 {
		sqlParams["DuckDBS3Paths"] = parquetPaths
	}
	if len(missing) > 0 {
		// Cold-absent columns for the #255 scan-source augmentation; the
		// renderer folds them into S3_SCAN_SOURCE.
		sqlParams["ColdMissingColumns"] = missing
	}
	// Per-request dirty-barrier cutoff (#252), anchored at the query's
	// path-resolution instant. The direct builder renders it as a literal;
	// the compiled path overwrites it with the splice sentinel so the cached
	// skeleton stays cutoff-independent.
	sqlParams["FlushGraceCutoffMs"] = graceCutoffMs
	return sqlParams
}

// duckPlanScopeParts assembles the plan-cache scope components: everything
// outside the shape hash that renders into the skeleton. The cold-missing
// set participates (#255): the rendered scan source depends on it, and a
// glob-hint path string does not change when the first flush lands a
// column — without this component a skeleton compiled cold-absent would
// keep projecting NULL and make the newly flushed data invisible until
// cache invalidation (the issue's poisoning hazard).
func duckPlanScopeParts(
	tables model.StorageTables,
	pgConn string,
	limit, offset int,
	hasDirty bool,
	parquetPaths []string,
	attributeOrders []model.AttributeOrder,
	missing []sqlgen.NullScanColumn,
) []string {
	parts := []string{
		"scope-v2",
		tables.EAVData, tables.EntityMain, tables.ChangeLog,
		pgConn,
		strconv.Itoa(limit), strconv.Itoa(offset),
		strconv.FormatBool(hasDirty),
	}
	// The resolved path set (hint or manifest) renders into the skeleton as a
	// SQL literal, so it must scope the cache key: a changed file set (new
	// delta flushed, base replaced) recompiles instead of reusing stale paths.
	parts = append(parts, parquetPaths...)
	for _, o := range attributeOrders {
		parts = append(parts, strconv.Itoa(int(o.AttrID)), o.AttrName, o.ColumnName,
			string(o.StorageLocation), string(o.SortOrder))
	}
	parts = append(parts, "cold-missing")
	for _, mc := range missing {
		parts = append(parts, mc.Name, mc.DuckDBType)
	}
	return parts
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
	parquetPaths []string,
	graceCutoffMs int64,
	missing []sqlgen.NullScanColumn,
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
	scopeParts := duckPlanScopeParts(tables, e.pgConnString, limit, offset, len(dirtyIDs) > 0, parquetPaths, attributeOrders, missing)
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
	sqlStr, args := entry.compiled.Bind(q, bound, dirtyIDs, graceCutoffMs)
	return sqlStr, args, true
}

func duckDBParquetPathsForQuery(q *model.FederatedAttributeQuery, cfg forma.DuckDBConfig) ([]string, error) {
	if q == nil || q.DuckDBHints == nil || q.DuckDBHints.S3ParquetPathTemplate == "" {
		return nil, nil
	}
	tmpl := q.DuckDBHints.S3ParquetPathTemplate
	// #456 layer 1: the template is a caller-controlled scan target, so it is
	// honored only where the operator has explicitly opted in. Reject rather
	// than ignore — a silent ignore would answer from a different path set than
	// the caller named, the same provenance switch the invalid-template rule
	// below exists to prevent.
	if !cfg.AllowCallerParquetPaths {
		return nil, forma.InvalidInputf(
			"caller-supplied s3_parquet_path_template %q is not permitted on this deployment", tmpl)
	}
	rendered, err := sqlgen.RenderS3ParquetPath(tmpl, q.SchemaID)
	if err != nil {
		// A caller-supplied template that cannot render is invalid input, not
		// an absent hint: swallowing it would silently serve the query from a
		// different path set (the manifest source, or none) than the caller
		// explicitly requested (#249 review). The template is the caller's own
		// payload field, so the published message may echo it; text/template's
		// render prose is operator detail and stays log-only (#313).
		return nil, forma.WithOperatorDetail(
			forma.InvalidInputf("render s3 parquet path template %q: the template is not renderable", tmpl), err)
	}
	parts := strings.Split(rendered, ",")
	paths := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			paths = append(paths, trimmed)
		}
	}
	if len(paths) == 0 {
		// The hint was present (we are past the early return), so an empty
		// result is a degenerate template — "," or whitespace-only segments —
		// not an absent hint. Returning zero paths here would let
		// resolveParquetPaths read it as "no hint" and consult the manifest
		// source instead, silently answering from a different path set than
		// the caller explicitly requested. Same rule as an unrenderable
		// template: explicit hints always win, including when they fail
		// (#250 PR review).
		return nil, forma.InvalidInputf("s3 parquet path template %q rendered no usable paths", tmpl)
	}
	// #456 layer 2: every rendered path must sit inside the configured bucket.
	if err := validateHintPathScope(paths, cfg.S3Bucket); err != nil {
		return nil, err
	}
	return paths, nil
}

// validateHintPathScope fails a caller-supplied parquet path set unless every
// rendered path sits inside the configured bucket and carries no character that
// could break out of the SQL string literal it is rendered into (#456). The
// trailing slash on the bucket prefix is load-bearing: it blocks the
// s3://<bucket>X/... prefix-collision bypass and a bare s3://<bucket> with no
// key. The bucket name is operator detail and stays out of the published
// message (#313); the caller's own path may be echoed.
func validateHintPathScope(paths []string, bucket string) error {
	if bucket == "" {
		return forma.WithOperatorDetail(
			forma.InvalidInputf("caller-supplied s3_parquet_path_template is not permitted on this deployment"),
			fmt.Errorf("duckdb.s3Bucket must be set to scope caller-supplied parquet paths"))
	}
	prefix := "s3://" + bucket + "/"
	for _, path := range paths {
		if strings.ContainsAny(path, "'\";\n\r") {
			return forma.InvalidInputf("s3 parquet path %q is not permitted: it contains a disallowed character", path)
		}
		if !strings.HasPrefix(path, prefix) {
			return forma.WithOperatorDetail(
				forma.InvalidInputf("s3 parquet path %q is outside the permitted parquet scope", path),
				fmt.Errorf("path must be under s3://%s/", bucket))
		}
	}
	return nil
}

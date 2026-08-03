package federated

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/telemetry"
)

// ErrSchemaMetadataCacheRequired marks a federated query that cannot build a
// correct entity_main projection because the schema's metadata cache is not
// loaded. It is a configuration / data-contract error, not a transient
// infrastructure failure, so the public Query path must not absorb it under
// AllowPartialDegradedMode — degrading would silently return a Postgres-only
// partial result and hide the missing-cache problem #151 makes loud.
var ErrSchemaMetadataCacheRequired = errors.New("schema metadata cache required but not loaded")

func isBenchmarkSchemaID(schemaID int16) bool { return sqlgen.IsBenchmarkSchemaID(schemaID) }

type duckDBRowsIterator interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// ExecuteDuckDBFederatedQuery runs the DuckDB optimized query template using the provided
// model.FederatedAttributeQuery. It fetches dirty IDs from the Postgres change_log (if available),
// injects exclusions into the DuckDB WHERE clause, executes the query against the global
// DuckDB client, and returns matched PersistentRecords along with the total record count.
//
// Note: This implementation performs a best-effort scan of columns produced by the
// optimized query template. It mirrors the column ordering used by the Postgres template:
//   - main table projection (entity_main columns, order defined by model.EntityMainColumnDescriptors)
//   - attributes_json (TEXT)
//   - total_records (bigint)
//   - total_pages (bigint)
//   - current_page (int)
//
// A read failure attributed to specific corrupt parquet objects is retried
// exactly once against the readable remainder (#251), so one call can issue two
// scans plus a per-object verification drain — worth knowing when sizing the
// caller's deadline. The execution plan describes only the pass that produced
// the returned page.
func (e *DBFederatedQueryEngine) ExecuteDuckDBFederatedQuery(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	limit, offset int,
	attributeOrders []model.AttributeOrder,
	opts *model.FederatedQueryOptions,
) ([]*model.PersistentRecord, int64, error) {
	mark := markExecutionPlan(opts)
	recs, total, err := e.collectDuckDBFederatedQuery(ctx, tables, q, limit, offset, attributeOrders, opts)
	var retry *corruptParquetRetryError
	if errors.As(err, &retry) {
		// The failed pass confirmed and cached the corrupt objects; path
		// resolution now excludes them, so one retry answers from the
		// readable remainder (#251). A second retryable failure surfaces:
		// corruption appearing mid-flight is indistinguishable from a sick
		// store and must not loop.
		mark.rewind(opts)
		recs, total, err = e.collectDuckDBFederatedQuery(ctx, tables, q, limit, offset, attributeOrders, opts)
		if err != nil {
			return nil, 0, fmt.Errorf("retry after excluding corrupt parquet %v: %w", retry.Corrupt, err)
		}
	}
	if err != nil {
		return nil, 0, err
	}
	return recs, total, nil
}

// executionPlanMark remembers how much of the caller's execution plan predates
// the first DuckDB pass, so a retry can drop what the failed pass recorded.
type executionPlanMark struct {
	sources int
	notes   int
}

// markExecutionPlan snapshots the plan's length before the first pass.
func markExecutionPlan(opts *model.FederatedQueryOptions) executionPlanMark {
	if opts == nil || opts.ExecutionPlan == nil {
		return executionPlanMark{}
	}
	return executionPlanMark{
		sources: len(opts.ExecutionPlan.Sources),
		notes:   len(opts.ExecutionPlan.Notes),
	}
}

// rewind drops everything the failed pass appended to the caller's execution
// plan, so the plan attached to the returned page describes the pass that
// actually produced it. Without it the retry reports both passes: two
// identically-labelled "duckdb template rendered" scans — the failed one
// carrying ActualRows=0, indistinguishable from a scan that legitimately
// matched nothing — and a double-counted hot-tier RowEstimate. The Notes that
// would explain the duplication never reach an API caller (toExecutionPlan
// projects Routing, Timings, Sources and Merge, but drops Notes), so the plan
// must be truthful by construction rather than by annotation. Rewind truncates
// Sources and Notes only: Timings is a merged map, not an append log, and is
// deliberately left as-is — it does cross the HTTP boundary, so a retried
// request can report both plan_cache_hit and plan_cache_miss (follow-up #<F2 issue>).
// Everything recorded BEFORE the first pass survives — the routing decision and
// its note are the caller's, not the failed pass's. The retry re-records the
// corrupt-exclusion note itself, via path resolution.
func (m executionPlanMark) rewind(opts *model.FederatedQueryOptions) {
	if opts == nil || opts.ExecutionPlan == nil {
		return
	}
	plan := opts.ExecutionPlan
	if len(plan.Sources) > m.sources {
		plan.Sources = plan.Sources[:m.sources]
	}
	if len(plan.Notes) > m.notes {
		plan.Notes = plan.Notes[:m.notes]
	}
}

// collectDuckDBFederatedQuery is one buffered pass of the streaming path; the
// fresh slice per pass is what makes the #251 retry safe after a mid-stream
// failure already delivered partial rows.
func (e *DBFederatedQueryEngine) collectDuckDBFederatedQuery(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	limit, offset int,
	attributeOrders []model.AttributeOrder,
	opts *model.FederatedQueryOptions,
) ([]*model.PersistentRecord, int64, error) {
	var recs []*model.PersistentRecord
	total, err := e.StreamDuckDBFederatedQuery(ctx, tables, q, limit, offset, attributeOrders, opts, func(ctx context.Context, rp *model.PersistentRecord) error {
		recs = append(recs, rp)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return recs, total, nil
}

// StreamDuckDBFederatedQuery streams DuckDB federated query results using a rowHandler callback.
// It reuses the same rowHandler semantics as Postgres' StreamOptimizedQuery to avoid loading the
// entire result set into memory.
func (e *DBFederatedQueryEngine) StreamDuckDBFederatedQuery(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	limit, offset int,
	attributeOrders []model.AttributeOrder,
	opts *model.FederatedQueryOptions,
	rowHandler func(context.Context, *model.PersistentRecord) error,
) (int64, error) {
	if q == nil {
		return 0, fmt.Errorf("query cannot be nil")
	}

	// Initialize execution plan tracking
	planCtx := newDuckDBExecutionPlanContext(opts)

	if e == nil || e.duck == nil {
		planCtx.recordClientUnavailable()
		return 0, fmt.Errorf("duckdb client not available: %w", ErrDuckDBUnavailable)
	}

	// Reject-before-DuckDB (#185): a non-admitted caller must short-circuit
	// before ANY DuckDB or S3 work — including the #189 pre-read schema
	// probes and path resolution, which reach storage. Allow also reserves
	// the half-open single probe (#246); early-error returns between here
	// and duck.Query abandon the probe, whose reservation lapses after
	// openDuration (see the CircuitBreaker type doc).
	if !e.breaker.Allow() {
		planCtx.recordClientUnavailable()
		return 0, fmt.Errorf("duckdb circuit breaker open, query rejected: %w", ErrDuckDBUnavailable)
	}

	// Everything between admission and duck.Query can fail without consulting
	// DuckDB or S3 at all — a misconfigured path set, invalid caller input,
	// missing schema metadata. Such a caller learned nothing about the
	// dependency's health, so it must hand the half-open probe slot back
	// instead of abandoning it: an abandoned reservation rejects the next
	// caller with the DEGRADABLE ErrDuckDBUnavailable, which under
	// AllowPartialDegradedMode answers a misconfiguration from Postgres alone
	// — silently, one request after the same misconfiguration was correctly
	// loud (#299 review P1). Once duck.Query returns, every path below resolves
	// the probe through RecordFailure or RecordSuccess.
	probeResolved := false
	defer func() {
		if !probeResolved {
			e.breaker.ReleaseProbe()
		}
	}()

	src, err := e.resolveScanSources(ctx, q)
	if err != nil {
		return 0, err
	}
	// A partial scan must be loud (#251): the plan names every excluded
	// object. Notes stay internal — toExecutionPlan drops them at the HTTP
	// boundary (#301/#306).
	planCtx.recordCorruptExclusion(src.excludedCorrupt)

	// Fetch dirty IDs and record in execution plan
	dirtyIDs, err := e.fetchAndRecordDirtyIDs(ctx, tables, q, planCtx)
	if err != nil {
		return 0, fmt.Errorf("fetch dirty row ids: %w", err)
	}

	// Build and execute the query
	sqlStr, args, translateMs, err := e.buildDuckDBQueryWithPlan(ctx, tables, q, dirtyIDs, attributeOrders, limit, offset, src.paths, src.graceCutoffMs, src.coldMissing, planCtx)
	if err != nil {
		return 0, fmt.Errorf("build duckdb federated query: %w", err)
	}

	// Record translation in plan
	planCtx.recordTranslation(sqlStr, args, translateMs, q)

	// From here the dependency is consulted, so the breaker gets real evidence
	// on every path and the helper below owns resolving the probe.
	probeResolved = true
	return e.executeAndStreamDuckDB(ctx, q, sqlStr, args, scan{
		parquetPaths:    src.paths,
		pathsFromSource: src.fromSource,
		dirtyIDs:        dirtyIDs,
	}, rowHandler, planCtx)
}

// fetchAndRecordDirtyIDs fetches dirty row IDs from Postgres and records in execution plan.
func (e *DBFederatedQueryEngine) fetchAndRecordDirtyIDs(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	planCtx *duckDBExecutionPlanContext,
) ([]uuid.UUID, error) {
	if tables.ChangeLog == "" {
		return nil, nil
	}
	if e == nil || e.dirtyIDFetcher == nil {
		return nil, fmt.Errorf("dirty id fetcher not available")
	}

	dirtyIDs, err := e.dirtyIDFetcher.FetchDirtyRowIDs(ctx, tables.ChangeLog, q.SchemaID)
	if err != nil {
		return nil, fmt.Errorf("fetch dirty ids: %w: %w", ErrPostgresReadFailed, err)
	}

	// Emit metric for dirty set size
	telemetry.EmitRowCount(ctx, "pg", int64(len(dirtyIDs)))

	// Record in execution plan
	planCtx.recordDirtyIDSource(tables.ChangeLog, q.SchemaID, len(dirtyIDs), sqlgen.FederatedQueryHasHot(q))

	return dirtyIDs, nil
}

// injectSchemaProjections computes schema-driven SQL fragments from the attribute
// cache and injects them into the template parameter map. For benchmark schemas
// (IDs 100-102) it uses the benchmark parquet shape; for production it uses the
// schema attribute cache for column bindings and EAV pivots.
// schemaProjection returns the (cached) projection for schemaID; the hit flag
// feeds the execution plan so cache behavior stays observable.
func (e *DBFederatedQueryEngine) schemaProjection(schemaID int16, cache forma.SchemaAttributeCache) (*sqlgen.SchemaProjection, bool, error) {
	var pc *sqlgen.ProjectionCache
	if e != nil {
		pc = e.projections
	}
	return pc.GetOrBuild(schemaID, func() (*sqlgen.SchemaProjection, error) {
		return sqlgen.BuildSchemaProjection(schemaID, cache)
	})
}

func (e *DBFederatedQueryEngine) injectSchemaProjections(sqlParams map[string]any, schemaID int16, cache forma.SchemaAttributeCache) (projectionCacheHit bool, err error) {
	if isBenchmarkSchemaID(schemaID) {
		// Benchmark schemas: use hardcoded benchmarks projections that match
		// the benchmark parquet shape exactly (flat columns for column-bound
		// attributes, JSON extraction for EAV-only attributes).
		proj := sqlgen.BuildBenchmarkProjections(schemaID)
		sqlParams["S3SourceSelect"] = proj.S3SourceSelect
		sqlParams["PGSourceSelect"] = proj.PGSourceSelect
		sqlParams["PGGroupBy"] = proj.PGGroupBy
		sqlParams["EAVPivotSelect"] = proj.EAVPivotSelect
		sqlParams["EAVPivotAttrs"] = proj.EAVPivotAttrs
		sqlParams["HasEAVPivot"] = len(proj.EAVPivotAttrs) > 0
		sqlParams["OuterSelect"] = sqlgen.BuildBenchmarkOuterSelect(schemaID)
		return false, nil
	}
	if len(cache) == 0 {
		// No schema metadata cache: fail fast. The final SELECT must align
		// positionally with model.EntityMainColumnDescriptors (the #147
		// positional-scan contract), so a correct projection can only be
		// derived from the schema's attribute cache. The retired fallback here
		// emitted a fixed toy-schema projection (name/age/tag, columns absent
		// from the descriptors) whose column set could not be scanned by
		// duckDBScanBuffers — a hard error at best, misaligned rows at worst.
		return false, fmt.Errorf("federated query for schema_id %d requires a schema metadata cache, but none is loaded: %w", schemaID, ErrSchemaMetadataCacheRequired)
	}

	// Production schema: compute projections from the attribute cache
	sp, hit, projErr := e.schemaProjection(schemaID, cache)
	return hit, applySchemaProjection(sqlParams, schemaID, sp, projErr)
}

// applySchemaProjection writes the seven schema-projection params into sqlParams
// from a computed projection, or refuses. Both a non-nil projErr and a nil
// projection are hard failures: with the toy-schema defaults retired (#222)
// there is nothing to fall through to, so an advanced-template render must fail
// fast rather than emit unset projection params. Extracted so the nil-guard is
// unit-testable (the real schemaProjection seam never yields sp==nil without an
// error).
func applySchemaProjection(sqlParams map[string]any, schemaID int16, sp *sqlgen.SchemaProjection, projErr error) error {
	if projErr != nil {
		// A non-nil projection error is a hard data-contract failure (e.g. the
		// alias-collision guard in BuildSchemaProjection). Propagate it.
		return fmt.Errorf("build schema projection for schema %d: %w", schemaID, projErr)
	}
	if sp == nil {
		return fmt.Errorf("schema projection for schema %d is nil with no error; refusing to render with an undefined projection", schemaID)
	}
	sqlParams["S3SourceSelect"] = sp.S3SourceSelect
	sqlParams["PGSourceSelect"] = sp.PGSourceSelect
	sqlParams["PGGroupBy"] = sp.PGGroupBy
	sqlParams["EAVPivotSelect"] = sp.EAVPivotSelect
	sqlParams["EAVPivotAttrs"] = sp.EAVPivotAttrs
	sqlParams["HasEAVPivot"] = len(sp.EAVPivotAttrs) > 0
	sqlParams["OuterSelect"] = sp.OuterSelect
	return nil
}

func duckDBPostgresScanLocation(name string) (string, string) {
	parts := strings.Split(name, ".")
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.Trim(part, " \"")
		if trimmed != "" {
			clean = append(clean, trimmed)
		}
	}
	if len(clean) >= 2 {
		return clean[0], clean[1]
	}
	if len(clean) == 1 {
		return "public", clean[0]
	}
	return "public", ""
}

// finalizeDuckDBExecutionPlan completes the execution plan with timing and metrics.
func (e *DBFederatedQueryEngine) finalizeDuckDBExecutionPlan(
	ctx context.Context,
	planCtx *duckDBExecutionPlanContext,
	dirtyIDs []uuid.UUID,
	totalRecords int64,
	rowCount int64,
) {
	if planCtx.opts == nil || !planCtx.opts.IncludeExecutionPlan || planCtx.opts.ExecutionPlan == nil {
		return
	}

	qMs := time.Since(planCtx.startQuery).Milliseconds()

	// Update the last source with actual rows and duration
	if len(planCtx.opts.ExecutionPlan.Sources) > 0 {
		idx := len(planCtx.opts.ExecutionPlan.Sources) - 1
		dp := planCtx.opts.ExecutionPlan.Sources[idx]
		dp.ActualRows = rowCount
		dp.DurationMs = qMs
		planCtx.opts.ExecutionPlan.Sources[idx] = dp
	}

	planCtx.opts.ExecutionPlan.Timings["duckdb_fetch"] = qMs
	planCtx.opts.ExecutionPlan.Timings["total"] = time.Since(planCtx.startTotal).Milliseconds()

	// Emit telemetry
	telemetry.EmitLatency(ctx, "execution", qMs)
	streamMs := max(time.Since(planCtx.startQuery).Milliseconds()-qMs, 0)
	telemetry.EmitLatency(ctx, "streaming", streamMs)
	telemetry.EmitRowCount(ctx, "duckdb", rowCount)

	// Compute pushdown efficiency
	pgRows := computePgRowCount(planCtx.opts.ExecutionPlan, dirtyIDs)
	finalRows := totalRecords
	if finalRows <= 0 {
		finalRows = rowCount
	}
	if finalRows <= 0 {
		finalRows = 1
	}
	ratio := float64(pgRows) / float64(finalRows)
	telemetry.EmitPushdownEfficiency(ctx, 0, ratio) // schemaID not available here, use 0

	planCtx.opts.ExecutionPlan.Notes = append(planCtx.opts.ExecutionPlan.Notes,
		fmt.Sprintf("pushdown_efficiency=%.3f (pg_rows=%d final_rows=%d)", ratio, pgRows, finalRows))
}

// needsEAVJoin checks whether the federated query requires an EAV data JOIN.
// It returns false when all filter conditions and sort keys reference only
// column-bound attributes, meaning the eav_data scan can be safely skipped.
func needsEAVJoin(q *model.FederatedAttributeQuery, cache forma.SchemaAttributeCache) bool {
	if q == nil || len(cache) == 0 {
		return true
	}
	if needsEAVForCondition(q.Condition, cache) {
		return true
	}
	for _, order := range q.AttributeOrders {
		if order.StorageLocation != forma.AttributeStorageLocationMain {
			return true
		}
	}
	return false
}

// needsEAVForCondition recursively checks if any condition references an
// EAV-only (non-column-bound) attribute.
func needsEAVForCondition(cond forma.Condition, cache forma.SchemaAttributeCache) bool {
	if cond == nil {
		return false
	}
	switch c := cond.(type) {
	case *forma.CompositeCondition:
		for _, child := range c.Conditions {
			if needsEAVForCondition(child, cache) {
				return true
			}
		}
	case *forma.KvCondition:
		if meta, ok := cache[c.Attr]; ok {
			if meta.ColumnBinding == nil {
				return true
			}
		}
	}
	return false
}

func computePgRowCount(plan *model.ExecutionPlan, dirtyIDs []uuid.UUID) int64 {
	var pgRows int64
	for _, src := range plan.Sources {
		if src.Engine == "postgres" {
			if src.ActualRows > 0 {
				pgRows += src.ActualRows
			} else if src.RowEstimate > 0 {
				pgRows += src.RowEstimate
			}
		}
	}
	return pgRows
}

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
func (e *DBFederatedQueryEngine) ExecuteDuckDBFederatedQuery(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	limit, offset int,
	attributeOrders []model.AttributeOrder,
	opts *model.FederatedQueryOptions,
) ([]*model.PersistentRecord, int64, error) {
	// Backwards-compatible wrapper that uses the streaming iterator internally
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

	// Resolve the parquet path set once (#187): explicit render hints win,
	// otherwise the manifest-driven source authors the list. Provenance
	// gates the read-error classification below. The flush-grace cutoff is
	// stamped BEFORE resolution (#252): any row marked flushed after this
	// instant may belong to a delta this path set does not list yet, so the
	// dirty barrier keeps it hot-readable for this query.
	graceCutoffMs := e.flushGraceCutoffMs(time.Now().UnixMilli())
	parquetPaths, pathsFromSource, err := e.resolveParquetPaths(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("resolve parquet paths: %w", err)
	}

	// Pre-read schema-invariant validation (#189): the scan's union_by_name
	// tolerates attribute-column drift across parquet generations, which
	// would let a wrong-schema object's rows silently vanish (NULL row_id
	// drops out of the dirty anti-join) instead of failing loudly (#187).
	// Schema violations fail here, classified and degradable; unreadable
	// footers are inconclusive and stay with the execution-path classifier.
	// No recordQueryFailure: the query never started, and its timing fields
	// would read from an unset start time. Benchmark schemas (100-102) are
	// exempt: their parquet is the legacy CSV-sniffed harness shape (row_id
	// VARCHAR, cast by the hardcoded benchmark projections) — the
	// parquetcheck invariant codifies the PRODUCTION exporters, which never
	// write those IDs (ValidateFixtureSchemaID reserves the range).
	if !isBenchmarkSchemaID(q.SchemaID) {
		if err := e.schemaValidator.Validate(ctx, e.duck, parquetPaths); err != nil {
			return 0, fmt.Errorf("pre-read parquet schema validation: %w", err)
		}
	}

	// Fetch dirty IDs and record in execution plan
	dirtyIDs, err := e.fetchAndRecordDirtyIDs(ctx, tables, q, planCtx)
	if err != nil {
		return 0, err
	}

	// Build and execute the query
	sqlStr, args, translateMs, err := e.buildDuckDBQueryWithPlan(ctx, tables, q, dirtyIDs, attributeOrders, limit, offset, parquetPaths, graceCutoffMs, planCtx)
	if err != nil {
		return 0, err
	}

	// Record translation in plan
	planCtx.recordTranslation(sqlStr, args, translateMs, q)

	// Execute query
	planCtx.recordQueryStart()
	rows, err := e.duck.Query(ctx, sqlStr, args...)
	if err != nil {
		planCtx.recordQueryFailure(err)
		// Record failure in circuit breaker
		if e.breaker != nil {
			e.breaker.RecordFailure()
		}
		return 0, fmt.Errorf("execute duckdb query: %w: %w", e.classifyDuckDBReadError(ctx, q, parquetPaths, pathsFromSource), err)
	}
	defer rows.Close()

	// Stream and process rows
	totalRecords, rowCount, err := e.streamDuckDBRows(ctx, rows, rowHandler)
	if err != nil {
		// Record failure in circuit breaker
		if e.breaker != nil {
			e.breaker.RecordFailure()
		}
		// A mid-stream read failure classifies like an execute failure:
		// DuckDB opens listed objects lazily, so a missing object can
		// surface here instead of at Query. Handler errors are not read
		// failures and pass through unclassified.
		if errors.Is(err, ErrFederatedReadFailed) {
			if classified := e.classifyDuckDBReadError(ctx, q, parquetPaths, pathsFromSource); classified != ErrFederatedReadFailed {
				return 0, fmt.Errorf("%w: %w", classified, err)
			}
		}
		return 0, err
	}

	// Record success in circuit breaker
	if e.breaker != nil {
		e.breaker.RecordSuccess()
	}

	// Finalize execution plan
	e.finalizeDuckDBExecutionPlan(ctx, planCtx, dirtyIDs, totalRecords, rowCount)

	return totalRecords, nil
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

// streamDuckDBRows iterates through DuckDB rows and invokes the handler.
func (e *DBFederatedQueryEngine) streamDuckDBRows(
	ctx context.Context,
	rows duckDBRowsIterator,
	rowHandler func(context.Context, *model.PersistentRecord) error,
) (int64, int64, error) {
	buffers := newDuckDBScanBuffers()

	var totalRecords int64
	totalSet := false
	rowCount := int64(0)

	for rows.Next() {
		scanArgs, attrsJSON, totalRec, _, _ := buffers.buildScanArgs()

		if err := rows.Scan(scanArgs...); err != nil {
			return 0, 0, fmt.Errorf("scan duckdb row: %w: %w", ErrFederatedReadFailed, err)
		}

		// Build record from buffers
		record := buffers.buildRecordFromBuffers()

		// Parse attributes JSON
		if attrsJSON.Valid {
			if err := parseDuckDBAttributesJSON(attrsJSON.String, record); err != nil {
				return 0, 0, err
			}
		}

		// Clean up empty maps
		model.CleanupEmptyMaps(record)

		if !totalSet && totalRec.Valid {
			totalRecords = totalRec.Int64
			totalSet = true
		}

		// Invoke handler
		if rowHandler != nil {
			if err := rowHandler(ctx, record); err != nil {
				return 0, 0, err
			}
		}

		rowCount++
	}

	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("iterate duckdb rows: %w: %w", ErrFederatedReadFailed, err)
	}

	return totalRecords, rowCount, nil
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

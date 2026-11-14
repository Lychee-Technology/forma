package federated

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
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

// ExecuteDuckDBFederatedQuery answers q's page and its exact total through
// one federated pass (design.md §6.1). The pass requires the engine's
// snapshot pool (WithPgxHotLeg) and a configured change_log table. It opens a
// REPEATABLE READ snapshot and reads the dirty set and, unless the request
// excludes the hot tier, the hot keys from it; stages the dirty set in
// DuckDB, whose parquet-only statement drops every row_id of it from the
// frozen parquet objects; merges the hot keys and the parquet window in Go
// and cuts the page; and reads the page's hot rows in the same snapshot.
// DuckDB reads no PostgreSQL.
//
// An empty parquet scan a publication raced (#720) is retried exactly once,
// so one call can issue two scans. A read failure attributed to corrupt
// parquet objects is not retried: it fails with CorruptParquetError (#722),
// after a per-object verification drain of the failed set — worth knowing
// when sizing the caller's deadline. The execution plan and the fed_query_*
// metrics describe only the pass that produced the returned page: the failed
// pass is rewound from the plan and emits no metric sample at all.
func (e *DBFederatedQueryEngine) ExecuteDuckDBFederatedQuery(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	limit, offset int,
	attributeOrders []model.AttributeOrder,
	opts *model.FederatedQueryOptions,
) ([]*model.PersistentRecord, int64, error) {
	// Validate the keyset cursor at this exported seam: this method reaches the
	// renderer directly, so validation cannot be inherited. Each seam validates
	// independently by design; the contract is pure and idempotent (#381).
	if q != nil {
		normalized, err := e.keysetQuery(q, attributeOrders)
		if err != nil {
			return nil, 0, fmt.Errorf("validate keyset cursor: %w", err)
		}
		q = normalized
	}
	mark := markExecutionPlan(opts)
	recs, total, err := e.collectDuckDBFederatedQuery(ctx, tables, q, limit, offset, attributeOrders, opts)
	if reason, ok := retryablePassFailure(err); ok {
		// One retry answers from the state the failed pass uncovered. A
		// second retryable failure surfaces: a publication racing the same
		// request twice must not loop.
		mark.rewind(opts)
		recs, total, err = e.collectDuckDBFederatedQuery(ctx, tables, q, limit, offset, attributeOrders, opts)
		if err != nil {
			return nil, 0, fmt.Errorf("retry after %s: %w", reason, err)
		}
	}
	if err != nil {
		return nil, 0, fmt.Errorf("federated read: %w", err)
	}
	return recs, total, nil
}

// executionPlanMark remembers how much of the caller's execution plan predates
// the first DuckDB pass, so a retry (#720) or the degraded fallback (#639) can
// drop what a failed pass recorded.
type executionPlanMark struct {
	sources int
	notes   int
	// timings snapshots the pre-pass Timings entries wholesale: Timings is a
	// merged map, not an append log, so truncation cannot express "drop what
	// the failed pass wrote" — and unlike Notes, Timings IS projected through
	// toExecutionPlan, so a stale plan_cache_hit next to the retry's
	// plan_cache_miss was publicly visible (#348 item 1).
	timings map[string]int64
}

// markExecutionPlan snapshots the plan's length and timings before the first pass.
func markExecutionPlan(opts *model.FederatedQueryOptions) executionPlanMark {
	if opts == nil || opts.ExecutionPlan == nil {
		return executionPlanMark{}
	}
	timings := make(map[string]int64, len(opts.ExecutionPlan.Timings))
	for k, v := range opts.ExecutionPlan.Timings {
		timings[k] = v
	}
	return executionPlanMark{
		sources: len(opts.ExecutionPlan.Sources),
		notes:   len(opts.ExecutionPlan.Notes),
		timings: timings,
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
// Sources and Notes and restores the pre-pass Timings snapshot: Timings is a
// merged map, not an append log, and it DOES cross the HTTP boundary —
// leaving the failed pass's entries in place let one retried request publicly
// report both plan_cache_hit and plan_cache_miss (#348 item 1).
// Everything recorded BEFORE the first pass survives — the routing decision and
// its note are the caller's, not the failed pass's. queryDuckDBRouted holds
// an outer mark for the degraded fallback, which rewinds every DuckDB pass of
// the request (a failed retry and a discarded page pass included) before it
// records the Postgres-only answer (#639).
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
	restored := make(map[string]int64, len(m.timings))
	for k, v := range m.timings {
		restored[k] = v
	}
	plan.Timings = restored
}

// collectDuckDBFederatedQuery is one buffered pass of the streaming path; the
// fresh slice per pass is what makes the #720 retry safe, and what keeps a
// pass that fails mid-stream from delivering the rows it already read.
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
		return nil, 0, fmt.Errorf("collect federated rows: %w", err)
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

	// Validate the keyset cursor: the fourth seam (#381). Each entry point to
	// the renderer validates independently by design, not through inheritance.
	q, err := e.keysetQuery(q, attributeOrders)
	if err != nil {
		return 0, fmt.Errorf("validate keyset cursor: %w", err)
	}
	if err := refuseParquetUnrepresentable(q); err != nil {
		return 0, err
	}

	// Initialize execution plan tracking
	planCtx := newDuckDBExecutionPlanContext(opts, e.clock())

	if e == nil || e.duck == nil {
		planCtx.recordClientUnavailable()
		return 0, fmt.Errorf("duckdb client not available: %w", ErrDuckDBUnavailable)
	}

	// Reject-before-DuckDB (#185): a non-admitted caller must short-circuit
	// before ANY DuckDB or S3 work — including the #189 pre-read schema
	// probes and path resolution, which reach storage. Allow also reserves
	// the half-open single probe (#246); early-error returns between here
	// and duck.Query hand it back through the deferred ReleaseProbe below.
	// breakerAllow reports the transition when this caller is the probe
	// (#634).
	admitted, probe := e.breakerAllow(ctx)
	if !admitted {
		planCtx.recordClientUnavailable()
		return 0, fmt.Errorf("%w: %w", errBreakerRejected, ErrDuckDBUnavailable)
	}

	// DuckDBConfig.QueryTimeout bounds one DuckDB pass end to end (#465):
	// path resolution and schema probes, the snapshot reads, the scan, and
	// the row streaming below all run under this deadline. duckdb-go v2
	// interrupts a running query when its context expires, so a slow scan is
	// genuinely cancelled rather than left running on the single connection.
	// A zero timeout leaves the caller's context as it is. Query arms the
	// same budget once for the whole DuckDB-routed request
	// (queryDuckDBRouted), so under it this deadline is never later than the
	// request's and a publication-race retry or the degraded fallback shares
	// the remainder; the arming here is what bounds callers that reach a
	// pass directly (ExecuteFederatedKeysetQuery, the benchmark harness).
	// The caller's own deadline (QueryConfig.DefaultTimeout) is the ceiling
	// over all of it.
	ctx, cancel := withQueryTimeout(ctx, e.cfg.QueryTimeout)
	defer cancel()

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
			e.breaker.ReleaseProbe(probe)
		}
	}()

	// The pgx pass resolves the probe itself (pgx_pass.go).
	probeResolved = true
	return e.streamPgxPass(ctx, tables, dispatchedQuery(q, limit, offset, attributeOrders), opts, planCtx, probe, rowHandler)
}

// withQueryTimeout bounds ctx by timeout; a non-positive timeout means no
// bound and hands back ctx with a no-op cancel.
func withQueryTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

// injectSchemaProjections computes schema-driven SQL fragments from the attribute
// cache and injects them into the template parameter map. For benchmark schemas
// (IDs 100-102) it uses the benchmark parquet shape; for production it uses the
// schema attribute cache for column bindings and EAV pivots.
// schemaProjection returns the (cached) projection for schemaID; the hit flag
// feeds the execution plan so cache behavior stays observable.
func (e *DBFederatedQueryEngine) schemaProjection(ctx context.Context, schemaID int16, cache forma.SchemaAttributeCache) (*sqlgen.SchemaProjection, bool, error) {
	var pc *sqlgen.ProjectionCache
	if e != nil {
		pc = e.projections
	}
	return pc.GetOrBuild(ctx, schemaID, func() (*sqlgen.SchemaProjection, error) {
		return sqlgen.BuildSchemaProjection(schemaID, cache)
	})
}

func (e *DBFederatedQueryEngine) injectSchemaProjections(ctx context.Context, sqlParams map[string]any, schemaID int16, cache forma.SchemaAttributeCache) (projectionCacheHit bool, err error) {
	if isBenchmarkSchemaID(schemaID) {
		// Benchmark schemas: use hardcoded benchmarks projections that match
		// the benchmark parquet shape exactly (flat columns for column-bound
		// attributes, JSON extraction for EAV-only attributes).
		proj := sqlgen.BuildBenchmarkProjections(schemaID)
		sqlParams["S3SourceSelect"] = proj.S3SourceSelect
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
	sp, hit, projErr := e.schemaProjection(ctx, schemaID, cache)
	return hit, applySchemaProjection(sqlParams, schemaID, sp, projErr)
}

// applySchemaProjection writes the schema-projection params the parquet-only
// statement reads, S3SourceSelect and OuterSelect, into sqlParams from a
// computed projection, or refuses. Both a non-nil projErr and a nil
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
	sqlParams["OuterSelect"] = sp.OuterSelect
	return nil
}

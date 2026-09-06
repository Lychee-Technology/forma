package federated

import (
	"context"
	"fmt"
	"text/template"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"go.uber.org/zap"
)

// PostgresFederatedSource is the Postgres-side seam the federated engine
// queries for hot-tier records. It is intentionally wider than one method:
// federated pagination needs the optimized clause/args path and hybrid
// condition building, which QueryPersistentRecords cannot substitute.
type PostgresFederatedSource interface {
	QueryPersistentRecords(ctx context.Context, query *model.PersistentRecordQuery) (*model.PersistentRecordPage, error)
	RunOptimizedQuery(ctx context.Context, tables model.StorageTables, schemaID int16, clause string, args []any, limit, offset int, attributeOrders []model.AttributeOrder, useMainTableAsAnchor bool) ([]*model.PersistentRecord, int64, error)
	BuildHybridConditions(tables model.StorageTables, fq *model.FederatedAttributeQuery) (string, []any, error)
}

// DirtyIDFetcher retrieves row IDs from the change log that are newer than
// the flushed Parquet tiers and must be excluded from DuckDB results.
type DirtyIDFetcher interface {
	FetchDirtyRowIDs(ctx context.Context, changeLogTable string, schemaID int16) ([]uuid.UUID, error)
}

// DuckDBQueryExecutor executes SQL against DuckDB. A nil executor means
// DuckDB is unavailable; the engine then degrades per model.FederatedQueryOptions.
type DuckDBQueryExecutor interface {
	Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error)
}

// DBFederatedQueryEngine executes federated queries across the Postgres hot
// tier and the DuckDB/Parquet warm+cold tiers: routing policy, dirty-set
// exclusion, DuckDB execution, merge, and pagination. It is the sole
// implementation of the FederatedQueryEngine interface.
type DBFederatedQueryEngine struct {
	pgSource       PostgresFederatedSource
	dirtyIDFetcher DirtyIDFetcher
	duck           DuckDBQueryExecutor
	breaker        *CircuitBreaker
	cfg            forma.DuckDBConfig
	metadataCache  *schemameta.MetadataCache
	pgConnString   string
	buildDuckSQL   func(*template.Template, any, *model.FederatedAttributeQuery, []uuid.UUID, *sqlgen.DualClauses) (string, []any, error)
	duckTemplate   *template.Template
	// projections caches per-schema DuckDB projection negotiation (#142);
	// valid for the engine's lifetime because metadata is immutable after
	// construction. Reset exists for future reload scenarios.
	projections *sqlgen.ProjectionCache
	// planCache holds compiled DuckDB query plans. It is injected (shared
	// across engine instances) so its lifetime spans repeated requests even
	// when callers construct transient engines per query — the benchmark and
	// production reuse lifecycle (#142 review finding 1). Nil disables plan
	// caching.
	planCache *queryplan.Cache
	// parquetSource resolves manifest-listed parquet paths (#187); nil keeps
	// the legacy hint-only path resolution. See WithParquetSource.
	parquetSource ParquetSource
	// flushVisibilityGraceMs is the #252 clock-skew margin subtracted from
	// the query's path-resolution timestamp to form the dirty-barrier
	// cutoff; zero = exact anchor (default), negative = widening disabled.
	// Copied from DuckDBConfig.FlushVisibilityGraceMs at construction.
	flushVisibilityGraceMs int64
	// schemaValidator enforces the parquet system-column invariant before
	// each scan (#189): union_by_name tolerates attribute-column evolution,
	// so schema corruption must be caught by probing instead of by the
	// binder. See parquet_schema_validation.go.
	schemaValidator *parquetSchemaValidator
	// corruptPaths remembers verification-confirmed corrupt parquet objects
	// (#251) so resolveParquetPaths can exclude them. TTL-bounded — see
	// corruptParquetCache. Only source-authored path sets consult it.
	corruptPaths *corruptParquetCache
	// logger is the engine's own operator outlet; zap.NewNop() when unset
	// (see log(), in engine_options.go beside the option that sets this).
	// Two writers use it: the pre-read validator's stamp cross-check (#256,
	// via schemaValidator.logger, set together in WithLogger) and the
	// guard-violation identification (#351), which needs a log precisely
	// because in degraded mode its error is absorbed by the postgres-only
	// fallback and plan Notes never reach API callers.
	logger *zap.Logger
}

// flushGraceCutoffMs computes the per-request dirty-barrier cutoff from the
// instant the query resolved its parquet path set (#252): rows with
// flushed_at at or after the cutoff count as dirty (inclusive, because
// millisecond stamps cannot order a mark and a path resolution landing in
// the same tick). Because the flush appends the manifest BEFORE marking and
// samples the mark stamp after the append, any row flushed before path
// resolution already has its delta listed in the resolved set — so the
// steady state is never widened; only rows racing this query stay
// hot-readable. The configured grace is a clock-skew margin, not a window.
func (e *DBFederatedQueryEngine) flushGraceCutoffMs(pathsResolvedAtMs int64) int64 {
	if e.flushVisibilityGraceMs < 0 {
		return sqlgen.FlushGraceCutoffDisabled
	}
	return pathsResolvedAtMs - e.flushVisibilityGraceMs
}

// NewDBFederatedQueryEngine assembles the engine from its injected seams.
// duck and breaker may be nil: a nil duck marks DuckDB as unavailable and a
// nil breaker disables circuit breaking.
func NewDBFederatedQueryEngine(pgSource PostgresFederatedSource, dirtyIDFetcher DirtyIDFetcher, duck DuckDBQueryExecutor, breaker *CircuitBreaker, cfg forma.DuckDBConfig, metadataCache *schemameta.MetadataCache, pgConnString string, opts ...EngineOption) *DBFederatedQueryEngine {
	e := &DBFederatedQueryEngine{
		pgSource:               pgSource,
		dirtyIDFetcher:         dirtyIDFetcher,
		duck:                   duck,
		breaker:                breaker,
		cfg:                    cfg,
		metadataCache:          metadataCache,
		pgConnString:           pgConnString,
		duckTemplate:           sqlgen.AdvancedQueryTemplateDuckDB,
		projections:            sqlgen.NewProjectionCache(),
		schemaValidator:        newParquetSchemaValidator(),
		flushVisibilityGraceMs: cfg.FlushVisibilityGraceMs,
		corruptPaths:           newCorruptParquetCache(defaultCorruptPathRetention),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// validateFederatedQueryTarget is the entry guard shared by Query and
// ExecuteFederatedPaginatedQuery. A non-positive schema ID can never name a
// schema (schema IDs are always positive), so a request carrying one is a
// caller invariant violation (an unguarded enumerator or a hand-inserted
// registry row), not a state of the read surface. It is refused here, before
// routing, because nothing below classifies it: the manifest source reports
// it as a plain error (#536), resolveParquetPaths relabels unclassified source
// failures as ErrFederatedReadFailed, and AllowPartialDegradedMode would then
// absorb it into a Postgres-only answer — the caller's error served as a
// hot-only page (PR #537 review). The Postgres leg already rejects the same
// ID, so the guard makes the two routes agree rather than adding a rule.
func validateFederatedQueryTarget(fq *model.FederatedAttributeQuery) error {
	if fq == nil {
		return fmt.Errorf("federated query cannot be nil")
	}
	if fq.SchemaID <= 0 {
		return fmt.Errorf("federated query for schema id %d cannot be served: schema id must be positive", fq.SchemaID)
	}
	return nil
}

// Query implements FederatedQueryEngine. Hot-only requests delegate directly
// to Postgres; otherwise the routing policy decides between Postgres and the
// DuckDB federated path, falling back to Postgres on DuckDB failure when
// opts.AllowPartialDegradedMode is set.
func (e *DBFederatedQueryEngine) Query(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions) (*model.PersistentRecordPage, error) {
	if err := validateFederatedQueryTarget(fq); err != nil {
		return nil, fmt.Errorf("validate federated query: %w", err)
	}
	if e == nil || e.pgSource == nil {
		return nil, fmt.Errorf("postgres federated source is not available")
	}
	// #348: the partial-scan out-parameter describes THIS call only. The
	// DuckDB path resets it per pass and the degraded fallback replaces it
	// with the #468 coverage marker, but the hot-only gate below never
	// reaches a pass and leaves the page unmarked — without this entry
	// reset, a caller reusing one options value across queries would read
	// the previous call's marker after a hot-only answer (PR #412 review).
	if opts != nil {
		opts.PartialScan = nil
	}
	// Guard the live renderer path: ExecuteDuckDBFederatedQuery below consumes
	// the cursor unvalidated (duckdb_template_renderer.go). validateKeysetCursor
	// (keyset.go) is THE contract — the same call ExecuteFederatedPaginatedQuery
	// makes, so the two seams cannot disagree about what a cursor may be (#381).
	if err := validateKeysetCursor(fq.KeysetCursor, fq.AttributeOrders); err != nil {
		return nil, fmt.Errorf("validate keyset cursor: %w", err)
	}
	// Only explicit hot-only requests short-circuit to Postgres. Empty
	// PreferredTiers means the default all-tier form (the same contract the
	// service layer and the template's HasHot derivation use) and flows into
	// EvaluateRoutingPolicy, whose default decision already carries all three
	// tiers — a direct engine caller must not silently lose warm/cold (#184).
	// isHotOnlyRequest (routing.go) is the shared spelling of this predicate,
	// so this gate and the #354 cursor override cannot drift apart.
	if isHotOnlyRequest(fq) {
		recordHotOnlyGatePlan(opts, tables)
		return e.queryPostgresOnlyWithPlan(ctx, tables, fq, opts)
	}

	if opts != nil && opts.IncludeExecutionPlan {
		if opts.ExecutionPlan == nil {
			opts.ExecutionPlan = &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
		}
		opts.ExecutionPlan.Notes = append(opts.ExecutionPlan.Notes, "EvaluateRoutingPolicy")
	}

	decision := EvaluateRoutingPolicy(e.cfg, fq, opts)
	if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
		opts.ExecutionPlan.Routing = decision
	}
	if !decision.UseDuckDB {
		recordRoutedPostgresSource(opts, tables, decision)
		page, err := e.queryPostgresOnlyWithPlan(ctx, tables, fq, opts)
		if err != nil {
			return nil, err
		}
		// The routed Postgres-only path reads entity_main alone; a request
		// that asked for warm/cold (explicitly, or all three by omission)
		// is told so (#468). Hot-only requests never reach here: the gate
		// above intercepts them and they are not marked.
		markHotTierOnly(page, opts, fq)
		return page, nil
	}

	records, totalRecords, err := e.ExecuteDuckDBFederatedQuery(ctx, tables, fq, fq.Limit, fq.Offset, fq.AttributeOrders, opts)
	if err != nil {
		if mayDegradeToPostgres(fq, opts, err) {
			return e.degradeToPostgresOnly(ctx, tables, fq, opts, err)
		}
		return nil, fmt.Errorf("duckdb federated query: %w", explainDeclinedDegradation(fq, opts, err))
	}

	// The template carries COUNT(*) OVER() on the data rows, so a page at or
	// beyond the last match returns zero rows and the count is unreadable —
	// totalRecords would misreport 0 while matches exist (#181). Recount with
	// offset 0 (cannot recurse); at offset 0 an empty result is a genuine 0.
	if len(records) == 0 && fq.Offset > 0 {
		countTotal, cerr := e.computeFederatedCount(ctx, tables, fq)
		if cerr != nil {
			// The recount is a DuckDB query like the page fetch above, so it
			// degrades under the same policy and the same exceptions: a
			// transient failure here must not fail a request the degraded
			// mode contract promises to serve Postgres-only.
			if mayDegradeToPostgres(fq, opts, cerr) {
				return e.degradeToPostgresOnly(ctx, tables, fq, opts, cerr)
			}
			return nil, fmt.Errorf("compute empty-page federated count: %w", explainDeclinedDegradation(fq, opts, cerr))
		}
		totalRecords = countTotal
	}

	limit := fq.Limit
	if limit <= 0 {
		limit = model.DefaultPageSize
	}
	currentPage := 1
	if limit > 0 {
		currentPage = fq.Offset/limit + 1
	}

	var execPlan *model.ExecutionPlan
	if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
		execPlan = opts.ExecutionPlan
	}

	return &model.PersistentRecordPage{
		Records:       records,
		TotalRecords:  totalRecords,
		TotalPages:    model.ComputeTotalPages(totalRecords, limit),
		CurrentPage:   currentPage,
		ExecutionPlan: execPlan,
		Partial:       partialScanFromOpts(opts),
	}, nil
}

// recordHotOnlyGatePlan fills the execution plan for requests the hot-only
// gate intercepts before EvaluateRoutingPolicy runs (#184): without it a
// PreferHot/[hot] plan carries neither a routing decision nor the postgres
// source actually served, and "the plan reflects actual access" would not
// hold for the hot-only contract.
func recordHotOnlyGatePlan(opts *model.FederatedQueryOptions, tables model.StorageTables) {
	if opts == nil || !opts.IncludeExecutionPlan {
		return
	}
	if opts.ExecutionPlan == nil {
		opts.ExecutionPlan = &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
	}
	opts.ExecutionPlan.Routing = model.RoutingDecision{
		Tiers:     []model.DataTier{model.DataTierHot},
		UseDuckDB: false,
		Reason:    "hot-only gate",
	}
	opts.ExecutionPlan.Sources = append(opts.ExecutionPlan.Sources, model.DataSourcePlan{
		Tier:   model.DataTierHot,
		Engine: "postgres",
		SQL:    fmt.Sprintf("OLTP optimized query over %s", tables.EntityMain),
		Reason: "hot-only gate (OLTP main)",
	})
}

// recordRoutedPostgresSource records the postgres source for a request that
// EvaluateRoutingPolicy sent to the Postgres-only path (decision.UseDuckDB
// false, but not via the hot-only gate). Without it the plan carries the
// routing decision but never names the source actually served, so "the plan
// reflects actual access" would not hold for the cost/policy-routed hot path
// (#243 — the plan must reach HTTP callers, and it must be truthful).
func recordRoutedPostgresSource(opts *model.FederatedQueryOptions, tables model.StorageTables, decision model.RoutingDecision) {
	if opts == nil || !opts.IncludeExecutionPlan || opts.ExecutionPlan == nil {
		return
	}
	opts.ExecutionPlan.Sources = append(opts.ExecutionPlan.Sources, model.DataSourcePlan{
		Tier:   model.DataTierHot,
		Engine: "postgres",
		SQL:    fmt.Sprintf("OLTP optimized query over %s", tables.EntityMain),
		Reason: fmt.Sprintf("routing: postgres-only (%s)", decision.Reason),
	})
}

// queryPostgresOnlyWithPlan serves the Postgres-only path and stitches the
// recorded execution plan onto the returned page so HTTP callers that only see
// the page observe the route actually taken (#243). The DuckDB path already
// carries the plan on the page it builds; the plain hot-only and routed
// Postgres-only returns previously dropped it.
func (e *DBFederatedQueryEngine) queryPostgresOnlyWithPlan(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions) (*model.PersistentRecordPage, error) {
	page, err := e.queryPostgresOnly(ctx, tables, fq)
	if err != nil {
		return nil, fmt.Errorf("postgres-only query: %w", err)
	}
	attachExecutionPlan(page, opts)
	return page, nil
}

// attachExecutionPlan stitches the recorded plan onto the returned page so
// engine callers that only see the page (not the options) still observe the
// degraded fallback (#185 review finding).
func attachExecutionPlan(page *model.PersistentRecordPage, opts *model.FederatedQueryOptions) {
	if page == nil || opts == nil || !opts.IncludeExecutionPlan || opts.ExecutionPlan == nil {
		return
	}
	page.ExecutionPlan = opts.ExecutionPlan
}

// partialScanFromOpts lifts the engine's partial-scan out-parameter onto the
// DuckDB-path page (#348), where its scope is the #251 corrupt exclusion.
// The routed and degraded Postgres-only pages take the #468 coverage marker
// through markHotTierOnly instead; only the hot-only gate leaves the page
// unmarked.
func partialScanFromOpts(opts *model.FederatedQueryOptions) *model.PartialScan {
	if opts == nil {
		return nil
	}
	return opts.PartialScan
}

func (e *DBFederatedQueryEngine) queryPostgresOnly(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery) (*model.PersistentRecordPage, error) {
	if err := rejectKeysetOnPostgresOnly(fq); err != nil {
		return nil, fmt.Errorf("validate keyset cursor routing: %w", err)
	}
	page, err := e.pgSource.QueryPersistentRecords(ctx, &model.PersistentRecordQuery{
		Tables:          tables,
		SchemaID:        fq.SchemaID,
		Condition:       fq.Condition,
		AttributeOrders: fq.AttributeOrders,
		Limit:           fq.Limit,
		Offset:          fq.Offset,
	})
	if err != nil {
		return nil, fmt.Errorf("query postgres source: %w: %w", ErrPostgresReadFailed, err)
	}
	return page, nil
}

func (e *DBFederatedQueryEngine) getDuckDBQueryBuilder() func(*template.Template, any, *model.FederatedAttributeQuery, []uuid.UUID, *sqlgen.DualClauses) (string, []any, error) {
	if e != nil && e.buildDuckSQL != nil {
		return e.buildDuckSQL
	}
	return sqlgen.BuildDuckDBQuery
}

func (e *DBFederatedQueryEngine) getDuckDBTemplate() *template.Template {
	if e != nil && e.duckTemplate != nil {
		return e.duckTemplate
	}
	return sqlgen.AdvancedQueryTemplateDuckDB
}

// DuckDBClientQueryExecutor adapts a live *DuckDBClient to the
// DuckDBQueryExecutor seam.
type DuckDBClientQueryExecutor struct {
	client *DuckDBClient
}

// NewDuckDBClientQueryExecutor wraps client as a DuckDBQueryExecutor. It
// returns a nil interface when client (or its DB) is nil so that the engine's
// duck==nil unavailability guard fires early — before dirty-set fetching and
// without recording circuit-breaker failures — matching the pre-extraction
// repository semantics.
func NewDuckDBClientQueryExecutor(client *DuckDBClient) DuckDBQueryExecutor {
	if client == nil || client.DB == nil {
		return nil
	}
	return &DuckDBClientQueryExecutor{client: client}
}

// Query executes sql against the wrapped DuckDB client.
func (e *DuckDBClientQueryExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	if e == nil || e.client == nil || e.client.DB == nil {
		return nil, fmt.Errorf("duckdb client not available")
	}
	return e.client.DB.QueryContext(ctx, sql, args...)
}

type dirtyIDPool interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type DirtyIDPool = dirtyIDPool

// PostgresDirtyIDFetcher reads unflushed row IDs from the change_log table,
// satisfying the DirtyIDFetcher seam with a live Postgres pool.
type PostgresDirtyIDFetcher struct {
	pool dirtyIDPool
}

// NewPostgresDirtyIDFetcher wraps pool as a DirtyIDFetcher.
func NewPostgresDirtyIDFetcher(pool dirtyIDPool) *PostgresDirtyIDFetcher {
	return &PostgresDirtyIDFetcher{pool: pool}
}

// FetchDirtyRowIDs returns the row IDs in changeLogTable for schemaID that
// have not been flushed to Parquet yet.
func (f *PostgresDirtyIDFetcher) FetchDirtyRowIDs(ctx context.Context, changeLogTable string, schemaID int16) ([]uuid.UUID, error) {
	if changeLogTable == "" {
		return nil, fmt.Errorf("change log table name cannot be empty")
	}
	if f == nil || f.pool == nil {
		return nil, fmt.Errorf("postgres dirty id fetcher is not available")
	}
	query := fmt.Sprintf(`SELECT row_id FROM %s WHERE schema_id = $1 AND flushed_at = 0`, sqlutil.SanitizeIdentifier(changeLogTable))
	rows, err := f.pool.Query(ctx, query, schemaID)
	if err != nil {
		return nil, fmt.Errorf("query dirty row ids: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan dirty row id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dirty row ids: %w", err)
	}
	return ids, nil
}

package federated

import (
	"github.com/lychee-technology/forma/internal/model"
	"context"
	"fmt"
	"text/template"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// PostgresFederatedSource is the Postgres-side seam the federated engine
// queries for hot-tier records. It is intentionally wider than one method:
// federated pagination needs the optimized clause/args path and hybrid
// condition building, which QueryPersistentRecords cannot substitute.
type PostgresFederatedSource interface {
	QueryPersistentRecords(ctx context.Context, query *PersistentRecordQuery) (*PersistentRecordPage, error)
	RunOptimizedQuery(ctx context.Context, tables StorageTables, schemaID int16, clause string, args []any, limit, offset int, attributeOrders []AttributeOrder, useMainTableAsAnchor bool) ([]*PersistentRecord, int64, error)
	BuildHybridConditions(tables StorageTables, fq *FederatedAttributeQuery) (string, []any, error)
}

// DirtyIDFetcher retrieves row IDs from the change log that are newer than
// the flushed Parquet tiers and must be excluded from DuckDB results.
type DirtyIDFetcher interface {
	FetchDirtyRowIDs(ctx context.Context, changeLogTable string, schemaID int16) ([]uuid.UUID, error)
}

// DuckDBQueryExecutor executes SQL against DuckDB. A nil executor means
// DuckDB is unavailable; the engine then degrades per FederatedQueryOptions.
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
	buildDuckSQL   func(*template.Template, any, *FederatedAttributeQuery, []uuid.UUID, *sqlgen.DualClauses) (string, []any, error)
	duckTemplate   *template.Template
}

// NewDBFederatedQueryEngine assembles the engine from its injected seams.
// duck and breaker may be nil: a nil duck marks DuckDB as unavailable and a
// nil breaker disables circuit breaking.
func NewDBFederatedQueryEngine(pgSource PostgresFederatedSource, dirtyIDFetcher DirtyIDFetcher, duck DuckDBQueryExecutor, breaker *CircuitBreaker, cfg forma.DuckDBConfig, metadataCache *schemameta.MetadataCache, pgConnString string) *DBFederatedQueryEngine {
	return &DBFederatedQueryEngine{
		pgSource:       pgSource,
		dirtyIDFetcher: dirtyIDFetcher,
		duck:           duck,
		breaker:        breaker,
		cfg:            cfg,
		metadataCache:  metadataCache,
		pgConnString:   pgConnString,
		duckTemplate:   sqlgen.AdvancedQueryTemplateDuckDB,
	}
}

// Query implements FederatedQueryEngine. Hot-only requests delegate directly
// to Postgres; otherwise the routing policy decides between Postgres and the
// DuckDB federated path, falling back to Postgres on DuckDB failure when
// opts.AllowPartialDegradedMode is set.
func (e *DBFederatedQueryEngine) Query(ctx context.Context, tables StorageTables, fq *FederatedAttributeQuery, opts *FederatedQueryOptions) (*PersistentRecordPage, error) {
	if fq == nil {
		return nil, fmt.Errorf("federated query cannot be nil")
	}
	if e == nil || e.pgSource == nil {
		return nil, fmt.Errorf("postgres federated source is not available")
	}
	if len(fq.PreferredTiers) == 0 || fq.PreferHot || (len(fq.PreferredTiers) == 1 && fq.PreferredTiers[0] == DataTierHot) {
		return e.queryPostgresOnly(ctx, tables, fq)
	}

	if opts != nil && opts.IncludeExecutionPlan {
		if opts.ExecutionPlan == nil {
			opts.ExecutionPlan = &ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
		}
		opts.ExecutionPlan.Notes = append(opts.ExecutionPlan.Notes, "EvaluateRoutingPolicy")
	}

	decision := EvaluateRoutingPolicy(e.cfg, fq, opts)
	if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
		opts.ExecutionPlan.Routing = decision
	}
	if !decision.UseDuckDB {
		return e.queryPostgresOnly(ctx, tables, fq)
	}

	records, totalRecords, err := e.ExecuteDuckDBFederatedQuery(ctx, tables, fq, fq.Limit, fq.Offset, fq.AttributeOrders, opts)
	if err != nil {
		if opts != nil && opts.AllowPartialDegradedMode {
			return e.queryPostgresOnly(ctx, tables, fq)
		}
		return nil, fmt.Errorf("duckdb federated query: %w", err)
	}

	limit := fq.Limit
	if limit <= 0 {
		limit = model.DefaultPageSize
	}
	currentPage := 1
	if limit > 0 {
		currentPage = fq.Offset/limit + 1
	}

	var execPlan *ExecutionPlan
	if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
		execPlan = opts.ExecutionPlan
	}

	return &PersistentRecordPage{
		Records:       records,
		TotalRecords:  totalRecords,
		TotalPages:    model.ComputeTotalPages(totalRecords, limit),
		CurrentPage:   currentPage,
		ExecutionPlan: execPlan,
	}, nil
}

func (e *DBFederatedQueryEngine) queryPostgresOnly(ctx context.Context, tables StorageTables, fq *FederatedAttributeQuery) (*PersistentRecordPage, error) {
	return e.pgSource.QueryPersistentRecords(ctx, &PersistentRecordQuery{
		Tables:          tables,
		SchemaID:        fq.SchemaID,
		Condition:       fq.Condition,
		AttributeOrders: fq.AttributeOrders,
		Limit:           fq.Limit,
		Offset:          fq.Offset,
	})
}

func (e *DBFederatedQueryEngine) getDuckDBQueryBuilder() func(*template.Template, any, *FederatedAttributeQuery, []uuid.UUID, *sqlgen.DualClauses) (string, []any, error) {
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

// DuckDBPostgresConnStringFromPool derives the libpq-style connection string
// DuckDB's postgres_scanner needs from an existing pgx pool. It returns ""
// for a nil pool or pool config.
func DuckDBPostgresConnStringFromPool(pool *pgxpool.Pool) string {
	if pool == nil {
		return ""
	}
	cfg := pool.Config()
	if cfg == nil {
		return ""
	}
	connCfg := cfg.ConnConfig
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		connCfg.Host,
		connCfg.Port,
		connCfg.User,
		connCfg.Password,
		connCfg.Database,
	)
}

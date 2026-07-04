package internal

import (
	"context"
	"fmt"
	"text/template"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
)

type PostgresFederatedSource interface {
	QueryPersistentRecords(ctx context.Context, query *PersistentRecordQuery) (*PersistentRecordPage, error)
	RunOptimizedQuery(ctx context.Context, tables StorageTables, schemaID int16, clause string, args []any, limit, offset int, attributeOrders []AttributeOrder, useMainTableAsAnchor bool) ([]*PersistentRecord, int64, error)
	BuildHybridConditions(tables StorageTables, fq *FederatedAttributeQuery) (string, []any, error)
}

type DirtyIDFetcher interface {
	FetchDirtyRowIDs(ctx context.Context, changeLogTable string, schemaID int16) ([]uuid.UUID, error)
}

type DuckDBQueryExecutor interface {
	Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error)
}

type DBFederatedQueryEngine struct {
	pgSource       PostgresFederatedSource
	dirtyIDFetcher DirtyIDFetcher
	duck           DuckDBQueryExecutor
	breaker        *CircuitBreaker
	cfg            forma.DuckDBConfig
	metadataCache  *MetadataCache
	pgConnString   string
	buildDuckSQL   func(*template.Template, any, *FederatedAttributeQuery, []uuid.UUID, *DualClauses) (string, []any, error)
	duckTemplate   *template.Template
}

func NewDBFederatedQueryEngine(pgSource PostgresFederatedSource, dirtyIDFetcher DirtyIDFetcher, duck DuckDBQueryExecutor, breaker *CircuitBreaker, cfg forma.DuckDBConfig, metadataCache *MetadataCache, pgConnString string) *DBFederatedQueryEngine {
	return &DBFederatedQueryEngine{
		pgSource:       pgSource,
		dirtyIDFetcher: dirtyIDFetcher,
		duck:           duck,
		breaker:        breaker,
		cfg:            cfg,
		metadataCache:  metadataCache,
		pgConnString:   pgConnString,
		duckTemplate:   AdvancedQueryTemplateDuckDB,
	}
}

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
		limit = defaultPageSize
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
		TotalPages:    computeTotalPages(totalRecords, limit),
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

func (e *DBFederatedQueryEngine) getDuckDBQueryBuilder() func(*template.Template, any, *FederatedAttributeQuery, []uuid.UUID, *DualClauses) (string, []any, error) {
	if e != nil && e.buildDuckSQL != nil {
		return e.buildDuckSQL
	}
	return BuildDuckDBQuery
}

func (e *DBFederatedQueryEngine) getDuckDBTemplate() *template.Template {
	if e != nil && e.duckTemplate != nil {
		return e.duckTemplate
	}
	return AdvancedQueryTemplateDuckDB
}

type DuckDBClientQueryExecutor struct {
	client *DuckDBClient
}

func NewDuckDBClientQueryExecutor(client *DuckDBClient) *DuckDBClientQueryExecutor {
	return &DuckDBClientQueryExecutor{client: client}
}

func (e *DuckDBClientQueryExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	if e == nil || e.client == nil || e.client.DB == nil {
		return nil, fmt.Errorf("duckdb client not available")
	}
	return e.client.DB.QueryContext(ctx, sql, args...)
}

type dirtyIDPool interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type PostgresDirtyIDFetcher struct {
	pool dirtyIDPool
}

func NewPostgresDirtyIDFetcher(pool dirtyIDPool) *PostgresDirtyIDFetcher {
	return &PostgresDirtyIDFetcher{pool: pool}
}

func (f *PostgresDirtyIDFetcher) FetchDirtyRowIDs(ctx context.Context, changeLogTable string, schemaID int16) ([]uuid.UUID, error) {
	if changeLogTable == "" {
		return nil, fmt.Errorf("change log table name cannot be empty")
	}
	if f == nil || f.pool == nil {
		return nil, fmt.Errorf("postgres dirty id fetcher is not available")
	}
	query := fmt.Sprintf(`SELECT row_id FROM %s WHERE schema_id = $1 AND flushed_at = 0`, sanitizeIdentifier(changeLogTable))
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

func DuckDBPostgresConnStringFromPool(pool any) string {
	cfgPool, ok := pool.(*pgxpool.Pool)
	if !ok || cfgPool == nil {
		return ""
	}
	cfg := cfgPool.Config()
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

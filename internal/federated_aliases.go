package internal

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/schemameta"
)

type DBFederatedQueryEngine = federated.DBFederatedQueryEngine
type PostgresFederatedSource = federated.PostgresFederatedSource
type DirtyIDFetcher = federated.DirtyIDFetcher
type DuckDBQueryExecutor = federated.DuckDBQueryExecutor
type DuckDBClientQueryExecutor = federated.DuckDBClientQueryExecutor
type PostgresDirtyIDFetcher = federated.PostgresDirtyIDFetcher
type DuckDBClient = federated.DuckDBClient
type CircuitBreaker = federated.CircuitBreaker

func NewDBFederatedQueryEngine(pgSource PostgresFederatedSource, dirtyIDFetcher DirtyIDFetcher, duck DuckDBQueryExecutor, breaker *CircuitBreaker, cfg forma.DuckDBConfig, metadataCache *schemameta.MetadataCache, pgConnString string) *DBFederatedQueryEngine {
	return federated.NewDBFederatedQueryEngine(pgSource, dirtyIDFetcher, duck, breaker, cfg, metadataCache, pgConnString)
}

func NewDuckDBClientQueryExecutor(client *DuckDBClient) DuckDBQueryExecutor {
	return federated.NewDuckDBClientQueryExecutor(client)
}

func NewPostgresDirtyIDFetcher(pool federated.DirtyIDPool) *PostgresDirtyIDFetcher {
	return federated.NewPostgresDirtyIDFetcher(pool)
}

func ValidateDuckDBConfig(cfg forma.DuckDBConfig) error { return federated.ValidateDuckDBConfig(cfg) }
func NewDuckDBClient(cfg forma.DuckDBConfig) (*DuckDBClient, error) {
	return federated.NewDuckDBClient(cfg)
}
func NewDuckDBClientContext(ctx context.Context, cfg forma.DuckDBConfig) (*DuckDBClient, error) {
	return federated.NewDuckDBClientContext(ctx, cfg)
}
func NewCircuitBreaker(threshold int, window, openDuration time.Duration) *CircuitBreaker {
	return federated.NewCircuitBreaker(threshold, window, openDuration)
}
func EvaluateRoutingPolicy(cfg forma.DuckDBConfig, fq *FederatedAttributeQuery, opts *FederatedQueryOptions) RoutingDecision {
	return federated.EvaluateRoutingPolicy(cfg, fq, opts)
}
func MergePersistentRecordsByTier(inputs map[DataTier][]*PersistentRecord, preferHot bool) ([]*PersistentRecord, error) {
	return federated.MergePersistentRecordsByTier(inputs, preferHot)
}
func RenderDirtyIDsValuesCSV(dirtyIDs []uuid.UUID) string {
	return federated.RenderDirtyIDsValuesCSV(dirtyIDs)
}
func MergeTemplateParamsWithDirtyIDs(params any, dirtyIDs []uuid.UUID) any {
	return federated.MergeTemplateParamsWithDirtyIDs(params, dirtyIDs)
}

func DuckDBPostgresConnStringFromPool(pool *pgxpool.Pool) string {
	return federated.DuckDBPostgresConnStringFromPool(pool)
}

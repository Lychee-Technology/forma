package federated

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
)

// queryParquetUnrepresentable answers a query whose schema the parquet tiers
// cannot hold (fq.ParquetUnrepresentable, #704) from PostgreSQL alone, which
// keeps every element of an array of objects. Neither the DuckDB path, whose
// hot-tier pivot reduces each member to one value as the parquet export
// does, nor its degraded fallback is tried.
//
// Every tier declaration is served this way, an explicit warm/cold one
// included, and marked hot_tier_only like any Postgres-only answer that was
// asked for more (#468): under the storage invariant PostgreSQL holds every
// live row, as it does when DuckDB is disabled. A keyset cursor is refused,
// since only the DuckDB template applies one; the refusal says why this
// schema cannot reach that template, where queryPostgresOnly's would advise
// enabling DuckDB.
func (e *DBFederatedQueryEngine) queryParquetUnrepresentable(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions) (*model.PersistentRecordPage, error) {
	if hasKeysetCursor(fq) {
		return nil, fmt.Errorf("keyset cursor cannot be served for schema id %d: its queries route to the postgres-only path, which applies no cursor predicate, because the parquet tiers cannot hold it (%s); paginate with limit/offset: %w: %w",
			fq.SchemaID, fq.ParquetUnrepresentable, ErrKeysetUnsupportedOnPostgres, ErrParquetUnrepresentable)
	}
	decision := model.RoutingDecision{
		Tiers:  []model.DataTier{model.DataTierHot},
		Reason: "parquet tiers cannot hold the schema: " + fq.ParquetUnrepresentable,
	}
	if opts != nil && opts.IncludeExecutionPlan {
		if opts.ExecutionPlan == nil {
			opts.ExecutionPlan = &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
		}
		opts.ExecutionPlan.Routing = decision
	}
	recordRoutedPostgresSource(opts, tables, decision)
	page, err := e.queryPostgresOnlyWithPlan(ctx, tables, fq, opts)
	if err != nil {
		return nil, err
	}
	markHotTierOnly(page, opts, fq)
	return page, nil
}

// refuseParquetUnrepresentable refuses a DuckDB pass over a query whose
// schema the parquet tiers cannot hold. Query never starts one (it answers
// such a query above), so this guards the entry points that reach a pass
// directly: ExecuteFederatedKeysetQuery, ExecuteDuckDBFederatedQuery and
// StreamDuckDBFederatedQuery, all of which run through
// StreamDuckDBFederatedQuery. It runs before circuit-breaker admission, so a
// refusal neither takes the half-open probe nor counts as a DuckDB failure.
func refuseParquetUnrepresentable(fq *model.FederatedAttributeQuery) error {
	if fq.ParquetUnrepresentable == "" {
		return nil
	}
	return fmt.Errorf("duckdb pass refused for schema id %d: the parquet tiers cannot hold it (%s), and the hot-tier pivot would reduce it the same way; read it through Query, which answers it from postgres: %w",
		fq.SchemaID, fq.ParquetUnrepresentable, ErrParquetUnrepresentable)
}

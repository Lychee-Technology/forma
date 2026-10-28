package federated

import (
	"context"
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

const unrepresentableReason = `attribute "items.sku" lies inside the array of objects "items"`

// TestQueryParquetUnrepresentableIsServedPostgresOnly pins #704 at the engine
// seam: a query whose schema the parquet tiers cannot hold is answered from
// Postgres alone under every tier declaration, an explicit warm/cold one
// included, with DuckDB enabled and never consulted. The plan names the
// reason and the page carries the #468 marker for the tiers it skipped.
func TestQueryParquetUnrepresentableIsServedPostgresOnly(t *testing.T) {
	all := []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold}
	for name, tiers := range map[string][]model.DataTier{"omitted": nil, "all three": all, "warm and cold": warmCold} {
		t.Run(name, func(t *testing.T) {
			pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
			duck := &fakeDuckDBExecutor{}
			engine := NewDBFederatedQueryEngine(pg, nil, duck, nil, hybridEnabledCfg(), nil, "")
			opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true, AllowPartialDegradedMode: true}

			page, err := engine.Query(context.Background(), testTables, &model.FederatedAttributeQuery{
				AttributeQuery:         model.AttributeQuery{SchemaID: 7, Limit: 10},
				PreferredTiers:         tiers,
				ParquetUnrepresentable: unrepresentableReason,
			}, opts)

			require.NoError(t, err)
			require.Equal(t, 1, pg.queryCalls)
			require.Zero(t, duck.calls, "DuckDB reduces each member of the array to one value")
			require.False(t, page.ExecutionPlan.Routing.UseDuckDB)
			require.Equal(t, []model.DataTier{model.DataTierHot}, page.ExecutionPlan.Routing.Tiers)
			require.Contains(t, page.ExecutionPlan.Routing.Reason, unrepresentableReason)
			require.Len(t, page.ExecutionPlan.Sources, 1)
			require.Equal(t, "postgres", page.ExecutionPlan.Sources[0].Engine)
			require.NotNil(t, page.Partial)
			require.Equal(t, warmCold, page.Partial.UnconsultedTiers)
			require.Same(t, page.Partial, opts.PartialScan)
		})
	}
}

// TestQueryParquetUnrepresentableKeepsTheHotOnlyGate: a hot-only request is
// served by the gate as before, unmarked, since it asked for hot alone.
func TestQueryParquetUnrepresentableKeepsTheHotOnlyGate(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	engine := NewDBFederatedQueryEngine(pg, nil, &fakeDuckDBExecutor{}, nil, hybridEnabledCfg(), nil, "")

	page, err := engine.Query(context.Background(), testTables, &model.FederatedAttributeQuery{
		AttributeQuery:         model.AttributeQuery{SchemaID: 7, Limit: 10},
		PreferHot:              true,
		ParquetUnrepresentable: unrepresentableReason,
	}, &model.FederatedQueryOptions{})

	require.NoError(t, err)
	require.Equal(t, 1, pg.queryCalls)
	require.Nil(t, page.Partial)
}

// TestQueryParquetUnrepresentableRefusesAKeysetCursor: only the DuckDB
// template applies a cursor, so the query is refused, naming the reason the
// schema cannot reach it, before Postgres is read.
func TestQueryParquetUnrepresentableRefusesAKeysetCursor(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{}}
	duck := &fakeDuckDBExecutor{}
	engine := NewDBFederatedQueryEngine(pg, nil, duck, nil, hybridEnabledCfg(), nil, "")
	fq := keysetTestQuery()
	fq.ParquetUnrepresentable = unrepresentableReason

	_, err := engine.Query(context.Background(), testTables, fq, &model.FederatedQueryOptions{})

	require.True(t, errors.Is(err, ErrKeysetUnsupportedOnPostgres), "got %v", err)
	require.True(t, errors.Is(err, ErrParquetUnrepresentable), "got %v", err)
	require.Contains(t, err.Error(), unrepresentableReason)
	require.Zero(t, pg.queryCalls)
	require.Zero(t, duck.calls)
}

// TestDuckDBEntryPointsRefuseAParquetUnrepresentableSchema: the entry points
// that reach a DuckDB pass without going through Query refuse the schema
// before any DuckDB or S3 work, with a cursor and on the cursorless first
// page alike. A pass would collapse each member of the array of objects.
func TestDuckDBEntryPointsRefuseAParquetUnrepresentableSchema(t *testing.T) {
	firstPage := func() *model.FederatedAttributeQuery {
		fq := keysetTestQuery()
		fq.KeysetCursor = nil
		return fq
	}
	calls := map[string]func(*DBFederatedQueryEngine, *model.FederatedAttributeQuery) error{
		"keyset coordinator": func(e *DBFederatedQueryEngine, fq *model.FederatedAttributeQuery) error {
			_, _, err := e.ExecuteFederatedKeysetQuery(context.Background(), testTables, fq, 10, nil, &model.FederatedQueryOptions{})
			return err
		},
		"buffered pass": func(e *DBFederatedQueryEngine, fq *model.FederatedAttributeQuery) error {
			_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), testTables, fq, 10, 0, nil, &model.FederatedQueryOptions{})
			return err
		},
		"streamed pass": func(e *DBFederatedQueryEngine, fq *model.FederatedAttributeQuery) error {
			_, err := e.StreamDuckDBFederatedQuery(context.Background(), testTables, fq, 10, 0, nil, &model.FederatedQueryOptions{},
				func(context.Context, *model.PersistentRecord) error { return nil })
			return err
		},
	}
	queries := map[string]func() *model.FederatedAttributeQuery{"with a cursor": keysetTestQuery, "first page": firstPage}
	for callName, call := range calls {
		for queryName, query := range queries {
			t.Run(callName+"/"+queryName, func(t *testing.T) {
				pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{}}
				duck := &fakeDuckDBExecutor{}
				engine := NewDBFederatedQueryEngine(pg, nil, duck, nil, hybridEnabledCfg(), nil, "")
				fq := query()
				fq.ParquetUnrepresentable = unrepresentableReason

				err := call(engine, fq)

				require.True(t, errors.Is(err, ErrParquetUnrepresentable), "got %v", err)
				require.Contains(t, err.Error(), unrepresentableReason)
				require.Zero(t, duck.calls)
				require.Zero(t, duck.describeCalls)
				require.Zero(t, pg.queryCalls)
			})
		}
	}
}

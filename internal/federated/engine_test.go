package federated

import (
	"context"
	"fmt"
	"testing"
	"text/template"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

func initTestDescriptors() func() {
	orig := model.EntityMainColumnDescriptors
	return func() { model.EntityMainColumnDescriptors = orig }
}

type fakePostgresFederatedSource struct {
	queryCalls int
	lastQuery  *model.PersistentRecordQuery
	page       *model.PersistentRecordPage
}

func (f *fakePostgresFederatedSource) QueryPersistentRecords(ctx context.Context, query *model.PersistentRecordQuery) (*model.PersistentRecordPage, error) {
	f.queryCalls++
	f.lastQuery = query
	if f.page != nil {
		return f.page, nil
	}
	return &model.PersistentRecordPage{}, nil
}

func (f *fakePostgresFederatedSource) RunOptimizedQuery(ctx context.Context, tables model.StorageTables, schemaID int16, clause string, args []any, limit, offset int, attributeOrders []model.AttributeOrder, useMainTableAsAnchor bool) ([]*model.PersistentRecord, int64, error) {
	return nil, 0, nil
}

func (f *fakePostgresFederatedSource) BuildHybridConditions(tables model.StorageTables, fq *model.FederatedAttributeQuery) (string, []any, error) {
	return "1=1", nil, nil
}

type fakeDirtyIDFetcher struct {
	calls int
	ids   []uuid.UUID
	err   error
}

func (f *fakeDirtyIDFetcher) FetchDirtyRowIDs(ctx context.Context, changeLogTable string, schemaID int16) ([]uuid.UUID, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.ids, nil
}

type fakeDuckDBExecutor struct {
	calls   int
	lastSQL string
	err     error
	rows    duckDBRowsIterator
}

func (f *fakeDuckDBExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	f.calls++
	f.lastSQL = sql
	if f.err != nil {
		return nil, f.err
	}
	return f.rows, nil
}

type singleDuckDBRow struct {
	rowID   uuid.UUID
	scanned bool
}

func (r *singleDuckDBRow) Next() bool {
	if r.scanned {
		return false
	}
	r.scanned = true
	return true
}

func (r *singleDuckDBRow) Scan(dest ...any) error {
	return populateDuckDBRow(dest, 7, r.rowID, 10, 20, nil, "[]", 1)
}

func (r *singleDuckDBRow) Err() error {
	return nil
}

func (r *singleDuckDBRow) Close() error {
	return nil
}

func TestDBFederatedQueryEngine_QueryHotOnlyDelegatesToPostgres(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 3}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

	page, err := engine.Query(context.Background(), model.StorageTables{EntityMain: "main", EAVData: "eav"}, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10, Offset: 20},
		PreferredTiers: []model.DataTier{model.DataTierHot},
	}, nil)

	require.NoError(t, err)
	require.Equal(t, int64(3), page.TotalRecords)
	require.Equal(t, 1, pg.queryCalls)
	require.Equal(t, int16(7), pg.lastQuery.SchemaID)
}

func TestDBFederatedQueryEngine_DuckDBFailureWithDegradedModeFallsBackToPostgres(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	duck := &fakeDuckDBExecutor{err: fmt.Errorf("forced duck failure")}
	engine := NewDBFederatedQueryEngine(pg, nil, duck, nil, forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, nil, "")

	page, err := engine.Query(context.Background(), model.StorageTables{EntityMain: "main", EAVData: "eav"}, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
	}, &model.FederatedQueryOptions{AllowPartialDegradedMode: true})

	require.NoError(t, err)
	require.Equal(t, int64(1), page.TotalRecords)
	require.Equal(t, 1, duck.calls)
	require.Equal(t, 1, pg.queryCalls)
}

func TestDBFederatedQueryEngine_DuckDBRouteUsesInjectedExecutorAndDirtyFetcher(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	rowID := uuid.New()
	dirtyID := uuid.New()
	pg := &fakePostgresFederatedSource{}
	dirty := &fakeDirtyIDFetcher{ids: []uuid.UUID{dirtyID}}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: rowID}}
	engine := NewDBFederatedQueryEngine(pg, dirty, duck, nil, forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, nil, "")
	engine.buildDuckSQL = func(tpl *template.Template, params any, q *model.FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *sqlgen.DualClauses) (string, []any, error) {
		require.Equal(t, []uuid.UUID{dirtyID}, dirtyIDs)
		return "SELECT fake", nil, nil
	}

	page, err := engine.Query(context.Background(), model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
	}, &model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.NoError(t, err)
	require.Equal(t, 1, dirty.calls)
	require.Equal(t, 1, duck.calls)
	require.Equal(t, "SELECT fake", duck.lastSQL)
	require.Len(t, page.Records, 1)
	require.Equal(t, rowID, page.Records[0].RowID)
}

func TestDBFederatedQueryEngine_DisabledRoutingDelegatesToPostgres(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 2}}
	duck := &fakeDuckDBExecutor{}
	engine := NewDBFederatedQueryEngine(pg, nil, duck, nil, forma.DuckDBConfig{Enabled: false}, nil, "")

	page, err := engine.Query(context.Background(), model.StorageTables{EntityMain: "main", EAVData: "eav"}, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
	}, nil)

	require.NoError(t, err)
	require.Equal(t, int64(2), page.TotalRecords)
	require.Equal(t, 1, pg.queryCalls)
	require.Equal(t, 0, duck.calls)
}

func TestDBFederatedQueryEngine_NilQueryReturnsError(t *testing.T) {
	engine := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

	page, err := engine.Query(context.Background(), model.StorageTables{EntityMain: "main", EAVData: "eav"}, nil, nil)

	require.Nil(t, page)
	require.EqualError(t, err, "federated query cannot be nil")
}

func TestDBFederatedQueryEngine_ExecutionPlanPopulated(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	pg := &fakePostgresFederatedSource{}
	dirty := &fakeDirtyIDFetcher{}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	engine := NewDBFederatedQueryEngine(pg, dirty, duck, nil, forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, nil, "")
	engine.buildDuckSQL = func(tpl *template.Template, params any, q *model.FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *sqlgen.DualClauses) (string, []any, error) {
		return "SELECT fake", nil, nil
	}

	page, err := engine.Query(context.Background(), model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
	}, &model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.NoError(t, err)
	require.NotNil(t, page.ExecutionPlan)
	require.True(t, page.ExecutionPlan.Routing.UseDuckDB)
	require.Contains(t, page.ExecutionPlan.Notes, "EvaluateRoutingPolicy")
	require.NotNil(t, page.ExecutionPlan.Timings)
}

func TestNewDuckDBClientQueryExecutorNilClientReturnsNil(t *testing.T) {
	require.Nil(t, NewDuckDBClientQueryExecutor(nil))
	require.Nil(t, NewDuckDBClientQueryExecutor(&DuckDBClient{}))
}

func TestDBFederatedQueryEngine_DuckDBUnavailableFailsBeforeDirtyFetch(t *testing.T) {
	dirty := &fakeDirtyIDFetcher{}
	engine := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, dirty, nil, nil, forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, nil, "")

	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	_, err := engine.Query(context.Background(), model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
	}, opts)

	require.ErrorContains(t, err, "duckdb client not available")
	require.Equal(t, 0, dirty.calls)
	require.Contains(t, opts.ExecutionPlan.Notes, "duckdb client unavailable")
}

// TestSchemaProjectionCache pins #142: the second projection build for the
// same schema is served from the engine cache, the execution plan records
// hit/miss, and Reset invalidates.
func TestSchemaProjectionCache(t *testing.T) {
	engine := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")
	cache := forma.SchemaAttributeCache{
		"name": {AttributeID: 5, ValueType: forma.ValueTypeText},
	}

	sp1, hit, err := engine.schemaProjection(7, cache)
	require.NoError(t, err)
	require.False(t, hit)
	require.NotNil(t, sp1)

	sp2, hit, err := engine.schemaProjection(7, cache)
	require.NoError(t, err)
	require.True(t, hit)
	require.Same(t, sp1, sp2, "cached projection must be shared (read-only contract)")

	hits, misses := engine.projections.Stats()
	require.Equal(t, int64(1), hits)
	require.Equal(t, int64(1), misses)

	engine.projections.Reset()
	_, hit, err = engine.schemaProjection(7, cache)
	require.NoError(t, err)
	require.False(t, hit, "Reset must invalidate cached projections")

	// Plan note observability via injectSchemaProjections.
	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true, ExecutionPlan: &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}}
	planCtx := newDuckDBExecutionPlanContext(opts)
	params := map[string]any{}
	hitFlag := engine.injectSchemaProjections(params, 7, cache)
	planCtx.recordProjectionCache(hitFlag)
	require.Contains(t, opts.ExecutionPlan.Notes, "schema_projection_cache_hit")
}

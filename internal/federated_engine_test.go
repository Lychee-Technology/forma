package internal

import (
	"context"
	"fmt"
	"testing"
	"text/template"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

type fakePostgresFederatedSource struct {
	queryCalls int
	lastQuery  *PersistentRecordQuery
	page       *PersistentRecordPage
}

func (f *fakePostgresFederatedSource) QueryPersistentRecords(ctx context.Context, query *PersistentRecordQuery) (*PersistentRecordPage, error) {
	f.queryCalls++
	f.lastQuery = query
	if f.page != nil {
		return f.page, nil
	}
	return &PersistentRecordPage{}, nil
}

func (f *fakePostgresFederatedSource) RunOptimizedQuery(ctx context.Context, tables StorageTables, schemaID int16, clause string, args []any, limit, offset int, attributeOrders []AttributeOrder, useMainTableAsAnchor bool) ([]*PersistentRecord, int64, error) {
	return nil, 0, nil
}

func (f *fakePostgresFederatedSource) BuildHybridConditions(tables StorageTables, fq *FederatedAttributeQuery) (string, []any, error) {
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
	pg := &fakePostgresFederatedSource{page: &PersistentRecordPage{TotalRecords: 3}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

	page, err := engine.Query(context.Background(), StorageTables{EntityMain: "main", EAVData: "eav"}, &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{SchemaID: 7, Limit: 10, Offset: 20},
		PreferredTiers: []DataTier{DataTierHot},
	}, nil)

	require.NoError(t, err)
	require.Equal(t, int64(3), page.TotalRecords)
	require.Equal(t, 1, pg.queryCalls)
	require.Equal(t, int16(7), pg.lastQuery.SchemaID)
}

func TestDBFederatedQueryEngine_DuckDBFailureWithDegradedModeFallsBackToPostgres(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &PersistentRecordPage{TotalRecords: 1}}
	duck := &fakeDuckDBExecutor{err: fmt.Errorf("forced duck failure")}
	engine := NewDBFederatedQueryEngine(pg, nil, duck, nil, forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, nil, "")

	page, err := engine.Query(context.Background(), StorageTables{EntityMain: "main", EAVData: "eav"}, &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []DataTier{DataTierHot, DataTierCold},
	}, &FederatedQueryOptions{AllowPartialDegradedMode: true})

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
	engine.buildDuckSQL = func(tpl *template.Template, params any, q *FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *DualClauses) (string, []any, error) {
		require.Equal(t, []uuid.UUID{dirtyID}, dirtyIDs)
		return "SELECT fake", nil, nil
	}

	page, err := engine.Query(context.Background(), StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}, &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []DataTier{DataTierHot, DataTierCold},
	}, &FederatedQueryOptions{IncludeExecutionPlan: true})

	require.NoError(t, err)
	require.Equal(t, 1, dirty.calls)
	require.Equal(t, 1, duck.calls)
	require.Equal(t, "SELECT fake", duck.lastSQL)
	require.Len(t, page.Records, 1)
	require.Equal(t, rowID, page.Records[0].RowID)
}

func TestDBFederatedQueryEngine_DisabledRoutingDelegatesToPostgres(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &PersistentRecordPage{TotalRecords: 2}}
	duck := &fakeDuckDBExecutor{}
	engine := NewDBFederatedQueryEngine(pg, nil, duck, nil, forma.DuckDBConfig{Enabled: false}, nil, "")

	page, err := engine.Query(context.Background(), StorageTables{EntityMain: "main", EAVData: "eav"}, &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{SchemaID: 7, Limit: 10},
		PreferredTiers: []DataTier{DataTierHot, DataTierCold},
	}, nil)

	require.NoError(t, err)
	require.Equal(t, int64(2), page.TotalRecords)
	require.Equal(t, 1, pg.queryCalls)
	require.Equal(t, 0, duck.calls)
}

func TestDBFederatedQueryEngine_NilQueryReturnsError(t *testing.T) {
	engine := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

	page, err := engine.Query(context.Background(), StorageTables{EntityMain: "main", EAVData: "eav"}, nil, nil)

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
	engine.buildDuckSQL = func(tpl *template.Template, params any, q *FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *DualClauses) (string, []any, error) {
		return "SELECT fake", nil, nil
	}

	page, err := engine.Query(context.Background(), StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}, &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []DataTier{DataTierHot, DataTierCold},
	}, &FederatedQueryOptions{IncludeExecutionPlan: true})

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

	opts := &FederatedQueryOptions{IncludeExecutionPlan: true}
	_, err := engine.Query(context.Background(), StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}, &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []DataTier{DataTierHot, DataTierCold},
	}, opts)

	require.ErrorContains(t, err, "duckdb client not available")
	require.Equal(t, 0, dirty.calls)
	require.Contains(t, opts.ExecutionPlan.Notes, "duckdb client unavailable")
}

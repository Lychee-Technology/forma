package federated

import (
	"context"
	"fmt"
	"testing"
	"text/template"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

func initTestDescriptors() func() {
	orig := model.EntityMainColumnDescriptors
	return func() { model.EntityMainColumnDescriptors = orig }
}

// testMetadataCacheSchema7 registers a minimal schema-7 attribute cache so the
// DuckDB render path can build a real column projection. Post-#151 the
// no-metadata-cache path fails fast instead of emitting a toy fallback
// projection, so engine tests that drive the render path must supply a cache.
func testMetadataCacheSchema7(t *testing.T) *schemameta.MetadataCache {
	t.Helper()
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("test", 7, forma.SchemaAttributeCache{
		"age": {
			AttributeID:   6,
			ValueType:     forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")},
		},
	}))
	return mc
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
	calls    int
	lastSQL  string
	lastArgs []any
	err      error
	rows     duckDBRowsIterator
}

func (f *fakeDuckDBExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	f.calls++
	f.lastSQL = sql
	f.lastArgs = args
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
	engine := NewDBFederatedQueryEngine(pg, nil, duck, nil, forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, testMetadataCacheSchema7(t), "")

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
	engine := NewDBFederatedQueryEngine(pg, dirty, duck, nil, forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, testMetadataCacheSchema7(t), "")
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
	engine := NewDBFederatedQueryEngine(pg, dirty, duck, nil, forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, testMetadataCacheSchema7(t), "")
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
	hitFlag, err := engine.injectSchemaProjections(params, 7, cache)
	require.NoError(t, err)
	planCtx.recordProjectionCache(hitFlag)
	require.Contains(t, opts.ExecutionPlan.Notes, "schema_projection_cache_hit")
}

// TestInjectSchemaProjectionsNoCacheFailsFast pins #151: a non-benchmark schema
// with no metadata cache must fail fast rather than emit a stale toy-schema
// projection that violates the positional-scan contract. The retired fallback
// wrote name/age/tag columns that duckDBScanBuffers cannot scan.
func TestInjectSchemaProjectionsNoCacheFailsFast(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	engine := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, &fakeDuckDBExecutor{}, nil,
		forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}},
		schemameta.NewMetadataCache(), "host=x")

	params := map[string]any{}
	hit, err := engine.injectSchemaProjections(params, 7, nil)
	require.Error(t, err)
	require.False(t, hit)
	require.Contains(t, err.Error(), "requires a schema metadata cache")
	require.NotContains(t, params, "OuterSelect", "no projection must be injected on the fail-fast path")

	require.ErrorIs(t, err, ErrSchemaMetadataCacheRequired)

	// Benchmark schemas remain unaffected (they never consult the cache).
	benchParams := map[string]any{}
	_, benchErr := engine.injectSchemaProjections(benchParams, int16(100), nil)
	require.NoError(t, benchErr)
	require.Contains(t, benchParams, "OuterSelect")
}

// TestDBFederatedQueryEngine_MissingSchemaCacheNotDegradable pins the #151 PR
// review finding: the missing-schema-cache error is a configuration error, not
// a transient DuckDB failure, so the public Query path must surface it even
// under AllowPartialDegradedMode instead of silently falling back to a
// Postgres-only partial result.
func TestDBFederatedQueryEngine_MissingSchemaCacheNotDegradable(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	// nil metadata cache → the DuckDB render path cannot build a projection.
	engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
		forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, nil, "host=x")

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
			PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
		},
		&model.FederatedQueryOptions{AllowPartialDegradedMode: true})

	require.ErrorIs(t, err, ErrSchemaMetadataCacheRequired)
	require.Equal(t, 0, pg.queryCalls, "missing-cache error must not degrade to a Postgres-only partial result")
}

// TestEngineCompiledPlanCache pins #142 phase 5 end to end: two same-shape
// requests through a shared plan cache render once, the second is a hit with
// fresh operand args, and the execution plan records hit/miss.
func TestEngineCompiledPlanCache(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	shared := queryplan.NewCache(64)
	cache := forma.SchemaAttributeCache{
		"age": {AttributeID: 6, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
	}
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("test", 7, cache))

	newEngine := func(duck *fakeDuckDBExecutor) *DBFederatedQueryEngine {
		return NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, nil,
			forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}},
			mc, "host=x", WithPlanCache(shared))
	}
	query := func(val string) (*model.FederatedAttributeQuery, *model.FederatedQueryOptions) {
		q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{
			SchemaID:  7,
			Condition: &forma.KvCondition{Attr: "age", Value: "gt:" + val},
			Limit:     2000,
		}}
		q.PreferredTiers = []model.DataTier{model.DataTierHot, model.DataTierCold}
		opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true,
			ExecutionPlan: &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}}
		return q, opts
	}
	tables := model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}

	// First request: transient engine A — compile (miss).
	duckA := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	qA, optsA := query("10")
	_, _, err := newEngine(duckA).ExecuteDuckDBFederatedQuery(context.Background(), tables, qA, qA.Limit, 0, nil, optsA)
	require.NoError(t, err)
	require.Contains(t, optsA.ExecutionPlan.Notes, "plan_cache=miss")
	require.Equal(t, int64(1), optsA.ExecutionPlan.Timings["plan_cache_miss"])

	// Second request: a DIFFERENT transient engine sharing the cache (the
	// benchmark lifecycle) — hit, same SQL, fresh operand value.
	duckB := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	qB, optsB := query("77")
	_, _, err = newEngine(duckB).ExecuteDuckDBFederatedQuery(context.Background(), tables, qB, qB.Limit, 0, nil, optsB)
	require.NoError(t, err)
	require.Contains(t, optsB.ExecutionPlan.Notes, "plan_cache=hit")
	require.Equal(t, int64(1), optsB.ExecutionPlan.Timings["plan_cache_hit"])
	require.Equal(t, duckA.lastSQL, duckB.lastSQL, "same shape must reuse the rendered skeleton")
	require.Contains(t, duckB.lastArgs, int64(77), "hit must bind the second request's operand")
	require.NotContains(t, duckB.lastArgs, int64(10))

	hits, misses := shared.Stats()
	require.Equal(t, int64(1), hits)
	require.Equal(t, int64(1), misses)

	// Different shape misses.
	duckC := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	qC, optsC := query("5")
	qC.Condition = &forma.KvCondition{Attr: "age", Value: "lt:5"}
	_, _, err = newEngine(duckC).ExecuteDuckDBFederatedQuery(context.Background(), tables, qC, qC.Limit, 0, nil, optsC)
	require.NoError(t, err)
	require.Contains(t, optsC.ExecutionPlan.Notes, "plan_cache=miss")
}

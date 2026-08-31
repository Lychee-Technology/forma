package federated

import (
	"context"
	"fmt"
	"strings"
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

const testParquetPath = "s3://b/7/a.parquet"

// withTestParquetPath supplies a resolvable single-object path set and marks it
// pre-validated. Two reasons, both mechanical:
//
// Since #299 a DuckDB-routed query whose path set resolves empty fails at
// resolution with ErrNoParquetPaths, so every engine test that drives the
// render/execute path must author one — these tests are about routing, plans,
// breakers and recounts, not about path resolution.
//
// A non-empty path set then activates the pre-read footer probe
// (parquetSchemaValidator), which issues its own DuckDB queries through the
// very executor these tests script and count. Pre-warming the validator's
// write-once cache keeps each test measuring only the calls it makes itself.
func withTestParquetPath() EngineOption {
	return func(e *DBFederatedQueryEngine) {
		WithParquetSource(&fakeParquetSource{paths: []string{testParquetPath}})(e)
		e.schemaValidator.markValidated(testParquetPath,
			map[string]string{"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT", "ltbase_created_at": "BIGINT"}, nil)
	}
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

// TestDBFederatedQueryEngine_HotOnlyGateAttachesPlanToPage pins the #243 gap:
// the hot-only gate recorded the plan on opts but returned the page from
// queryPostgresOnly without stitching it on, so an HTTP caller that only sees
// the page could not tell a hot-routed federated read from a DuckDB one.
func TestDBFederatedQueryEngine_HotOnlyGateAttachesPlanToPage(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 3}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

	page, err := engine.Query(context.Background(), model.StorageTables{EntityMain: "main", EAVData: "eav"}, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		PreferredTiers: []model.DataTier{model.DataTierHot},
	}, &model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.NoError(t, err)
	require.NotNil(t, page.ExecutionPlan, "hot-only gate must stitch the plan onto the page (#243)")
	require.False(t, page.ExecutionPlan.Routing.UseDuckDB)
	require.Equal(t, []model.DataTier{model.DataTierHot}, page.ExecutionPlan.Routing.Tiers)
	requirePlanHasPostgresSource(t, page.ExecutionPlan, "hot-only gate")
}

// TestDBFederatedQueryEngine_RoutedPostgresOnlyAttachesPlanToPage pins the
// second #243 gap: when EvaluateRoutingPolicy (not the hot-only gate) sends a
// multi-tier request to Postgres-only — e.g. DuckDB globally disabled — the
// plan must ride on the returned page and name the postgres source served.
func TestDBFederatedQueryEngine_RoutedPostgresOnlyAttachesPlanToPage(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 5}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: false}, nil, "")

	page, err := engine.Query(context.Background(), model.StorageTables{EntityMain: "main", EAVData: "eav"}, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
	}, &model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.NoError(t, err)
	require.Equal(t, 1, pg.queryCalls)
	require.NotNil(t, page.ExecutionPlan, "routed postgres-only must stitch the plan onto the page (#243)")
	require.False(t, page.ExecutionPlan.Routing.UseDuckDB)
	requirePlanHasPostgresSource(t, page.ExecutionPlan, "routing: postgres-only")
}

func TestDBFederatedQueryEngine_DuckDBFailureWithDegradedModeFallsBackToPostgres(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	duck := &fakeDuckDBExecutor{err: fmt.Errorf("forced duck failure")}
	engine := NewDBFederatedQueryEngine(pg, nil, duck, nil, forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, testMetadataCacheSchema7(t), "", withTestParquetPath())

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
	engine := NewDBFederatedQueryEngine(pg, dirty, duck, nil, forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, testMetadataCacheSchema7(t), "", withTestParquetPath())
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

// TestDBFederatedQueryEngine_EmptyTiersUseAllThree pins the #184 default
// contract at the engine boundary: empty PreferredTiers is the all-tier form
// and must flow into EvaluateRoutingPolicy (default decision carries all
// three tiers) rather than short-circuiting to Postgres-only — direct engine
// callers must not silently lose warm/cold.
func TestDBFederatedQueryEngine_EmptyTiersUseAllThree(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	pg := &fakePostgresFederatedSource{}
	dirty := &fakeDirtyIDFetcher{}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	engine := NewDBFederatedQueryEngine(pg, dirty, duck, nil, forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, testMetadataCacheSchema7(t), "", withTestParquetPath())
	engine.buildDuckSQL = func(tpl *template.Template, params any, q *model.FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *sqlgen.DualClauses) (string, []any, error) {
		return "SELECT fake", nil, nil
	}

	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	page, err := engine.Query(context.Background(), model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
	}, opts)

	require.NoError(t, err)
	require.Equal(t, 1, duck.calls, "empty tiers must reach the DuckDB federated path, not the hot-only gate")
	require.Equal(t, 0, pg.queryCalls)
	require.True(t, page.ExecutionPlan.Routing.UseDuckDB)
	require.Equal(t, []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold}, page.ExecutionPlan.Routing.Tiers)
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
	engine := NewDBFederatedQueryEngine(pg, dirty, duck, nil, forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, testMetadataCacheSchema7(t), "", withTestParquetPath())
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
		schemameta.NewMetadataCache(), "host=x", withTestParquetPath())

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
		forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, nil, "host=x", withTestParquetPath())

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
			mc, "host=x", WithPlanCache(shared), withTestParquetPath())
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
	// The per-request #252 cutoff is the only legitimate difference between
	// the two bound SQLs; normalize it so the skeleton-reuse pin is exact.
	require.Equal(t, normalizeFlushGraceCutoff(duckA.lastSQL), normalizeFlushGraceCutoff(duckB.lastSQL),
		"same shape must reuse the rendered skeleton")
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

// requirePlanHasNoteContaining asserts one plan note contains substr.
func requirePlanHasNoteContaining(t *testing.T, plan *model.ExecutionPlan, substr string) {
	t.Helper()
	require.NotNil(t, plan)
	for _, n := range plan.Notes {
		if strings.Contains(n, substr) {
			return
		}
	}
	t.Fatalf("no plan note contains %q; notes: %v", substr, plan.Notes)
}

// requirePlanHasPostgresSource asserts the plan records a postgres source
// whose reason contains reasonSubstr.
func requirePlanHasPostgresSource(t *testing.T, plan *model.ExecutionPlan, reasonSubstr string) {
	t.Helper()
	require.NotNil(t, plan)
	for _, s := range plan.Sources {
		if s.Engine == "postgres" && strings.Contains(s.Reason, reasonSubstr) {
			return
		}
	}
	t.Fatalf("no postgres source with reason containing %q; sources: %+v", reasonSubstr, plan.Sources)
}

// TestDBFederatedQueryEngine_DegradedFallbackRecordsExecutionPlan pins the
// #185 scenario-6 contract: when AllowPartialDegradedMode absorbs a DuckDB
// failure, the returned execution plan must reflect the postgres-only
// fallback instead of keeping the stale pre-failure routing decision.
func TestDBFederatedQueryEngine_DegradedFallbackRecordsExecutionPlan(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	duck := &fakeDuckDBExecutor{err: fmt.Errorf("forced duck failure")}
	engine := NewDBFederatedQueryEngine(pg, nil, duck, nil, forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}, testMetadataCacheSchema7(t), "", withTestParquetPath())

	opts := &model.FederatedQueryOptions{
		AllowPartialDegradedMode: true,
		IncludeExecutionPlan:     true,
	}
	page, err := engine.Query(context.Background(), model.StorageTables{EntityMain: "main", EAVData: "eav"}, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
	}, opts)

	require.NoError(t, err)
	require.NotNil(t, opts.ExecutionPlan)
	require.False(t, opts.ExecutionPlan.Routing.UseDuckDB,
		"plan must not claim DuckDB after degraded fallback: %+v", opts.ExecutionPlan.Routing)
	require.Equal(t, []model.DataTier{model.DataTierHot}, opts.ExecutionPlan.Routing.Tiers)
	require.Contains(t, opts.ExecutionPlan.Routing.Reason, "degraded fallback")
	requirePlanHasNoteContaining(t, opts.ExecutionPlan, "forced duck failure")
	requirePlanHasPostgresSource(t, opts.ExecutionPlan, "degraded fallback")
	// The plan must also ride on the returned page, not just opts: a caller
	// that only sees the page must still observe the degraded fallback (#185).
	require.NotNil(t, page.ExecutionPlan)
	require.False(t, page.ExecutionPlan.Routing.UseDuckDB)
}

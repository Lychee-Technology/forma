package federated

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/require"
)

// planKeyRender is what one federated request handed to DuckDB, or why it
// handed nothing.
type planKeyRender struct {
	sql  string
	args []any
	err  error
}

// planKeyEngine is an engine over the planKeyMetadataCache schema, with or
// without a plan cache, that records each request's rendered main scan.
type planKeyEngine struct {
	e     *DBFederatedQueryEngine
	duck  *fakeDuckDBExecutor
	cache *queryplan.Cache
}

func newPlanKeyEngine(mc *schemameta.MetadataCache, withCache bool) *planKeyEngine {
	pe := &planKeyEngine{duck: &fakeDuckDBExecutor{}}
	opts := []EngineOption{}
	if withCache {
		pe.cache = queryplan.NewCache(16)
		opts = append(opts, WithPlanCache(pe.cache))
	}
	pe.e = NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, pe.duck, nil,
		hybridDuckConfig(), mc, "host=pg dbname=x", opts...)
	return pe
}

func (pe *planKeyEngine) run(t *testing.T, cond forma.Condition) planKeyRender {
	t.Helper()
	pe.duck.rows = &singleDuckDBRow{}
	pe.duck.lastSQL, pe.duck.lastArgs = "", nil
	q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 100, Limit: 25, Condition: cond}}
	q.PreferredTiers = []model.DataTier{model.DataTierHot, model.DataTierCold}
	q.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: "s3://b/bench/{schema_id}/base.parquet"}
	tables := model.StorageTables{EntityMain: "entity_main_b", EAVData: "eav_b", ChangeLog: "cl_b"}
	_, _, err := pe.e.ExecuteDuckDBFederatedQuery(t.Context(), tables, q, 25, 0, nil, nil)
	if err != nil {
		return planKeyRender{err: err}
	}
	return planKeyRender{sql: normalizeFlushGraceCutoff(pe.duck.lastSQL), args: pe.duck.lastArgs}
}

func planKeyMetadataCache(t *testing.T) *schemameta.MetadataCache {
	t.Helper()
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("bench", 100, forma.SchemaAttributeCache{
		"symbol": {AttributeID: 3, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
		"qty": {AttributeID: 4, ValueType: forma.ValueTypeBigInt,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("bigint_01")}},
	}))
	return mc
}

// TestPlanCacheHitRendersLikeDirectBuild pins the assumption the federated
// plan cache's hit path rests on (#441): serveFromPlanCache binds a request's
// own args into the clause skeletons cached under its key, so two requests
// that share a key must render the same clauses. Each case sends one
// condition and then the other through one cached engine, and requires the
// second request to render exactly what the direct builder renders for it
// alone, or to fail as the direct builder fails.
//
// The unregistered-attribute pair shares a key today and renders different
// clauses; it is safe only because one side is refused before the cache is
// consulted. Turning that refusal into a skip without also extending the
// shape hash (queryplan.HashFederatedQueryShape) fails this test, so the
// cache's correctness rests on its key rather than on that refusal.
func TestPlanCacheHitRendersLikeDirectBuild(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()
	mc := planKeyMetadataCache(t)

	cases := []struct {
		name string
		attr string
		a, b string // operator:value of the two single-leaf conditions
		// hit requires the second request to be served from the first's
		// entry, so the case exercises the hit path today.
		hit bool
	}{
		{
			// The shape hash canonicalizes eq to equals, and so does the
			// pg-main classifier since #469: both push to entity_main.
			name: "operator alias on a column-bound attribute",
			attr: "symbol", a: "equals:x", b: "eq:x",
			hit: true,
		},
		{
			// The DuckDB leg infers an unregistered attribute's type from
			// the operand (detectValueType), which the shape hash does not
			// see. The EAV leg refuses an unregistered attribute.
			name: "operand type on an unregistered attribute",
			attr: "ghost", a: "equals:5", b: "equals:abc",
		},
		{
			// The operand class the key does see (#677): equals:7 renders
			// "=" on a bigint column, equals:7.5 a different comparison.
			// Without the class in the shape hash this case fails.
			name: "fractional operand on a whole-number column",
			attr: "qty", a: "equals:7", b: "equals:7.5",
		},
		{
			name: "values only",
			attr: "qty", a: "gt:7", b: "gt:8",
			hit: true,
		},
	}
	for _, tc := range cases {
		for _, order := range [][2]string{{tc.a, tc.b}, {tc.b, tc.a}} {
			first := &forma.KvCondition{Attr: tc.attr, Value: order[0]}
			second := &forma.KvCondition{Attr: tc.attr, Value: order[1]}
			t.Run(tc.name+"/"+order[1]+" after "+order[0], func(t *testing.T) {
				requireCachedRendersLikeDirect(t, mc, first, second, tc.hit)
			})
		}
	}
}

// TestPlanCacheHitAliasInsideOr is the #469 case the single-leaf pairs above
// cannot reach. Inside an OR the pg-main guard does not refuse an
// unpushable branch, it drops the whole group from PG_WHERE_CLAUSE. While
// the classifier read the raw spelling, or(eq:x, …) was served the
// skeleton cached for or(equals:x, …), with two more placeholders than the
// args it bound.
func TestPlanCacheHitAliasInsideOr(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()
	mc := planKeyMetadataCache(t)
	or := func(op string) forma.Condition {
		return &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "symbol", Value: op + ":x"},
			&forma.KvCondition{Attr: "qty", Value: "gt:1"},
		}}
	}
	t.Run("eq after equals", func(t *testing.T) { requireCachedRendersLikeDirect(t, mc, or("equals"), or("eq"), true) })
	t.Run("equals after eq", func(t *testing.T) { requireCachedRendersLikeDirect(t, mc, or("eq"), or("equals"), true) })
}

// requireCachedRendersLikeDirect sends first and then second through one
// cached engine, and requires the second request to render exactly what the
// direct builder renders for it alone, or to fail as the direct builder
// fails. hit additionally requires the second to be served from the first's
// entry.
func requireCachedRendersLikeDirect(t *testing.T, mc *schemameta.MetadataCache, first, second forma.Condition, hit bool) {
	t.Helper()
	cached := newPlanKeyEngine(mc, true)
	cached.run(t, first)
	hitsBefore, _ := cached.cache.Stats()
	got := cached.run(t, second)
	hitsAfter, _ := cached.cache.Stats()
	want := newPlanKeyEngine(mc, false).run(t, second)

	if hit {
		require.Equal(t, hitsBefore+1, hitsAfter, "the second request must be served from the first's entry")
	}
	if want.err != nil {
		require.Error(t, got.err, "the direct builder refuses this request; the cached engine must too")
		return
	}
	require.NoError(t, got.err)
	require.NotEmpty(t, want.sql, "the direct builder must reach DuckDB")
	require.Equal(t, want.sql, got.sql,
		"a request sharing a cached entry must render what the direct builder renders for it")
	require.Equal(t, want.args, got.args)
}

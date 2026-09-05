package federated

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

var testTables = model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}

var warmCold = []model.DataTier{model.DataTierWarm, model.DataTierCold}

// TestQueryImplicitTiersSmallPageIsMarkedHotTierOnly pins the marker half of
// #468: a request that omitted preferred_tiers (default all-tier form) under
// the hybrid small-result heuristic is answered from entity_main alone, and
// the page must say so — the out-parameter and the page agree, and the
// marker does not depend on include_execution_plan.
func TestQueryImplicitTiersSmallPageIsMarkedHotTierOnly(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 3}}
	duck := &fakeDuckDBExecutor{}
	engine := NewDBFederatedQueryEngine(pg, nil, duck, nil, hybridEnabledCfg(), nil, "")
	opts := &model.FederatedQueryOptions{}

	page, err := engine.Query(context.Background(), testTables, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
	}, opts)

	require.NoError(t, err)
	require.Equal(t, 1, pg.queryCalls)
	require.Zero(t, duck.calls)
	require.NotNil(t, page.Partial, "a postgres-only answer to an all-tier request must be marked (#468)")
	require.Equal(t, warmCold, page.Partial.UnconsultedTiers)
	require.Empty(t, page.Partial.ExcludedObjects)
	require.Same(t, page.Partial, opts.PartialScan, "the out-parameter must describe the same answer")
}

// TestQueryExplicitTiersSmallPageReachesDuckDBUnmarked is the override half at
// the engine seam: an explicit three-tier declaration with an ordinary page
// size must reach the DuckDB builder, leave postgres untouched, and carry no
// coverage marker because every declared tier was consulted.
func TestQueryExplicitTiersSmallPageReachesDuckDBUnmarked(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 5}}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	var built []model.FederatedAttributeQuery
	engine := newEmptyPageTestEngine(t, pg, duck, &built)
	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}

	page, err := engine.Query(context.Background(), testTables, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
	}, opts)

	require.NoError(t, err)
	require.Zero(t, pg.queryCalls, "an explicit multi-tier request must not be served postgres-only")
	require.Equal(t, 1, duck.calls)
	require.Len(t, built, 1)
	require.True(t, page.ExecutionPlan.Routing.UseDuckDB)
	require.Contains(t, page.ExecutionPlan.Routing.Reason, "preferred_tiers")
	require.Nil(t, page.Partial, "every declared tier was consulted")
	require.Nil(t, opts.PartialScan)
}

// TestQueryHotOnlyGateIsNotMarked pins that the marker never fires for a
// hot-only declaration: the caller asked for hot and got hot, in both
// spellings.
func TestQueryHotOnlyGateIsNotMarked(t *testing.T) {
	for _, tc := range []struct {
		name string
		fq   *model.FederatedAttributeQuery
	}{
		{"prefer_hot", &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10}, PreferHot: true}},
		{"tiers_hot", &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10}, PreferredTiers: []model.DataTier{model.DataTierHot}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
			engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, hybridEnabledCfg(), nil, "")
			opts := &model.FederatedQueryOptions{}

			page, err := engine.Query(context.Background(), testTables, tc.fq, opts)

			require.NoError(t, err)
			require.Equal(t, 1, pg.queryCalls)
			require.Nil(t, page.Partial)
			require.Nil(t, opts.PartialScan)
		})
	}
}

// TestQueryDisabledDuckDBIsMarkedForRequestedTiers pins the maintainer ruling
// on #468: a disabled engine is a final routing verdict, but the answer is
// still hot-only, so the caller is told which requested tiers went
// unconsulted — the explicit list for a declaration, warm+cold for the
// default form.
func TestQueryDisabledDuckDBIsMarkedForRequestedTiers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tiers []model.DataTier
		want  []model.DataTier
	}{
		{"explicit_hot_cold", []model.DataTier{model.DataTierHot, model.DataTierCold}, []model.DataTier{model.DataTierCold}},
		{"explicit_warm_cold", warmCold, warmCold},
		{"implicit", nil, warmCold},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 2}}
			engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: false}, nil, "")

			page, err := engine.Query(context.Background(), testTables, &model.FederatedAttributeQuery{
				AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
				PreferredTiers: tc.tiers,
			}, nil)

			require.NoError(t, err)
			require.Equal(t, 1, pg.queryCalls)
			require.NotNil(t, page.Partial, "nil options must not drop the page-level marker")
			require.Equal(t, tc.want, page.Partial.UnconsultedTiers)
		})
	}
}

// TestQueryDegradedFallbackIsMarkedHotTierOnly pins the third postgres-only
// path: the §7.2 degraded fallback promised a partial signal that until #468
// lived only on the plan-gated Routing.Reason. It now carries the same
// coverage marker as the routed path, replacing whatever the failed DuckDB
// pass recorded.
func TestQueryDegradedFallbackIsMarkedHotTierOnly(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	duck := &fakeDuckDBExecutor{err: fmt.Errorf("forced duck failure")}
	engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil, hybridEnabledCfg(), testMetadataCacheSchema7(t), "", withTestParquetPath())
	opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true, PartialScan: &model.PartialScan{ExcludedObjects: []string{"stale"}}}

	page, err := engine.Query(context.Background(), testTables, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
	}, opts)

	require.NoError(t, err)
	require.Equal(t, 1, duck.calls)
	require.Equal(t, 1, pg.queryCalls)
	require.NotNil(t, page.Partial)
	require.Equal(t, []model.DataTier{model.DataTierCold}, page.Partial.UnconsultedTiers)
	require.Empty(t, page.Partial.ExcludedObjects, "a stale corrupt-exclusion marker must not survive into a postgres-only answer")
	require.Same(t, page.Partial, opts.PartialScan)
}

// TestQueryDisabledDuckDBPlanTiersAgreeWithMarker pins the execution-plan
// side of the disabled path (PR #525 review): a multi-tier request served
// Postgres-only because the engine is off must report routing.tiers [hot] —
// the route actually taken — and the coverage marker on the same page names
// the rest. The two views of one answer must not disagree.
func TestQueryDisabledDuckDBPlanTiersAgreeWithMarker(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 2}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: false}, nil, "")

	page, err := engine.Query(context.Background(), testTables, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
	}, &model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.NoError(t, err)
	require.NotNil(t, page.ExecutionPlan)
	require.False(t, page.ExecutionPlan.Routing.UseDuckDB)
	require.Equal(t, []model.DataTier{model.DataTierHot}, page.ExecutionPlan.Routing.Tiers,
		"plan must report the hot-only route actually taken, not the declared tiers")
	require.NotNil(t, page.Partial)
	require.Equal(t, warmCold, page.Partial.UnconsultedTiers)
}

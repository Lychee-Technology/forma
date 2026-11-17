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

// The engine's Postgres-only answers: the routed small-page shortcut, a
// disabled engine, the hot-only gate and the degraded fallback. Each reads
// entity_main alone, which holds every live row (design.md §3.3), so none of
// them is marked partial (#715, which retired the #468 hot_tier_only
// marker); the plan, when requested, names the route actually taken.

// TestQueryImplicitTiersSmallPageIsServedPostgresOnly: a query in the
// default all-tier form, the only form the entity query service sends, stays
// on the hybrid small-result shortcut, and the plan says so.
func TestQueryImplicitTiersSmallPageIsServedPostgresOnly(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 3}}
	duck := &fakeDuckDBExecutor{}
	engine := NewDBFederatedQueryEngine(pg, duck, nil, hybridEnabledCfg(), nil, withFakeHotLeg(nil))

	page, err := engine.Query(context.Background(), testTables, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
	}, &model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.NoError(t, err)
	require.Equal(t, 1, pg.queryCalls)
	require.Zero(t, duck.calls)
	require.False(t, page.ExecutionPlan.Routing.UseDuckDB)
	require.Equal(t, []model.DataTier{model.DataTierHot}, page.ExecutionPlan.Routing.Tiers)
	require.Equal(t, "hybrid small result set", page.ExecutionPlan.Routing.Reason)
}

// TestQueryExplicitTiersSmallPageReachesDuckDB is the #468 override at the
// engine seam, which direct engine callers still use: an explicit three-tier
// declaration with an ordinary page size reaches the DuckDB builder and
// leaves postgres untouched.
func TestQueryExplicitTiersSmallPageReachesDuckDB(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 5}}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	engine := newEmptyPageTestEngine(t, pg, duck)
	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}

	page, err := engine.Query(context.Background(), testTables, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
	}, opts)

	require.NoError(t, err)
	require.Zero(t, pg.queryCalls, "an explicit multi-tier request must not be served postgres-only")
	require.Equal(t, 1, duck.calls)
	require.True(t, page.ExecutionPlan.Routing.UseDuckDB)
	require.Contains(t, page.ExecutionPlan.Routing.Reason, "preferred_tiers")
}

// TestQueryHotOnlyGateServesPostgres pins the gate in both spellings.
func TestQueryHotOnlyGateServesPostgres(t *testing.T) {
	for _, tc := range []struct {
		name string
		fq   *model.FederatedAttributeQuery
	}{
		{"prefer_hot", &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10}, PreferHot: true}},
		{"tiers_hot", &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10}, PreferredTiers: []model.DataTier{model.DataTierHot}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
			engine := NewDBFederatedQueryEngine(pg, nil, nil, hybridEnabledCfg(), nil)

			page, err := engine.Query(context.Background(), testTables, tc.fq, &model.FederatedQueryOptions{IncludeExecutionPlan: true})

			require.NoError(t, err)
			require.Equal(t, 1, pg.queryCalls)
			require.Equal(t, "hot-only gate", page.ExecutionPlan.Routing.Reason)
		})
	}
}

// TestQueryDegradedFallbackPlanNamesTheFallback pins the §7.2 fallback's
// visibility once the marker is gone (#715): the answer is the Postgres-only
// read, and the plan, when requested, reports the fallback.
func TestQueryDegradedFallbackPlanNamesTheFallback(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	duck := &fakeDuckDBExecutor{err: fmt.Errorf("forced duck failure")}
	engine := NewDBFederatedQueryEngine(pg, duck, nil, hybridEnabledCfg(), testMetadataCacheSchema7(t), withTestParquetPath(), withFakeHotLeg(nil))
	opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true, IncludeExecutionPlan: true}

	page, err := engine.Query(context.Background(), testTables, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
	}, opts)

	require.NoError(t, err)
	require.Equal(t, 1, duck.calls)
	require.Equal(t, 1, pg.queryCalls)
	require.False(t, page.ExecutionPlan.Routing.UseDuckDB)
	require.Equal(t, "degraded fallback (AllowPartialDegradedMode)", page.ExecutionPlan.Routing.Reason)
}

// TestQueryDisabledDuckDBPlanReportsTheHotRoute pins the execution-plan side
// of the disabled path (PR #525 review): a multi-tier request served
// Postgres-only because the engine is off must report routing.tiers [hot],
// the route actually taken.
func TestQueryDisabledDuckDBPlanReportsTheHotRoute(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 2}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, forma.DuckDBConfig{Enabled: false}, nil)

	page, err := engine.Query(context.Background(), testTables, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
	}, &model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.NoError(t, err)
	require.Equal(t, 1, pg.queryCalls)
	require.NotNil(t, page.ExecutionPlan)
	require.False(t, page.ExecutionPlan.Routing.UseDuckDB)
	require.Equal(t, []model.DataTier{model.DataTierHot}, page.ExecutionPlan.Routing.Tiers,
		"plan must report the hot-only route actually taken, not the declared tiers")
}

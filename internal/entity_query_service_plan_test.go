package internal

import (
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/stretchr/testify/require"
)

// TestToExecutionPlan_NilReturnsNil pins that a query with no recorded plan
// (non-federated, or federated without include_execution_plan) leaves
// QueryResult.ExecutionPlan omitted.
func TestToExecutionPlan_NilReturnsNil(t *testing.T) {
	require.Nil(t, toExecutionPlan(nil))
}

// TestToExecutionPlan_MapsRoutingAndSources pins the model->public projection
// used to surface the route on the HTTP response (#243): routing decision,
// per-tier sources, merge, timings and notes must all carry across so a caller
// can distinguish a DuckDB read from a hot-path read.
func TestToExecutionPlan_MapsRoutingAndSources(t *testing.T) {
	in := &model.ExecutionPlan{
		Routing: model.RoutingDecision{
			UseDuckDB: true,
			Tiers:     []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
			Reason:    "hybrid",
		},
		Sources: []model.DataSourcePlan{
			{Tier: model.DataTierCold, Engine: "duckdb", SQL: "SELECT 1", Params: []string{"7"}, ActualRows: 42, PredicatePushdown: true},
		},
		Merge:   model.MergePlan{Strategy: model.MergeStrategyLastWriteWins, PreferHot: true, DedupKeys: []string{"row_id"}},
		Timings: map[string]int64{"plan_cache_hit": 1},
		Notes:   []string{"EvaluateRoutingPolicy"},
	}

	out := toExecutionPlan(in)
	require.NotNil(t, out)
	require.True(t, out.Routing.UsedDuckDB)
	require.Equal(t, []string{"hot", "warm", "cold"}, out.Routing.Tiers)
	require.Equal(t, "hybrid", out.Routing.Reason)

	require.Len(t, out.Sources, 1)
	require.Equal(t, "cold", out.Sources[0].Tier)
	require.Equal(t, "duckdb", out.Sources[0].Engine)
	require.Equal(t, int64(42), out.Sources[0].ActualRows)
	require.True(t, out.Sources[0].PredicatePushdown)

	require.NotNil(t, out.Merge)
	require.Equal(t, string(model.MergeStrategyLastWriteWins), out.Merge.Strategy)
	require.True(t, out.Merge.PreferHot)

	require.Equal(t, int64(1), out.Timings["plan_cache_hit"])
	require.Contains(t, out.Notes, "EvaluateRoutingPolicy")
}

// TestToExecutionPlan_EmptyMergeOmitted pins that an empty merge plan does not
// produce a hollow merge object on the response.
func TestToExecutionPlan_EmptyMergeOmitted(t *testing.T) {
	out := toExecutionPlan(&model.ExecutionPlan{
		Routing: model.RoutingDecision{UseDuckDB: false, Tiers: []model.DataTier{model.DataTierHot}},
	})
	require.NotNil(t, out)
	require.Nil(t, out.Merge)
	require.False(t, out.Routing.UsedDuckDB)
}

package internal

import (
	"encoding/json"
	"testing"

	"github.com/lychee-technology/forma"
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
// per-tier sources, merge and timings carry across so a caller can distinguish
// a DuckDB read from a hot-path read.
func TestToExecutionPlan_MapsRoutingAndSources(t *testing.T) {
	in := &model.ExecutionPlan{
		Routing: model.RoutingDecision{
			UseDuckDB: true,
			Tiers:     []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
			Reason:    "hybrid",
		},
		Sources: []model.DataSourcePlan{
			{Tier: model.DataTierCold, Engine: "duckdb", ActualRows: 42, PredicatePushdown: true},
		},
		Merge:   model.MergePlan{Strategy: model.MergeStrategyLastWriteWins, PreferHot: true, DedupKeys: []string{"row_id"}},
		Timings: map[string]int64{"plan_cache_hit": 1},
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
}

// TestToExecutionPlan_DoesNotLeakCredentials is the regression guard for the
// P0: the internal DuckDB source SQL embeds the postgres_scan connection string
// (with the DB password) and notes can echo raw errors. The public projection
// must never carry them, so a marshaled response cannot expose the password.
func TestToExecutionPlan_DoesNotLeakCredentials(t *testing.T) {
	const secret = "SuperSecretPassword123"
	in := &model.ExecutionPlan{
		Routing: model.RoutingDecision{UseDuckDB: true, Tiers: []model.DataTier{model.DataTierCold}},
		Sources: []model.DataSourcePlan{{
			Tier:   model.DataTierCold,
			Engine: "duckdb",
			SQL:    "SELECT * FROM postgres_scan('host=db port=5432 user=forma password=" + secret + " dbname=forma', ...)",
			Params: []string{secret, "other"},
		}},
		Notes: []string{"degraded fallback to postgres-only: dial error host=db password=" + secret},
	}

	out := toExecutionPlan(in)
	require.NotNil(t, out)

	blob, err := json.Marshal(out)
	require.NoError(t, err)
	require.NotContains(t, string(blob), secret, "execution plan projection must not leak credentials")
	require.NotContains(t, string(blob), "postgres_scan", "raw SQL must not be projected")
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

// TestToPartialResultProjectsCountOnly pins that the partial-result
// projection surfaces only the reason and excluded-object count; storage
// keys are security-sensitive internals and must never cross the HTTP
// boundary (#301/#306).
func TestToPartialResultProjectsCountOnly(t *testing.T) {
	require.Nil(t, toPartialResult(nil))
	require.Nil(t, toPartialResult(&model.PartialScan{}))

	out := toPartialResult(&model.PartialScan{
		ExcludedObjects: []string{"s3://b/7/rot1.parquet", "s3://b/7/rot2.parquet"},
	})
	require.NotNil(t, out)
	require.Equal(t, forma.PartialReasonCorruptParquetExcluded, out.Reason)
	require.Equal(t, 2, out.ExcludedObjectCount)

	raw, err := json.Marshal(out)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "s3://",
		"storage keys must not cross the HTTP boundary (#301/#306)")
}

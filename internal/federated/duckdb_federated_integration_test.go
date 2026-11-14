package federated

import (
	"context"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestEvaluateRoutingPolicy_VariousStrategies(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled: true,
		Routing: forma.RoutingPolicy{
			Strategy:          forma.RoutingStrategyHybrid,
			MaxDuckDBScanRows: 5000,
			AllowS3Fallback:   true,
		},
	}

	dec := EvaluateRoutingPolicy(cfg, nil, nil)
	require.True(t, dec.UseDuckDB, "hybrid should use duckdb by default")

	fq := &model.FederatedAttributeQuery{PreferHot: true}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.False(t, dec.UseDuckDB, "PreferHot should disable duckdb")

	cfg.Routing.Strategy = forma.RoutingStrategyCostFirst
	dec = EvaluateRoutingPolicy(cfg, nil, &model.FederatedQueryOptions{MaxRows: 100000})
	require.True(t, dec.UseDuckDB, "cost-first large scan should enable duckdb")

	cfg.Enabled = false
	dec = EvaluateRoutingPolicy(cfg, nil, nil)
	require.False(t, dec.UseDuckDB, "disabled config should not use duckdb")
}

func TestEvaluateRoutingPolicy_QueryShapeAware(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled: true,
		Routing: forma.RoutingPolicy{
			Strategy: forma.RoutingStrategyHybrid,
		},
	}

	fq := &model.FederatedAttributeQuery{PreferHot: true}
	dec := EvaluateRoutingPolicy(cfg, fq, nil)
	require.False(t, dec.UseDuckDB)
	require.Equal(t, []model.DataTier{model.DataTierHot}, dec.Tiers)

	fq = &model.FederatedAttributeQuery{PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.True(t, dec.UseDuckDB)

	fq = &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Limit: 20, Offset: 0}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.False(t, dec.UseDuckDB)

	fq = &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Limit: 2000, Offset: 0}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.True(t, dec.UseDuckDB)

	fq = &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Limit: 20, Offset: 10000}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.True(t, dec.UseDuckDB)

	dec = EvaluateRoutingPolicy(cfg, nil, nil)
	require.True(t, dec.UseDuckDB)

	fq = &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Limit: 50, Offset: 0}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.False(t, dec.UseDuckDB)

	fq = &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Limit: 10000, Offset: 0}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.True(t, dec.UseDuckDB)

	fq = &model.FederatedAttributeQuery{PreferHot: true, AttributeQuery: model.AttributeQuery{Limit: 10000, Offset: 0}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.False(t, dec.UseDuckDB)
}

func TestNewDuckDBClient_HealthCheck(t *testing.T) {
	skipIfNoDuckDB(t)
	client, err := NewDuckDBClient(forma.DuckDBConfig{Enabled: true, DBPath: ":memory:"})
	require.NoError(t, err)
	defer client.Close()

	err = client.HealthCheck(context.Background())
	require.NoError(t, err)
}

func TestExecuteDuckDBFederatedQuery_NilQuery(t *testing.T) {
	engine := &DBFederatedQueryEngine{}
	_, _, err := engine.ExecuteDuckDBFederatedQuery(context.Background(), model.StorageTables{}, nil, 10, 0, nil, nil)
	require.Error(t, err)
}

// TestRecordScanOutcome_CaptureDisabled: with no plan requested the plan
// recorder is a no-op.
func TestRecordScanOutcome_CaptureDisabled(t *testing.T) {
	opts := &model.FederatedQueryOptions{}
	planCtx := newDuckDBExecutionPlanContext(opts, time.Now)
	outcome := duckDBScanOutcome{executeMs: 1, streamMs: 2, rowCount: 3, dirtyRows: 5}
	require.NotPanics(t, func() {
		planCtx.recordScanOutcome(outcome)
	})
	require.Nil(t, opts.ExecutionPlan)
}

func skipIfNoDuckDB(t *testing.T) {
	t.Helper()
}

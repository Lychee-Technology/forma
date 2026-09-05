package federated

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

func hybridEnabledCfg() forma.DuckDBConfig {
	return forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}
}

// TestEvaluateRoutingPolicyExplicitTiersOverrideSmallResultHeuristic pins the
// #468 override: an explicit multi-tier PreferredTiers is the caller's
// coverage declaration, and like the #354 keyset cursor it outranks the
// hybrid "small result set" cost heuristic. Without it every ordinary API
// page (items_per_page <= 100) is served from entity_main alone and the
// declaration is silently ignored.
func TestEvaluateRoutingPolicyExplicitTiersOverrideSmallResultHeuristic(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tiers []model.DataTier
	}{
		{"all_three", []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold}},
		{"hot_and_cold", []model.DataTier{model.DataTierHot, model.DataTierCold}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dec := EvaluateRoutingPolicy(hybridEnabledCfg(), &model.FederatedAttributeQuery{
				AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
				PreferredTiers: tc.tiers,
			}, nil)

			require.True(t, dec.UseDuckDB, "explicit multi-tier preferred_tiers must reach the federated path (#468)")
			require.Contains(t, dec.Reason, "hybrid small result set", "the heuristic verdict stays visible")
			require.Contains(t, dec.Reason, "preferred_tiers", "the override must be visible in the plan, not silent")
			require.Equal(t, tc.tiers, dec.Tiers, "the decision must keep the declared tiers, not collapse to hot")
		})
	}
}

// TestEvaluateRoutingPolicyImplicitTiersKeepSmallResultHeuristic pins the
// other half of #468: an omitted (empty) PreferredTiers is the default
// all-tier form, not a declaration, so the cost heuristic still wins and the
// caller is told through the coverage marker instead (engine-level tests).
func TestEvaluateRoutingPolicyImplicitTiersKeepSmallResultHeuristic(t *testing.T) {
	dec := EvaluateRoutingPolicy(hybridEnabledCfg(), &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
	}, nil)

	require.False(t, dec.UseDuckDB, "an implicit request keeps the small-result shortcut")
	require.Equal(t, "hybrid small result set", dec.Reason)
	require.Equal(t, []model.DataTier{model.DataTierHot}, dec.Tiers)
}

// TestEvaluateRoutingPolicyExplicitTiersDoNotOverrideHotOnly pins that the
// override never reinterprets a hot-only declaration: both spellings are
// semantic choices, and a single-element [hot] list is explicit but asks for
// nothing the Postgres path cannot serve.
func TestEvaluateRoutingPolicyExplicitTiersDoNotOverrideHotOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		fq   *model.FederatedAttributeQuery
	}{
		{"prefer_hot_with_tiers", &model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			PreferHot:      true,
			PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
		}},
		{"tiers_hot_only", &model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			PreferredTiers: []model.DataTier{model.DataTierHot},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dec := EvaluateRoutingPolicy(hybridEnabledCfg(), tc.fq, nil)
			require.False(t, dec.UseDuckDB, "a hot-only declaration is never rerouted")
			require.NotContains(t, dec.Reason, "overridden")
			require.Equal(t, []model.DataTier{model.DataTierHot}, dec.Tiers)
		})
	}
}

// TestEvaluateRoutingPolicyExplicitTiersDoNotOverrideDisabledDuckDB pins that
// the override is a cost-heuristic override only: a disabled engine is a
// final verdict, and the caller learns about the missing tiers through the
// coverage marker, not by being routed onto an engine that is not there.
func TestEvaluateRoutingPolicyExplicitTiersDoNotOverrideDisabledDuckDB(t *testing.T) {
	cfg := forma.DuckDBConfig{Enabled: false, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}
	dec := EvaluateRoutingPolicy(cfg, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
	}, nil)

	require.False(t, dec.UseDuckDB)
	require.Equal(t, "duckdb disabled", dec.Reason)
}

// TestEvaluateRoutingPolicyExplicitTiersLeaveFederatedReasonIntact pins the
// truthfulness gate shared with #354: a decision that already chose DuckDB
// was not overridden, so the plan must not say it was.
func TestEvaluateRoutingPolicyExplicitTiersLeaveFederatedReasonIntact(t *testing.T) {
	dec := EvaluateRoutingPolicy(hybridEnabledCfg(), &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
	}, nil)

	require.True(t, dec.UseDuckDB)
	require.Equal(t, "hybrid use duckdb", dec.Reason)
}

// TestEvaluateRoutingPolicyExplicitTiersOverrideCostFirstSmallScan covers the
// non-default strategy that also parks small scans on Postgres by omission:
// under cost-first a small scan never trips the "large scan" branch, and with
// PreferHot the hot-only branch fires. Neither may swallow an explicit
// multi-tier declaration made without PreferHot.
func TestEvaluateRoutingPolicyExplicitTiersOverrideCostFirstSmallScan(t *testing.T) {
	cfg := forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyCostFirst, MaxDuckDBScanRows: 100000}}
	dec := EvaluateRoutingPolicy(cfg, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
	}, nil)

	require.True(t, dec.UseDuckDB, "cost-first leaves UseDuckDB at cfg.Enabled for small scans; explicit tiers must not lose that")
	require.Equal(t, "default", dec.Reason, "nothing overrode a decision that already chose DuckDB")
}

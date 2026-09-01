package federated

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// TestHotOnlyHeuristicStaysPreferHotOnly pins #381 item 5. applyStrategyHeuristics
// uses a LOCAL hotOnly that means PreferHot only, deliberately narrower than the
// shared isHotOnlyRequest (which also counts PreferredTiers == [hot]). The
// asymmetry is correct but was guarded only by a comment: a mutation widening
// hotOnly survived the whole package during the #354 review, because nothing
// pinned the shape it would change.
//
// This is that shape. Under freshness-first and cost-first with a high scan-size
// threshold, a cursor-free request with PreferredTiers == [hot] and PreferHot false
// hits no strategy branch, so UseDuckDB keeps the default cfg.Enabled and Reason
// stays "default". Widening hotOnly to include PreferredTiers == [hot] would force
// UseDuckDB to false and reroute the query.
func TestHotOnlyHeuristicStaysPreferHotOnly(t *testing.T) {
	for _, strategy := range []forma.RoutingStrategy{
		forma.RoutingStrategyFreshnessFirst,
		forma.RoutingStrategyCostFirst,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			cfg := forma.DuckDBConfig{Enabled: true}
			cfg.Routing.Strategy = strategy
			cfg.Routing.MaxDuckDBScanRows = 100000 // High threshold ensures no scan-size branch fires

			dec := EvaluateRoutingPolicy(cfg, &model.FederatedAttributeQuery{
				AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
				PreferredTiers: []model.DataTier{model.DataTierHot},
			}, nil)

			require.True(t, dec.UseDuckDB,
				"PreferredTiers:[hot] without PreferHot must not trip the strategy hot-only branch")
			require.Equal(t, "default", dec.Reason,
				"no strategy branch fires for hot-only without PreferHot; widening the local hotOnly would change this and reroute cursor-free queries")

			decPreferHot := EvaluateRoutingPolicy(cfg, &model.FederatedAttributeQuery{
				AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
				PreferHot:      true,
			}, nil)
			require.False(t, decPreferHot.UseDuckDB,
				"PreferHot is what the strategy heuristics react to")
			expectedReason := map[forma.RoutingStrategy]string{
				forma.RoutingStrategyFreshnessFirst: "prefer hot",
				forma.RoutingStrategyCostFirst:      "cost-first prefer hot",
			}[strategy]
			require.Equal(t, expectedReason, decPreferHot.Reason,
				"PreferHot triggers the strategy-specific hot-only branch")
		})
	}
}

// TestEvaluateRoutingPolicyDisagreesWithEngineOnHotOnlyCursor pins #381 item 8.
// For PreferredTiers == [hot] plus a cursor, the exported EvaluateRoutingPolicy
// answers UseDuckDB: true. engine.Query on the same input answers the opposite:
// the isHotOnlyRequest gate intercepts before the policy runs, routes to the
// postgres-only path, and that path refuses the cursor.
//
// There is no live bug — the gate dominates every reachable path — but the
// exported contract genuinely disagrees with the engine, and a standalone
// caller of the exported function must know it. Pinned so the disagreement is
// a documented decision rather than a discovery.
func TestEvaluateRoutingPolicyDisagreesWithEngineOnHotOnlyCursor(t *testing.T) {
	cfg := forma.DuckDBConfig{Enabled: true}
	cfg.Routing.Strategy = forma.RoutingStrategyFreshnessFirst
	opts := &model.FederatedQueryOptions{}

	newQuery := func() *model.FederatedAttributeQuery {
		return &model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			PreferredTiers: []model.DataTier{model.DataTierHot},
			KeysetCursor:   keysetCursorAfterCreatedAt(),
		}
	}

	dec := EvaluateRoutingPolicy(cfg, newQuery(), opts)
	require.True(t, dec.UseDuckDB,
		"the exported policy says the federated path serves this cursor")

	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, cfg, nil, "")
	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav"},
		newQuery(), opts)

	require.Nil(t, page)
	require.ErrorIs(t, err, ErrKeysetUnsupportedOnPostgres,
		"the engine says the opposite for the same input: the hot-only gate reaches postgres-only, which cannot apply a cursor")
	require.Zero(t, pg.queryCalls,
		"fail closed: postgres must not be queried at all, or the engine pays for a page the caller must never see")
}

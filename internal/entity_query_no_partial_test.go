package internal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/stretchr/testify/require"
)

// TestQueryResultCarriesNoPartialMarker pins #715's removal of the
// hot_tier_only marker: under the storage invariant PostgreSQL holds every
// live row, so an answer the engine served from PostgreSQL alone, routed
// there or degraded, is complete for current state. With routing the
// engine's own, the marker would otherwise stamp ordinary answers. Neither
// the result nor its JSON carries a partial field; a degraded answer stays
// visible through the plan's routing reason when the plan is requested.
func TestQueryResultCarriesNoPartialMarker(t *testing.T) {
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	require.NoError(t, err)
	engine := &mockFederatedQueryEngine{}
	engine.queryFunc = func(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions) (*model.PersistentRecordPage, error) {
		return &model.PersistentRecordPage{CurrentPage: 1, ExecutionPlan: &model.ExecutionPlan{
			Routing: model.RoutingDecision{Tiers: []model.DataTier{model.DataTierHot}, Reason: "degraded fallback (AllowPartialDegradedMode)"},
		}}, nil
	}
	config := createTestConfig()
	config.DuckDB.Enabled = true
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry),
		newMockPersistentRecordRepository(), engine, registry, config, nil)

	result, err := em.Query(context.Background(), &forma.QueryRequest{
		SchemaName: "visit", Page: 1, ItemsPerPage: 10, IncludeExecutionPlan: true,
	})
	require.NoError(t, err)
	require.NotNil(t, result.ExecutionPlan)
	require.Equal(t, "degraded fallback (AllowPartialDegradedMode)", result.ExecutionPlan.Routing.Reason)

	raw, err := json.Marshal(result)
	require.NoError(t, err)
	require.NotContains(t, string(raw), `"partial"`)
	require.NotContains(t, string(raw), `unconsulted_tiers`)
}

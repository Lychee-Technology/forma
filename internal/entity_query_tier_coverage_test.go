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

// TestFederatedPreferredTiersForwardsOmissionAsNil pins the explicitness
// contract #468 relies on: the service must not fill the default three tiers
// for a caller that omitted preferred_tiers (or named only unknown tiers),
// because the engine treats empty as the default all-tier form (#184) and a
// filled list is indistinguishable from an explicit declaration.
func TestFederatedPreferredTiersForwardsOmissionAsNil(t *testing.T) {
	require.Nil(t, federatedPreferredTiers(nil))
	require.Nil(t, federatedPreferredTiers([]string{}))
	require.Nil(t, federatedPreferredTiers([]string{"lukewarm"}), "unknown tiers alone are not a declaration")
	require.Equal(t, []model.DataTier{model.DataTierHot, model.DataTierCold},
		federatedPreferredTiers([]string{"hot", "lukewarm", "cold"}), "known tiers survive, unknown ones are dropped")
}

// TestQueryForwardsOmittedPreferredTiersAsNil is the wiring leg: through
// EntityManager.Query an omitted list must reach the engine as nil.
func TestQueryForwardsOmittedPreferredTiersAsNil(t *testing.T) {
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	require.NoError(t, err)

	engine := &mockFederatedQueryEngine{}
	engine.queryFunc = func(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions) (*model.PersistentRecordPage, error) {
		return &model.PersistentRecordPage{CurrentPage: 1}, nil
	}
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry),
		newMockPersistentRecordRepository(), engine, registry, createTestConfig(), nil)

	_, err = em.Query(context.Background(), &forma.QueryRequest{
		SchemaName: "visit", Page: 1, ItemsPerPage: 10,
		Federated: &forma.FederatedQueryRequest{Enabled: true},
	})
	require.NoError(t, err)
	require.NotNil(t, engine.lastQuery)
	require.Nil(t, engine.lastQuery.PreferredTiers, "an omitted preferred_tiers must not be filled by the service (#468)")
}

// TestToPartialResultProjectsUnconsultedTiers pins the public projection of
// the #468 coverage marker: reason hot_tier_only plus the unconsulted tier
// names, no excluded-object count.
func TestToPartialResultProjectsUnconsultedTiers(t *testing.T) {
	out := toPartialResult(&model.PartialScan{
		UnconsultedTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
	})
	require.NotNil(t, out)
	require.Equal(t, forma.PartialReasonHotTierOnly, out.Reason)
	require.Equal(t, []string{"warm", "cold"}, out.UnconsultedTiers)
	require.Zero(t, out.ExcludedObjectCount)

	raw, err := json.Marshal(out)
	require.NoError(t, err)
	require.JSONEq(t, `{"reason":"hot_tier_only","unconsulted_tiers":["warm","cold"]}`, string(raw))
}

// TestQueryResultCarriesHotTierOnlyMarker is the end-to-end service leg for
// the coverage marker, mirroring the #348 wiring test.
func TestQueryResultCarriesHotTierOnlyMarker(t *testing.T) {
	result := queryWithFederatedPartial(t, &model.PartialScan{
		UnconsultedTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
	})

	require.NotNil(t, result.Partial)
	require.Equal(t, forma.PartialReasonHotTierOnly, result.Partial.Reason)
	require.Equal(t, []string{"warm", "cold"}, result.Partial.UnconsultedTiers)

	blob, err := json.Marshal(result)
	require.NoError(t, err)
	require.Contains(t, string(blob), `"partial":{"reason":"hot_tier_only","unconsulted_tiers":["warm","cold"]}`)
}

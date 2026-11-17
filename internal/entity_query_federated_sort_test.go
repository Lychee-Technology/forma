package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/transform"
)

// TestValidateFederatedSortRefusesListAttributes pins #853's decision on list
// sort keys: a federated query that sorts on a list attribute is refused as
// invalid input before routing, because the hot leg's comparator has no
// DuckDB LIST order. A scalar sort is left alone.
func TestValidateFederatedSortRefusesListAttributes(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"tags":  {AttributeName: "tags", AttributeID: 1, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeText},
		"count": {AttributeName: "count", AttributeID: 2, ValueType: forma.ValueTypeNumeric},
	}
	check := func(attr string) error {
		req := &forma.QueryRequest{SchemaName: "s", Sort: []forma.OrderBy{{Attribute: attr}}}
		orders, err := buildAttributeOrders(req, cache)
		require.NoError(t, err)
		return validateFederatedSort(req, orders)
	}

	err := check("tags")
	require.Error(t, err)
	require.True(t, errors.Is(err, forma.ErrInvalidInput), "a list sort on the federated route is a 4xx, got %v", err)
	require.Contains(t, err.Error(), "list attribute 'tags'")

	require.NoError(t, check("count"), "a scalar sort stays federated")
}

// TestQueryListSortFollowsTheDeployment: whether a list sort is refused is
// the deployment's route, not the request's (#715). A deployment that runs
// Query through the federated engine refuses it before the engine is
// consulted; one without DuckDB keeps it on PostgreSQL.
func TestQueryListSortFollowsTheDeployment(t *testing.T) {
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	require.NoError(t, err)
	req := func() *forma.QueryRequest {
		return &forma.QueryRequest{SchemaName: "visit", Page: 1, ItemsPerPage: 10, Sort: []forma.OrderBy{{Attribute: "attendees"}}}
	}

	for _, duckdb := range []bool{true, false} {
		config := createTestConfig()
		config.DuckDB.Enabled = duckdb
		repo := newMockPersistentRecordRepository()
		engine := &mockFederatedQueryEngine{}
		em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), repo, engine, registry, config, nil)

		_, err := em.Query(context.Background(), req())
		require.Nil(t, engine.lastQuery, "duckdb=%v: the engine must not be consulted", duckdb)
		if duckdb {
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			continue
		}
		require.NoError(t, err)
		require.NotNil(t, repo.lastQuery, "the PostgreSQL route keeps its list sort")
	}
}

package internal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// TestValidateFederatedSortRefusesListAttributes pins #853's decision on list
// sort keys: a federated request that sorts on a list attribute is refused as
// invalid input before routing, because the hot leg's comparator has no
// DuckDB LIST order. A scalar sort, and a list sort without the federated
// flag, are left alone.
func TestValidateFederatedSortRefusesListAttributes(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"tags":  {AttributeName: "tags", AttributeID: 1, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeText},
		"count": {AttributeName: "count", AttributeID: 2, ValueType: forma.ValueTypeNumeric},
	}
	request := func(attr string, federated bool) *forma.QueryRequest {
		req := &forma.QueryRequest{SchemaName: "s", Sort: []forma.OrderBy{{Attribute: attr}}}
		if federated {
			req.Federated = &forma.FederatedQueryRequest{Enabled: true}
		}
		return req
	}
	check := func(req *forma.QueryRequest) error {
		orders, err := buildAttributeOrders(req, cache)
		require.NoError(t, err)
		return validateFederatedSort(req, orders)
	}

	err := check(request("tags", true))
	require.Error(t, err)
	require.True(t, errors.Is(err, forma.ErrInvalidInput), "a list sort on the federated route is a 4xx, got %v", err)
	require.Contains(t, err.Error(), "list attribute 'tags'")

	require.NoError(t, check(request("count", true)), "a scalar sort stays federated")
	require.NoError(t, check(request("tags", false)), "the PostgreSQL route keeps its list sort")
}

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

// queryWithFederatedPartial drives EntityManager.Query over a fake federated
// engine that answers with the given partial marker, and returns the public
// result. It exists so the two legs below differ only in the marker.
func queryWithFederatedPartial(t *testing.T, partial *model.PartialScan) *forma.QueryResult {
	t.Helper()

	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	require.NoError(t, err, "build schema registry")

	engine := &mockFederatedQueryEngine{}
	engine.queryFunc = func(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions) (*model.PersistentRecordPage, error) {
		return &model.PersistentRecordPage{TotalRecords: 0, TotalPages: 0, CurrentPage: 1, Partial: partial}, nil
	}

	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry),
		newMockPersistentRecordRepository(), engine, registry, createTestConfig(), nil)

	result, err := em.Query(context.Background(), &forma.QueryRequest{
		SchemaName:   "visit",
		Page:         1,
		ItemsPerPage: 10,
		Federated:    &forma.FederatedQueryRequest{Enabled: true},
	})
	require.NoError(t, err, "federated query")
	require.NotNil(t, result)
	return result
}

// TestQueryResultOmitsPartialMarkerForCleanPage is the omitempty leg: a page
// with no partial marker must not grow a hollow `partial` object on every
// ordinary response.
func TestQueryResultOmitsPartialMarkerForCleanPage(t *testing.T) {
	result := queryWithFederatedPartial(t, nil)

	require.Nil(t, result.Partial)

	blob, err := json.Marshal(result)
	require.NoError(t, err)
	require.NotContains(t, string(blob), `"partial"`,
		"a clean page must omit the marker entirely")
}

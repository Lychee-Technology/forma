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

// TestQueryResultCarriesPartialMarkerFromEngine pins the service-level wiring
// of the #348 public partial marker: the engine reports the reduced scan on the
// page, and entityQueryService.Query must project it onto forma.QueryResult.
// Without this the projection function is unit-tested but nothing observes that
// it is actually called — deleting the `Partial:` field from the QueryResult
// literal leaves every other test green while the marker silently disappears
// from every HTTP response.
//
// The marshal legs are load-bearing in both directions: the `partial` JSON tag
// is the caller's only view of the field, and the excluded storage keys must
// not ride along with it (#301/#306).
func TestQueryResultCarriesPartialMarkerFromEngine(t *testing.T) {
	result := queryWithFederatedPartial(t, &model.PartialScan{
		ExcludedObjects: []string{"s3://b/7/rot1.parquet"},
	})

	require.NotNil(t, result.Partial, "a partial engine page must surface a partial result marker")
	require.Equal(t, forma.PartialReasonCorruptParquetExcluded, result.Partial.Reason)
	require.Equal(t, 1, result.Partial.ExcludedObjectCount)

	blob, err := json.Marshal(result)
	require.NoError(t, err)
	require.Contains(t, string(blob), `"partial"`,
		"the marker must reach the wire under its published key")
	require.NotContains(t, string(blob), "s3://",
		"storage keys must not cross the HTTP boundary (#301/#306)")
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

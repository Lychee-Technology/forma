package federated

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// The PostgreSQL-outage half of the Query contract (#635). A federated query
// needs PostgreSQL for the hot rows and for the dirty set that keeps a
// stale or deleted parquet version out of the answer, so with PostgreSQL
// down every route fails, and no parquet scan answers in its place.
// AllowPartialDegradedMode does not change that: its fallback is itself a
// PostgreSQL read. No request option, and since #715 no request carries
// any, permits an answer without PostgreSQL.
func TestPostgresOutageFailsEveryFederatedRoute(t *testing.T) {
	routes := []struct {
		name string
		fq   func() *model.FederatedAttributeQuery
	}{
		{"duckdb route", coldTierQuery},
		{"routed postgres-only page", func() *model.FederatedAttributeQuery {
			return &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10}}
		}},
		{"hot-only gate", func() *model.FederatedAttributeQuery {
			return &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10}, PreferHot: true}
		}},
	}
	for _, route := range routes {
		for _, degraded := range []bool{false, true} {
			name := fmt.Sprintf("%s/degraded=%t", route.name, degraded)
			t.Run(name, func(t *testing.T) {
				restore := initTestDescriptors()
				defer restore()

				down := fmt.Errorf("dial tcp 127.0.0.1:5432: connect: connection refused")
				pg := &fakePostgresFederatedSource{err: down}
				duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
				engine := NewDBFederatedQueryEngine(pg, duck, nil,
					hybridDuckConfig(), testMetadataCacheSchema7(t), withTestParquetPath(),
					withFakeHotLeg(&fakeHotLeg{changeLogErr: down}))
				opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: degraded}

				page, err := engine.Query(context.Background(), testTables, route.fq(), opts)

				require.ErrorIs(t, err, ErrPostgresReadFailed)
				require.Nil(t, page)
				require.Zero(t, duck.calls, "no parquet scan may answer in PostgreSQL's place")
			})
		}
	}
}

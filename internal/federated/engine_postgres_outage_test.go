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
// needs PostgreSQL for the hot rows and for the dirty-ID set that keeps a
// stale or deleted parquet version out of the answer, so with PostgreSQL
// down every route fails, and no parquet scan answers in its place.
// AllowPartialDegradedMode does not change that: its fallback is itself a
// PostgreSQL read. ConsistencyMode is forwarded to the engine but read by
// nothing, and "eventual" in particular is not permission to skip
// PostgreSQL.
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
	modes := []model.ConsistencyMode{"", model.ConsistencyModeStrict, model.ConsistencyModeEventual}

	for _, route := range routes {
		for _, degraded := range []bool{false, true} {
			for _, mode := range modes {
				name := fmt.Sprintf("%s/degraded=%t/consistency_mode=%q", route.name, degraded, mode)
				t.Run(name, func(t *testing.T) {
					restore := initTestDescriptors()
					defer restore()

					down := fmt.Errorf("dial tcp 127.0.0.1:5432: connect: connection refused")
					pg := &fakePostgresFederatedSource{err: down}
					dirty := &fakeDirtyIDFetcher{err: down}
					duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
					engine := NewDBFederatedQueryEngine(pg, dirty, duck, nil,
						hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", withTestParquetPath())
					opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: degraded, ConsistencyMode: mode}

					page, err := engine.Query(context.Background(), testTables, route.fq(), opts)

					require.ErrorIs(t, err, ErrPostgresReadFailed)
					require.Nil(t, page)
					require.Zero(t, duck.calls, "no parquet scan may answer in PostgreSQL's place")
					require.Nil(t, opts.PartialScan, "a failed query leaves no partial marker")
				})
			}
		}
	}
}

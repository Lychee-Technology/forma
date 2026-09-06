package federated

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/model"
)

// nonPositiveSchemaQuery is coldTierQuery() re-targeted at an ID that can
// never name a schema.
func nonPositiveSchemaQuery(schemaID int16) *model.FederatedAttributeQuery {
	fq := coldTierQuery()
	fq.SchemaID = schemaID
	return fq
}

// TestQuery_NonPositiveSchemaIDIsNotDegradable pins the PR #537 review
// finding: a request for schema 0 or a negative ID is a caller invariant
// violation, and VerifySchemaStamp reports it as a plain error (#536) that
// resolveParquetPaths would relabel ErrFederatedReadFailed — degradable. With
// AllowPartialDegradedMode the invalid request would then be served as a
// Postgres-only page. The engine refuses it at entry instead: no source is
// consulted, no DuckDB pass runs, and no Postgres-only fallback answers.
func TestQuery_NonPositiveSchemaIDIsNotDegradable(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	for _, schemaID := range []int16{0, -1} {
		t.Run(fmt.Sprintf("schema_%d", schemaID), func(t *testing.T) {
			// The source answers the way the manifest package does for this
			// ID: a plain, unclassified error. If the guard regressed, this is
			// what the degraded fallback would absorb.
			src := &fakeParquetSource{pathsErr: fmt.Errorf(
				"manifest manifest/%d.json: requested schema id %d is not a schema (schema IDs are positive); refusing to verify the stamp",
				schemaID, schemaID)}
			duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
			pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 3}}
			engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
				hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))
			opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true, IncludeExecutionPlan: true}

			page, err := engine.Query(context.Background(),
				model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
				nonPositiveSchemaQuery(schemaID), opts)

			require.Nil(t, page)
			require.ErrorContains(t, err, "schema id must be positive")
			require.NotErrorIs(t, err, ErrFederatedReadFailed,
				"an invalid schema id is the caller's error, not a read failure to degrade around")
			require.Equal(t, 0, pg.queryCalls, "degraded mode must not serve an invalid schema id as a Postgres-only page")
			require.Equal(t, 0, duck.calls)
			require.Equal(t, 0, src.pathsCalls, "the guard runs before the parquet source is consulted")
			require.Nil(t, opts.ExecutionPlan, "refused before routing: no plan is recorded")
		})
	}
}

// TestExecuteFederatedPaginatedQuery_NonPositiveSchemaIDRefused pins the same
// guard on the second entry point: the paginated coordinator shares the
// degraded fallback through its keyset and ordered paths, so it must refuse
// the same request at entry rather than rely on the Postgres leg to notice.
func TestExecuteFederatedPaginatedQuery_NonPositiveSchemaIDRefused(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	for _, schemaID := range []int16{0, -1} {
		t.Run(fmt.Sprintf("schema_%d", schemaID), func(t *testing.T) {
			src := &fakeParquetSource{paths: []string{"s3://b/1/a.parquet"}}
			duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
			pg := &fakePostgresFederatedSource{}
			engine := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
				hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))

			records, total, err := engine.ExecuteFederatedPaginatedQuery(context.Background(),
				model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
				nonPositiveSchemaQuery(schemaID), 10, 0, nil,
				&model.FederatedQueryOptions{AllowPartialDegradedMode: true})

			require.Nil(t, records)
			require.Zero(t, total)
			require.ErrorContains(t, err, "schema id must be positive")
			require.NotErrorIs(t, err, ErrFederatedReadFailed)
			require.Equal(t, 0, pg.queryCalls)
			require.Equal(t, 0, pg.runOptimizedCalls)
			require.Equal(t, 0, duck.calls)
			require.Equal(t, 0, src.pathsCalls)
		})
	}
}

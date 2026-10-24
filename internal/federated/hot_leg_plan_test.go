package federated

import (
	"context"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// TestHotLegPredicateIsNotReportedAsPushdown pins #631: the hot leg reads
// PostgreSQL through postgres_scan, which sends no predicate, so DuckDB
// evaluates the main-table clause on the copied tables. The execution plan
// must still show that clause, and no source may claim it was pushed down.
func TestHotLegPredicateIsNotReportedAsPushdown(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &sequencedDuckDBExecutor{rows: []duckDBRowsIterator{&emptyDuckDBRows{}}}
	var built []model.FederatedAttributeQuery
	engine := newEmptyPageTestEngine(t, &fakePostgresFederatedSource{}, duck, &built)
	opts := &model.FederatedQueryOptions{
		IncludeExecutionPlan: true,
		ExecutionPlan:        &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}},
	}

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{
				SchemaID:  7,
				Condition: &forma.KvCondition{Attr: "age", Value: "gt:18"},
				Limit:     10,
			},
			PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
		}, opts)
	require.NoError(t, err)
	require.True(t, opts.ExecutionPlan.Routing.UseDuckDB, "precondition: the query must render the DuckDB template")

	var hotLeg *model.DataSourcePlan
	for i, src := range opts.ExecutionPlan.Sources {
		require.Falsef(t, src.PredicatePushdown, "source %q claims a pushdown the hot leg does not do", src.Reason)
		if src.Reason == hotLegPredicateReason {
			hotLeg = &opts.ExecutionPlan.Sources[i]
		}
	}
	require.NotNil(t, hotLeg, "the plan must record the hot leg's main-table predicate")
	require.Equal(t, "postgres", hotLeg.Engine)
	require.Equal(t, model.DataTierHot, hotLeg.Tier)
	require.Contains(t, hotLeg.SQL, "integer_01", "the source must carry the column-bound clause DuckDB evaluates")
}

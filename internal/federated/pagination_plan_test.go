package federated

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// TestExecuteFederatedPaginatedQueryRecordsExecutionPlan is the first coverage
// ExecuteFederatedPaginatedQuery has ever had: its only caller is the keyset
// benchmark path (benchmark/execute.go), so neither the unit suite nor the
// federated e2e suite reached it. #319 splits its two plan-recording blocks
// out for headroom, and this is what makes that split verifiable.
func TestExecuteFederatedPaginatedQueryRecordsExecutionPlan(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &sequencedDuckDBExecutor{rows: []duckDBRowsIterator{&emptyDuckDBRows{}}}
	var built []model.FederatedAttributeQuery
	engine := newEmptyPageTestEngine(t, &fakePostgresFederatedSource{}, duck, &built)

	opts := &model.FederatedQueryOptions{
		IncludeExecutionPlan: true,
		ExecutionPlan:        &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}},
	}

	recs, total, err := engine.ExecuteFederatedPaginatedQuery(
		context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7},
		},
		10, 0, nil, opts)

	require.NoError(t, err)
	require.Empty(t, recs)
	require.Zero(t, total)

	// The Postgres source is recorded with the hybrid WHERE clause the
	// coordinator can see, not the full optimized SQL rendered downstream.
	// It is the FIRST entry: the downstream ExecuteDuckDBFederatedQuery call
	// appends its own source entries (dirty id set, pushdown fragment, duckdb
	// template) onto the same plan afterwards, so the slice is not length 1.
	require.NotEmpty(t, opts.ExecutionPlan.Sources)
	var optimizedQuerySources int
	for _, src := range opts.ExecutionPlan.Sources {
		if src.Reason == "postgres optimized query" {
			optimizedQuerySources++
		}
	}
	require.Equal(t, 1, optimizedQuerySources,
		"the coordinator records exactly one postgres optimized query source: %+v",
		opts.ExecutionPlan.Sources)
	require.Equal(t, model.DataTierHot, opts.ExecutionPlan.Sources[0].Tier)
	require.Equal(t, "postgres", opts.ExecutionPlan.Sources[0].Engine)
	require.Equal(t, "1=1", opts.ExecutionPlan.Sources[0].SQL)
	require.Equal(t, "postgres optimized query", opts.ExecutionPlan.Sources[0].Reason)
	require.Contains(t, opts.ExecutionPlan.Timings, "postgres_fetch")

	require.Equal(t, model.MergeStrategyLastWriteWins, opts.ExecutionPlan.Merge.Strategy)
	require.Equal(t, []string{"SchemaID:RowID"}, opts.ExecutionPlan.Merge.DedupKeys)
	require.Contains(t, opts.ExecutionPlan.Timings, "merge")
}

// TestExecuteFederatedPaginatedQuerySkipsPlanWhenNotRequested guards the other
// half: without IncludeExecutionPlan the recorders must stay silent rather
// than write into a plan the caller never asked for.
func TestExecuteFederatedPaginatedQuerySkipsPlanWhenNotRequested(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &sequencedDuckDBExecutor{rows: []duckDBRowsIterator{&emptyDuckDBRows{}}}
	var built []model.FederatedAttributeQuery
	engine := newEmptyPageTestEngine(t, &fakePostgresFederatedSource{}, duck, &built)

	opts := &model.FederatedQueryOptions{
		ExecutionPlan: &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}},
	}

	_, _, err := engine.ExecuteFederatedPaginatedQuery(
		context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7},
		},
		10, 0, nil, opts)

	require.NoError(t, err)
	require.Empty(t, opts.ExecutionPlan.Sources)
	require.Empty(t, opts.ExecutionPlan.Timings)
}

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
			// PreferHot is the one value the #319 extraction actually
			// rewired: it went from an in-body fq.PreferHot reference to an
			// argument passed at the call site. Setting it true (rather than
			// leaving the zero value) is what makes the assertion below able
			// to tell a correct hand-off from a dropped or inverted one.
			PreferHot: true,
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
	// These index into Sources[0] rather than into the entry matched by Reason
	// above, which couples them to append order: ExecuteDuckDBFederatedQuery
	// appends three further sources (dirty id set, pushdown fragment, duckdb
	// template) onto this same opts after the coordinator records the postgres
	// one, so index 0 is the postgres entry only because it is recorded first.
	require.Equal(t, model.DataTierHot, opts.ExecutionPlan.Sources[0].Tier)
	require.Equal(t, "postgres", opts.ExecutionPlan.Sources[0].Engine)
	require.Equal(t, "1=1", opts.ExecutionPlan.Sources[0].SQL)
	require.Equal(t, "postgres optimized query", opts.ExecutionPlan.Sources[0].Reason)
	// Both are deterministic in this fixture: RunOptimizedQuery returns no
	// records, and BuildHybridConditions returns no args.
	require.Zero(t, opts.ExecutionPlan.Sources[0].ActualRows)
	require.Empty(t, opts.ExecutionPlan.Sources[0].Params)
	require.Contains(t, opts.ExecutionPlan.Timings, "postgres_fetch")

	require.Equal(t, model.MergeStrategyLastWriteWins, opts.ExecutionPlan.Merge.Strategy)
	require.Equal(t, []string{"SchemaID:RowID"}, opts.ExecutionPlan.Merge.DedupKeys)
	require.Equal(t, []string{"attribute-level deduplication applied"}, opts.ExecutionPlan.Merge.Notes)
	// The rewired argument, pinned: a dropped hand-off (recordMergePlan(opts,
	// false, ...)) or an inverted one (PreferHot: !preferHot) both fail here.
	require.True(t, opts.ExecutionPlan.Merge.PreferHot)
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

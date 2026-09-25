package federated

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// keysetTestQuery returns a schema-7 query continuing after one created_at
// DESC / row_id ASC cursor row — the shape the keyset benchmark sends.
func keysetTestQuery() *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
		KeysetCursor: &model.KeysetCursor{
			Columns: []model.KeysetColumn{
				{Attribute: "created_at", Direction: forma.SortOrderDesc},
				{Attribute: "row_id", Direction: forma.SortOrderAsc},
			},
			Values: []interface{}{int64(5), "r1"},
			Mode:   model.KeysetCursorModeAfter,
		},
	}
}

// TestKeysetQueryReportsTheFullMatchCount pins the contract that keeps
// ExecuteFederatedKeysetQuery alive beside Query (#442). The template's
// COUNT(*) OVER() runs after the post-dedup keyset filter (#212), so on a
// continued page it counts only the rows remaining after the cursor. The
// coordinator strips the cursor, recounts, and reports the full match count —
// what the keyset benchmark asserts against its expected total.
func TestKeysetQueryReportsTheFullMatchCount(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &sequencedDuckDBExecutor{rows: []duckDBRowsIterator{
		&totalOnlyDuckDBRow{total: 2},  // the page: 2 rows remain after the cursor
		&totalOnlyDuckDBRow{total: 42}, // the recount: 42 rows match in all
	}}
	var built []model.FederatedAttributeQuery
	engine := newEmptyPageTestEngine(t, &fakePostgresFederatedSource{}, duck, &built)

	records, total, err := engine.ExecuteFederatedKeysetQuery(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		keysetTestQuery(), 10, nil, &model.FederatedQueryOptions{})

	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, int64(42), total, "the full match count, not the rows remaining after the cursor")
	require.Equal(t, 2, duck.calls, "one page scan and one recount")
	require.Len(t, built, 2)
	require.True(t, built[0].KeysetCursor.IsActive(), "the page applies the cursor")
	require.Nil(t, built[1].KeysetCursor, "the recount strips it")
}

// TestKeysetQueryRecordsRoutingPlan pins the one plan entry the coordinator
// writes itself: every tier through DuckDB, for keyset pagination. The
// retired merge path's recorders had coverage; this entry, the only one on a
// reachable path, had none.
func TestKeysetQueryRecordsRoutingPlan(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	var built []model.FederatedAttributeQuery
	engine := newEmptyPageTestEngine(t, &fakePostgresFederatedSource{}, &sequencedDuckDBExecutor{}, &built)
	opts := &model.FederatedQueryOptions{
		IncludeExecutionPlan: true,
		ExecutionPlan:        &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}},
	}

	_, _, err := engine.ExecuteFederatedKeysetQuery(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		keysetTestQuery(), 10, nil, opts)

	require.NoError(t, err)
	require.Equal(t, model.RoutingDecision{
		Tiers:     []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
		UseDuckDB: true,
		Reason:    "keyset pagination",
	}, opts.ExecutionPlan.Routing)
	require.Contains(t, opts.ExecutionPlan.Notes, "keyset pagination")
}

// TestKeysetQuerySkipsPlanWhenNotRequested guards the other half: without
// IncludeExecutionPlan the coordinator must not write into a plan the caller
// never asked for.
func TestKeysetQuerySkipsPlanWhenNotRequested(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	var built []model.FederatedAttributeQuery
	engine := newEmptyPageTestEngine(t, &fakePostgresFederatedSource{}, &sequencedDuckDBExecutor{}, &built)
	opts := &model.FederatedQueryOptions{
		ExecutionPlan: &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}},
	}

	_, _, err := engine.ExecuteFederatedKeysetQuery(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		keysetTestQuery(), 10, nil, opts)

	require.NoError(t, err)
	require.Zero(t, opts.ExecutionPlan.Routing.Reason)
	require.Empty(t, opts.ExecutionPlan.Notes)
	require.Empty(t, opts.ExecutionPlan.Sources)
	require.Empty(t, opts.ExecutionPlan.Timings)
}

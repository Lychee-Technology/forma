package federated

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"

	"github.com/stretchr/testify/require"
)

// TestPaginatedKeysetQueryDispatchesTheEffectiveLimit pins that the keyset
// branch of ExecuteFederatedPaginatedQuery renders the limit it normalised and
// clamped, not the caller's fq.Limit (#381 review). The advanced template
// reads LIMIT/OFFSET from the query object, so before dispatchedQuery a zero
// fq.Limit rendered LIMIT 0 and an over-MaxRows one rendered verbatim, with
// only the in-memory slice honouring the clamp. The captured query is the one
// the builder received, i.e. what renders.
func TestPaginatedKeysetQueryDispatchesTheEffectiveLimit(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	cases := []struct {
		name      string
		fqLimit   int
		argLimit  int
		maxRows   int
		wantLimit int
	}{
		{"a zero fq.Limit renders the default page size, not LIMIT 0", 0, 0, 0, model.DefaultPageSize},
		{"an over-MaxRows fq.Limit renders the clamp", 5000, 5000, 100, 100},
		{"the argument wins over a differing fq.Limit", 3, 25, 0, 25},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var built []model.FederatedAttributeQuery
			engine := newEmptyPageTestEngine(t, &fakePostgresFederatedSource{}, &sequencedDuckDBExecutor{}, &built)
			fq := &model.FederatedAttributeQuery{
				// A stale Offset proves the keyset dispatch zeroes it too.
				AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: tc.fqLimit, Offset: 30},
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

			_, _, err := engine.ExecuteFederatedPaginatedQuery(context.Background(),
				model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
				fq, tc.argLimit, 0, nil, &model.FederatedQueryOptions{MaxRows: tc.maxRows})
			require.NoError(t, err)

			require.GreaterOrEqual(t, len(built), 2, "the keyset page and its count are both rendered")
			page := built[0]
			require.Equal(t, tc.wantLimit, page.Limit, "the page renders the effective limit")
			require.Zero(t, page.Offset, "a keyset page has no offset")
			require.True(t, page.KeysetCursor.IsActive(), "the page keeps its cursor")
			require.Equal(t, 1, built[1].Limit, "the recount renders LIMIT 1")
			require.Nil(t, built[1].KeysetCursor, "the recount strips the cursor")
			require.Equal(t, tc.fqLimit, fq.Limit, "the caller's query is not mutated")
			require.Equal(t, 30, fq.Offset, "the caller's query is not mutated")
		})
	}
}

// TestDispatchedQueryCopiesThePaginationArguments pins the helper itself:
// the copy carries the arguments, the original is untouched, and a nil query
// stays nil.
func TestDispatchedQueryCopiesThePaginationArguments(t *testing.T) {
	orders := []model.AttributeOrder{{AttrID: 3, AttrName: "count", SortOrder: forma.SortOrderAsc}}
	q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 0, Offset: 40}}

	page := dispatchedQuery(q, 25, 0, orders)
	require.Equal(t, 25, page.Limit)
	require.Zero(t, page.Offset)
	require.Equal(t, orders, page.AttributeOrders)
	require.Equal(t, int16(7), page.SchemaID)
	require.Zero(t, q.Limit, "the original is not mutated")
	require.Equal(t, 40, q.Offset, "the original is not mutated")
	require.Nil(t, dispatchedQuery(nil, 25, 0, nil))
}

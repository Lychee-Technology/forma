package federated

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"

	"github.com/stretchr/testify/require"
)

// TestKeysetQueryDispatchesTheEffectiveLimit pins that the keyset coordinator
// (ExecuteFederatedKeysetQuery) renders the limit it normalised and clamped,
// not the caller's fq.Limit (#381 review). The retired attach template read
// LIMIT/OFFSET from the query object, so before dispatchedQuery a zero fq.Limit
// rendered LIMIT 0 and an over-MaxRows one rendered verbatim, with only the
// in-memory slice honouring the clamp. The captured window is the one the
// parquet-only statement bound, i.e. what renders.
func TestKeysetQueryDispatchesTheEffectiveLimit(t *testing.T) {
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
			duck := &sequencedDuckDBExecutor{}
			engine := newEmptyPageTestEngine(t, &fakePostgresFederatedSource{}, duck)
			fq := &model.FederatedAttributeQuery{
				// A stale Offset proves the keyset dispatch zeroes it too.
				AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: tc.fqLimit, Offset: 30},
				PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
				KeysetCursor: &model.KeysetCursor{
					Columns: []model.KeysetColumn{
						{Attribute: "created_at", Direction: forma.SortOrderDesc},
						{Attribute: "row_id", Direction: forma.SortOrderAsc},
					},
					Values: []interface{}{int64(5), "11111111-1111-1111-1111-111111111111"},
					Mode:   model.KeysetCursorModeAfter,
				},
			}

			_, _, err := engine.ExecuteFederatedKeysetQuery(context.Background(),
				model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
				fq, tc.argLimit, nil, &model.FederatedQueryOptions{MaxRows: tc.maxRows})
			require.NoError(t, err)

			require.GreaterOrEqual(t, duck.calls, 2, "the keyset page and its count are both rendered")
			page := duck.window(t, 0)
			require.Equal(t, int64(tc.wantLimit), page.limit, "the page renders the effective limit")
			require.True(t, page.keyset, "the page keeps its cursor, and a keyset page has no offset")
			require.Equal(t, renderedWindow{limit: 1}, duck.window(t, 1), "the recount strips the cursor and renders LIMIT 1 OFFSET 0")
			require.Equal(t, tc.fqLimit, fq.Limit, "the caller's query is not mutated")
			require.Equal(t, 30, fq.Offset, "the caller's query is not mutated")
		})
	}
}

// TestKeysetQueryWithoutCursorRendersTheOpenFirstPage pins what a nil cursor
// dispatches. Since #442 there is no offset branch behind the coordinator: a
// query without an active cursor takes the same path, which renders the open
// first page (model.KeysetCursor.IsActive) at offset 0 — a stale fq.Offset
// included — and still recounts, so the total stays the full match count.
func TestKeysetQueryWithoutCursorRendersTheOpenFirstPage(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &sequencedDuckDBExecutor{}
	engine := newEmptyPageTestEngine(t, &fakePostgresFederatedSource{}, duck)
	fq := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10, Offset: 30},
	}

	_, _, err := engine.ExecuteFederatedKeysetQuery(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		fq, 10, nil, &model.FederatedQueryOptions{})
	require.NoError(t, err)

	require.Equal(t, 2, duck.calls, "the first page and its count are both rendered")
	require.Equal(t, renderedWindow{limit: 10}, duck.window(t, 0),
		"the first page renders without a cursor, and the coordinator never renders an offset")
	require.Equal(t, renderedWindow{limit: 1}, duck.window(t, 1), "the recount renders LIMIT 1 OFFSET 0")
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

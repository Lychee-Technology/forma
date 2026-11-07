package federated

import (
	"context"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// The count passes below run after a page pass that succeeded, so they are
// where corruption manifesting between two passes of one request is first
// confirmed (#413). computeFederatedCount discards the options it hands its
// pass. Under #251's verify-and-exclude that dropped the partial marker and
// shrank the total quietly. Under #722 the pass fails instead, and these tests
// pin that the failure reaches the caller or the marked degraded answer.

// countPassCorruptDuck answers the page pass with pagePass and confirms
// retryCorruptPath1 corrupt on the count pass that follows it.
func countPassCorruptDuck(pagePass retryPass) *retryFakeDuck {
	return &retryFakeDuck{passes: []retryPass{
		pagePass,
		{midStreamFail: true, drainFails: []string{retryCorruptPath1}},
	}}
}

// TestKeysetCountPassConfirmedCorruptionFailsTheRequest pins the keyset
// coordinator's count pass: the page pass read the full set, the count pass
// confirms corruption, and the coordinator returns the refusal instead of the
// page with a total counted without the corrupt object.
func TestKeysetCountPassConfirmedCorruptionFailsTheRequest(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := countPassCorruptDuck(retryPass{})
	e := newRetryEngine(t, duck, []string{retryKeptPathA, retryCorruptPath1})

	opts := &model.FederatedQueryOptions{}
	recs, total, err := e.ExecuteFederatedKeysetQuery(context.Background(), corruptTables, coldTierQuery(), 10, nil, opts)
	requireCorruptRefusal(t, err, retryCorruptPath1)
	require.ErrorContains(t, err, "compute federated count")
	require.Nil(t, recs, "the page must not be returned without its count")
	require.Zero(t, total)
	require.Len(t, duck.mainSQL, 2, "the page pass and the count pass, and no retry over the remainder")
	require.Nil(t, opts.PartialScan, "a failed request carries no marker")
}

// TestEmptyPageRecountConfirmedCorruptionIsNeverQuiet pins Query's #181
// empty-page recount: a deep page streams zero rows, and the recount that
// would supply total_records confirms corruption. Without degraded mode the
// request fails with the refusal. With it, the answer is the Postgres-only
// page, and that page carries the #468 marker for the tier it skipped.
func TestEmptyPageRecountConfirmedCorruptionIsNeverQuiet(t *testing.T) {
	for _, degraded := range []bool{false, true} {
		t.Run(fmt.Sprintf("degraded=%v", degraded), func(t *testing.T) {
			restore := initTestDescriptors()
			defer restore()

			duck := countPassCorruptDuck(retryPass{empty: true})
			pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 3}}
			e := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
				hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x",
				WithParquetSource(&fakeParquetSource{paths: []string{retryKeptPathA, retryCorruptPath1}}))

			fq := coldTierQuery()
			fq.Offset = 50
			opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: degraded}
			page, err := e.Query(context.Background(), corruptTables, fq, opts)
			require.Len(t, duck.mainSQL, 2, "the page pass and the recount, and no retry over the remainder")

			if !degraded {
				requireCorruptRefusal(t, err, retryCorruptPath1)
				require.ErrorContains(t, err, "compute empty-page federated count")
				require.Nil(t, page, "an empty page must not be answered with a total counted without the corrupt object")
				require.Zero(t, pg.queryCalls)
				require.Nil(t, opts.PartialScan, "a failed request carries no marker")
				return
			}
			require.NoError(t, err, "degraded mode must absorb the recount's refusal")
			require.Equal(t, 1, pg.queryCalls, "the answer is the Postgres-only read")
			require.NotNil(t, page.Partial, "the degraded answer must carry the partial marker")
			require.Equal(t, []model.DataTier{model.DataTierCold}, page.Partial.UnconsultedTiers)
			require.Same(t, page.Partial, opts.PartialScan, "the out-parameter must agree with the page")
		})
	}
}

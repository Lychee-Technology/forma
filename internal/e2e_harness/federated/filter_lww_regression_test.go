//go:build e2e

package federated_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
	bench "github.com/lychee-technology/forma/internal/e2e_harness/federated/benchmark"
)

// TestFilterAfterLWW_StaleVersionNotResurrected pins #213: the harness builder
// must apply attribute filters after LWW dedup. v1(region=NA, older) in base,
// v2(region=EU, newer) in delta: a region=NA query must return nothing —
// filtering per-tier pre-dedup dropped v2 and resurrected the stale v1, the
// harness twin of the production bug fixed in #173.
func TestFilterAfterLWW_StaleVersionNotResurrected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := federated.NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)
	require.NoError(t, h.SetupSchema(bench.SchemaIDCustomer, "benchmark_customer"))

	rowID := uuid.Must(uuid.NewV7())
	now := time.Now()

	// v1 (older) in base: matches the filter.
	require.NoError(t, h.WriteParquet(ctx, "base", "lww_filter_base.parquet", []federated.TestRecord{{
		RowID:      rowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "v1", "version": 1, "region": "NA"},
		ChangedAt:  now.Add(-100 * time.Hour).UnixMilli(),
	}}))

	// v2 (newer) in delta: no longer matches the filter.
	require.NoError(t, h.WriteParquet(ctx, "delta", "lww_filter_delta.parquet", []federated.TestRecord{{
		RowID:      rowID,
		SchemaID:   h.SchemaID,
		Attributes: map[string]any{"name": "v2", "version": 2, "region": "EU"},
		ChangedAt:  now.Add(-10 * time.Hour).UnixMilli(),
	}}))

	// Filter matching only the stale version: the row must be invisible.
	result, err := h.ExecuteFederatedQuery(ctx, &federated.QueryOptions{
		Limit:  10,
		Filter: &federated.Filter{Conditions: map[string]any{"region": "NA"}},
	})
	require.NoError(t, err, "federated query failed")
	require.Empty(t, result.Records, "stale base version must not resurrect through a filter only it matches")
	require.Zero(t, result.TotalRecords, "count path must agree with select path")

	// Control: filter matching the latest version returns exactly that version.
	result, err = h.ExecuteFederatedQuery(ctx, &federated.QueryOptions{
		Limit:  10,
		Filter: &federated.Filter{Conditions: map[string]any{"region": "EU"}},
	})
	require.NoError(t, err, "federated control query failed")
	require.Len(t, result.Records, 1, "latest version must stay visible under its own filter")
	require.Equal(t, int64(1), result.TotalRecords)
	require.Equal(t, rowID, result.Records[0].RowID)
}

// TestFilterAfterLWW_TradeTimeWindowNotResurrected is the trade-time-window
// variant of the same #213 failure mode: the window predicate is an attribute
// predicate too and must evaluate against the rn = 1 winner only.
func TestFilterAfterLWW_TradeTimeWindowNotResurrected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := federated.NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)
	require.NoError(t, h.SetupSchema(bench.SchemaIDTrade, "benchmark_trade"))

	rowID := uuid.Must(uuid.NewV7())
	now := time.Now()
	attrs := func(version, tradeTime int) map[string]any {
		return map[string]any{
			"name": "trade", "version": version,
			"symbol": "SYM00001", "exchange": "NYSE", "region": "NA",
			"tradeType": 1, "tradeTime": tradeTime,
		}
	}

	// v1 (older) in base: tradeTime inside the queried window.
	require.NoError(t, h.WriteParquet(ctx, "base", "lww_window_base.parquet", []federated.TestRecord{{
		RowID: rowID, SchemaID: h.SchemaID,
		Attributes: attrs(1, 1500),
		ChangedAt:  now.Add(-100 * time.Hour).UnixMilli(),
	}}))

	// v2 (newer) in delta: tradeTime moved outside the window.
	require.NoError(t, h.WriteParquet(ctx, "delta", "lww_window_delta.parquet", []federated.TestRecord{{
		RowID: rowID, SchemaID: h.SchemaID,
		Attributes: attrs(2, 5000),
		ChangedAt:  now.Add(-10 * time.Hour).UnixMilli(),
	}}))

	// Window matching only the stale version: the row must be invisible.
	result, err := h.ExecuteFederatedQuery(ctx, &federated.QueryOptions{
		Limit: 10, TradeTimeStart: 1000, TradeTimeEnd: 2000,
	})
	require.NoError(t, err, "federated window query failed")
	require.Empty(t, result.Records, "stale base version must not resurrect through a window only it matches")
	require.Zero(t, result.TotalRecords)

	// Control: window containing the latest version returns exactly it.
	result, err = h.ExecuteFederatedQuery(ctx, &federated.QueryOptions{
		Limit: 10, TradeTimeStart: 4000, TradeTimeEnd: 6000,
	})
	require.NoError(t, err, "federated window control query failed")
	require.Len(t, result.Records, 1)
	require.Equal(t, int64(1), result.TotalRecords)
	require.Equal(t, rowID, result.Records[0].RowID)
}

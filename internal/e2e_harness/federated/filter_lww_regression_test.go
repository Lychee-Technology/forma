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

// filterLWWScenario drives one #213 regression scenario: the same row_id gets
// v1 (older) in base matching only staleOpts and v2 (newer) in delta matching
// only controlOpts. The stale query must return nothing — filtering per-tier
// pre-dedup dropped v2 and resurrected the stale v1, the harness twin of the
// production bug fixed in #173 — and the control query must return exactly v2.
type filterLWWScenario struct {
	schemaID    int16
	schemaName  string
	filePrefix  string
	v1Attrs     map[string]any
	v2Attrs     map[string]any
	staleOpts   federated.QueryOptions
	controlOpts federated.QueryOptions
}

func runFilterAfterLWWScenario(t *testing.T, sc filterLWWScenario) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	h, err := federated.NewFederatedTestHarness(ctx)
	require.NoError(t, err, "failed to create harness")
	defer h.Cleanup(ctx)
	require.NoError(t, h.SetupSchema(sc.schemaID, sc.schemaName))

	rowID := uuid.Must(uuid.NewV7())
	now := time.Now()

	// v1 (older) in base: matches only the stale predicate.
	require.NoError(t, h.WriteParquet(ctx, "base", sc.filePrefix+"_base.parquet", []federated.TestRecord{{
		RowID:      rowID,
		SchemaID:   h.SchemaID,
		Attributes: sc.v1Attrs,
		ChangedAt:  now.Add(-100 * time.Hour).UnixMilli(),
	}}))

	// v2 (newer) in delta: matches only the control predicate.
	require.NoError(t, h.WriteParquet(ctx, "delta", sc.filePrefix+"_delta.parquet", []federated.TestRecord{{
		RowID:      rowID,
		SchemaID:   h.SchemaID,
		Attributes: sc.v2Attrs,
		ChangedAt:  now.Add(-10 * time.Hour).UnixMilli(),
	}}))

	// Predicate matching only the stale version: the row must be invisible.
	result, err := h.ExecuteFederatedQuery(ctx, &sc.staleOpts)
	require.NoError(t, err, "federated stale-predicate query failed")
	require.Empty(t, result.Records, "stale base version must not resurrect through a predicate only it matches")
	require.Zero(t, result.TotalRecords, "count path must agree with select path")

	// Control: predicate matching the latest version returns exactly it.
	result, err = h.ExecuteFederatedQuery(ctx, &sc.controlOpts)
	require.NoError(t, err, "federated control query failed")
	require.Len(t, result.Records, 1, "latest version must stay visible under its own predicate")
	require.Equal(t, int64(1), result.TotalRecords)
	require.Equal(t, rowID, result.Records[0].RowID)
}

// TestFilterAfterLWW_StaleVersionNotResurrected pins #213 for attribute
// filters: v1(region=NA, older) in base, v2(region=EU, newer) in delta — a
// region=NA query must return nothing.
func TestFilterAfterLWW_StaleVersionNotResurrected(t *testing.T) {
	runFilterAfterLWWScenario(t, filterLWWScenario{
		schemaID:    bench.SchemaIDCustomer,
		schemaName:  "benchmark_customer",
		filePrefix:  "lww_filter",
		v1Attrs:     map[string]any{"name": "v1", "version": 1, "region": "NA"},
		v2Attrs:     map[string]any{"name": "v2", "version": 2, "region": "EU"},
		staleOpts:   federated.QueryOptions{Limit: 10, Filter: &federated.Filter{Conditions: map[string]any{"region": "NA"}}},
		controlOpts: federated.QueryOptions{Limit: 10, Filter: &federated.Filter{Conditions: map[string]any{"region": "EU"}}},
	})
}

// TestFilterAfterLWW_TradeTimeWindowNotResurrected is the trade-time-window
// variant of the same #213 failure mode: the window predicate is an attribute
// predicate too and must evaluate against the rn = 1 winner only. v1 sits
// inside the [1000, 2000] window, v2 moved to 5000.
func TestFilterAfterLWW_TradeTimeWindowNotResurrected(t *testing.T) {
	tradeAttrs := func(version, tradeTime int) map[string]any {
		return map[string]any{
			"name": "trade", "version": version,
			"symbol": "SYM00001", "exchange": "NYSE", "region": "NA",
			"tradeType": 1, "tradeTime": tradeTime,
		}
	}
	runFilterAfterLWWScenario(t, filterLWWScenario{
		schemaID:    bench.SchemaIDTrade,
		schemaName:  "benchmark_trade",
		filePrefix:  "lww_window",
		v1Attrs:     tradeAttrs(1, 1500),
		v2Attrs:     tradeAttrs(2, 5000),
		staleOpts:   federated.QueryOptions{Limit: 10, TradeTimeStart: 1000, TradeTimeEnd: 2000},
		controlOpts: federated.QueryOptions{Limit: 10, TradeTimeStart: 4000, TradeTimeEnd: 6000},
	})
}

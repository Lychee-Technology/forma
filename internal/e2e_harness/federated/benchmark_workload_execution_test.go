//go:build e2e

package federated_test

import (
	"context"
	"testing"
	"time"

	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
	bench "github.com/lychee-technology/forma/internal/e2e_harness/federated/benchmark"
	"github.com/stretchr/testify/require"
)

func TestBenchmarkWorkloadExecution_RunWithHarness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	h, err := federated.NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	runner, err := bench.NewRunner(bench.Config{
		Mode:          bench.ExecutionModePlan,
		Scale:         bench.ScaleSmall,
		Distribution:  bench.DistributionHotspot,
		Iterations:    1,
		PageSize:      20,
		Seed:          42,
		TradeCount:    120,
		CustomerCount: 12,
		SecurityCount: 6,
		OverlapRatio:  0.10,
		DeleteRatio:   0.05,
		Workloads: []string{
			"baseline-page-1",
			"hot-selective-page",
			"eav-selective-page",
			"deep-page-1000",
			"deep-page-100000",
		},
	}.WithDefaults())
	require.NoError(t, err)

	result, err := runner.RunWithHarness(ctx, h, bench.TierMixBalanced)
	require.NoError(t, err)
	require.False(t, result.ValidationOnly)
	require.Len(t, result.Executions, 5)
	filteredSeen := false
	eavSeen := false
	expectedOracleAssertionsSeen := false
	for _, execution := range result.Executions {
		require.NotEmpty(t, execution.Name)
		require.GreaterOrEqual(t, execution.ResultCount, 0)
		require.GreaterOrEqual(t, execution.TotalRecords, int64(0))
		require.NotEmpty(t, execution.Assertions)
		require.NotEqual(t, bench.FailureKindInfra, execution.FailureKind, "expected live benchmark execution to avoid infrastructure failures")
		if execution.Name == "hot-selective-page" {
			filteredSeen = true
		}
		if execution.Name == "eav-selective-page" {
			eavSeen = true
		}
		for _, assertion := range execution.Assertions {
			if assertion.Name == "total-records-match-expected" || assertion.Name == "page-row-ids-match-expected" {
				expectedOracleAssertionsSeen = true
			}
		}
	}
	require.True(t, filteredSeen)
	require.True(t, eavSeen)
	require.True(t, expectedOracleAssertionsSeen, "expected result oracle assertions to be exercised")
}

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
			"customer-region-page",
			"security-symbol-page",
			"hot-selective-page",
			"eav-selective-page",
			"mixed-tier-window",
			"deep-page-1000",
			"deep-page-100000",
		},
	}.WithDefaults())
	require.NoError(t, err)

	result, err := runner.RunWithHarness(ctx, h, bench.TierMixBalanced)
	require.NoError(t, err)
	require.False(t, result.ValidationOnly)
	require.Len(t, result.Executions, 8)
	customerSeen := false
	securitySeen := false
	filteredSeen := false
	eavSeen := false
	mixedTierSeen := false
	expectedOracleAssertionsSeen := false
	tradeTimeWindowAssertionSeen := false
	for _, execution := range result.Executions {
		require.NotEmpty(t, execution.Name)
		require.GreaterOrEqual(t, execution.ResultCount, 0)
		require.GreaterOrEqual(t, execution.TotalRecords, int64(0))
		require.NotEmpty(t, execution.Assertions)
		require.NotEqual(t, bench.FailureKindInfra, execution.FailureKind, "expected live benchmark execution to avoid infrastructure failures")
		if execution.Name == "hot-selective-page" {
			filteredSeen = true
			require.Empty(t, execution.FailureKind, "expected hot selective workload to pass correctness in live execution")
		}
		if execution.Name == "customer-region-page" {
			customerSeen = true
			require.Empty(t, execution.FailureKind, "expected customer workload to pass correctness in live execution")
		}
		if execution.Name == "security-symbol-page" {
			securitySeen = true
			require.Empty(t, execution.FailureKind, "expected security workload to pass correctness in live execution")
		}
		if execution.Name == "eav-selective-page" {
			eavSeen = true
			require.Empty(t, execution.FailureKind, "expected EAV selective workload to pass correctness in live execution")
		}
		if execution.Name == "mixed-tier-window" {
			mixedTierSeen = true
		}
		for _, assertion := range execution.Assertions {
			if assertion.Name == "total-records-match-expected" || assertion.Name == "page-row-ids-match-expected" {
				expectedOracleAssertionsSeen = true
			}
			if execution.Name == "mixed-tier-window" && assertion.Name == "tradeTime-window-match-request" {
				tradeTimeWindowAssertionSeen = true
			}
		}
	}
	require.True(t, customerSeen)
	require.True(t, securitySeen)
	require.True(t, filteredSeen)
	require.True(t, eavSeen)
	require.True(t, mixedTierSeen)
	require.True(t, expectedOracleAssertionsSeen, "expected result oracle assertions to be exercised")
	require.True(t, tradeTimeWindowAssertionSeen, "expected mixed-tier window assertion to be exercised")
}

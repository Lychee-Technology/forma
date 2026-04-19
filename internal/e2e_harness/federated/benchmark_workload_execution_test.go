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
		Iterations:    2,
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
			"hot-low-selectivity-page",
			"eav-selective-page",
			"mixed-hot-eav-page",
			"mixed-tier-window",
			"hot-only-window",
			"cold-only-window",
			"deep-page-1000",
			"deep-page-100000",
		},
	}.WithDefaults())
	require.NoError(t, err)

	result, err := runner.RunWithHarness(ctx, h, bench.TierMixBalanced)
	require.NoError(t, err)
	require.False(t, result.ValidationOnly)
	require.Len(t, result.Executions, 24)
	require.Contains(t, result.Notes, "oracle_modes loaded_state=8 truth_pass=4")
	customerSeen := false
	securitySeen := false
	filteredSeen := false
	lowSelectiveSeen := false
	eavSeen := false
	mixedFilterSeen := false
	mixedTierSeen := false
	hotOnlySeen := false
	coldOnlySeen := false
	expectedOracleAssertionsSeen := false
	tradeTimeWindowAssertionSeen := false
	repeatedRunStabilitySeen := false
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
		if execution.Name == "hot-low-selectivity-page" {
			lowSelectiveSeen = true
			require.Empty(t, execution.FailureKind, "expected low-selectivity workload to pass correctness in live execution")
		}
		if execution.Name == "mixed-hot-eav-page" {
			mixedFilterSeen = true
			require.Empty(t, execution.FailureKind, "expected mixed hot+EAV workload to pass correctness in live execution")
		}
		if execution.Name == "mixed-tier-window" {
			mixedTierSeen = true
		}
		if execution.Name == "hot-only-window" {
			hotOnlySeen = true
		}
		if execution.Name == "cold-only-window" {
			coldOnlySeen = true
		}
		for _, assertion := range execution.Assertions {
			if assertion.Name == "total-records-match-expected" || assertion.Name == "page-row-ids-match-expected" {
				expectedOracleAssertionsSeen = true
			}
			if assertion.Name == "repeated-run-failure-kind-stable" || assertion.Name == "repeated-run-total-records-stable" || assertion.Name == "repeated-run-page-row-ids-stable" {
				repeatedRunStabilitySeen = true
			}
			if (execution.Name == "mixed-tier-window" || execution.Name == "hot-only-window" || execution.Name == "cold-only-window") && assertion.Name == "tradeTime-window-match-request" {
				tradeTimeWindowAssertionSeen = true
			}
		}
	}
	require.True(t, customerSeen)
	require.True(t, securitySeen)
	require.True(t, filteredSeen)
	require.True(t, lowSelectiveSeen)
	require.True(t, eavSeen)
	require.True(t, mixedFilterSeen)
	require.True(t, mixedTierSeen)
	require.True(t, hotOnlySeen)
	require.True(t, coldOnlySeen)
	require.True(t, expectedOracleAssertionsSeen, "expected result oracle assertions to be exercised")
	require.True(t, repeatedRunStabilitySeen, "expected repeated-run stability assertions to be exercised")
	require.True(t, tradeTimeWindowAssertionSeen, "expected mixed-tier window assertion to be exercised")
}

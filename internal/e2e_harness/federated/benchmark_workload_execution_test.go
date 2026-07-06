//go:build e2e

package federated_test

import (
	"context"
	"strings"
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
			"tier-pushdown-hot",
			"tier-pushdown-eav",
			"tier-pushdown-mixed",
			"tier-pushdown-cold-only",
		},
	}.WithDefaults())
	require.NoError(t, err)

	result, err := runner.RunWithHarness(ctx, h, bench.TierMixBalanced)
	require.NoError(t, err)
	require.False(t, result.ValidationOnly)
	require.Len(t, result.Executions, 32)
	require.Contains(t, result.Notes, "oracle_modes loaded_state=8 truth_pass=8")
	require.Contains(t, result.Notes, "prefer_hot expresses workload intent and report provenance, not hard execution routing")
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
	pushdownHotSeen := false
	pushdownEAVSeen := false
	pushdownMixedSeen := false
	pushdownColdOnlySeen := false
	pushdownAssertionsSeen := false
	for _, execution := range result.Executions {
		require.NotEmpty(t, execution.Name)
		require.GreaterOrEqual(t, execution.ResultCount, 0)
		require.GreaterOrEqual(t, execution.TotalRecords, int64(0))
		if len(execution.Assertions) == 0 {
			t.Logf("WARNING: execution %q has zero assertions (failure_kind=%q infra_error=%q)", execution.Name, execution.FailureKind, execution.InfraError)
		}
		require.NotEmpty(t, execution.Assertions)
		require.NotEqual(t, bench.FailureKindInfra, execution.FailureKind, "expected live benchmark execution to avoid infrastructure failures")
		if execution.Name == "hot-selective-page" {
			filteredSeen = true
			require.NotEqual(t, bench.FailureKindInfra, execution.FailureKind, "expected hot selective workload to execute without infra failure")
		}
		if execution.Name == "customer-region-page" {
			customerSeen = true
			require.NotEqual(t, bench.FailureKindInfra, execution.FailureKind, "expected customer workload to execute without infra failure")
		}
		if execution.Name == "security-symbol-page" {
			securitySeen = true
			require.NotEqual(t, bench.FailureKindInfra, execution.FailureKind, "expected security workload to execute without infra failure")
		}
		if execution.Name == "eav-selective-page" {
			eavSeen = true
			require.NotEqual(t, bench.FailureKindInfra, execution.FailureKind, "expected EAV selective workload to execute without infra failure")
		}
		if execution.Name == "hot-low-selectivity-page" {
			lowSelectiveSeen = true
			require.Contains(t, execution.PlanNotes, "entity_manager_query_service")
			require.NotEqual(t, bench.FailureKindInfra, execution.FailureKind, "expected low-selectivity workload to execute without infra failure")
		}
		if execution.Name == "mixed-hot-eav-page" {
			mixedFilterSeen = true
			require.Contains(t, execution.PlanNotes, "entity_manager_query_service")
			require.NotEqual(t, bench.FailureKindInfra, execution.FailureKind, "expected mixed hot+EAV workload to execute without infra failure")
		}
		if execution.Name == "mixed-tier-window" {
			mixedTierSeen = true
		}
		if execution.Name == "hot-only-window" {
			hotOnlySeen = true
			require.True(t, execution.PreferHot)
			require.Contains(t, execution.PlanNotes, "prefer_hot=true (intent/provenance only; no hard routing override yet)")
			require.Contains(t, execution.PlanNotes, "prefer_hot_execution=true (postgres-only override active for tier-mix workload)")
		}
		if execution.Name == "cold-only-window" {
			coldOnlySeen = true
		}
		if execution.Name == "tier-pushdown-hot" {
			pushdownHotSeen = true
			require.NotNil(t, execution.PerTier, "expected per-tier metrics for pushdown-hot workload")
			require.Contains(t, execution.PlanNotes, "entity_manager_query_service")
		}
		if execution.Name == "tier-pushdown-eav" {
			pushdownEAVSeen = true
			require.NotNil(t, execution.PerTier, "expected per-tier metrics for pushdown-eav workload")
		}
		if execution.Name == "tier-pushdown-mixed" {
			pushdownMixedSeen = true
			require.NotNil(t, execution.PerTier, "expected per-tier metrics for pushdown-mixed workload")
		}
		if execution.Name == "tier-pushdown-cold-only" {
			pushdownColdOnlySeen = true
			require.NotNil(t, execution.PerTier, "expected per-tier metrics for pushdown-cold-only workload")
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
			if assertion.Name == "pushdown-plan-sources-present" || assertion.Name == "pushdown-pg-rows-tracked" {
				pushdownAssertionsSeen = true
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
	require.True(t, pushdownHotSeen, "expected tier-pushdown-hot workload to be executed")
	require.True(t, pushdownEAVSeen, "expected tier-pushdown-eav workload to be executed")
	require.True(t, pushdownMixedSeen, "expected tier-pushdown-mixed workload to be executed")
	require.True(t, pushdownColdOnlySeen, "expected tier-pushdown-cold-only workload to be executed")
	require.True(t, pushdownAssertionsSeen, "expected pushdown assertions to be exercised")
}

func TestBenchmarkTruthPassSampledSpotCheck_RunWithHarness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	h, err := federated.NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	runner, err := bench.NewRunner(bench.Config{
		Mode:               bench.ExecutionModeLive,
		Scale:              bench.ScaleSmall,
		Distribution:       bench.DistributionHotspot,
		Iterations:         1,
		PageSize:           20,
		Seed:               42,
		TradeCount:         300,
		CustomerCount:      20,
		SecurityCount:      10,
		OverlapRatio:       0.10,
		DeleteRatio:        0.05,
		TruthPassSampleCap: 5,
		Workloads: []string{
			"baseline-page-1",
			"hot-selective-page",
			"eav-selective-page",
		},
	}.WithDefaults())
	require.NoError(t, err)

	result, err := runner.RunWithHarness(ctx, h, bench.TierMixBalanced)
	require.NoError(t, err)
	require.True(t, result.Passed, "sampled heavy-live semantics must still pass at tiny scale")
	require.Equal(t, string(bench.OracleModeTruthPassSampled), result.OracleModes["hot-selective-page"])
	require.Equal(t, string(bench.OracleModeTruthPassSampled), result.OracleModes["eav-selective-page"])

	sampledNote := false
	for _, note := range result.Notes {
		if strings.Contains(note, "truth_pass_sampled=2") {
			sampledNote = true
		}
	}
	require.True(t, sampledNote, "expected sampled oracle summary note, got %v", result.Notes)

	sampledWorkloads := map[string]bool{"hot-selective-page": false, "eav-selective-page": false}
	for _, execution := range result.Executions {
		if _, ok := sampledWorkloads[execution.Name]; ok {
			require.Equal(t, string(bench.OracleModeTruthPassSampled), execution.OracleMode,
				"execution record for %s must carry the run-time (sampled) oracle mode", execution.Name)
			sampledWorkloads[execution.Name] = true
		}
	}
	for name, seen := range sampledWorkloads {
		require.True(t, seen, "expected at least one execution record for workload %s", name)
	}

	summary := bench.SummarizeRunResult(result)
	found := false
	for _, provenance := range summary.OracleProvenance {
		if provenance.Mode != string(bench.OracleModeTruthPassSampled) {
			continue
		}
		found = true
		require.Contains(t, provenance.Workloads, "hot-selective-page")
		require.Contains(t, provenance.Workloads, "eav-selective-page")
	}
	require.True(t, found, "expected oracle provenance group for truth-pass-sampled, got %+v", summary.OracleProvenance)
}

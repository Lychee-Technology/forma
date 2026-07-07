//go:build e2e

package federated_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
	bench "github.com/lychee-technology/forma/internal/e2e_harness/federated/benchmark"
	"github.com/stretchr/testify/require"
)

// knownFailingAssertions allowlists the EXACT (workload, assertion-name) pairs
// that are known to fail for a pre-existing bug, each tagged with its tracking
// issue. #150 fails the suite on any failed correctness assertion except these
// specific pairs — so a NEW kind of failure in one of these same workloads (a
// different assertion) still turns the suite red. This is an explicit,
// issue-referenced allowlist, not a whole-workload skip. Remove entries as
// their issues land.
//
//   - eav-low-selectivity-page: the truth-pass oracle's harness query cannot
//     filter the pure-EAV attribute orderChannel (absent from the hardcoded
//     benchmarkQueryColumn / targetedHotFilterExpression / hot-pivot /
//     parquet-projection maps), so it undercounts hot-tier candidates. The #156
//     batching proved (via TestTruthPassBatchEqualsPerCandidate) that this is a
//     harness truth-query bug independent of the per-candidate/batch split, so
//     it is tracked on its own (#163).
//   - mixed-hot-eav-page / tier-pushdown-mixed: the service path returns zero
//     rows for the composite main+EAV filter (symbol AND exchange) though each
//     condition works alone (#161).
var knownFailingAssertions = map[string]map[string]string{
	"eav-low-selectivity-page": {
		"total-records-match-expected": "#163",
		"page-row-ids-match-expected":  "#163",
	},
	"mixed-hot-eav-page": {
		"total-records-match-expected": "#161",
		"page-row-ids-match-expected":  "#161",
	},
	"tier-pushdown-mixed": {
		"total-records-match-expected": "#161",
		"page-row-ids-match-expected":  "#161",
	},
}

// assertWorkloadOraclesGreen fails the test if any workload has a failed
// correctness assertion that is not in the documented knownFailingAssertions
// allowlist. Allowlisting is per (workload, assertion-name), so a new failure
// mode in an already-known-failing workload still fails the suite. The failure
// message names the workload and the specific failing assertion (#150).
func assertWorkloadOraclesGreen(t *testing.T, result *bench.RunResult) {
	t.Helper()
	var failures []string
	for _, execution := range result.Executions {
		if execution.Passed {
			continue
		}
		allowed := knownFailingAssertions[execution.Name]
		for _, a := range execution.Assertions {
			if a.Passed {
				continue
			}
			detail := fmt.Sprintf("workload %q assertion %q: %s", execution.Name, a.Name, a.Message)
			if issue, known := allowed[a.Name]; known {
				t.Logf("KNOWN-FAILING (tracked in %s) — %s", issue, detail)
				continue
			}
			failures = append(failures, detail)
		}
	}
	require.Empty(t, failures, "benchmark oracle regressions detected:\n%s", strings.Join(failures, "\n"))
}

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
	requireWorkloadCoverage(t, scanWorkloadCoverage(t, result))

	// #150: fail on any failed correctness assertion that is not an explicitly
	// allowlisted (workload, assertion) pair tracked in a separate issue.
	assertWorkloadOraclesGreen(t, result)
}

// workloadCoverage records which expected workloads and assertion families the
// harness run exercised. It exists to split the pre-existing coverage scan out
// of the test body so each function stays within the 100-line limit
// (coding-standard.md §1).
type workloadCoverage struct {
	customer, security, filtered, lowSelective, eav, mixedFilter bool
	mixedTier, hotOnly, coldOnly                                 bool
	expectedOracleAssertions, tradeTimeWindow, repeatedStability bool
	pushdownHot, pushdownEAV, pushdownMixed, pushdownColdOnly     bool
	pushdownAssertions                                           bool
}

// scanWorkloadCoverage walks the executions, runs the per-workload structural
// checks (infra-free execution, expected plan notes / per-tier metrics), and
// records which workloads and assertion families were seen.
func scanWorkloadCoverage(t *testing.T, result *bench.RunResult) workloadCoverage {
	t.Helper()
	var c workloadCoverage
	for _, execution := range result.Executions {
		require.NotEmpty(t, execution.Name)
		require.GreaterOrEqual(t, execution.ResultCount, 0)
		require.GreaterOrEqual(t, execution.TotalRecords, int64(0))
		if len(execution.Assertions) == 0 {
			t.Logf("WARNING: execution %q has zero assertions (failure_kind=%q infra_error=%q)", execution.Name, execution.FailureKind, execution.InfraError)
		}
		require.NotEmpty(t, execution.Assertions)
		require.NotEqual(t, bench.FailureKindInfra, execution.FailureKind, "expected live benchmark execution to avoid infrastructure failures")
		switch execution.Name {
		case "hot-selective-page":
			c.filtered = true
		case "customer-region-page":
			c.customer = true
		case "security-symbol-page":
			c.security = true
		case "eav-selective-page":
			c.eav = true
		case "hot-low-selectivity-page":
			c.lowSelective = true
			require.Contains(t, execution.PlanNotes, "entity_manager_query_service")
		case "mixed-hot-eav-page":
			c.mixedFilter = true
			require.Contains(t, execution.PlanNotes, "entity_manager_query_service")
		case "mixed-tier-window":
			c.mixedTier = true
		case "hot-only-window":
			c.hotOnly = true
			require.True(t, execution.PreferHot)
			require.Contains(t, execution.PlanNotes, "prefer_hot=true (intent/provenance only; no hard routing override yet)")
			require.Contains(t, execution.PlanNotes, "prefer_hot_execution=true (postgres-only override active for tier-mix workload)")
		case "cold-only-window":
			c.coldOnly = true
		case "tier-pushdown-hot":
			c.pushdownHot = true
			require.NotNil(t, execution.PerTier, "expected per-tier metrics for pushdown-hot workload")
			require.Contains(t, execution.PlanNotes, "entity_manager_query_service")
		case "tier-pushdown-eav":
			c.pushdownEAV = true
			require.NotNil(t, execution.PerTier, "expected per-tier metrics for pushdown-eav workload")
		case "tier-pushdown-mixed":
			c.pushdownMixed = true
			require.NotNil(t, execution.PerTier, "expected per-tier metrics for pushdown-mixed workload")
		case "tier-pushdown-cold-only":
			c.pushdownColdOnly = true
			require.NotNil(t, execution.PerTier, "expected per-tier metrics for pushdown-cold-only workload")
		}
		for _, assertion := range execution.Assertions {
			switch {
			case assertion.Name == "total-records-match-expected" || assertion.Name == "page-row-ids-match-expected":
				c.expectedOracleAssertions = true
			case assertion.Name == "repeated-run-failure-kind-stable" || assertion.Name == "repeated-run-total-records-stable" || assertion.Name == "repeated-run-page-row-ids-stable":
				c.repeatedStability = true
			case (execution.Name == "mixed-tier-window" || execution.Name == "hot-only-window" || execution.Name == "cold-only-window") && assertion.Name == "tradeTime-window-match-request":
				c.tradeTimeWindow = true
			case assertion.Name == "pushdown-plan-sources-present" || assertion.Name == "pushdown-pg-rows-tracked":
				c.pushdownAssertions = true
			}
		}
	}
	return c
}

// requireWorkloadCoverage asserts every expected workload and assertion family
// was exercised by the run.
func requireWorkloadCoverage(t *testing.T, c workloadCoverage) {
	t.Helper()
	require.True(t, c.customer)
	require.True(t, c.security)
	require.True(t, c.filtered)
	require.True(t, c.lowSelective)
	require.True(t, c.eav)
	require.True(t, c.mixedFilter)
	require.True(t, c.mixedTier)
	require.True(t, c.hotOnly)
	require.True(t, c.coldOnly)
	require.True(t, c.expectedOracleAssertions, "expected result oracle assertions to be exercised")
	require.True(t, c.repeatedStability, "expected repeated-run stability assertions to be exercised")
	require.True(t, c.tradeTimeWindow, "expected mixed-tier window assertion to be exercised")
	require.True(t, c.pushdownHot, "expected tier-pushdown-hot workload to be executed")
	require.True(t, c.pushdownEAV, "expected tier-pushdown-eav workload to be executed")
	require.True(t, c.pushdownMixed, "expected tier-pushdown-mixed workload to be executed")
	require.True(t, c.pushdownColdOnly, "expected tier-pushdown-cold-only workload to be executed")
	require.True(t, c.pushdownAssertions, "expected pushdown assertions to be exercised")
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

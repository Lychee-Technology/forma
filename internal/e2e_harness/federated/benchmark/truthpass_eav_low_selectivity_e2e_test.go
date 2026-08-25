//go:build e2e

package benchmark

import (
	"context"
	"testing"
	"time"

	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
	"github.com/stretchr/testify/require"
)

// TestTruthPassOracleFiltersPureEAVAttribute pins #163: the truth-pass oracle
// must not undercount a filter on a pure-EAV attribute that is absent from the
// harness's hardcoded attribute maps (orderChannel). The undercount only
// manifests under a distribution that puts updated records in the hot tier
// (hotspot): hot records are reconstructed from Postgres, and before the fix
// that reconstruction dropped every EAV attribute outside the hardcoded set, so
// hot orderChannel="web" candidates were silently excluded from the oracle's
// expected set while the production/service query returned them — expected <
// actual. This asserts the oracle's expected result matches the service actual
// for eav-low-selectivity-page (total-records and page-row-ids).
func TestTruthPassOracleFiltersPureEAVAttribute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	h, err := federated.NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.CleanupOrLog(ctx, t)

	// Hotspot distribution is required: it appends updated records that rewrite
	// orderChannel and land in the hot tier, which is where the reconstruction
	// gap bit. Uniform (all-cold) does not reproduce the undercount.
	runner, err := NewRunner(Config{
		Mode:          ExecutionModePlan,
		Scale:         ScaleSmall,
		Distribution:  DistributionHotspot,
		Iterations:    2,
		PageSize:      20,
		Seed:          42,
		TradeCount:    120,
		CustomerCount: 12,
		SecurityCount: 6,
		OverlapRatio:  0.10,
		DeleteRatio:   0.05,
		Workloads: []string{
			"eav-selective-page",       // control: hardcoded EAV attr (exchange)
			"eav-low-selectivity-page", // the pure-EAV attr under test (orderChannel)
			"mixed-hot-eav-page",       // control: mixed hot + EAV
		},
	}.WithDefaults())
	require.NoError(t, err)

	result, err := runner.RunWithHarness(ctx, h, TierMixBalanced)
	require.NoError(t, err)

	var checked int
	for _, execution := range result.Executions {
		if execution.Name != "eav-low-selectivity-page" {
			continue
		}
		checked++
		require.True(t, execution.Passed,
			"eav-low-selectivity-page must pass all correctness assertions (#163)")
		for _, a := range execution.Assertions {
			switch a.Name {
			case "total-records-match-expected", "page-row-ids-match-expected":
				require.True(t, a.Passed,
					"assertion %q must pass (oracle must not undercount pure-EAV orderChannel): %s",
					a.Name, a.Message)
			}
		}
	}
	require.Positive(t, checked, "expected eav-low-selectivity-page to be executed")
}

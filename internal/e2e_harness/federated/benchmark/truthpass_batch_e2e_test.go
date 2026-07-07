//go:build e2e

package benchmark

import (
	"context"
	"testing"
	"time"

	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
	"github.com/stretchr/testify/require"
)

// TestTruthPassBatchEqualsPerCandidate is the #156 characterization test: it
// proves the batched visibility sweep (collectVisibleRowIDs) yields the exact
// same per-candidate visibility verdict as the retired per-candidate
// Limit:1 + RowID probe. Since buildTruthPassExpected is unchanged, identical
// visibility over the candidate set means identical expected results.
func TestTruthPassBatchEqualsPerCandidate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	h, err := federated.NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.Cleanup(ctx)

	runner, err := NewRunner(Config{
		Mode:          ExecutionModePlan,
		Scale:         ScaleSmall,
		Distribution:  DistributionUniform,
		Iterations:    1,
		PageSize:      20,
		Seed:          42,
		TradeCount:    300,
		CustomerCount: 20,
		SecurityCount: 10,
		OverlapRatio:  0.10,
		DeleteRatio:   0.05,
		Workloads: []string{
			"eav-selective-page",
			"eav-low-selectivity-page",
			"mixed-hot-eav-page",
			"tier-pushdown-mixed",
		},
	}.WithDefaults())
	require.NoError(t, err)

	// Replicate the RunWithHarness load path so we hold the same loaded state
	// the oracle sees.
	generator, err := NewGenerator(runner.genConfig)
	require.NoError(t, err)
	dataset, err := generator.Generate()
	require.NoError(t, err)
	tiered, err := SplitIntoTiers(dataset, TierMixBalanced)
	require.NoError(t, err)
	h.Registry = runner.registry
	require.NoError(t, LoadTieredDataset(ctx, h, tiered))
	loadedRecords, _, err := buildLoadedStateSnapshot(ctx, h, tiered)
	require.NoError(t, err)

	perCandidateProbe := func(workload WorkloadDefinition, candidate GeneratedRecord) bool {
		result, probeErr := h.ExecuteFederatedQuery(ctx, &federated.QueryOptions{
			Limit: 1,
			Filter: &federated.Filter{
				RowID:      candidate.RowID,
				Conditions: workload.ResolvedFilterConditions(),
			},
			SortBy:   "tradeTime",
			SortDesc: true,
		})
		require.NoError(t, probeErr)
		return result.TotalRecords > 0
	}

	checked := 0
	for _, workload := range runner.workloads {
		if workload.ResolvedOracleMode() != OracleModeTruthPass {
			continue
		}
		if !workload.SupportsDistribution(runner.genConfig.Distribution) {
			continue
		}
		schemaID, err := workloadSchemaID(workload.TargetSchema)
		require.NoError(t, err)
		h.SchemaID = schemaID

		batch, err := collectVisibleRowIDs(ctx, h, workload)
		require.NoError(t, err)

		candidates := filterExpectedRecordsForWorkload(
			expectedVisibleRecords(loadedRecords), workload, semanticsForWorkload(workload, runner.genConfig))
		require.NotEmpty(t, candidates, "workload %s produced no candidates to compare", workload.Name)

		for _, candidate := range candidates {
			_, inBatch := batch[candidate.RowID]
			require.Equal(t, perCandidateProbe(workload, candidate), inBatch,
				"workload %s candidate %s: batch membership must match the per-candidate probe", workload.Name, candidate.RowID)
			checked++
		}
	}
	require.Positive(t, checked, "expected at least one truth-pass candidate to be compared")
	t.Logf("compared %d candidates across truth-pass workloads; batch == per-candidate", checked)
}

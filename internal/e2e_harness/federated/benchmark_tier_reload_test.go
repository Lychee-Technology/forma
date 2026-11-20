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

// Reloading the harness from a published dataset to one in which a schema
// has no parquet must not leave that schema's record pointing at objects the
// reset deleted. The reset replaces every published generation with an
// empty one before it deletes the old objects, so the trade schema, hot-only
// in the second dataset, reads as "nothing in parquet": its record names an
// empty generation whose manifest is still there, and every object the first
// dataset published is gone.
func TestBenchmarkTierReload_PublishedToHotOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	h, err := federated.NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.CleanupOrLog(ctx, t)

	g, err := bench.NewGenerator(bench.GeneratorConfig{
		Scale: bench.ScaleSmall, Distribution: bench.DistributionHotspot, Seed: 7,
		TradeCount: 60, CustomerCount: 6, SecurityCount: 4, OverlapRatio: 0.10,
	})
	require.NoError(t, err)
	dataset, err := g.Generate()
	require.NoError(t, err)
	tiered, err := bench.SplitIntoTiers(dataset, bench.TierMixBalanced)
	require.NoError(t, err)
	require.NoError(t, bench.LoadTieredDataset(ctx, h, tiered))
	published, err := h.PublishedFiles(ctx, bench.SchemaIDTrade)
	require.NoError(t, err)
	require.NotEmpty(t, published, "the first dataset publishes the trade schema's parquet")

	hotOnly := &bench.TieredDataset{Profile: tiered.Profile, Hot: tiered.Hot}
	require.NoError(t, bench.LoadTieredDataset(ctx, h, hotOnly))

	files, err := h.PublishedFiles(ctx, bench.SchemaIDTrade)
	require.NoError(t, err, "the record names a generation whose manifest the reset kept")
	require.Empty(t, files, "a hot-only schema's generation lists nothing")
	for _, f := range published {
		exists, err := h.ObjectExists(ctx, f.Key)
		require.NoError(t, err)
		require.False(t, exists, "the reset deleted %s", f.Key)
	}
}

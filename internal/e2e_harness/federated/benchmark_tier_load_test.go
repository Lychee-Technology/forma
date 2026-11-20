//go:build e2e

package federated_test

import (
	"context"
	"testing"
	"time"

	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
	bench "github.com/lychee-technology/forma/internal/e2e_harness/federated/benchmark"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/stretchr/testify/require"
)

func TestBenchmarkTierLoad_SmallDataset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	h, err := federated.NewFederatedTestHarness(ctx)
	require.NoError(t, err)
	defer h.CleanupOrLog(ctx, t)

	g, err := bench.NewGenerator(bench.GeneratorConfig{
		Scale:         bench.ScaleSmall,
		Distribution:  bench.DistributionHotspot,
		Seed:          42,
		TradeCount:    120,
		CustomerCount: 12,
		SecurityCount: 6,
		OverlapRatio:  0.10,
		DeleteRatio:   0.05,
	})
	require.NoError(t, err)
	dataset, err := g.Generate()
	require.NoError(t, err)
	tiered, err := bench.SplitIntoTiers(dataset, bench.TierMixBalanced)
	require.NoError(t, err)
	require.NoError(t, bench.LoadTieredDataset(ctx, h, tiered))

	// The trade schema's base and delta rows are published: its current
	// generation lists a base and a delta object, together holding every
	// parquet row the split put there (#896).
	files, err := h.PublishedFiles(ctx, bench.SchemaIDTrade)
	require.NoError(t, err)
	rows := map[manifestv2.Tier]int64{}
	for _, f := range files {
		rows[f.Tier] += f.RowCount
	}
	var baseTrade, deltaTrade int
	for _, r := range tiered.Base {
		if r.SchemaID == bench.SchemaIDTrade {
			baseTrade++
		}
	}
	for _, r := range tiered.Delta {
		if r.SchemaID == bench.SchemaIDTrade {
			deltaTrade++
		}
	}
	require.Len(t, files, 2)
	require.Equal(t, int64(baseTrade), rows[manifestv2.TierBase])
	require.Equal(t, int64(deltaTrade), rows[manifestv2.TierDelta])
	require.Greater(t, h.CountUnflushedRecords(ctx), 0)
	if tiered.Summary.DeletedInHot > 0 {
		var deletedHot int
		err = h.PGDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM change_log WHERE schema_id = $1 AND flushed_at = 0 AND deleted_at > 0`, bench.SchemaIDTrade).Scan(&deletedHot)
		require.NoError(t, err)
		require.Greater(t, deletedHot, 0)
	}
}

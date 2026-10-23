package federated

import (
	"context"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestDuckDBWindowOverEmptyUnionBranchSurvivesRepetition guards #702. DuckDB
// before 1.5.6 let a parallel window operator finalize before every sink task
// ran when one input block was empty, as when a UNION ALL branch filters to
// nothing. The federated template feeds its windows exactly such a union (the
// parquet tiers and pg_source), and the race surfaced as "INTERNAL Error:
// Attempted to access index 0 within vector of size 0", which invalidates the
// whole database. On 1.4.5 this loop failed within a few hundred iterations
// at threads=2 (upstream duckdb#22846, fixed by duckdb#25844 in 1.5.6).
func TestDuckDBWindowOverEmptyUnionBranchSurvivesRepetition(t *testing.T) {
	client, err := NewDuckDBClient(forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MaxConnections: 1,
		MaxParallelism: 2,
		QueryTimeout:   time.Minute,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	for _, stmt := range []string{
		`CREATE TABLE cold AS SELECT 'a' AS row_id, 2.25::DOUBLE AS ratio`,
		`CREATE TABLE hot AS SELECT 'b' AS row_id, 1.25::DOUBLE AS ratio`,
	} {
		_, err := client.DB.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	// The cold branch filters to no rows; the hot branch holds the one match.
	const query = `
WITH unified AS (
  SELECT row_id, 1 AS source_tier_priority FROM cold WHERE ratio IN (1.25)
  UNION ALL
  SELECT row_id, 3 AS source_tier_priority FROM hot WHERE ratio IN (1.25)
)
SELECT row_id, COUNT(*) OVER () AS total_records FROM unified ORDER BY row_id`

	for i := 0; i < 2000; i++ {
		var rowID string
		var total int64
		err := client.DB.QueryRowContext(ctx, query).Scan(&rowID, &total)
		require.NoError(t, err, "iteration %d", i)
		require.Equal(t, "b", rowID)
		require.Equal(t, int64(1), total)
	}
}

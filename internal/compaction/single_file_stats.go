package compaction

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lychee-technology/forma/internal/manifest"
)

// SingleFileStats recomputes one parquet file's manifest metadata (row
// count, row_id min/max, changed_at min/max) from its contents via the
// given DuckDB session. It runs the same stats query the merge path uses
// for a freshly merged file, so a manifest entry rebuilt from it matches
// what compaction itself would have written. The manifest-reconcile tool
// uses this to repair orphaned delta files (#203) without trusting
// filenames.
func SingleFileStats(ctx context.Context, db *sql.DB, uri string) (MergeStats, error) {
	statsSQL, err := buildMergeStatsSQL(uri)
	if err != nil {
		return MergeStats{}, fmt.Errorf("build single-file stats sql: %w", err)
	}
	var stats MergeStats
	if err := db.QueryRowContext(ctx, statsSQL).Scan(
		&stats.RowsOut, &stats.RowIDMin, &stats.RowIDMax, &stats.CreatedMin, &stats.CreatedMax,
	); err != nil {
		return MergeStats{}, fmt.Errorf("collect parquet stats from %s: %w", uri, err)
	}
	stats.RowsIn = stats.RowsOut
	return stats, nil
}

// IsConcurrentModification reports whether err is a confirmed HTTP 412
// conditional-put rejection — the only save failure that proves the write
// did not commit. The V1 manifest-reconcile tool saves V1 manifests under an
// etag and classifies its conflicts with it until garbage collection (#891)
// replaces it; the compactor no longer saves a mutable manifest.
func IsConcurrentModification(err error) bool {
	if err == nil {
		return false
	}
	return manifest.IsPreconditionFailed(err)
}

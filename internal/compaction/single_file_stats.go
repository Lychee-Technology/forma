package compaction

import (
	"context"
	"database/sql"
	"fmt"
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
// did not commit. Exported for callers outside this package (the
// manifest-reconcile tool) that save manifests under an etag without going
// through the compactor's saveManifestChecked.
func IsConcurrentModification(err error) bool {
	if err == nil {
		return false
	}
	return isPreconditionFailed(err)
}

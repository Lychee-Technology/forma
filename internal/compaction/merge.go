package compaction

import (
	"context"
	"database/sql"
	"fmt"
)

// MergeStats carries what the rewrite orchestration needs to build the new
// base FileEntry and the CompactionResult counters.
type MergeStats struct {
	RowsIn     int64  // version rows read across all source files
	RowsOut    int64  // surviving LWW winners written to the merged file
	RowIDMin   string // "" when the merge produced zero rows
	RowIDMax   string
	CreatedMin int64
	CreatedMax int64
}

// Merger folds a schema's complete base+delta parquet set into one merged
// base file at tmpURI. Implementations only stage the tmp object; the
// compactor owns promotion to the final key, the manifest swap, and source
// cleanup.
type Merger interface {
	MergeToTmp(ctx context.Context, sourceURIs []string, tmpURI string) (MergeStats, error)
}

// DuckMerger implements Merger over a DuckDB connection that already has
// httpfs and S3 credentials configured — cdc.NewDuckExporter's DB is the
// production source of exactly that.
type DuckMerger struct {
	DB *sql.DB
	// CopyOptions overrides the parquet COPY options (defaults to the CDC
	// exporters' FORMAT PARQUET, V2, ZSTD level 3).
	CopyOptions string
}

func (d *DuckMerger) MergeToTmp(ctx context.Context, sourceURIs []string, tmpURI string) (MergeStats, error) {
	if d == nil || d.DB == nil {
		return MergeStats{}, fmt.Errorf("duck merger has no database")
	}

	mergeSQL, err := buildMergeSQL(sourceURIs, tmpURI, d.CopyOptions)
	if err != nil {
		return MergeStats{}, fmt.Errorf("build merge sql: %w", err)
	}
	if _, err := d.DB.ExecContext(ctx, mergeSQL); err != nil {
		return MergeStats{}, fmt.Errorf("merge %d parquet sources to %s: %w", len(sourceURIs), tmpURI, err)
	}

	stats := MergeStats{}
	rowsInSQL, err := buildMergeRowsInSQL(sourceURIs)
	if err != nil {
		return MergeStats{}, fmt.Errorf("build rows-in sql: %w", err)
	}
	if err := d.DB.QueryRowContext(ctx, rowsInSQL).Scan(&stats.RowsIn); err != nil {
		return MergeStats{}, fmt.Errorf("count merge input rows: %w", err)
	}

	statsSQL, err := buildMergeStatsSQL(tmpURI)
	if err != nil {
		return MergeStats{}, fmt.Errorf("build stats sql: %w", err)
	}
	if err := d.DB.QueryRowContext(ctx, statsSQL).Scan(
		&stats.RowsOut, &stats.RowIDMin, &stats.RowIDMax, &stats.CreatedMin, &stats.CreatedMax,
	); err != nil {
		return MergeStats{}, fmt.Errorf("collect merged file stats from %s: %w", tmpURI, err)
	}
	return stats, nil
}

var _ Merger = (*DuckMerger)(nil)

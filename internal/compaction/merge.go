package compaction

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lychee-technology/forma/internal/parquetcheck"
	"go.uber.org/zap"
)

// MergeStats carries what the compaction needs to describe the merged
// object in its manifest entry and its result.
type MergeStats struct {
	RowsIn     int64  // version rows read across all source files
	RowsOut    int64  // each row's newest version, tombstones included, written to the merged file
	RowIDMin   string // "" when the merge produced zero rows
	RowIDMax   string
	CreatedMin int64
	CreatedMax int64
	// Columns is the merged file's footer schema (name to DuckDB type),
	// which its manifest entry lists. A V2 entry always lists them.
	Columns map[string]string
}

// Merger folds parquet sources into one merged file at tmpURI, keeping
// each row's newest version, tombstones included. Implementations only write
// that file; the compactor stages it under a fresh write-once key and
// publishes the generation.
type Merger interface {
	MergeToTmp(ctx context.Context, sourceURIs []string, tmpURI string) (MergeStats, error)
}

// DuckMerger implements Merger over a DuckDB connection that already has
// httpfs and S3 credentials configured; cdc.NewDuckExporter's DB is the
// production source of exactly that.
type DuckMerger struct {
	DB *sql.DB
	// CopyOptions overrides the parquet COPY options (defaults to the CDC
	// exporters' FORMAT PARQUET, V2, ZSTD level 3).
	CopyOptions string
	// Logger is optional; nil is safe.
	Logger *zap.Logger
	// describeColumns is a test seam for the post-merge footer read; nil
	// uses parquetcheck over this merger's own connection.
	describeColumns func(ctx context.Context, uri string) (map[string]string, error)
}

func (d *DuckMerger) MergeToTmp(ctx context.Context, sourceURIs []string, tmpURI string) (MergeStats, error) {
	if d == nil || d.DB == nil {
		return MergeStats{}, fmt.Errorf("duck merger has no database")
	}

	// The merge runs union_by_name (#189), which would NULL-pad the system
	// columns of a malformed source: every such row then folds into the
	// single NULL row_id partition and all but one are silently discarded. If
	// published, that lossy output would replace the sources in the next
	// generation, and readers of it would no longer consult them. Enforce the
	// export invariant per source BEFORE any write. An unreadable footer is
	// inconclusive and passes through: the merge itself must read every file
	// and will fail loudly on it.
	if err := validateMergeSourceSchemas(ctx, d.DB, sourceURIs); err != nil {
		return MergeStats{}, fmt.Errorf("pre-merge parquet schema validation: %w", err)
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

	describe := d.describeColumns
	if describe == nil {
		describe = func(ctx context.Context, uri string) (map[string]string, error) {
			return parquetcheck.DescribeColumns(ctx, d.DB, uri)
		}
	}
	cols, err := describe(ctx, tmpURI)
	if err != nil {
		// A V2 manifest entry lists the file's columns and has no
		// footer-probe fallback, so a merged file that cannot be described
		// is not staged.
		return MergeStats{}, fmt.Errorf("read the footer columns of the merged file %s: %w", tmpURI, err)
	}
	if len(cols) == 0 && stats.RowsOut > 0 {
		return MergeStats{}, fmt.Errorf("merged file %s has no column in its footer", tmpURI)
	}
	stats.Columns = cols
	return stats, nil
}

var _ Merger = (*DuckMerger)(nil)

// validateMergeSourceSchemas checks every merge source against the
// parquetcheck system-column invariant. Compaction sources are
// manifest-listed and rendered through sqlutil.EscapeLiteral at every
// site (#478, #546), so their spelling cannot break the SQL, but a rogue
// registration (the #187 fabrication class) must abort the merge before
// it can misfold rows.
func validateMergeSourceSchemas(ctx context.Context, db *sql.DB, sourceURIs []string) error {
	for _, uri := range sourceURIs {
		if err := validateMergeURI(uri); err != nil {
			return fmt.Errorf("validate merge source: %w", err)
		}
		cols, err := parquetcheck.DescribeColumns(ctx, db, uri)
		if err != nil {
			continue // inconclusive: the merge read will fail loudly on it
		}
		if err := parquetcheck.Check(uri, cols); err != nil {
			return fmt.Errorf("merge source rejected: %w", err)
		}
	}
	return nil
}

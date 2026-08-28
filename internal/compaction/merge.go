package compaction

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lychee-technology/forma/internal/parquetcheck"
	"go.uber.org/zap"
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
	// Columns is the merged file's footer schema (name → DuckDB type),
	// stamped into the manifest entry (#256). Nil when the self-describe
	// failed; the entry then stays unstamped and reads fall back to probing.
	Columns map[string]string
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
	// Logger reports the best-effort manifest stamp probe (#256). Optional —
	// nil is safe and silences it.
	Logger *zap.Logger
	// describeColumns is a test seam for the post-merge footer probe that
	// stamps the manifest entry (#256), mirroring cdc's flush-side seam; nil
	// uses parquetcheck over this merger's own connection. The probe is the
	// one step whose failure must be observable without being reproducible
	// through the connection that just succeeded at everything before it.
	describeColumns func(ctx context.Context, uri string) (map[string]string, error)
}

// log is the nil-safe accessor for the optional logger.
func (d *DuckMerger) log() *zap.Logger {
	if d == nil || d.Logger == nil {
		return zap.NewNop()
	}
	return d.Logger
}

func (d *DuckMerger) MergeToTmp(ctx context.Context, sourceURIs []string, tmpURI string) (MergeStats, error) {
	if d == nil || d.DB == nil {
		return MergeStats{}, fmt.Errorf("duck merger has no database")
	}

	// The merge runs union_by_name (#189), which would NULL-pad the system
	// columns of a malformed source: every such row then folds into the
	// single NULL row_id partition and all but one are silently discarded. If
	// published, that lossy output would splice the source entries out of the
	// manifest; although the source bytes remain as unlisted GC candidates,
	// the manifest read path would no longer consult them. Enforce the export
	// invariant per source BEFORE any write. An unreadable footer is
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
		// Best-effort (#256): an unstamped entry only costs the read path a
		// footer probe, so this must never fail a merge that already read and
		// wrote these bytes. It is still worth saying out loud — a DESCRIBE
		// failing on an object DuckDB just wrote is a signal about the store,
		// and silently dropping it leaves the resulting probe traffic with no
		// explanation anywhere.
		d.log().Warn("failed to describe merged parquet; manifest entry stays unstamped",
			zap.String("tmp_uri", tmpURI), zap.Error(err))
		return stats, nil
	}
	stats.Columns = cols

	return stats, nil
}

var _ Merger = (*DuckMerger)(nil)

// validateMergeSourceSchemas checks every merge source against the
// parquetcheck system-column invariant. Compaction sources are
// manifest-listed and URI-validated by buildMergeSQL's quoting rules, but a
// rogue registration (the #187 fabrication class) must abort the merge
// before it can misfold rows.
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

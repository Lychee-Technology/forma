package federated

import (
	"context"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// hasNoParquetToRead reports whether a source-authored path set can be
// answered without scanning parquet (#720): every path is a glob, every glob
// matches no object, and the schema holds no version that only parquet can
// serve (HasRowsOnlyInParquet) under the query's dirty-barrier cutoff.
//
// A source authors a glob only as its level-3 fallback (design.md §4.3.1),
// for a schema whose manifest is confirmed absent or empty. That is the
// state of a schema that has never been flushed, and DuckDB's read_parquet
// refuses a glob that matches nothing, so such a query used to fail and
// count against the breaker. An empty glob is not proof by itself, though:
// a lost manifest over objects written under another prefix, or over
// objects since deleted, looks the same. PostgreSQL tells the two apart.
// It keeps every live row, so when every live row has a change_log entry
// the hot leg serves, and no entry was flushed before the cutoff, the hot
// leg's answer is the whole answer. A flushed entry, or a live row a
// cdc-init export left without one, means parquet should hold data, and
// the read keeps failing as before.
//
// A hot-excluded query keeps the strict flushed_at = 0 barrier, so any
// flushed entry counts for it. Path sets from a caller hint are left alone:
// they name objects no manifest vouches for (#456), and #715 removes them.
// A failed glob listing is inconclusive and leaves the scan to report it.
func (e *DBFederatedQueryEngine) hasNoParquetToRead(
	ctx context.Context, tables model.StorageTables, q *model.FederatedAttributeQuery,
	paths []string, fromSource bool, graceCutoffMs int64,
) (bool, error) {
	if !fromSource || tables.ChangeLog == "" || tables.EntityMain == "" || e.duck == nil || e.dirtyIDFetcher == nil {
		return false, nil
	}
	for _, path := range paths {
		if !strings.ContainsAny(path, forma.ParquetGlobMetacharacters) {
			return false, nil
		}
	}
	for _, path := range paths {
		matched, err := globParquetPaths(ctx, e.duck, path)
		if err != nil || len(matched) > 0 {
			return false, nil
		}
	}
	cutoffMs := graceCutoffMs
	if !sqlgen.FederatedQueryHasHot(q) {
		cutoffMs = sqlgen.FlushGraceCutoffDisabled
	}
	found, err := e.dirtyIDFetcher.HasRowsOnlyInParquet(ctx, tables, q.SchemaID, cutoffMs)
	if err != nil {
		return false, fmt.Errorf("check whether schema %d has rows only parquet serves: %w: %w", q.SchemaID, ErrPostgresReadFailed, err)
	}
	return !found, nil
}

// emptyParquetScanSet is the scan treatment of a read that hasNoParquetToRead
// cleared: the scan source holds no object, and every attribute column is
// missing from it, as from a footer union of no files.
func emptyParquetScanSet(cache forma.SchemaAttributeCache) coldScanSet {
	set := coldScanColumns(cache, newColumnUnion())
	set.noObjects = true
	return set
}

// NoteEmptyParquetScan marks a plan whose parquet leg scanned nothing because
// the schema has no parquet to read yet (#720). Notes stay internal, like
// NotePartialParquetExclusion.
const NoteEmptyParquetScan = "parquet scan skipped: the fallback glob matches no object and every row is served hot"

// recordEmptyParquetScan notes an empty parquet leg on the plan. No-op when
// the leg scanned objects or no plan was requested.
func (c *duckDBExecutionPlanContext) recordEmptyParquetScan(noObjects bool) {
	if !noObjects || c == nil || c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}
	c.opts.ExecutionPlan.Notes = append(c.opts.ExecutionPlan.Notes, NoteEmptyParquetScan)
}

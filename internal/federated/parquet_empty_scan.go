package federated

import (
	"context"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// hasNoParquetToRead reports whether a source-authored path set can be
// answered without scanning parquet (#720): every path is a glob, every glob
// matches no object, and PostgreSQL records no parquet publication for the
// schema (HasParquetHistory).
//
// A source authors a glob only as its level-3 fallback (design.md §4.3.1),
// for a schema whose manifest is confirmed absent or empty. That is the
// state of a schema that has never been flushed, and DuckDB's read_parquet
// refuses a glob that matches nothing, so such a query used to fail and
// count against the breaker. An empty glob is not proof by itself, though:
// a lost manifest over objects written under another prefix, or over
// objects since deleted, looks the same. PostgreSQL tells the two apart. A
// flushed change_log entry of any age, or a live row a cdc-init export left
// without an entry, means parquet should hold data, and the read keeps
// failing as before. No timestamp is compared: flushed_at is stamped on the
// CDC host, so its order against this reader's clock proves nothing.
//
// A writer can publish between path resolution and this check. A flushed
// entry found here may then belong to a publication the resolution missed,
// not to a lost one, so the source is resolved again: a set that is no
// longer an empty glob set asks the pass to retry. Publications after the
// check are confirmNoParquetPublished's to catch.
//
// Path sets from a caller hint are left alone: they name objects no manifest
// vouches for (#456), and #715 removes them. A failed glob listing is
// inconclusive and leaves the scan to report it.
func (e *DBFederatedQueryEngine) hasNoParquetToRead(
	ctx context.Context, tables model.StorageTables, q *model.FederatedAttributeQuery,
	paths []string, fromSource bool,
) (bool, error) {
	if !fromSource || tables.ChangeLog == "" || tables.EntityMain == "" || e.duck == nil || e.dirtyIDFetcher == nil {
		return false, nil
	}
	if !globsMatchNothing(ctx, e.duck, paths) {
		return false, nil
	}
	found, err := e.dirtyIDFetcher.HasParquetHistory(ctx, tables, q.SchemaID)
	if err != nil {
		return false, fmt.Errorf("check whether schema %d has parquet history: %w: %w", q.SchemaID, ErrPostgresReadFailed, err)
	}
	if !found {
		return true, nil
	}
	if !e.sourceResolvesNoObjects(ctx, q) {
		return false, &parquetPublishedDuringReadError{SchemaID: q.SchemaID}
	}
	return false, nil
}

// confirmNoParquetPublished runs after an empty-scan pass has read pg_source
// and before its page is used. The empty scan source was fixed before
// DuckDB ran, so it cannot see a publication that landed in between, while
// the change_log writes that follow such a publication can move rows out of
// the hot leg: a flush marks them flushed, and the onboarding handoff clears
// the log once cdc-init has published. Either way the page may be short.
//
// Every writer publishes before it touches change_log. The flush copies its
// objects and appends the manifest before it marks rows, and the handoff
// runs after cdc-init has published. So any change_log write the hot leg saw
// follows a publication this check sees: the source now resolves a manifest,
// or the glob now matches an object. The argument rests on that causal
// order, not on any clock. It covers every publication the reader can see
// at all. A writer that keeps no manifest and writes outside the reader's
// glob is invisible to every read, as design.md §4.3.1 level 3 documents.
// A publication whose manifest and objects are lost before this check runs
// is not seen either; the page is short, and the next read fails on the
// change_log history it left. #760 tracks closing that window.
func (e *DBFederatedQueryEngine) confirmNoParquetPublished(ctx context.Context, q *model.FederatedAttributeQuery) error {
	if e.sourceResolvesNoObjects(ctx, q) {
		return nil
	}
	return &parquetPublishedDuringReadError{SchemaID: q.SchemaID}
}

// sourceResolvesNoObjects resolves q's path set again and reports whether it
// is still a source-authored glob set that matches no object. Any failure is
// a "no": the retry it leads to resolves from scratch and reports the
// failure through the usual paths.
func (e *DBFederatedQueryEngine) sourceResolvesNoObjects(ctx context.Context, q *model.FederatedAttributeQuery) bool {
	paths, fromSource, _, _, err := e.resolveParquetPaths(ctx, q)
	return err == nil && fromSource && globsMatchNothing(ctx, e.duck, paths)
}

// globsMatchNothing reports whether paths is a non-empty set of globs that
// all list no object. A failed listing is inconclusive and answers false.
func globsMatchNothing(ctx context.Context, duck DuckDBQueryExecutor, paths []string) bool {
	if len(paths) == 0 {
		return false
	}
	for _, path := range paths {
		if !strings.ContainsAny(path, forma.ParquetGlobMetacharacters) {
			return false
		}
	}
	for _, path := range paths {
		matched, err := globParquetPaths(ctx, duck, path)
		if err != nil || len(matched) > 0 {
			return false
		}
	}
	return true
}

// parquetPublishedDuringReadError marks an empty-scan pass that a parquet
// publication raced (#720). ExecuteDuckDBFederatedQuery discards the pass
// and retries once; the retry resolves the publication and scans it. A
// second race fails the read as a degradable ErrFederatedReadFailed, whose
// PostgreSQL-only fallback is complete by the storage invariant. The race is
// not a DuckDB failure, so it never reaches the breaker.
type parquetPublishedDuringReadError struct {
	SchemaID int16
}

func (e *parquetPublishedDuringReadError) Error() string {
	return fmt.Sprintf("schema %d: parquet was published while the read was answering from PostgreSQL alone, so rows the publication moved out of the hot tier may be missing", e.SchemaID)
}

func (e *parquetPublishedDuringReadError) Unwrap() error { return ErrFederatedReadFailed }

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

package federated

import (
	"context"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// confirmNoParquetPublished runs after a pass that settled on no parquet
// objects (#720) has read its hot leg, and before its page is used. The empty scan source was fixed before
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
	paths, fromSource, _, err := e.resolveParquetPaths(ctx, q)
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

// emptyParquetScanSet is the scan treatment of a frozen set that lists no
// object (#720): the scan source holds no object, and every attribute column is
// missing from it, as from a footer union of no files.
func emptyParquetScanSet(cache forma.SchemaAttributeCache) coldScanSet {
	set := coldScanColumns(cache, newColumnUnion())
	set.noObjects = true
	return set
}

// NoteEmptyParquetScan marks a plan whose parquet leg scanned nothing because
// the schema has no parquet to read yet (#720). Notes stay internal:
// toExecutionPlan drops them at the HTTP boundary (#301/#306).
const NoteEmptyParquetScan = "parquet scan skipped: the fallback glob matches no object and every row is served hot"

// recordEmptyParquetScan notes an empty parquet leg on the plan. No-op when
// the leg scanned objects or no plan was requested.
func (c *duckDBExecutionPlanContext) recordEmptyParquetScan(noObjects bool) {
	if !noObjects || c == nil || c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}
	c.opts.ExecutionPlan.Notes = append(c.opts.ExecutionPlan.Notes, NoteEmptyParquetScan)
}

package federated

import (
	"context"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// confirmNoParquetPublished runs after a pass that settled on no parquet
// objects (#720) has read its page and ended its snapshot S, and before the
// page is used. If the source no longer resolves an empty glob set, a
// publication landed since path resolution, and the page is discarded.
//
// The check is conservative: the page it guards is already complete as of
// S. The parquet-history record, the hot keys and the page's hot rows all
// come from S, and every writer publishes before it touches change_log (the
// flush appends to the manifest before it marks rows, and the onboarding
// handoff clears the log after cdc-init has published). A publication's
// change_log write that landed before S is history in S, and
// settleEmptyCandidate retried or failed the pass. One that lands after S
// cannot remove a row from what the pass reads from S: there every live row
// has an unflushed entry, and the hot leg serves it. So a publication whose
// manifest and objects are lost before this check does not shorten the
// page; its change_log write fails later reads as history instead. The
// check was kept from the attach pass, which read the hot leg outside any
// snapshot and could lose rows a racing flush marked.
//
// What stays open is the durable history itself: a cdc-init export whose
// rows have all been written again since the handoff leaves no record in
// PostgreSQL. Its rows are shadowed, so losing it changes no answer. #760
// tracks a durable initialization record (design.md §4.3.1 level 3).
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

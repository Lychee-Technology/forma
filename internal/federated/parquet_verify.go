package federated

import (
	"context"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// verifyParquetPaths re-reads each path individually via a full SELECT *
// drain and returns the paths confirmed corrupt. The drain reads a superset
// of any scan's columns, so it deterministically reproduces whatever
// per-file failure made the set scan fail; metadata-only probes (DESCRIBE,
// COUNT(*)) cannot, because DuckDB answers them without touching data pages
// (#251 spike). Corruption that decodes silently is invisible to every
// reader — the set scan included — so it can never reach this function;
// that pre-existing integrity gap is documented in the #251 spike findings
// and tracked in #347.
//
// The guarded sibling — identifyGuardViolations in parquet_guard_identify.go —
// deliberately inverts this drain's blindness: it reads through the #256 scan
// guard to NAME a schema-wrong object (#351), where this pass reads bare to
// EXCLUDE a byte-corrupt one. Keep the two drains distinct: guarding this one
// would auto-exclude schema-wrong objects, which #351 forbids.
//
// Confirmation requires TWO consecutive failed drains (#349 review R2-1):
// deterministic corruption fails every drain — same bytes, same decode —
// while a transient object-level fault (an S3 timeout, a reset connection)
// that fails once and reads clean on the immediate re-drain must NOT be
// cached as corruption; a single failure is inconclusive and leaves the
// query on the ordinary RecordFailure path. Glob entries are skipped:
// unverifiable means unexcludable, and the main scan keeps its all-or-nothing
// behavior for them. A cancelled context confirms nothing —
// cancellation is indistinguishable from corruption for the remaining
// paths. Runs only on the read-failure path, so its cost is bounded by
// failures, not by queries.
func verifyParquetPaths(ctx context.Context, duck DuckDBQueryExecutor, paths []string) []string {
	if duck == nil {
		return nil
	}
	var corrupt []string
	for _, path := range paths {
		if unverifiablePath(path) {
			continue
		}
		if err := drainParquet(ctx, duck, path); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if err2 := drainParquet(ctx, duck, path); err2 != nil {
				if ctx.Err() != nil {
					return nil
				}
				corrupt = append(corrupt, path)
			}
		}
	}
	return corrupt
}

// unverifiablePath reports whether a path cannot be probed on its own: a glob
// names a set rather than one object, so a solo drain of it proves nothing
// about any single object. These are exactly the entries the main scan keeps
// all-or-nothing behavior for — unverifiable means neither excludable (#251)
// nor nameable (#351). Quote-bearing entries used to be skipped too, back
// when both drains interpolated the path raw; since #456 every render site
// escapes it via sqlutil.EscapeLiteral, so a quote, double quote, or
// semicolon in an object key no longer withholds per-file coverage (#479).
func unverifiablePath(path string) bool {
	return strings.ContainsAny(path, forma.ParquetGlobMetacharacters)
}

// drainParquet opens one parquet object and iterates it to exhaustion.
func drainParquet(ctx context.Context, duck DuckDBQueryExecutor, path string) error {
	rows, err := duck.Query(ctx, fmt.Sprintf("SELECT * FROM read_parquet('%s')", sqlutil.EscapeLiteral(path)))
	if err != nil {
		return fmt.Errorf("open parquet %s: %w", path, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read parquet %s: %w", path, err)
	}
	return nil
}

// corruptParquetRetryError marks a scan failure attributed to specific
// corrupt objects that are now cached for exclusion (#251): one immediate
// retry will scan without them. Unwrap keeps the original classification
// chain (ErrFederatedReadFailed), so a caller that does not retry — a direct
// Stream consumer — degrades exactly as before.
type corruptParquetRetryError struct {
	Corrupt []string
	cause   error
}

func (e *corruptParquetRetryError) Error() string {
	return fmt.Sprintf("corrupt parquet objects excluded, retry available %v: %v", e.Corrupt, e.cause)
}

func (e *corruptParquetRetryError) Unwrap() error { return e.cause }

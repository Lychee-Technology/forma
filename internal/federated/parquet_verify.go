package federated

import (
	"context"
	"fmt"
	"strings"
)

// verifyParquetPaths re-reads each path individually via a full SELECT *
// drain and returns the paths whose drain failed. The drain reads a superset
// of any scan's columns, so it deterministically reproduces whatever
// per-file failure made the set scan fail; metadata-only probes (DESCRIBE,
// COUNT(*)) cannot, because DuckDB answers them without touching data pages
// (#251 spike). Corruption that decodes silently is invisible to every
// reader — the set scan included — so it can never reach this function;
// that pre-existing integrity gap is documented in the #251 spike findings
// and tracked in a follow-up issue. Glob and quote-bearing
// entries are skipped: unverifiable means unexcludable, and the main scan
// keeps its all-or-nothing behavior for them. A cancelled context confirms
// nothing — cancellation is indistinguishable from corruption for the
// remaining paths. Runs only on the read-failure path, so its cost is
// bounded by failures, not by queries.
func verifyParquetPaths(ctx context.Context, duck DuckDBQueryExecutor, paths []string) []string {
	if duck == nil {
		return nil
	}
	var corrupt []string
	for _, path := range paths {
		if strings.ContainsAny(path, `'";`) || strings.ContainsAny(path, "*?[") {
			continue
		}
		if err := drainParquet(ctx, duck, path); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			corrupt = append(corrupt, path)
		}
	}
	return corrupt
}

// drainParquet opens one parquet object and iterates it to exhaustion.
func drainParquet(ctx context.Context, duck DuckDBQueryExecutor, path string) error {
	rows, err := duck.Query(ctx, fmt.Sprintf("SELECT * FROM read_parquet('%s')", path))
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

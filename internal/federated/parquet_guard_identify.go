package federated

import (
	"context"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/sqlgen"
)

// identifyGuardViolations attributes a #256 scan-guard failure to the parquet
// object(s) that violate the export schema invariant (#351). The set scan is a
// multi-file union, so DuckDB raises one error for the whole set; the #251
// verify pass drains bare SELECT * and reads a schema-wrong object clean; and
// the missing-key classifier only speaks for objects that are absent. This
// pass closes that triage gap by re-reading each concrete path through the
// GUARDED single-file scan (sqlgen.BuildParquetScanSource) and attributing by
// differential: a path is a violator iff its guarded drain fails
// deterministically — two consecutive failures, mirroring #251 R2-1 — while
// its bare drain succeeds. Byte corruption and a sick store fail BOTH drains,
// so they can never be misattributed as schema violations; the differential is
// also what keeps this free of driver-message matching (errors.go), which the
// CAST channel would otherwise force.
//
// Guarded-first ordering is the cost choice: a healthy path costs exactly one
// guarded drain, so the common one-rogue-in-N failure costs ~N+3 drains; the
// bare confirmation leg runs only for paths already failing the guard twice.
// Glob and quote-bearing entries are skipped — unverifiable per-file, exactly
// as in verifyParquetPaths. A cancelled context attributes nothing.
//
// Identification only (#351): callers must never exclude the returned paths —
// unlike byte corruption, a schema-wrong object may legitimately own rows that
// exclusion would silently drop once the object is repaired. Runs only on the
// read-failure path, so cost is bounded by failures, not by queries.
func identifyGuardViolations(ctx context.Context, duck DuckDBQueryExecutor, paths []string) []string {
	if duck == nil {
		return nil
	}
	var violating []string
	for _, path := range paths {
		if strings.ContainsAny(path, `'";`) || strings.ContainsAny(path, "*?[") {
			continue
		}
		if err := drainGuardedParquet(ctx, duck, path); err == nil {
			continue
		}
		if ctx.Err() != nil {
			return nil
		}
		if err := drainGuardedParquet(ctx, duck, path); err == nil {
			continue // transient blip, not a deterministic violation
		}
		if ctx.Err() != nil {
			return nil
		}
		if err := drainParquet(ctx, duck, path); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			continue // bare drain fails too: bytes or store, not schema — #251 territory
		}
		violating = append(violating, path)
	}
	return violating
}

// drainGuardedParquet opens one parquet object through the guarded scan
// source and iterates it to exhaustion, so it reproduces whatever guard
// failure the object contributes to a set scan: the error() presence guards,
// the BIGINT CAST type guard, and the binder failure when a guarded column is
// absent from the single-file set (proved against the pinned DuckDB in
// sqlgen/duckdb_cold_scan_identify_test.go).
func drainGuardedParquet(ctx context.Context, duck DuckDBQueryExecutor, path string) error {
	src := sqlgen.BuildParquetScanSource(fmt.Sprintf("'%s'", path), nil)
	rows, err := duck.Query(ctx, "SELECT * FROM "+src)
	if err != nil {
		return fmt.Errorf("open guarded parquet %s: %w", path, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read guarded parquet %s: %w", path, err)
	}
	return nil
}

// ParquetGuardViolationError decorates a guard-classified read failure with
// the objects identified by the guarded per-file drain (#351). Deliberate
// wording: the paths FAIL the guarded single-file scan — an invariant
// statement, not a causation claim, because a single-file scan is strictly
// stricter than the set scan (a file missing only deleted_at fails alone but
// is tolerated in a set where a sibling carries the column). Unwrap keeps the
// original classification chain (ErrFederatedReadFailed): identification must
// not change degradability, retry, or breaker behavior. Paths are full
// storage URIs — operator detail; safe internally because httpapi redacts
// non-published error text and toExecutionPlan drops Notes (#301/#306), the
// same boundary contract corruptParquetRetryError relies on.
type ParquetGuardViolationError struct {
	SchemaID int16
	Paths    []string
	cause    error
}

func (e *ParquetGuardViolationError) Error() string {
	return fmt.Sprintf(
		"schema %d: %d parquet object(s) fail the guarded single-file scan (export schema invariant #189/#256) %v: %v",
		e.SchemaID, len(e.Paths), e.Paths, e.cause)
}

func (e *ParquetGuardViolationError) Unwrap() error { return e.cause }

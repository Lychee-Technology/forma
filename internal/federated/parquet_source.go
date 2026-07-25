package federated

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
)

// ParquetSource resolves the authoritative parquet object set of a schema —
// typically from the CDC manifest — so federated reads scan exactly the
// listed objects instead of expanding a storage glob. The distinction is
// what makes cold-tier loss detectable (#187 scenario 2): a glob silently
// shrinks to whatever objects survive, while a listed object missing from
// storage fails the scan and classifies via MissingIn.
type ParquetSource interface {
	// Paths returns the schema's parquet objects as full s3:// URIs (or a
	// fallback glob for schemas with no manifest yet). Returning empty fails
	// the read with ErrNoParquetPaths (#299): there is nothing to scan, and
	// every query reaching the DuckDB engine wants warm and/or cold data, so
	// an empty set cannot be answered honestly. A schema with no data yet
	// should yield its fallback glob rather than nothing.
	Paths(ctx context.Context, schemaID int16) ([]string, error)
	// MissingIn probes the given scanned path set (full s3:// URIs; glob
	// and foreign-bucket entries are skipped as unprovable) and returns the
	// bucket-relative keys absent from storage. It is consulted only on the
	// read-error path — zero happy-path probes — and only over the exact
	// set the failed scan used: re-resolving the manifest here would
	// classify against a newer snapshot than the one that failed, so a
	// concurrent flush/compaction could hide the lost key or surface an
	// unrelated one (#249 review).
	MissingIn(ctx context.Context, scanned []string) ([]string, error)
}

// WithParquetSource injects the manifest-driven parquet path resolver. A nil
// source keeps the legacy behavior: paths come only from the query's render
// hints (caller-supplied glob or explicit list).
func WithParquetSource(src ParquetSource) EngineOption {
	return func(e *DBFederatedQueryEngine) { e.parquetSource = src }
}

// resolveParquetPaths resolves the parquet path set once per query. An
// explicit caller-supplied render hint always wins (#184 pins that an
// explicit S3ParquetPathTemplate directs read_parquet at the specified
// location) — including its failure: a hint that cannot render is invalid
// input and must not silently fall through to the manifest source. The
// source is the default when no hint is present. fromSource reports whether
// the paths came from the source, so read-error classification only probes
// path sets the source authored.
func (e *DBFederatedQueryEngine) resolveParquetPaths(ctx context.Context, q *model.FederatedAttributeQuery) (paths []string, fromSource bool, err error) {
	hinted, err := duckDBParquetPathsForQuery(q)
	if err != nil {
		return nil, false, err
	}
	if len(hinted) > 0 {
		return hinted, false, nil
	}
	if e == nil || e.parquetSource == nil || q == nil {
		return nil, false, nil
	}
	resolved, err := e.parquetSource.Paths(ctx, q.SchemaID)
	if err != nil {
		return nil, false, fmt.Errorf("manifest parquet source: %w: %w", ErrFederatedReadFailed, err)
	}
	return resolved, true, nil
}

// classifyDuckDBReadError classifies a failed DuckDB read by storage state:
// when the failed scan ran over source-authored paths and probing that exact
// scanned set finds listed keys missing from storage, the failure is
// manifest inconsistency (cold-tier loss); anything else — probe errors
// included, since an unreachable store cannot prove loss — stays a plain
// read failure.
func (e *DBFederatedQueryEngine) classifyDuckDBReadError(ctx context.Context, q *model.FederatedAttributeQuery, scanned []string, pathsFromSource bool) error {
	if !pathsFromSource || e == nil || e.parquetSource == nil || q == nil {
		return ErrFederatedReadFailed
	}
	missing, err := e.parquetSource.MissingIn(ctx, scanned)
	if err != nil || len(missing) == 0 {
		return ErrFederatedReadFailed
	}
	return &ParquetSetInconsistentError{SchemaID: q.SchemaID, MissingKeys: missing}
}

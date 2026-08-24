package federated

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma"
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
	//
	// stamps carries each stamped entry's write-time footer columns keyed by
	// its returned path; nil/absent keys mean unstamped — the validator falls
	// back to probing (#256).
	Paths(ctx context.Context, schemaID int16) (paths []string, stamps map[string]map[string]string, err error)
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
// path sets the source authored. Only source-authored sets are filtered
// against the verification-confirmed corrupt objects (#251); excludedCorrupt
// names what was dropped so the execution plan can say the scan was partial.
//
// stamps are the source's manifest-recorded footer columns (#256), keyed by
// the returned paths, for the pre-read validator to consult instead of
// probing. Only a source can author them: hint-authored paths name objects no
// manifest entry vouches for, so they stay nil and probe as before. They are
// forwarded unfiltered — a stamp key for a #251-excluded object is harmless
// because the validator looks up by surviving path.
func (e *DBFederatedQueryEngine) resolveParquetPaths(ctx context.Context, q *model.FederatedAttributeQuery) (paths []string, fromSource bool, stamps map[string]map[string]string, excludedCorrupt []string, err error) {
	hinted, err := duckDBParquetPathsForQuery(q, e.cfg)
	if err != nil {
		return nil, false, nil, nil, fmt.Errorf("render parquet path hint: %w", err)
	}
	if len(hinted) > 0 {
		return hinted, false, nil, nil, nil
	}
	if e == nil || e.parquetSource == nil || q == nil {
		return nil, false, nil, nil, nil
	}
	resolved, stamps, err := e.parquetSource.Paths(ctx, q.SchemaID)
	if err != nil {
		// Resolution failures are transient infrastructure by default (S3
		// unreachable, manifest unreadable) and classify as degradable. But a
		// source that reports a read-path *consistency* problem has already
		// classified itself, and relabelling it ErrFederatedReadFailed would
		// hand it to the degraded fallback — silencing exactly the state it
		// exists to report.
		if errors.Is(err, forma.ErrManifestSchemaMismatch) {
			return nil, false, nil, nil, fmt.Errorf("manifest parquet source: %w", err)
		}
		return nil, false, nil, nil, fmt.Errorf("manifest parquet source: %w: %w", ErrFederatedReadFailed, err)
	}
	// Exclude verification-confirmed corrupt objects (#251). If that would
	// empty the set, scan the full set instead: total corruption must fail
	// loudly with its own classification, not as ErrNoParquetPaths.
	kept, excluded := e.corruptPaths.Split(resolved)
	if len(kept) == 0 {
		return resolved, true, stamps, nil, nil
	}
	return kept, true, stamps, excluded, nil
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

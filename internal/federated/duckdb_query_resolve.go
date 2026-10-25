package federated

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// scanSources is the resolved storage context of one federated read.
type scanSources struct {
	paths           []string
	fromSource      bool
	graceCutoffMs   int64
	cold            coldScanSet
	excludedCorrupt []string
}

// schemaCacheByID is the nil-safe metadata-cache accessor shared by the
// pre-flight (#255/#371 cold scan-set computation) and the SQL build.
func (e *DBFederatedQueryEngine) schemaCacheByID(schemaID int16) (forma.SchemaAttributeCache, bool) {
	if e == nil || e.metadataCache == nil {
		return nil, false
	}
	return e.metadataCache.GetSchemaCacheByID(schemaID)
}

// resolveScanSources is the storage-facing pre-flight of a DuckDB federated
// query: it stamps the #252 flush-grace cutoff, resolves the parquet path
// set, validates the parquet system-column invariant and the #732 attribute
// types, and computes the #255 cold-missing and #371 cold-pinned column sets
// from the footer union the validation produced.
func (e *DBFederatedQueryEngine) resolveScanSources(ctx context.Context, q *model.FederatedAttributeQuery) (scanSources, error) {
	// The flush-grace cutoff is stamped BEFORE resolution (#252): any row
	// marked flushed at or after this instant may belong to a delta this
	// path set does not list yet, so the dirty barrier keeps it hot-readable
	// for this query.
	graceCutoffMs := e.flushGraceCutoffMs(time.Now().UnixMilli())

	// Resolve the parquet path set once (#187): explicit render hints win,
	// otherwise the manifest-driven source authors the list. Provenance
	// gates the read-error classification. A source also hands back its
	// recorded footer stamps (#256) so the validation below can skip probing
	// the objects the manifest already vouches for.
	paths, fromSource, stamps, excludedCorrupt, err := e.resolveParquetPaths(ctx, q)
	if err != nil {
		return scanSources{}, fmt.Errorf("resolve parquet paths: %w", err)
	}

	// An empty path set cannot answer this query (#299). Only warm/cold-wanting
	// queries reach here — hot-only and PreferHot short-circuit to Postgres in
	// Query — so there is nothing to scan and no honest partial answer. Failing
	// here keeps the misconfiguration distinguishable from a transient S3
	// outage, which the previous read_parquet(<no value>) parser error was not.
	// It must precede the validator: that walks the path set and so reports
	// nothing at all on an empty one.
	if len(paths) == 0 {
		return scanSources{}, &NoParquetPathsError{
			SchemaID:         q.SchemaID,
			SourceConfigured: e.parquetSource != nil,
		}
	}

	// Benchmark schemas (100-102) are exempt from the pre-read checks: their
	// parquet is the legacy CSV-sniffed harness shape (row_id VARCHAR, cast by
	// the hardcoded benchmark projections) — the parquetcheck invariant
	// codifies the PRODUCTION exporters, which never write those IDs
	// (ValidateFixtureSchemaID reserves the range).
	var cold coldScanSet
	if !isBenchmarkSchemaID(q.SchemaID) {
		cold, err = e.validateScanSet(ctx, q.SchemaID, paths, fromSource, stamps)
		if err != nil {
			return scanSources{}, err
		}
	}
	return scanSources{
		paths:           paths,
		fromSource:      fromSource,
		graceCutoffMs:   graceCutoffMs,
		cold:            cold,
		excludedCorrupt: excludedCorrupt,
	}, nil
}

// validateScanSet runs the pre-read checks over the resolved path set and
// returns the cold scan treatment they license.
//
// Schema-invariant validation (#189): the scan's union_by_name tolerates
// attribute-column drift across parquet generations, which would let a
// wrong-schema object's rows silently vanish (NULL row_id drops out of the
// dirty anti-join) instead of failing loudly (#187). Schema violations fail
// here, classified and degradable; unreadable footers are inconclusive. No
// recordQueryFailure: the query never started, and its timing fields would
// read from an unset start time.
//
// The type guard (#732) then judges every object holding a column whose type
// the declaration does not export, and refuses a read that would change
// values. Objects it could not judge — a manifest-listed object whose footer
// could not be read, or any object whose value proof failed to read — fail
// the read closed as an unverifiedParquetError, which the caller classifies
// like a failed scan. A glob-expanded hint set is not a manifest inventory,
// so its unreadable footers keep deferring to the main read.
func (e *DBFederatedQueryEngine) validateScanSet(
	ctx context.Context, schemaID int16, paths []string, fromSource bool, stamps map[string]map[string]string,
) (coldScanSet, error) {
	union, complete, err := e.schemaValidator.Validate(ctx, e.duck, paths, stamps)
	if err != nil {
		return coldScanSet{}, fmt.Errorf("pre-read parquet schema validation: %w", err)
	}
	cache, ok := e.schemaCacheByID(schemaID)
	if !ok {
		// The build refuses a query without metadata (#151); nothing to judge.
		return coldScanSet{}, nil
	}
	unverified, err := e.schemaValidator.CheckTypes(ctx, e.duck, schemaID, cache, union, stamps)
	if err != nil {
		return coldScanSet{}, fmt.Errorf("pre-read parquet type guard: %w", err)
	}
	if fromSource {
		unverified = append(slices.Clone(union.unverified), unverified...)
	}
	if len(unverified) > 0 {
		slices.Sort(unverified)
		return coldScanSet{}, &unverifiedParquetError{
			SchemaID: schemaID, Paths: slices.Compact(unverified), scanned: paths, fromSource: fromSource,
		}
	}
	// Never-flushed column augmentation (#255) and stale-type pinning
	// (#371): only a COMPLETE footer union may drive them — augmenting a
	// column that exists in an unprobed file would collide with the real
	// column, and a pin decided without the drifted file would miss it.
	// Incomplete unions fall back to the plain guarded scan, whose binder or
	// conversion failure stays loud, classified, and degradable (today's
	// contract).
	//
	// Accepted listing skew (adjudicated): for a GLOB path set the validator
	// expands the glob itself while read_parquet re-lists at execution, so a
	// flush landing between the two puts an unprobed file in the scan while
	// complete=true. The augmented NULL alias then collides with that file's
	// real column — a one-time loud, classified binder failure that
	// self-heals on the next query, because the scan set is recomputed per
	// query. This is accepted; do not widen the gate to avoid it.
	if !complete {
		return coldScanSet{}, nil
	}
	return coldScanColumns(cache, union), nil
}

// unverifiedParquetError names the objects the pre-read checks could not
// read, so their column types could not be judged against the declarations
// (#732). It is the cause handed to failDuckDBScan, which classifies it the
// way it classifies a failed scan: a missing object is #187 inconsistency, a
// corrupt one is excluded for a #251 partial retry, and anything else is a
// degradable ErrFederatedReadFailed. Paths are storage locations, which the
// HTTP boundary redacts with the rest of an internal-class error.
type unverifiedParquetError struct {
	SchemaID int16
	Paths    []string
	// scanned and fromSource are the resolved path set the classification
	// probes, as for a failed scan.
	scanned    []string
	fromSource bool
}

func (e *unverifiedParquetError) Error() string {
	return fmt.Sprintf("schema %d: %d live parquet object(s) could not be read to verify their column types against the declared attribute types, so the read fails closed: %s",
		e.SchemaID, len(e.Paths), strings.Join(e.Paths, ", "))
}

package federated

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/lychee-technology/forma"
)

// scanSources is the resolved storage context of one federated read: the
// objects its generation lists, and the cold scan treatment the pre-read
// checks licensed over them.
type scanSources struct {
	paths []string
	cold  coldScanSet
}

// schemaCacheByID is the nil-safe metadata-cache accessor shared by the
// pre-flight (#255/#371 cold scan-set computation) and the SQL build.
func (e *DBFederatedQueryEngine) schemaCacheByID(schemaID int16) (forma.SchemaAttributeCache, bool) {
	if e == nil || e.metadataCache == nil {
		return nil, false
	}
	return e.metadataCache.GetSchemaCacheByID(schemaID)
}

// validateScanSet runs the pre-read checks over the generation's objects and
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
// values. Objects it could not judge — a listed object whose footer could
// not be read, or any object whose value proof failed to read — fail the read
// closed as an unverifiedParquetError, which the caller classifies like a
// failed scan. Its cause carries the read errors and, when the request ran out
// of budget, the context error, so a pre-read timeout is as non-degradable as
// a main-scan one.
func (e *DBFederatedQueryEngine) validateScanSet(
	ctx context.Context, schemaID int16, paths []string, stamps map[string]map[string]string,
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
	proofs, err := e.schemaValidator.CheckTypes(ctx, e.duck, schemaID, cache, union, stamps)
	if err != nil {
		return coldScanSet{}, fmt.Errorf("pre-read parquet type guard: %w", err)
	}
	unverified := slices.Clone(union.unverified)
	causes := make([]error, 0, len(proofs)+1)
	for _, p := range proofs {
		unverified = append(unverified, p.path)
		causes = append(causes, p.err)
	}
	if len(unverified) > 0 {
		slices.Sort(unverified)
		return coldScanSet{}, &unverifiedParquetError{
			SchemaID: schemaID, Paths: slices.Compact(unverified), cause: unverifiedCause(ctx, causes),
		}
	}
	// Never-flushed column augmentation (#255) and stale-type pinning
	// (#371): only a COMPLETE footer union may drive them — augmenting a
	// column that exists in an unprobed file would collide with the real
	// column, and a pin decided without the drifted file would miss it.
	// Incomplete unions fall back to the plain guarded scan, whose binder or
	// conversion failure stays loud, classified, and degradable (today's
	// contract).
	if !complete {
		return coldScanSet{}, nil
	}
	return coldScanColumns(cache, union), nil
}

// unverifiedParquetError names the objects the pre-read checks could not
// read, so their column types could not be judged against the declarations
// (#732). It is the cause handed to failDuckDBScan, which classifies it the
// way it classifies a failed scan: a missing object is a publication
// inconsistency (#759), a corrupt one fails as CorruptParquetError (#722), and
// anything else is a degradable ErrFederatedReadFailed. Paths are storage locations, which the
// HTTP boundary redacts with the rest of an internal-class error.
type unverifiedParquetError struct {
	SchemaID int16
	Paths    []string
	// cause is why the objects could not be read. It stays in the chain so
	// failDuckDBScan's classification keeps an expired budget
	// non-degradable, exactly as for a timed-out main scan (#465).
	cause error
}

func (e *unverifiedParquetError) Error() string {
	msg := fmt.Sprintf("schema %d: %d live parquet object(s) could not be read to verify their column types against the declared attribute types, so the read fails closed: %s",
		e.SchemaID, len(e.Paths), strings.Join(e.Paths, ", "))
	if e.cause != nil {
		msg += ": " + e.cause.Error()
	}
	return msg
}

func (e *unverifiedParquetError) Unwrap() error { return e.cause }

// unverifiedCause joins the failed proof reads with the request's context
// error. The context error is added unless a proof already carries it: an
// unreadable footer records no error (it is inconclusive by design), so a
// footer probe the budget cut short would otherwise leave nothing in the
// chain to say the budget is spent.
func unverifiedCause(ctx context.Context, proofErrs []error) error {
	cause := errors.Join(proofErrs...)
	if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(cause, ctxErr) {
		cause = errors.Join(cause, ctxErr)
	}
	return cause
}

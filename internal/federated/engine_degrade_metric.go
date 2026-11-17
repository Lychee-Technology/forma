package federated

import (
	"context"
	"errors"
)

// The label values of federated_degraded_fallback_total (#714). Both sets are
// fixed: a value is never built from error text, a path or a schema, so the
// counter has at most len(degradeCauses) × len(degradeOutcomes) series per
// emitter. Adding a value is a change to a published metric contract.
const (
	degradeCauseBreakerRejected    = "breaker_rejected"
	degradeCauseDuckDBUnavailable  = "duckdb_unavailable"
	degradeCauseCorruptParquet     = "corrupt_parquet"
	degradeCauseScanGuardViolation = "scan_guard_violation"
	degradeCauseReadFailed         = "read_failed"
	degradeCausePostgresReadFailed = "postgres_read_failed"
	degradeCauseOther              = "other"

	degradeOutcomeServed = "served"
	degradeOutcomeFailed = "failed"
)

// degradeCauses lists every value classifyDegradeCause can return.
var degradeCauses = []string{
	degradeCauseBreakerRejected,
	degradeCauseDuckDBUnavailable,
	degradeCauseCorruptParquet,
	degradeCauseScanGuardViolation,
	degradeCauseReadFailed,
	degradeCausePostgresReadFailed,
	degradeCauseOther,
}

// degradeOutcomes lists every outcome label value.
var degradeOutcomes = []string{degradeOutcomeServed, degradeOutcomeFailed}

// classifyDegradeCause maps the failure a degraded fallback absorbs to its
// cause class. It reads only sentinels and error types, never the message.
// The more specific class is tested first wherever one error carries two:
// a breaker rejection also wraps ErrDuckDBUnavailable, and a corrupt-object
// refusal or a scan-guard violation also wraps ErrFederatedReadFailed. Those
// two need a repair of the named objects, which a transient read failure
// does not. A degradable failure that carries none of them is "other", so
// every attempt is counted under exactly one class.
func classifyDegradeCause(cause error) string {
	var corrupt *CorruptParquetError
	var guard *ParquetGuardViolationError
	switch {
	case errors.Is(cause, errBreakerRejected):
		return degradeCauseBreakerRejected
	case errors.Is(cause, ErrDuckDBUnavailable):
		return degradeCauseDuckDBUnavailable
	case errors.As(cause, &corrupt):
		return degradeCauseCorruptParquet
	case errors.As(cause, &guard):
		return degradeCauseScanGuardViolation
	case errors.Is(cause, ErrFederatedReadFailed):
		return degradeCauseReadFailed
	case errors.Is(cause, ErrPostgresReadFailed):
		return degradeCausePostgresReadFailed
	default:
		return degradeCauseOther
	}
}

// reportDegradedFallback counts one degraded fallback attempt once its
// PostgreSQL-only read has returned (#714). fallbackErr is that read's error:
// nil means the degraded answer is being returned. The cause class comes
// from the absorbed failure, not from fallbackErr, so a fallback that fails
// still says what it was degrading around. A nil sink emits nothing.
func (e *DBFederatedQueryEngine) reportDegradedFallback(ctx context.Context, cause, fallbackErr error) {
	outcome := degradeOutcomeServed
	if fallbackErr != nil {
		outcome = degradeOutcomeFailed
	}
	e.metrics.EmitDegradedFallback(ctx, classifyDegradeCause(cause), outcome)
}

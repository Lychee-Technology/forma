package federated

import (
	"context"

	"go.uber.org/zap"
)

// Engine side of the circuit breaker's transition reporting (#634). The
// breaker computes the transition each call caused under its own lock and
// returns it; these wrappers report it once the call has returned, so neither
// the embedder's metric emitter nor the logger ever runs under the breaker's
// mutex. Every engine call into Allow, RecordFailure and RecordSuccess goes
// through them. ReleaseProbe never changes the reported state and is called
// directly.

// breakerAllow admits or rejects one DuckDB pass and reports the transition
// to half-open when this caller is admitted as the probe.
func (e *DBFederatedQueryEngine) breakerAllow(ctx context.Context) (bool, ProbeToken) {
	admitted, probe, transition := e.breaker.Allow()
	e.reportBreakerTransition(ctx, transition, nil)
	return admitted, probe
}

// breakerRecordFailure records a failed DuckDB pass. cause is the pass's
// error, already redacted at its source (#306); it is logged only when this
// failure opens the breaker.
func (e *DBFederatedQueryEngine) breakerRecordFailure(ctx context.Context, cause error) {
	e.reportBreakerTransition(ctx, e.breaker.RecordFailure(), cause)
}

// breakerRecordSuccess records a successful DuckDB pass.
func (e *DBFederatedQueryEngine) breakerRecordSuccess(ctx context.Context) {
	e.reportBreakerTransition(ctx, e.breaker.RecordSuccess(), nil)
}

// reportBreakerTransition emits one duckdb_circuit_breaker_transition_total
// sample and logs one line per transition. Volume is bounded by transitions,
// not requests: through a sustained outage it is one trip, then one probe and
// one re-open per open period. Concurrent transitions on different goroutines
// may reach the emitter and the log in a different order than the breaker
// took them; the counts stay exact.
func (e *DBFederatedQueryEngine) reportBreakerTransition(ctx context.Context, t BreakerTransition, cause error) {
	if !t.Changed() {
		return
	}
	e.metrics.EmitCircuitBreakerTransition(ctx, t.To.String())
	switch t.To {
	case BreakerOpen:
		e.log().Warn("duckdb circuit breaker opened",
			zap.Stringer("from", t.From), zap.Error(cause))
	case BreakerHalfOpen:
		e.log().Info("duckdb circuit breaker half-open: probe admitted")
	case BreakerClosed:
		e.log().Info("duckdb circuit breaker closed", zap.Stringer("from", t.From))
	}
}

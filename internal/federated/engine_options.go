package federated

import (
	"time"

	"github.com/lychee-technology/forma/internal/queryplan"
	"go.uber.org/zap"
)

// Engine construction options. WithParquetSource lives with the seam it
// injects (parquet_source.go); the rest collect here so engine.go stays the
// engine itself.

// EngineOption customizes optional engine collaborators.
type EngineOption func(*DBFederatedQueryEngine)

// WithPlanCache injects a shared compiled-plan cache (#142).
func WithPlanCache(c *queryplan.Cache) EngineOption {
	return func(e *DBFederatedQueryEngine) { e.planCache = c }
}

// WithLogger gives the engine a logger; the default is zap.NewNop(). The
// engine reports itself through returned errors and the execution plan, so
// this is deliberately narrow: it carries the pre-read validator's
// stamp-versus-footer cross-check (#256), which has no other outlet because
// the read it observes SUCCEEDS. A manifest entry whose column stamp
// contradicts the object's real footer is an operator's problem — no caller's
// result would ever mention it.
func WithLogger(l *zap.Logger) EngineOption {
	return func(e *DBFederatedQueryEngine) {
		if l == nil || e.schemaValidator == nil {
			return
		}
		e.schemaValidator.logger = l
	}
}

// defaultCorruptPathRetention bounds how long a confirmed-corrupt parquet
// object stays excluded before being re-verified (#251).
const defaultCorruptPathRetention = 5 * time.Minute

// WithCorruptPathRetention overrides how long a verification-confirmed
// corrupt parquet object stays excluded from path resolution (#251). The
// entry always expires — a terminal verdict must never be memoized forever
// (#326): repair, compaction, or manifest reconcile self-heal only through
// re-verification. A non-positive d effectively disables exclusion — entries
// expire the moment they are added — so misconfigured callers fail open to
// today's all-or-nothing scan; production callers should keep the default.
func WithCorruptPathRetention(d time.Duration) EngineOption {
	return func(e *DBFederatedQueryEngine) { e.corruptPaths = newCorruptParquetCache(d) }
}

// WithFlushVisibilityGrace overrides the #252 clock-skew margin subtracted
// from the query's path-resolution timestamp when computing the dirty-barrier
// cutoff. d == 0 is the exact anchor (the default); d > 0 hardens against
// cross-host clock skew (flushed_at is stamped on the CDC host, the cutoff on
// the query host) at the cost of hot-serving rows flushed up to d before the
// query; d < 0 disables the widening entirely (the pre-#252 barrier).
func WithFlushVisibilityGrace(d time.Duration) EngineOption {
	return func(e *DBFederatedQueryEngine) {
		if d < 0 {
			e.flushVisibilityGraceMs = -1
			return
		}
		e.flushVisibilityGraceMs = d.Milliseconds()
	}
}

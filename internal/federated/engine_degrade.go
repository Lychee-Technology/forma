package federated

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/redact"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// The §7.2 degraded fallback: a DuckDB-path failure absorbed under
// AllowPartialDegradedMode into a Postgres-only answer. Split out of
// engine.go to keep it under the 500-line limit.

// recordDegradedFallbackPlan rewrites the execution plan when a DuckDB-side
// failure degrades the query to Postgres-only (#185): without it the plan
// keeps the pre-failure routing decision (UseDuckDB=true) and never mentions
// the fallback, so "the plan reflects actual access" would not hold for the
// degraded-mode contract. Tiers states physical coverage — postgres serves
// the hot storage regardless of the tiers the request asked for (#184).
func recordDegradedFallbackPlan(opts *model.FederatedQueryOptions, tables model.StorageTables, cause error) {
	if opts == nil || !opts.IncludeExecutionPlan {
		return
	}
	if opts.ExecutionPlan == nil {
		opts.ExecutionPlan = &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
	}
	opts.ExecutionPlan.Routing = model.RoutingDecision{
		Tiers:     []model.DataTier{model.DataTierHot},
		UseDuckDB: false,
		Reason:    "degraded fallback (AllowPartialDegradedMode)",
	}
	opts.ExecutionPlan.Notes = append(opts.ExecutionPlan.Notes,
		fmt.Sprintf("degraded fallback to postgres-only: %v", cause))
	opts.ExecutionPlan.Sources = append(opts.ExecutionPlan.Sources, model.DataSourcePlan{
		Tier:   model.DataTierHot,
		Engine: "postgres",
		SQL:    fmt.Sprintf("OLTP optimized query over %s", tables.EntityMain),
		Reason: "degraded fallback (postgres-only)",
	})
}

// degradeToPostgresOnly serves the Postgres-only fallback for a degradable
// DuckDB-path failure: it records the fallback decision and its cause on the
// execution plan (#185 — the plan must reach callers that only see the page),
// logs them for the operator (#638), and stitches the plan onto the returned
// page. mark is the plan as it stood before the DuckDB attempt; see the
// rewind below.
func (e *DBFederatedQueryEngine) degradeToPostgresOnly(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions, mark executionPlanMark, cause error) (*model.PersistentRecordPage, error) {
	if opts != nil {
		// The failed DuckDB pass may have recorded a #348 partial marker; the
		// out-parameter must agree with the page this path returns, and the
		// marker's scope (#251 corrupt exclusion) never describes a
		// postgres-only answer.
		opts.PartialScan = nil
	}
	// The same holds for the plan (#639): whatever the abandoned attempt
	// recorded — a failed pass's sources and timings, or a whole page pass
	// whose page the failed recount discarded — would sit next to
	// UseDuckDB=false with only internal Notes to say it describes the
	// attempt, not the answer. Rewind first so the cause note below survives.
	mark.rewind(opts)
	recordDegradedFallbackPlan(opts, tables, cause)
	// Logged before the Postgres read: a failed fallback returns its own
	// error, which no longer carries the cause it was absorbing.
	e.logDegradedFallback(fq, cause)
	page, perr := e.queryPostgresOnly(ctx, tables, fq)
	if perr != nil {
		return nil, fmt.Errorf("degraded postgres-only fallback: %w", perr)
	}
	attachExecutionPlan(page, opts)
	// §7.2 promises a partial signal on the degraded answer; Routing.Reason
	// only reaches callers that asked for the plan, so the fallback carries
	// the same coverage marker as the routed path (#468).
	markHotTierOnly(page, opts, fq)
	return page, nil
}

// degradedFallbackMsg is the line every degraded fallback writes (#638).
const degradedFallbackMsg = "federated query degraded to postgres-only"

// logDegradedFallback writes the one line a degraded fallback leaves (#638).
// The request succeeds and toExecutionPlan drops the plan note that records
// the cause, so without this line the absorbed failure reaches no log, no
// metric and no caller. Warn fits a request answered with less than it asked
// for. A breaker rejection logs at Debug: the breaker's "opened" line (#634)
// logged the failure that tripped it once, and a Warn per rejected request
// would repeat it for the whole open period with only the breaker's own
// sentinel as the cause. The scan-guard attribution (#351) keeps its own
// Error line; that line names the objects, and this one says the request was
// answered without them. The cause passes redact.Error once more: DuckDB's
// errors are scrubbed at their source (#306), but a degradable cause can also
// come from path resolution or the dirty-ID fetch, which that scrub does not
// cover.
func (e *DBFederatedQueryEngine) logDegradedFallback(fq *model.FederatedAttributeQuery, cause error) {
	level := zapcore.WarnLevel
	if errors.Is(cause, errBreakerRejected) {
		level = zapcore.DebugLevel
	}
	e.log().Log(level, degradedFallbackMsg,
		zap.Int16("schema_id", fq.SchemaID), zap.Error(redact.Error(cause)))
}

// degradableFederatedError reports whether a DuckDB-path failure may be
// absorbed by the Postgres-only fallback under AllowPartialDegradedMode.
// Five classes must surface instead: a missing schema metadata cache is a
// configuration/data-contract error whose masking #151 made loud; manifest
// inconsistency (#187) exists to make cold-tier loss loud, and a fallback
// would return exactly the silent-loss answer it prevents; an empty parquet
// path set (#299) is the same bargain one level earlier — the read surface is
// misconfigured for a query that asked for warm/cold, so a Postgres-only
// answer would be silently short precisely where the cold tier was wanted; a
// manifest stamped for a different schema means the read surface is
// misaddressed, and a partial answer is the wrong response to a state that can
// serve another schema's rows under this identity; and invalid caller input (e.g. an unrenderable path template) is the
// caller's error to see, not infrastructure to degrade around. A sixth,
// an expired deadline (#465), is the budget doing its job: the pass was
// cancelled because it ran too long, and answering from Postgres afterwards
// would spend more time past a limit that exists to bound latency; a DuckDB
// that keeps timing out opens the breaker, and *that* rejection degrades.
func degradableFederatedError(err error) bool {
	return !errors.Is(err, ErrSchemaMetadataCacheRequired) &&
		!errors.Is(err, ErrParquetSetInconsistent) &&
		!errors.Is(err, ErrNoParquetPaths) &&
		!errors.Is(err, ErrManifestSchemaMismatch) &&
		!errors.Is(err, forma.ErrInvalidInput) &&
		!errors.Is(err, context.DeadlineExceeded)
}

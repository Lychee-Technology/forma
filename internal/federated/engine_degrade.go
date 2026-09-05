package federated

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
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
// execution plan (#185 — the plan must reach callers that only see the page)
// and stitches the plan onto the returned page.
func (e *DBFederatedQueryEngine) degradeToPostgresOnly(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions, cause error) (*model.PersistentRecordPage, error) {
	if opts != nil {
		// The failed DuckDB pass may have recorded a #348 partial marker; the
		// out-parameter must agree with the page this path returns, and the
		// marker's scope (#251 corrupt exclusion) never describes a
		// postgres-only answer.
		opts.PartialScan = nil
	}
	recordDegradedFallbackPlan(opts, tables, cause)
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
// caller's error to see, not infrastructure to degrade around.
func degradableFederatedError(err error) bool {
	return !errors.Is(err, ErrSchemaMetadataCacheRequired) &&
		!errors.Is(err, ErrParquetSetInconsistent) &&
		!errors.Is(err, ErrNoParquetPaths) &&
		!errors.Is(err, ErrManifestSchemaMismatch) &&
		!errors.Is(err, forma.ErrInvalidInput)
}

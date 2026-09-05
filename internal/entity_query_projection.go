package internal

import (
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// Projections from the engine's internal page metadata onto the public
// forma.QueryResult surface. Split out of entity_query_service.go to keep it
// under the 500-line limit.

// toExecutionPlan converts the engine's internal execution plan into the
// public forma.ExecutionPlan projection surfaced on QueryResult. It returns nil
// when no plan was recorded (non-federated requests, or federated requests that
// did not ask for the plan), so QueryResult.ExecutionPlan stays omitted.
func toExecutionPlan(plan *model.ExecutionPlan) *forma.ExecutionPlan {
	if plan == nil {
		return nil
	}

	// SECURITY: do not project src.SQL / src.Params or plan.Notes / merge.Notes.
	// Since #306, the database password is redacted at the source (plan SQL and
	// failure notes carry password=***REDACTED***), but the rendered DuckDB SQL
	// still embeds host/user/dbname, table internals; Params carry query arguments;
	// notes carry storage keys and engine internals — surfacing them on the HTTP
	// response would leak internals to any advanced_query caller. Only safe
	// routing/tier metadata crosses into the public projection.
	out := &forma.ExecutionPlan{
		Routing: forma.ExecutionRouting{
			UsedDuckDB: plan.Routing.UseDuckDB,
			Tiers:      tiersToStrings(plan.Routing.Tiers),
			Reason:     plan.Routing.Reason,
		},
		Timings: plan.Timings,
	}

	if len(plan.Sources) > 0 {
		out.Sources = make([]forma.ExecutionSource, 0, len(plan.Sources))
		for _, src := range plan.Sources {
			out.Sources = append(out.Sources, forma.ExecutionSource{
				Tier:              string(src.Tier),
				Engine:            src.Engine,
				RowEstimate:       src.RowEstimate,
				ActualRows:        src.ActualRows,
				PredicatePushdown: src.PredicatePushdown,
				DurationMs:        src.DurationMs,
				Reason:            src.Reason,
			})
		}
	}

	if plan.Merge.Strategy != "" || plan.Merge.PreferHot || len(plan.Merge.DedupKeys) > 0 {
		out.Merge = &forma.ExecutionMerge{
			Strategy:   string(plan.Merge.Strategy),
			PreferHot:  plan.Merge.PreferHot,
			DedupKeys:  plan.Merge.DedupKeys,
			DurationMs: plan.Merge.DurationMs,
		}
	}

	return out
}

// toPartialResult projects the engine's partial-scan report into the public
// marker (#348). SECURITY: only the reason and the excluded-object COUNT
// cross the boundary — the object keys are storage internals (#301/#306)
// and stay on the internal plan Notes for embedders and operators.
//
// The two causes are mutually exclusive by construction — corrupt exclusion
// happens only on a DuckDB pass, a coverage gap only on a Postgres-only
// answer (#468) — so the reason is chosen by which field is populated.
func toPartialResult(p *model.PartialScan) *forma.PartialResultInfo {
	if p == nil {
		return nil
	}
	if len(p.ExcludedObjects) > 0 {
		return &forma.PartialResultInfo{
			Reason:              forma.PartialReasonCorruptParquetExcluded,
			ExcludedObjectCount: len(p.ExcludedObjects),
		}
	}
	if len(p.UnconsultedTiers) > 0 {
		return &forma.PartialResultInfo{
			Reason:           forma.PartialReasonHotTierOnly,
			UnconsultedTiers: tiersToStrings(p.UnconsultedTiers),
		}
	}
	return nil
}

func tiersToStrings(tiers []model.DataTier) []string {
	if len(tiers) == 0 {
		return nil
	}
	out := make([]string, 0, len(tiers))
	for _, t := range tiers {
		out = append(out, string(t))
	}
	return out
}

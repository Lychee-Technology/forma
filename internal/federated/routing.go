package federated

import (
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// isHotOnlyRequest reports whether the caller explicitly declared a hot-only
// request. Both spellings mean the same thing and must be treated identically:
// the PreferHot flag, and the single-element PreferredTiers form. An empty
// PreferredTiers is NOT hot-only — it is the default all-tier shape (#184).
//
// This is the caller's SEMANTIC declaration, not a cost hint, which is why the
// #354 cursor override below refuses to reinterpret it and engine.go's hot-only
// gate short-circuits on the same predicate.
func isHotOnlyRequest(fq *model.FederatedAttributeQuery) bool {
	if fq == nil {
		return false
	}
	return fq.PreferHot || (len(fq.PreferredTiers) == 1 && fq.PreferredTiers[0] == model.DataTierHot)
}

// hasExplicitTierCoverage reports whether the caller explicitly declared the
// tiers to read. Non-empty means declared; the service layer forwards an
// omitted preferred_tiers as nil so the two cannot be confused (#468).
func hasExplicitTierCoverage(fq *model.FederatedAttributeQuery) bool {
	return fq != nil && len(fq.PreferredTiers) > 0
}

// EvaluateRoutingPolicy makes a routing decision based on config, query hints and options.
func EvaluateRoutingPolicy(cfg forma.DuckDBConfig, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions) model.RoutingDecision {
	dec := model.RoutingDecision{
		Tiers:           []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
		UseDuckDB:       cfg.Enabled,
		Reason:          "default",
		MaxScanRows:     cfg.Routing.MaxDuckDBScanRows,
		QueryTimeout:    cfg.QueryTimeout,
		AllowS3Fallback: cfg.Routing.AllowS3Fallback,
	}

	// Honor explicit PreferredTiers
	if fq != nil && len(fq.PreferredTiers) > 0 {
		dec.Tiers = fq.PreferredTiers
	}

	// If DuckDB disabled, never use it. The verdict is final, so the
	// heuristics and overrides below are skipped — which means the common
	// tier collapse at the end is skipped too. Collapse here as well: the
	// Postgres-only path reads entity_main alone, and the plan must report
	// that route, not the caller's declared tiers, or it would contradict
	// the hot_tier_only coverage marker set on the same answer (#468).
	if !cfg.Enabled {
		dec.UseDuckDB = false
		dec.Reason = "duckdb disabled"
		dec.Tiers = []model.DataTier{model.DataTierHot}
		return dec
	}

	applyStrategyHeuristics(cfg, fq, opts, &dec)

	// A keyset cursor can only be applied by the DuckDB federated template
	// (sqlgen/keyset_where.go); the Postgres-only path has no keyset support
	// and pre-#354 dropped the cursor silently. The strategy rules above are
	// COST heuristics — advisory — so a cursor outranks them and reroutes the
	// query onto the federated path. Four conjuncts:
	//
	//   hasKeysetCursor(fq)   — the trigger. An absent or empty cursor is the
	//                           open first page, which Postgres serves fine.
	//   !dec.UseDuckDB        — not a routing gate (the assignment below would
	//                           be a no-op without it) but a TRUTHFULNESS gate:
	//                           it keeps the suffix off reasons that already
	//                           chose DuckDB on their own, so the plan never
	//                           claims an override that did not happen.
	//   !isHotOnlyRequest(fq) — load-bearing for this exported function's
	//                           standalone contract only; through engine.Query
	//                           it is as unreachable as cfg.Enabled, since the
	//                           gate at engine.go:152 intercepts hot-only
	//                           before EvaluateRoutingPolicy is called. A
	//                           hot-only request is the
	//                           caller's semantic choice, not a cost hint, so
	//                           it is never silently reinterpreted; the engine
	//                           rejects it instead. Both spellings count, which
	//                           is why this is the shared helper and not the
	//                           local hotOnly (that one is PreferHot only, and
	//                           widening it would change routing for
	//                           cursor-free queries under other strategies).
	//   cfg.Enabled           — redundant and unreachable today: the
	//                           !cfg.Enabled early return above already left.
	//                           Kept as a cheap invariant so that reordering
	//                           this clause above that return cannot start
	//                           rerouting onto an engine that is not there.
	//
	// No tier repair is needed: the block below collapses dec.Tiers only when
	// the decision stays postgres-only, and the DuckDB path derives its tier
	// set from fq.PreferredTiers (duckdb_query_helpers.go,
	// sqlgen/duckdb_template_renderer.go), not from dec.Tiers, which is plan
	// bookkeeping.
	if hasKeysetCursor(fq) && !dec.UseDuckDB && !isHotOnlyRequest(fq) && cfg.Enabled {
		dec.UseDuckDB = true
		dec.Reason += " (overridden: keyset cursor requires the federated path, #354)"
	}

	// An explicit PreferredTiers naming anything beyond hot is the caller's
	// COVERAGE declaration (#468) and outranks the cost heuristics for the
	// same reason a cursor does: the Postgres-only path reads entity_main
	// alone, so honoring the heuristic would silently drop the declared
	// warm/cold tiers. Same conjunct shape as #354 — !dec.UseDuckDB keeps the
	// suffix truthful, !isHotOnlyRequest leaves a [hot] declaration where it
	// asked to be, cfg.Enabled is the reordering invariant. An EMPTY
	// PreferredTiers is the default all-tier form, not a declaration: it
	// keeps the heuristic and is told what was consulted through the
	// coverage marker (engine_tier_coverage.go, markHotTierOnly).
	if hasExplicitTierCoverage(fq) && !dec.UseDuckDB && !isHotOnlyRequest(fq) && cfg.Enabled {
		dec.UseDuckDB = true
		dec.Reason += " (overridden: explicit preferred_tiers require the federated path, #468)"
	}

	// If DuckDB is disabled by decision, ensure tiers reflect that
	if !dec.UseDuckDB {
		dec.Tiers = []model.DataTier{model.DataTierHot}
	}

	return dec
}

// applyStrategyHeuristics applies the configured routing strategy's COST
// heuristics to dec. These are advisory: they trade freshness against scan
// size and may be overridden by a correctness requirement such as a keyset
// cursor (#354).
func applyStrategyHeuristics(cfg forma.DuckDBConfig, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions, dec *model.RoutingDecision) {
	// Determine whether hot-only or cold-only preference is implied by tier selection
	hotOnly := fq != nil && fq.PreferHot
	coldOnly := false
	if fq != nil && len(fq.PreferredTiers) > 0 {
		hasHot := false
		for _, t := range fq.PreferredTiers {
			if t == model.DataTierHot {
				hasHot = true
				break
			}
		}
		coldOnly = !hasHot
	}

	// Determine effective rows for cost-based decisions.
	// Prefer query-level Limit/Offset (model.AttributeQuery) over unset opts.MaxRows.
	effectiveLimit := 0
	effectiveOffset := 0
	if fq != nil {
		effectiveLimit = fq.Limit
		effectiveOffset = fq.Offset
	}
	effectiveRows := effectiveLimit + effectiveOffset

	// Strategy-based heuristics
	switch cfg.Routing.Strategy {
	case forma.RoutingStrategyFreshnessFirst:
		if hotOnly {
			dec.UseDuckDB = false
			dec.Tiers = []model.DataTier{model.DataTierHot}
			dec.Reason = "prefer hot"
		} else if coldOnly {
			dec.UseDuckDB = true
			dec.Reason = "freshness-first cold only"
		}
	case forma.RoutingStrategyCostFirst:
		// hot-only preference takes priority over scan size
		if hotOnly {
			dec.UseDuckDB = false
			dec.Tiers = []model.DataTier{model.DataTierHot}
			dec.Reason = "cost-first prefer hot"
			break
		}
		// Prefer DuckDB for large scans; fall back to opts.MaxRows if no query-level info
		scanRows := effectiveRows
		if scanRows == 0 && opts != nil && opts.MaxRows > 0 {
			scanRows = opts.MaxRows
		}
		if scanRows > 0 && scanRows > cfg.Routing.MaxDuckDBScanRows {
			dec.UseDuckDB = true
			dec.Reason = "cost-first large scan"
		}
	case forma.RoutingStrategyHybrid:
		if hotOnly {
			dec.UseDuckDB = false
			dec.Tiers = []model.DataTier{model.DataTierHot}
			dec.Reason = "hybrid prefer hot"
		} else if coldOnly {
			dec.UseDuckDB = true
			dec.Reason = "hybrid cold only"
		} else if effectiveOffset > 0 && effectiveOffset >= 10000 {
			dec.UseDuckDB = true
			dec.Reason = "hybrid deep pagination"
		} else if effectiveLimit > 0 && effectiveLimit < 1000 {
			// Small first-page query: prefer PG hot tier
			dec.UseDuckDB = false
			dec.Reason = "hybrid small result set"
		} else if opts != nil && opts.MaxRows > 0 && opts.MaxRows < 1000 {
			dec.UseDuckDB = false
			dec.Reason = "hybrid small result set"
		} else {
			dec.UseDuckDB = true
			dec.Reason = "hybrid use duckdb"
		}
	default:
		dec.Reason = "unknown strategy - default"
	}
}

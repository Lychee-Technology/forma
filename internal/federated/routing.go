package federated

import (
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

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

	// If DuckDB disabled, never use it
	if !cfg.Enabled {
		dec.UseDuckDB = false
		dec.Reason = "duckdb disabled"
		return dec
	}

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

	// A keyset cursor can only be applied by the DuckDB federated template
	// (sqlgen/keyset_where.go); the Postgres-only path has no keyset support
	// and pre-#354 dropped the cursor silently. The strategy rules above are
	// COST heuristics — advisory — so a cursor outranks them and reroutes the
	// query onto the federated path. Three conjuncts, each load-bearing:
	// !hotOnly keeps an explicit hot-only request (the caller's semantic
	// choice) from being silently reinterpreted — the engine rejects those
	// instead; cfg.Enabled avoids rerouting onto an engine that is not there,
	// leaving the engine's postgres-only guard to fail the request; and the
	// reason string keeps the reroute visible in the execution plan.
	//
	// No tier repair is needed: the block below collapses dec.Tiers only when
	// the decision stays postgres-only, and the DuckDB path derives its tier
	// set from fq.PreferredTiers (duckdb_query_helpers.go, duckdb_template_renderer.go),
	// not from dec.Tiers, which is plan bookkeeping.
	if hasKeysetCursor(fq) && !dec.UseDuckDB && !hotOnly && cfg.Enabled {
		dec.UseDuckDB = true
		dec.Reason += " (overridden: keyset cursor requires the federated path, #354)"
	}

	// If DuckDB is disabled by decision, ensure tiers reflect that
	if !dec.UseDuckDB {
		dec.Tiers = []model.DataTier{model.DataTierHot}
	}

	return dec
}

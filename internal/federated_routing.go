package internal

import (
	"time"

	"github.com/lychee-technology/forma"
)

// RoutingDecision indicates which tiers to query and whether to prefer DuckDB.
type RoutingDecision struct {
	Tiers        []DataTier
	UseDuckDB    bool
	Reason       string
	MaxScanRows  int
	QueryTimeout time.Duration
}

// EvaluateRoutingPolicy makes a routing decision based on config, query hints and options.
func EvaluateRoutingPolicy(cfg forma.DuckDBConfig, fq *FederatedAttributeQuery, opts *FederatedQueryOptions) RoutingDecision {
	dec := RoutingDecision{
		Tiers:        []DataTier{DataTierHot, DataTierWarm, DataTierCold},
		UseDuckDB:    cfg.Enabled,
		Reason:       "default",
		MaxScanRows:  cfg.Routing.MaxDuckDBScanRows,
		QueryTimeout: cfg.QueryTimeout,
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

	// Simple strategy switch
	switch cfg.Routing.Strategy {
	case "freshness-first":
		// prefer hot only if PreferHot or recent TTL required
		if fq != nil && fq.PreferHot {
			dec.UseDuckDB = false
			dec.Tiers = []DataTier{DataTierHot}
			dec.Reason = "prefer hot"
		}
	case "cost-first":
		// prefer DuckDB for large scans
		if opts != nil && opts.MaxRows > 0 && opts.MaxRows > cfg.Routing.MaxDuckDBScanRows {
			dec.UseDuckDB = true
			dec.Reason = "cost-first large scan"
		}
	case "hybrid":
		// hybrid: use duckdb unless PreferHot or small MaxRows
		if fq != nil && fq.PreferHot {
			dec.UseDuckDB = false
			dec.Tiers = []DataTier{DataTierHot}
			dec.Reason = "hybrid prefer hot"
		} else if opts != nil && opts.MaxRows > 0 && opts.MaxRows < 1000 {
			dec.UseDuckDB = false
			dec.Reason = "hybrid small result set"
		} else {
			dec.UseDuckDB = true
			dec.Reason = "hybrid use duckdb"
		}
	default:
		// unknown strategy: keep defaults
		dec.Reason = "unknown strategy - default"
	}

	// Respect allow fallback
	if !cfg.Routing.AllowS3Fallback {
		dec.UseDuckDB = false
		dec.Reason = "s3 fallback disabled"
	}

	// Allow options to override timeout/parallelism via QueryTimeout
	if opts != nil && opts.Parallelism > 0 {
		// no-op here; caller may translate to pragmas
	}

	// If DuckDB is disabled by decision, ensure tiers reflect that
	if !dec.UseDuckDB {
		dec.Tiers = []DataTier{DataTierHot}
	}

	return dec
}

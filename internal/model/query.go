package model

import (
	"time"
)

type DataTier string

const (
	DataTierHot  DataTier = "hot"
	DataTierWarm DataTier = "warm"
	DataTierCold DataTier = "cold"
)

type FederatedAttributeQuery struct {
	AttributeQuery
	// PreferredTiers and PreferHot narrow the tiers an engine call reads.
	// They are engine-level knobs for direct engine callers (the e2e and
	// benchmark harnesses): the entity query service never sets them, since
	// a request carries no routing configuration (#715), so its queries take
	// the default all-tier form and the configured routing strategy.
	PreferredTiers []DataTier
	PreferHot      bool
	DuckDBHints    *DuckDBRenderHints
	KeysetCursor   *KeysetCursor
	// ParquetUnrepresentable says why the parquet tiers cannot hold the
	// schema's rows (#704), or is empty. Set, it routes the query to
	// PostgreSQL alone. The service sets it from the schema registry's
	// cache, since the engine's metadata cache carries no array ancestry.
	ParquetUnrepresentable string
}

// DuckDBRenderHints are engine-level render hints. S3ParquetPathTemplate
// pins the parquet objects a pass reads instead of the manifest or the
// fallback glob; only direct engine callers (the e2e harnesses) set it. No
// request can: parquet paths come from deployment configuration and the
// manifest (#715).
type DuckDBRenderHints struct {
	S3ParquetPathTemplate string
	TimeEncodingHint      string
}

type FederatedQueryOptions struct {
	MaxRows     int
	Parallelism int
	// AllowPartialDegradedMode enables the §7.2 degraded fallback for this
	// call. The entity query service copies it from the deployment's
	// DuckDBConfig.AllowPartialDegradedMode; no request sets it (#715).
	AllowPartialDegradedMode bool
	IncludeExecutionPlan     bool
	ExecutionPlan            *ExecutionPlan
}

type RoutingDecision struct {
	Tiers           []DataTier
	UseDuckDB       bool
	Reason          string
	MaxScanRows     int
	QueryTimeout    time.Duration
	AllowS3Fallback bool
}

type ExecutionPlan struct {
	Routing RoutingDecision
	Sources []DataSourcePlan
	Merge   MergePlan
	Timings map[string]int64
	Notes   []string
}

type DataSourcePlan struct {
	Tier   DataTier
	Engine string
	SQL    string
	// Params holds the string forms of the bind parameters for SQL, in
	// order. Populated only when FederatedQueryOptions.IncludeExecutionPlan
	// is set, so diagnostic artifacts can replay the exact query (#173).
	Params            []string
	RowEstimate       int64
	PredicatePushdown bool
	ActualRows        int64
	DurationMs        int64
	Reason            string
}

type MergeStrategy string

const MergeStrategyLastWriteWins MergeStrategy = "last-write-wins"

type MergePlan struct {
	Strategy   MergeStrategy
	PreferHot  bool
	DedupKeys  []string
	DurationMs int64
	Notes      []string
}

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
	PreferredTiers  []DataTier
	PreferHot       bool
	UseMainAsAnchor bool
	DuckDBHints     *DuckDBRenderHints
	KeysetCursor    *KeysetCursor
}

type DuckDBRenderHints struct {
	S3ParquetPathTemplate string
	TimeEncodingHint      string
}

type ConsistencyMode string

const (
	ConsistencyModeStrict   ConsistencyMode = "strict"
	ConsistencyModeEventual ConsistencyMode = "eventual"
)

type FederatedQueryOptions struct {
	MaxRows                  int
	Parallelism              int
	AllowPartialDegradedMode bool
	IncludeExecutionPlan     bool
	ExecutionPlan            *ExecutionPlan
	ConsistencyMode          ConsistencyMode
	// PartialScan is an engine out-parameter like ExecutionPlan, but NOT
	// gated on IncludeExecutionPlan: the #348 public partial marker must
	// reach callers that never asked for a plan. The last executed DuckDB
	// pass overwrites it, so it describes the pass that produced the page,
	// and Query resets it at entry, so after any call — including a
	// postgres-only answer that never runs a pass — it describes that call
	// even when one options value is reused across queries.
	// Being an out-parameter, it has nowhere to land when the caller passes
	// nil options: such a call drops the marker, so a caller that needs it
	// must pass a non-nil *FederatedQueryOptions.
	PartialScan *PartialScan
}

// PartialScan reports that the DuckDB pass that answered this query ran over
// a deliberately reduced object set: verification-confirmed corrupt parquet
// objects (#251) were excluded and the page came from the readable remainder
// plus the hot tier. Internal form — ExcludedObjects carries full storage
// keys for embedders and operators; the public projection
// (forma.QueryResult.Partial) surfaces only the reason and the count (#348,
// #301/#306 boundary).
type PartialScan struct {
	ExcludedObjects []string
	// UnconsultedTiers names the tiers the request asked for (explicitly, or
	// all three by omission) that a Postgres-only answer never read (#468):
	// the routed small-page shortcut, a disabled engine, and the degraded
	// fallback all answer from entity_main alone. Never set on a DuckDB
	// pass, so it and ExcludedObjects are mutually exclusive.
	UnconsultedTiers []DataTier
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

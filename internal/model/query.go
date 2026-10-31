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
	// ParquetUnrepresentable says why the parquet tiers cannot hold the
	// schema's rows (#704), or is empty. Set, it routes the query to
	// PostgreSQL alone. The service sets it from the schema registry's
	// cache, since the engine's metadata cache carries no array ancestry.
	ParquetUnrepresentable string
}

type DuckDBRenderHints struct {
	S3ParquetPathTemplate string
	TimeEncodingHint      string
}

// ConsistencyMode is forwarded from the request and read by nothing: read
// consistency is fixed per API (#635), and #715 deletes the field.
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

// PartialScan reports that the answer to this query read fewer tiers than
// the request asked for. Internal form of forma.QueryResult.Partial (#348).
type PartialScan struct {
	// UnconsultedTiers names the tiers the request asked for (explicitly, or
	// all three by omission) that a Postgres-only answer never read (#468):
	// the routed small-page shortcut, a disabled engine, a schema the parquet
	// tiers cannot hold, and the degraded fallback all answer from
	// entity_main alone. Never set on a DuckDB pass, which reads every
	// listed object or fails (#722).
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

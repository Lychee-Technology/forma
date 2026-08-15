package model

import (
	"time"

	"github.com/lychee-technology/forma"
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
	KeysetEnabled            bool
	IncludeExecutionPlan     bool
	ExecutionPlan            *ExecutionPlan
	ConsistencyMode          ConsistencyMode
	// PartialScan is an engine out-parameter like ExecutionPlan, but NOT
	// gated on IncludeExecutionPlan: the #348 public partial marker must
	// reach callers that never asked for a plan. The last executed DuckDB
	// pass overwrites it, so it describes the pass that produced the page.
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

type KeysetCursorMode string

const (
	KeysetCursorModeAfter  KeysetCursorMode = "after"
	KeysetCursorModeBefore KeysetCursorMode = "before"
)

type KeysetColumn struct {
	Attribute string
	Direction forma.SortOrder
}

type KeysetCursor struct {
	Columns []KeysetColumn
	Values  []interface{}
	Mode    KeysetCursorMode
}

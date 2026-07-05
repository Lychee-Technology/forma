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
	Tier              DataTier
	Engine            string
	SQL               string
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

package internal

// Federated query related types and options.
// These extend the existing AttributeQuery to carry hints for
// multi-tier federated execution (Postgres + DuckDB/S3).

type DataTier string

const (
	DataTierHot  DataTier = "hot"
	DataTierWarm DataTier = "warm"
	DataTierCold DataTier = "cold"
)

// FederatedAttributeQuery extends AttributeQuery with federated-specific hints.
// It embeds the existing AttributeQuery so it remains compatible with existing code paths.
type FederatedAttributeQuery struct {
	AttributeQuery

	// PreferredTiers is an ordered list of preferred data tiers to query.
	// Example: []DataTier{DataTierHot, DataTierWarm, DataTierCold}
	PreferredTiers []DataTier

	// PreferHot indicates strong preference for reading from the hot (Postgres) tier
	// when the same data exists in multiple tiers.
	PreferHot bool

	// UseMainAsAnchor controls whether the main table (entity_main) should be used
	// as the anchor for predicate pushdown. This mirrors existing repository logic.
	UseMainAsAnchor bool

	// DuckDBHints carries optional DuckDB-specific rendering hints, e.g. external
	// parquet path templates or casting preferences.
	DuckDBHints *DuckDBRenderHints
}

// DuckDBRenderHints provides optional parameters that guide DuckDB SQL generation.
type DuckDBRenderHints struct {
	// S3ParquetPathTemplate is a template (with placeholders) for locating parquet files in S3.
	// Example: "s3://bucket/path/schema_{{.SchemaID}}/data.parquet"
	S3ParquetPathTemplate string

	// TimeEncodingHint indicates how date/time values should be encoded in DuckDB side.
	// e.g. "unix_ms" or "iso8601"
	TimeEncodingHint string
}

// FederatedQueryOptions contains runtime options for federated execution.
type FederatedQueryOptions struct {
	// MaxRows limits the number of rows read from remote/columnar sources per shard.
	MaxRows int

	// Parallelism controls how many parallel DuckDB scan workers to use.
	Parallelism int

	// AllowPartialDegradedMode if true will allow executing the query with only a subset
	// of data tiers available (useful for the early MVP).
	AllowPartialDegradedMode bool

	// IncludeExecutionPlan when true instructs the repository to collect an execution plan
	// for debugging/observability. If set, the repository will allocate and populate
	// ExecutionPlan and assign it to ExecutionPlan (below) so callers may inspect it.
	IncludeExecutionPlan bool

	// ExecutionPlan is populated by the repository when IncludeExecutionPlan==true.
	// Callers should pass a non-nil opts pointer and inspect this field after call.
	ExecutionPlan *ExecutionPlan
}

// ExecutionPlan is a diagnostic structure capturing the federated query execution
// choices and timings. It is intended for debugging and observability only.
type ExecutionPlan struct {
	// Routing decision snapshot (which tiers were considered/selected)
	Routing RoutingDecision

	// Per-source plans for each data source touched by the federated execution.
	Sources []DataSourcePlan

	// Merge describes the merge-on-read strategy applied to results across tiers.
	Merge MergePlan

	// Timings: coarse-grained durations in milliseconds for major stages.
	// Keys typically: "translate", "postgres_fetch", "duckdb_fetch", "merge", "total"
	Timings map[string]int64

	// Notes and warnings captured during planning/execution.
	Notes []string
}

// DataSourcePlan captures per-source execution details.
type DataSourcePlan struct {
	// Tier indicates the logical data tier (hot/warm/cold).
	Tier DataTier

	// Engine indicates the execution engine, e.g., "postgres" or "duckdb".
	Engine string

	// SQL optionally contains the generated SQL fragment or rendered template used.
	// For privacy/performance reasons this may be truncated by the repository.
	SQL string

	// RowEstimate is the planner's estimated rows to be scanned/returned (if available).
	RowEstimate int64

	// PredicatePushdown indicates whether predicates were pushed to the source.
	PredicatePushdown bool

	// ActualRows contains the actual rows returned from this source (filled post-execution).
	ActualRows int64

	// DurationMs measures execution time for this source in milliseconds.
	DurationMs int64

	// Reason provides human-readable explanation for selection/behavior.
	Reason string
}

// MergePlan describes merge-on-read semantics used to combine tiered results.
type MergePlan struct {
	// Strategy name, e.g., "last-write-wins"
	Strategy string

	// PreferHot indicates whether preferHot tiebreaker was used.
	PreferHot bool

	// DedupKeys lists the keys used for deduplication (typically SchemaID:RowID).
	DedupKeys []string

	// DurationMs time spent merging in milliseconds.
	DurationMs int64

	// Notes optional additional details about attribute-level merging.
	Notes []string
}

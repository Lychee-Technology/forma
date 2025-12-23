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
}

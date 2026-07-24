package forma

import (
	"strings"
	"text/template"
	"time"
)

// DuckDBConfig contains DuckDB connection and S3 settings for federated queries
type DuckDBConfig struct {
	Enabled        bool          `json:"enabled"`
	DBPath         string        `json:"dbPath"`        // path to local DuckDB file (or ":memory:")
	MemoryLimitMB  int           `json:"memoryLimitMB"` // memory limit for DuckDB in MB
	EnableS3       bool          `json:"enableS3"`      // enable S3/http file system
	S3Endpoint     string        `json:"s3Endpoint"`    // custom S3 endpoint (for MinIO)
	S3AccessKey    string        `json:"s3AccessKey"`
	S3SecretKey    string        `json:"s3SecretKey"`
	S3Region       string        `json:"s3Region"`
	EnableParquet  bool          `json:"enableParquet"` // enable parquet extension
	Extensions     []string      `json:"extensions"`    // additional extensions to load
	MaxConnections int           `json:"maxConnections"`
	QueryTimeout   time.Duration `json:"queryTimeout"`   // per-query timeout for DuckDB access
	MaxParallelism int           `json:"maxParallelism"` // max threads/pragmas for DuckDB
	// Deprecated: ignored failure-rate threshold; use
	// CircuitBreakerFailureThreshold instead.
	CircuitBreakerThreshold float64 `json:"circuitBreakerThreshold"`

	// CircuitBreakerFailureThreshold is the number of consecutive failures that
	// opens the DuckDB circuit breaker. Zero means use the built-in default.
	CircuitBreakerFailureThreshold int `json:"circuitBreakerFailureThreshold"`
	// CircuitBreakerWindow is the sliding window for counting DuckDB failures.
	// Zero means use the built-in default.
	CircuitBreakerWindow time.Duration `json:"circuitBreakerWindow"`
	// CircuitBreakerOpenDuration is how long the DuckDB breaker stays open after
	// tripping. Zero means use the built-in default.
	CircuitBreakerOpenDuration time.Duration `json:"circuitBreakerOpenDuration"`

	// FlushVisibilityGraceMs is the #252 clock-skew margin for the federated
	// dirty barrier. Each query anchors its cutoff at the instant it resolved
	// its parquet path set: rows marked flushed after that instant (their
	// delta may be missing from the resolved set) stay hot-readable, minus
	// this margin to absorb CDC-host vs query-host clock skew. Zero (the
	// default) is the exact anchor — the steady state is never widened; a
	// positive value hot-serves rows flushed up to that long before the
	// query; a negative value disables the widening (the pre-#252 barrier).
	FlushVisibilityGraceMs int64 `json:"flushVisibilityGraceMs"`

	// S3Bucket is the bucket holding the lakehouse parquet files and the
	// manifests that index them. Required whenever ManifestTemplate is set.
	S3Bucket string `json:"s3Bucket"`
	// S3DataPrefix mirrors the CDC write side's S3Prefix (the prefix under
	// which delta/base parquet files are written). It is used *only* for the
	// legacy glob fallback covering schemas that have never been flushed and
	// therefore have no manifest object yet. Empty disables that fallback:
	// a schema without a manifest then resolves to zero parquet paths and its
	// query returns hot-tier rows only — a semantic cliff, silent by nature,
	// so leave this set unless every schema is known to be manifested.
	S3DataPrefix string `json:"s3DataPrefix"`
	// ManifestPrefix is the root prefix for manifest objects in S3. It must
	// match the CDC/compaction write side's ManifestPrefix.
	ManifestPrefix string `json:"manifestPrefix"`
	// ManifestTemplate is the manifest path template (e.g.
	// "manifest/{{.SchemaID}}.json") and doubles as the enable gate for
	// manifest-driven parquet resolution. It must be identical to the write
	// side's template: a writer-on/reader-off mismatch makes the reader miss
	// the cold tier entirely and lose those rows silently — no error is
	// raised, the result set is merely short. Empty (the default) keeps the
	// pre-existing glob-based read path.
	ManifestTemplate string `json:"manifestTemplate"`

	Routing RoutingPolicy `json:"routing"` // routing policy for federated queries
}

// ManifestReadEnabled reports whether manifest-driven parquet path resolution
// is configured. ManifestTemplate is the single enable gate.
func (d DuckDBConfig) ManifestReadEnabled() bool {
	return strings.TrimSpace(d.ManifestTemplate) != ""
}

// ValidateManifestRead validates the manifest read surface. It returns a
// *ConfigError naming the offending field, or nil when the combination is
// coherent. Validation is independent of Enabled: whether the settings take
// effect is the factory's gate, but an incoherent combination is always a
// configuration error.
func (d DuckDBConfig) ValidateManifestRead() error {
	if !d.ManifestReadEnabled() {
		// Manifest fields set without a template would sit inert and read as
		// "manifest reads are on" to an operator. Reject rather than ignore.
		if d.ManifestPrefix != "" || d.S3DataPrefix != "" {
			return &ConfigError{
				Field:   "duckdb.manifestTemplate",
				Message: "must be set when duckdb.manifestPrefix or duckdb.s3DataPrefix is configured",
			}
		}
		return nil
	}

	if strings.TrimSpace(d.S3Bucket) == "" {
		return &ConfigError{
			Field:   "duckdb.s3Bucket",
			Message: "must be set when duckdb.manifestTemplate is configured",
		}
	}
	// Parsed with text/template directly (not internal/manifest) to keep the
	// public config package free of internal dependencies; the manifest
	// resolver uses the same parser, so a template accepted here renders there.
	if _, err := template.New("manifest").Parse(d.ManifestTemplate); err != nil {
		return &ConfigError{
			Field:   "duckdb.manifestTemplate",
			Message: "must be a valid text/template path template: " + err.Error(),
		}
	}

	return nil
}

// RoutingStrategy specifies the federated query routing algorithm.
type RoutingStrategy string

const (
	// RoutingStrategyFreshnessFirst prefers the hot (PostgreSQL) tier for
	// queries that explicitly request fresh data (PreferHot flag).
	RoutingStrategyFreshnessFirst RoutingStrategy = "freshness-first"

	// RoutingStrategyCostFirst routes large scans to DuckDB to reduce
	// PostgreSQL load; small scans stay on the hot tier.
	RoutingStrategyCostFirst RoutingStrategy = "cost-first"

	// RoutingStrategyHybrid uses DuckDB by default but short-circuits to
	// PostgreSQL when the result set is expected to be small or hot data is
	// explicitly preferred.
	RoutingStrategyHybrid RoutingStrategy = "hybrid"
)

// RoutingPolicy defines federated query routing behavior
type RoutingPolicy struct {
	Strategy          RoutingStrategy `json:"strategy"`          // "freshness-first", "cost-first", "hybrid"
	HotTTL            time.Duration   `json:"hotTTL"`            // TTL to consider data "hot"
	MaxDuckDBScanRows int             `json:"maxDuckDBScanRows"` // threshold for preferring cold scans
	AllowS3Fallback   bool            `json:"allowS3Fallback"`   // allow falling back to S3/DuckDB when PG not used
}

// defaultDuckDBConfig returns default DuckDB configuration.
func defaultDuckDBConfig() DuckDBConfig {
	return DuckDBConfig{
		Enabled:       false,
		DBPath:        ":memory:",
		EnableS3:      false,
		EnableParquet: false,
		Extensions:    []string{},
		// MemoryLimitMB / MaxParallelism defaults mirror the PRAGMAs the
		// federated query template used to hardcode (memory_limit='4GB',
		// threads=4); connection-level pragmas are now the single source of
		// truth, so these defaults keep the effective behavior unchanged.
		MemoryLimitMB:                  4096,
		MaxConnections:                 1,
		QueryTimeout:                   30 * time.Second,
		MaxParallelism:                 4,
		CircuitBreakerThreshold:        0,
		CircuitBreakerFailureThreshold: 5,
		CircuitBreakerWindow:           time.Minute,
		CircuitBreakerOpenDuration:     time.Minute,
		// The manifest read surface (S3Bucket / S3DataPrefix / ManifestPrefix /
		// ManifestTemplate) deliberately defaults to all-zero. The cdc CLI
		// tools default ManifestTemplate to "manifest/{{.SchemaID}}.json", but
		// mirroring that here would flip every existing deployment from glob
		// reads to manifest-driven reads on upgrade. Manifest reads must be
		// opted into explicitly.
		Routing: RoutingPolicy{
			Strategy:          RoutingStrategyHybrid,
			HotTTL:            5 * time.Minute,
			MaxDuckDBScanRows: 100000,
			AllowS3Fallback:   true,
		},
	}
}

// validateDuckDBConfig validates DuckDB-specific configuration.
func (c *Config) validateDuckDBConfig() error {
	if c.DuckDB.MemoryLimitMB < 0 {
		return &ConfigError{Field: "duckdb.memoryLimitMB", Message: "must be greater than or equal to 0"}
	}
	if c.DuckDB.Enabled && c.DuckDB.MaxConnections <= 0 {
		return &ConfigError{Field: "duckdb.maxConnections", Message: "must be greater than 0 when duckdb enabled"}
	}
	if c.DuckDB.QueryTimeout < 0 {
		return &ConfigError{Field: "duckdb.queryTimeout", Message: "must be greater than or equal to 0"}
	}
	if c.DuckDB.MaxParallelism < 0 {
		return &ConfigError{Field: "duckdb.maxParallelism", Message: "must be greater than or equal to 0"}
	}
	if c.DuckDB.CircuitBreakerFailureThreshold < 0 {
		return &ConfigError{Field: "duckdb.circuitBreakerFailureThreshold", Message: "must be greater than or equal to 0"}
	}
	if c.DuckDB.CircuitBreakerWindow < 0 {
		return &ConfigError{Field: "duckdb.circuitBreakerWindow", Message: "must be greater than or equal to 0"}
	}
	if c.DuckDB.CircuitBreakerOpenDuration < 0 {
		return &ConfigError{Field: "duckdb.circuitBreakerOpenDuration", Message: "must be greater than or equal to 0"}
	}
	allowed := map[RoutingStrategy]bool{
		RoutingStrategyFreshnessFirst: true,
		RoutingStrategyCostFirst:      true,
		RoutingStrategyHybrid:         true,
		"":                            true,
	}
	if !allowed[c.DuckDB.Routing.Strategy] {
		return &ConfigError{Field: "duckdb.routing.strategy", Message: "invalid routing strategy"}
	}
	if c.DuckDB.Routing.HotTTL < 0 {
		return &ConfigError{Field: "duckdb.routing.hotTTL", Message: "must be greater than or equal to 0"}
	}
	if c.DuckDB.Routing.MaxDuckDBScanRows < 0 {
		return &ConfigError{Field: "duckdb.routing.maxDuckDBScanRows", Message: "must be greater than or equal to 0"}
	}
	if err := c.DuckDB.ValidateManifestRead(); err != nil {
		return err
	}

	return nil
}

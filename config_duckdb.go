package forma

import (
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

	// FlushVisibilityGraceMs widens the federated dirty barrier (#252): rows
	// whose flushed_at is within this many milliseconds of query start stay
	// hot-readable, covering the reader-side staleness between loading the
	// manifest and scanning change_log while a flush publishes. Zero means the
	// built-in default (60s); a negative value disables the grace and restores
	// the exact flushed_at = 0 barrier. Widening is a safe over-approximation:
	// affected rows are served from Postgres, which always holds current state.
	FlushVisibilityGraceMs int64 `json:"flushVisibilityGraceMs"`

	Routing RoutingPolicy `json:"routing"` // routing policy for federated queries
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

	return nil
}

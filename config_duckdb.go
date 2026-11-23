package forma

import (
	"fmt"
	"strings"
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
	QueryTimeout   time.Duration `json:"queryTimeout"`   // budget for all DuckDB work of one federated query (#465)
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

	// HotLegTransport names how a federated pass reads the hot tier
	// (design.md §6.1). HotLegTransportPgx, or empty, which means the same, is
	// the only value: the pass reads the hot tier over the engine's pgx pool
	// in one REPEATABLE READ snapshot, has DuckDB read only the parquet tiers,
	// and merges the two in Go. The field stays after the attach transport
	// was retired (#858) so that a configuration still selecting "attach"
	// fails validation instead of changing transport silently. A pass holds
	// one pooled PostgreSQL connection for its whole duration, and the engine
	// admits at most MaxConnections of them at once, so size the pool above
	// MaxConnections.
	HotLegTransport HotLegTransport `json:"hotLegTransport"`
	// HotLegMaxDirtyRows caps the dirty set a pgx pass stages. Past it the
	// pass fails as a degradable federated read failure before DuckDB scans
	// anything, so AllowPartialDegradedMode answers it from PostgreSQL
	// alone. Zero, the default, sets no cap.
	HotLegMaxDirtyRows int `json:"hotLegMaxDirtyRows"`

	// S3Bucket is the bucket holding the lakehouse: the parquet objects and
	// the immutable manifests of every published generation
	// (docs/publication-protocol-v2.md). The reader takes each schema's
	// manifest key from its publication record and scans exactly the objects
	// the manifest lists, as s3://<S3Bucket>/<key>, so it needs no prefix or
	// path template of its own. Required when Enabled.
	S3Bucket string `json:"s3Bucket"`

	// AllowPartialDegradedMode lets a federated query whose DuckDB pass
	// fails with a degradable error be answered from PostgreSQL alone (the
	// design.md §7.2 degraded fallback). Under the storage invariant every
	// live row is in PostgreSQL, so the fallback costs no correctness; it
	// costs load, since an S3 or DuckDB outage then moves every scan onto
	// PostgreSQL. Whether PostgreSQL can absorb that is the operator's call,
	// so it is deployment configuration, off by default (#715): off, the
	// failure is the query's error.
	AllowPartialDegradedMode bool `json:"allowPartialDegradedMode"`

	Routing RoutingPolicy `json:"routing"` // routing policy for federated queries
}

// rejectPaddedValue rejects a value carrying leading or trailing whitespace.
// The manifest template and the bucket are shared with tools that never trim
// them: a padded value (a YAML quoting slip, a trailing newline from a mounted
// secret) would address one object on one side and another on the other side
// for the very same configured string. Rejecting keeps both sides
// byte-identical instead of normalizing on one side only.
func rejectPaddedValue(field, value string) error {
	if value != strings.TrimSpace(value) {
		return &ConfigError{Field: field, Message: "must not have leading or trailing whitespace"}
	}
	return nil
}

// ValidateParquetRead validates the federated reader's storage settings. It
// returns a *ConfigError naming the offending field, or nil. A padded bucket
// is refused whether or not DuckDB is enabled, as a property of the value; an
// enabled engine needs a bucket, because every published generation it reads
// is addressed in it.
func (d DuckDBConfig) ValidateParquetRead() error {
	if err := rejectPaddedValue("duckdb.s3Bucket", d.S3Bucket); err != nil {
		return err
	}
	if d.Enabled && d.S3Bucket == "" {
		return &ConfigError{
			Field:   "duckdb.s3Bucket",
			Message: "must be set when duckdb is enabled: the federated reader scans the published generations in it",
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
	// explicitly preferred. The small-result shortcut is a cost heuristic:
	// a keyset cursor (#354) overrides it onto the federated path. A query
	// it keeps on Postgres is complete for current state, since every live
	// row is in PostgreSQL (design.md §3.3).
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
		// The pgx hot leg, the only transport since #858.
		HotLegTransport: HotLegTransportPgx,
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
	if err := c.DuckDB.ValidateHotLeg(); err != nil {
		return err
	}
	if err := c.DuckDB.ValidateParquetRead(); err != nil {
		return fmt.Errorf("invalid duckdb parquet read configuration: %w", err)
	}
	return nil
}

// ParquetGlobMetacharacters are the characters DuckDB's read_parquet treats as
// glob metacharacters in an S3 path (`**` is two of the first). The federated
// reader refuses a published generation listing an address that holds one
// (federated.ErrPublishedPathNotConcrete), and ValidateParquetPrefix refuses
// them in every writer's parquet prefix (#477, #526 review, #527).
const ParquetGlobMetacharacters = "*?["

// ValidateParquetPrefix rejects a parquet data prefix carrying any of
// ParquetGlobMetacharacters (#527) or a `.` or `..` path segment
// (HasDotPathSegment). The CDC writers store the prefix as a literal S3 key,
// which S3 permits, while the reader hands every listed object's address to
// DuckDB's read_parquet. DuckDB would expand a glob, so a prefix of `data/*`
// would make every object under it unreadable as listed, and it normalizes a
// dot segment away, so `data/../x` would address another object than the one
// the writer stored. DuckDB's S3 path handling has no documented escape
// syntax for either, so the prefix is refused rather than escaped. field
// names the setting in the returned ConfigError: the writer tools call it on
// their prefix flags, and the CDC writers on their configured prefix. An
// empty prefix passes.
func ValidateParquetPrefix(field, prefix string) error {
	if strings.ContainsAny(prefix, ParquetGlobMetacharacters) {
		return &ConfigError{
			Field: field,
			Message: "must not contain a glob metacharacter (*, ?, [): the writers store it as a literal S3 key " +
				"but DuckDB's read_parquet would expand it",
		}
	}
	if HasDotPathSegment(prefix) {
		return &ConfigError{
			Field: field,
			Message: "must not contain a . or .. path segment: the writers store it as a literal S3 key " +
				"but DuckDB normalizes the path and would address another object",
		}
	}
	return nil
}

// HasDotPathSegment reports whether path has a segment, between slashes or
// at either end, that is exactly `.` or `..`. The AWS SDK keeps such a
// segment in the object key it writes, while DuckDB normalizes the path
// before it addresses the object, so the two name different objects (#896).
func HasDotPathSegment(path string) bool {
	for _, segment := range strings.Split(path, "/") {
		if segment == "." || segment == ".." {
			return true
		}
	}
	return false
}

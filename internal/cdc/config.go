package cdc

import "time"

// CDCConfig controls change_log flushing and export behavior.
// S3 client is injected separately via RunOnce parameter to allow callers to
// provide either AWS or MinIO implementations.
type CDCConfig struct {
	// Change log table
	ChangeLogTable string

	// Thresholds
	MinRecords int   // flush when unflushed rows >= MinRecords
	MaxAgeMs   int64 // flush when oldest unflushed row age >= MaxAgeMs
	BatchSize  int   // maximum rows per snapshot

	// Postgres connection
	PGHost     string
	PGPort     int
	PGUser     string
	PGPassword string
	PGDB       string
	PGUseIAM   bool
	PGSSLMode  string

	// DuckDB export options
	DuckDBPath              string        // optional on-disk duckdb; empty for :memory:
	DuckThreads             int           // PRAGMA threads
	DuckMemLimit            string        // e.g. "4GB"
	QueryTimeout            time.Duration // timeout for duckdb export
	ParquetCompression      string        // e.g. "zstd"
	ParquetCompressionLevel int           // codec level if supported

	// S3
	S3Bucket   string
	S3Prefix   string // prefix inside bucket for delta files
	S3Endpoint string
	S3Region   string
	S3UseSSL   bool
	S3UsePath  bool // path style addressing
}

// CompactionConfig controls Base/Delta maintenance.
type CompactionConfig struct {
	SchemaID         int16         // optional filter; 0 means all
	ManifestPath     string        // s3 path or fs path to manifest JSON for the schema
	TargetBaseSizeMB int           // default 256
	MaxDeltaSizeMB   int           // default 50
	DirtyRatioPct    int           // default 5 (rewrite when updated rows/base rows > 5%)
	RunInterval      time.Duration // scheduler interval when used in a loop
	TempPrefix       string        // temp path prefix for rewrites
	MaxParallelFiles int           // optional parallelism for rewrites

	// Backoff parameters for S3/manifest operations
	MaxRetries  int           // default 5
	BaseBackoff time.Duration // default 100ms
	MaxBackoff  time.Duration // default 10s
}

// ManifestConfig describes where manifests are stored and how to resolve parquet paths.
type ManifestConfig struct {
	Bucket       string
	Prefix       string // root prefix for manifests (e.g., manifest/<project>/)
	PathTemplate string // optional template per schema, e.g., "manifest/{{.SchemaID}}.json"
}

// Default constants
const (
	DefaultParquetCompression      = "zstd"
	DefaultParquetCompressionLevel = 3
	DefaultMinRecords              = 1000
	DefaultMaxAgeMs                = 60000 // 1 minute
	DefaultBatchSize               = 10000
	DefaultTargetBaseSizeMB        = 256
	DefaultMaxDeltaSizeMB          = 50
	DefaultDirtyRatioPct           = 5
	DefaultMaxRetries              = 5
	DefaultBaseBackoffMs           = 100
	DefaultMaxBackoffMs            = 10000
	DefaultPGSSLMode               = "require"
)

// WithDefaults returns a copy of CDCConfig with missing fields set to defaults.
func (c CDCConfig) WithDefaults() CDCConfig {
	if c.MinRecords <= 0 {
		c.MinRecords = DefaultMinRecords
	}
	if c.MaxAgeMs <= 0 {
		c.MaxAgeMs = DefaultMaxAgeMs
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.ParquetCompression == "" {
		c.ParquetCompression = DefaultParquetCompression
	}
	if c.ParquetCompressionLevel <= 0 {
		c.ParquetCompressionLevel = DefaultParquetCompressionLevel
	}
	if c.DuckThreads <= 0 {
		c.DuckThreads = 4
	}
	if c.DuckMemLimit == "" {
		c.DuckMemLimit = "4GB"
	}
	if c.QueryTimeout <= 0 {
		c.QueryTimeout = 5 * time.Minute
	}
	if c.ChangeLogTable == "" {
		c.ChangeLogTable = "change_log"
	}
	if c.PGSSLMode == "" {
		c.PGSSLMode = DefaultPGSSLMode
	}
	return c
}

// WithDefaults returns a copy of CompactionConfig with missing fields set to defaults.
func (c CompactionConfig) WithDefaults() CompactionConfig {
	if c.TargetBaseSizeMB <= 0 {
		c.TargetBaseSizeMB = DefaultTargetBaseSizeMB
	}
	if c.MaxDeltaSizeMB <= 0 {
		c.MaxDeltaSizeMB = DefaultMaxDeltaSizeMB
	}
	if c.DirtyRatioPct <= 0 {
		c.DirtyRatioPct = DefaultDirtyRatioPct
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = DefaultMaxRetries
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = time.Duration(DefaultBaseBackoffMs) * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = time.Duration(DefaultMaxBackoffMs) * time.Millisecond
	}
	return c
}

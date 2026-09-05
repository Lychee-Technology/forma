package cdc

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// CDCConfig controls change_log flushing and export behavior.
// S3 client is injected separately via RunOnce parameter to allow callers to
// provide either AWS or MinIO implementations.
type CDCConfig struct {
	// Table names
	ChangeLogTable  string
	EntityMainTable string
	EAVDataTable    string

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
	EstimatedRowBytes       int           // rough row size estimate for batch sizing
	MaxBatchBytes           int64         // optional cap to limit batch size by bytes

	// Init-specific options (cdc-init base file export)
	TargetFileSizeMB int // target parquet file size in MB (0 = use BatchSize)
	MaxBatchSize     int // maximum rows per batch to cap memory usage

	// S3
	S3Bucket          string
	S3Prefix          string // prefix inside bucket for delta files
	S3Endpoint        string
	S3Region          string
	S3UseSSL          bool
	S3UsePath         bool   // path style addressing
	S3AccessKeyID     string // AWS access key ID; overrides environment variable AWS_ACCESS_KEY_ID
	S3SecretAccessKey string // AWS secret access key; overrides environment variable AWS_SECRET_ACCESS_KEY
	S3SessionToken    string // AWS session token for temporary credentials (STS/roles); effective only with same-source S3AccessKeyID/S3SecretAccessKey, see ResolveStaticS3Credentials (#329)

	// Manifest (optional - when set, flush updates manifest after export)
	ManifestPrefix   string // root prefix for manifests in S3
	ManifestTemplate string // path template, e.g. "manifest/{{.SchemaID}}.json"

	// BeforeExportHook, when non-nil, runs per batch after dirty-ID selection
	// (the snapshot is already captured) and before the DuckDB export. Test
	// seam for driving mutations inside the selection->export race window
	// (#182); always nil in production. A hook error aborts the batch before
	// any side effect. The init backfill fires the same hook per batch
	// (#462); it has no snapshot clock, so there the hook receives 0.
	BeforeExportHook func(ctx context.Context, schemaID int16, batchIDs []uuid.UUID, snapshot int64) error
}

// CompactionConfig controls Base/Delta maintenance.
type CompactionConfig struct {
	SchemaID         int16  // optional filter; 0 means all
	ManifestPath     string // s3 path or fs path to manifest JSON for the schema
	TargetBaseSizeMB int    // default 256
	// TargetBaseSizeBytes is the byte-precise promotion threshold. Zero
	// derives it from TargetBaseSizeMB; the compactor compares delta-tier
	// bytes against it directly, so sub-MB tiers are not truncated to 0 MB.
	TargetBaseSizeBytes int64
	MaxDeltaSizeMB      int           // default 50
	DirtyRatioPct       int           // default 5 (rewrite when updated rows/base rows > 5%)
	RunInterval         time.Duration // scheduler interval when used in a loop
	TempPrefix          string        // temp path prefix for rewrites
	MaxParallelFiles    int           // optional parallelism for rewrites

	// SkipInputChecksumVerify opts out of the pre-merge verification of
	// rewrite inputs (#347). The zero value verifies: a rewrite folds its
	// sources into one new base and splices their entries out of the
	// manifest (the objects themselves are retained for in-flight readers,
	// #461), so the gate is the last moment silent corruption is both
	// detectable and attributable to a named, still-listed object, and that
	// has to hold for deployments that never set this struct field.
	// Setting it true trades that detection away to save one GET per stamped
	// source. It waives hashing only: a source outside the compactor's bucket
	// is still refused (compaction.ErrForeignSource, #417), because that is a
	// scope check, not a checksum check.
	SkipInputChecksumVerify bool

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
	DefaultMinRecords              = 20000
	DefaultMaxAgeMs                = 3600000 // 1 hour
	DefaultEstimatedRowBytes       = 1024
	DefaultMaxBatchBytes           = int64(50 * 1024 * 1024)
	DefaultBatchSize               = 10000
	DefaultTargetBaseSizeMB        = 256
	DefaultTargetFileSizeMB        = 256
	DefaultMaxBatchSize            = 10000000 // 10M rows max
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
	if c.EstimatedRowBytes <= 0 {
		c.EstimatedRowBytes = DefaultEstimatedRowBytes
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = DefaultMaxBatchBytes
	}
	if c.MaxBatchSize <= 0 {
		c.MaxBatchSize = DefaultMaxBatchSize
	}
	if c.ChangeLogTable == "" {
		c.ChangeLogTable = "change_log"
	}
	if c.EntityMainTable == "" {
		c.EntityMainTable = "entity_main"
	}
	if c.EAVDataTable == "" {
		c.EAVDataTable = "eav_data"
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
	if c.TargetBaseSizeBytes <= 0 {
		c.TargetBaseSizeBytes = int64(c.TargetBaseSizeMB) << 20
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

package cdc

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/parquetformat"
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
	// BatchSize is the most unflushed entries one flush selects and
	// publishes. On PostgreSQL the V2 writer marks the whole batch in one
	// statement, so it is at most cdcv2.MaxMarkEntries (ValidateFlush); on
	// Aurora DSQL the batch publishes in generations of at most
	// cdcv2.DSQLGenerationLimit entries.
	BatchSize int

	// Postgres connection
	PGHost     string
	PGPort     int
	PGUser     string
	PGPassword string
	PGDB       string
	PGUseIAM   bool
	PGSSLMode  string
	// Engine is the database the PG settings reach (#633). It decides the
	// per-schema lock (NewSchemaLocker), how the V2 writer marks entries and
	// how many one publication marks, so every flusher, cdc-init and
	// manifest-reconcile of one database must name the same engine. Empty
	// means PostgreSQL.
	Engine forma.DatabaseEngine

	// DuckDB export options
	DuckDBPath              string        // optional on-disk duckdb; empty for :memory:
	DuckThreads             int           // PRAGMA threads
	DuckMemLimit            string        // e.g. "4GB"
	QueryTimeout            time.Duration // timeout for duckdb export
	ParquetCompression      string        // e.g. "zstd"
	ParquetCompressionLevel int           // codec level if supported
	EstimatedRowBytes       int           // rough row size estimate for batch sizing
	MaxBatchBytes           int64         // optional cap to limit batch size by bytes

	// Init-specific options (cdc-init base file export). Each base file is
	// one export snapshot of the V2 writer (cdcv2.Config.InitBatchSize).
	TargetFileSizeMB int // target parquet file size in MB (0 = use BatchSize)
	MaxBatchSize     int // maximum rows per batch to cap memory usage

	// S3. S3Prefix is the data prefix of the V2 object layout
	// (manifestv2.Layout): cdc-flush and cdc-init stage their parquet objects
	// under {S3Prefix}/{schema_id}/e{epoch}/{base|delta}/ and their manifests
	// under {S3Prefix}/{schema_id}/_manifests/, so every writer of one
	// deployment must name the same prefix. A flush's generation lists the
	// files of the init before it, and a manifest that lists a key under
	// another prefix is refused before it is written.
	S3Bucket          string
	S3Prefix          string
	S3Endpoint        string
	S3Region          string
	S3UseSSL          bool
	S3UsePath         bool   // path style addressing
	S3AccessKeyID     string // AWS access key ID; overrides environment variable AWS_ACCESS_KEY_ID
	S3SecretAccessKey string // AWS secret access key; overrides environment variable AWS_SECRET_ACCESS_KEY
	S3SessionToken    string // AWS session token for temporary credentials (STS/roles); effective only with same-source S3AccessKeyID/S3SecretAccessKey, see ResolveStaticS3Credentials (#329)

	// BeforeExportHook, when non-nil, runs per batch after dirty-ID selection
	// (the snapshot is already captured) and before the DuckDB export. Test
	// seam for driving mutations inside the selection->export race window
	// (#182); always nil in production. A hook error aborts the batch before
	// any side effect. The init backfill fires the same hook per batch
	// (#462); it has no snapshot clock, so there the hook receives 0.
	BeforeExportHook func(ctx context.Context, schemaID int16, batchIDs []uuid.UUID, snapshot int64) error

	// PublishFailpoint, when non-nil, is called at every boundary of a
	// publication the V2 writer passes (internal/cdcv2's Point names, such as
	// "stage-objects", "begin", "publish", "marks", "commit",
	// "commit-answer", "handoff" and "fence"). An error it answers fails the
	// step there, as a crash or a fault would. Test seam for the wired
	// cdc-flush and cdc-init paths; always nil in production.
	PublishFailpoint func(ctx context.Context, point string) error

	// RegistryEpochs is the epoch of each schema's publication record as
	// read before the run's SchemaRegistry loaded the schemas' metadata
	// (publication.Epochs). Every drop, and every registration of a dropped
	// id, moves a record to its next epoch, so a writer that finds the record
	// at another epoch under its schema lock refuses the schema
	// (cdcv2.ErrEpochChanged) before any export: the metadata it holds may be
	// the dropped schema's. A schema without an entry is refused the same
	// way. Required: a run without them is refused before any export
	// (ErrRegistryEpochsRequired), since the registry it is given loaded
	// before the run started and an epoch the run read itself proves nothing
	// about it. Read them with ReadRegistryEpochs before loading the
	// registry, and again whenever the registry is loaded again.
	RegistryEpochs map[int16]int64
}

// CompactionConfig controls Base/Delta maintenance.
type CompactionConfig struct {
	SchemaID         int16 // the schema to compact (required)
	TargetBaseSizeMB int   // default 256
	// TargetBaseSizeBytes is the byte-precise promotion threshold. Zero
	// derives it from TargetBaseSizeMB; the compactor compares delta-tier
	// bytes against it directly, so sub-MB tiers are not truncated to 0 MB.
	TargetBaseSizeBytes int64
	MaxDeltaSizeMB      int // default 50
	DirtyRatioPct       int // default 5 (rewrite when updated rows/base rows > 5%)

	// SkipInputChecksumVerify opts out of the pre-merge verification of
	// merge sources (#347). The zero value verifies: a merge folds its
	// sources into one new base and the published generation stops listing
	// them, so the gate is the last moment silent corruption is both
	// detectable and attributable to a listed object, and that has to hold
	// for deployments that never set this struct field. Setting it true
	// trades that detection away to save one GET per source.
	SkipInputChecksumVerify bool

	// MaxRetries bounds how often a compaction that lost its compare-and-set
	// to a rival compaction or cdc-init reads the head and merges again
	// (default 5). One that lost to flushes rebases without merging again.
	MaxRetries int
}

// ManifestConfig describes where manifests are stored and how to resolve parquet paths.
type ManifestConfig struct {
	Bucket       string
	Prefix       string // root prefix for manifests (e.g., manifest/<project>/)
	PathTemplate string // optional template per schema, e.g., "manifest/{{.SchemaID}}.json"
}

// Default constants
const (
	DefaultParquetCompression      = parquetformat.DefaultCompression
	DefaultParquetCompressionLevel = parquetformat.DefaultCompressionLevel
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
	return c
}

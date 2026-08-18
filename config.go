package forma

import (
	"time"
)

// Config consolidates settings from both modules
type Config struct {
	Database       DatabaseConfig    `json:"database"`
	Query          QueryConfig       `json:"query"`
	Entity         EntityConfig      `json:"entity"`
	Transaction    TransactionConfig `json:"transaction"`
	Performance    PerformanceConfig `json:"performance"`
	Logging        LoggingConfig     `json:"logging"`
	Metrics        MetricsConfig     `json:"metrics"`
	Reference      ReferenceConfig   `json:"reference"`
	DuckDB         DuckDBConfig      `json:"duckdb"`
	SchemaRegistry SchemaRegistry    `json:"-"` // Custom schema registry implementation (optional)
}

// DatabaseConfig contains database connection settings
type DatabaseConfig struct {
	Host            string        `json:"host"`
	Port            int           `json:"port"`
	Database        string        `json:"database"`
	Username        string        `json:"username"`
	Password        string        `json:"password"`
	SSLMode         string        `json:"sslMode"`
	Schema          string        `json:"schema"`
	MaxConnections  int           `json:"maxConnections"`
	MaxIdleConns    int           `json:"maxIdleConns"`
	ConnMaxLifetime time.Duration `json:"connMaxLifetime"`
	ConnMaxIdleTime time.Duration `json:"connMaxIdleTime"`
	Timeout         time.Duration `json:"timeout"`
	TableNames      TableNames    `json:"tableNames"`
}

// QueryConfig contains query execution settings
type QueryConfig struct {
	DefaultTimeout     time.Duration `json:"defaultTimeout"`
	MaxRows            int           `json:"maxRows"`
	DefaultPageSize    int           `json:"defaultPageSize"`
	MaxPageSize        int           `json:"maxPageSize"`
	EnableQueryPlan    bool          `json:"enableQueryPlan"`
	EnableOptimization bool          `json:"enableOptimization"`
	CacheQueryPlans    bool          `json:"cacheQueryPlans"`
	QueryPlanCacheTTL  time.Duration `json:"queryPlanCacheTTL"`
}

// EntityConfig contains entity management settings
type EntityConfig struct {
	EnableReferenceValidation bool          `json:"enableReferenceValidation"`
	EnableCascadeDelete       bool          `json:"enableCascadeDelete"`
	BatchSize                 int           `json:"batchSize"`
	CacheEnabled              bool          `json:"cacheEnabled"`
	CacheTTL                  time.Duration `json:"cacheTTL"`
	MaxEntitySize             int           `json:"maxEntitySize"`
	EnableVersioning          bool          `json:"enableVersioning"`
	SchemaDirectory           string        `json:"schemaDirectory"`

	// ValidateUpdatesStrict makes update payloads that violate the entity's
	// JSON Schema fail with 4xx instead of only being logged (#314).
	//
	// Default false: rows written before schema enforcement may already violate
	// their schema, and rejecting on update would make them un-updatable — a
	// caller touching one unrelated field would be refused for a pre-existing
	// violation elsewhere. Creates are always enforced; they have no legacy data.
	//
	// Whether it is safe to flip this yet is answered by the #317 aggregate:
	// the "report-only schema validation violations reached a milestone" Warn
	// line (per schema, at the 1st and every 100th accepted violation) and the
	// entity_report_only_validation_violation_total telemetry counter. While
	// those lines keep appearing for a schema, its rows are not yet repaired.
	// Their absence is not proof of the converse: the signal fires only when a
	// violating row is written, and only every 100th time thereafter within one
	// process, so a schema whose bad rows are never updated stays silent.
	// Confirm with an e2e pass over real data before flipping
	// (docs/error-handling.md).
	ValidateUpdatesStrict bool `json:"validateUpdatesStrict"`
}

// TransactionConfig contains transaction settings
type TransactionConfig struct {
	DefaultTimeout           time.Duration `json:"defaultTimeout"`
	MaxTimeout               time.Duration `json:"maxTimeout"`
	MaxRetryAttempts         int           `json:"maxRetryAttempts"`
	RetryAttempts            int           `json:"retryAttempts"`
	RetryDelay               time.Duration `json:"retryDelay"`
	IsolationLevel           string        `json:"isolationLevel"`
	EnableDeadlockDetection  bool          `json:"enableDeadlockDetection"`
	DeadlockCheckInterval    time.Duration `json:"deadlockCheckInterval"`
	DeadlockMaxWaitTime      time.Duration `json:"deadlockMaxWaitTime"`
	SlowTransactionThreshold time.Duration `json:"slowTransactionThreshold"`
	MinSuccessRate           float64       `json:"minSuccessRate"`
	MaxAverageDuration       time.Duration `json:"maxAverageDuration"`
	MaxConnectionPoolUsage   float64       `json:"maxConnectionPoolUsage"`
}

// PerformanceConfig contains performance monitoring settings
type PerformanceConfig struct {
	EnableMonitoring          bool          `json:"enableMonitoring"`
	SlowQueryThreshold        time.Duration `json:"slowQueryThreshold"`
	SlowOperationThreshold    time.Duration `json:"slowOperationThreshold"`
	MetricsCollectionInterval time.Duration `json:"metricsCollectionInterval"`
	BatchSize                 int           `json:"batchSize"`
	MaxBatchSize              int           `json:"maxBatchSize"`
	Batch                     BatchConfig   `json:"batch"`

	// Unified monitoring settings
	MaxMetricsHistory      int           `json:"maxMetricsHistory"`
	MaxAlertsHistory       int           `json:"maxAlertsHistory"`
	MaxRecommendations     int           `json:"maxRecommendations"`
	EnableAlerting         bool          `json:"enableAlerting"`
	EnableRecommendations  bool          `json:"enableRecommendations"`
	AlertingInterval       time.Duration `json:"alertingInterval"`
	RecommendationInterval time.Duration `json:"recommendationInterval"`

	// Memory monitoring
	EnableMemoryMonitoring bool  `json:"enableMemoryMonitoring"`
	MemoryThreshold        int64 `json:"memoryThreshold"`

	// Correlation tracking
	EnableCorrelationTracking bool          `json:"enableCorrelationTracking"`
	CorrelationTTL            time.Duration `json:"correlationTTL"`
}

// BatchConfig contains batch processing settings
type BatchConfig struct {
	EnableDynamicSizing      bool `json:"enableDynamicSizing"`
	EnableParallelProcessing bool `json:"enableParallelProcessing"`
	EnableBatchStreaming     bool `json:"enableBatchStreaming"`
	ParallelThreshold        int  `json:"parallelThreshold"`
	StreamingThreshold       int  `json:"streamingThreshold"`
	MaxParallelWorkers       int  `json:"maxParallelWorkers"`
	StreamingChunkSize       int  `json:"streamingChunkSize"`
	StreamingDelay           int  `json:"streamingDelay"` // milliseconds
	MaxComplexityPerBatch    int  `json:"maxComplexityPerBatch"`
	AttributeComplexityScore int  `json:"attributeComplexityScore"`
	OptimalChunkSize         int  `json:"optimalChunkSize"`
}

// LoggingConfig contains logging settings
type LoggingConfig struct {
	Level                  string        `json:"level"`
	Format                 string        `json:"format"`
	EnableStructured       bool          `json:"enableStructured"`
	EnablePerformance      bool          `json:"enablePerformance"`
	EnableQueryLogging     bool          `json:"enableQueryLogging"`
	LogSlowQueries         bool          `json:"logSlowQueries"`
	SlowQueryThreshold     time.Duration `json:"slowQueryThreshold"`
	MaxLogSize             int           `json:"maxLogSize"`
	LogRotation            bool          `json:"logRotation"`
	SanitizeParameters     bool          `json:"sanitizeParameters"`
	LogQueries             bool          `json:"logQueries"`
	LogErrors              bool          `json:"logErrors"`
	LogSecurityEvents      bool          `json:"logSecurityEvents"`
	LogPerformanceWarnings bool          `json:"logPerformanceWarnings"`
	LogAllOperations       bool          `json:"logAllOperations"`
	EnableDetailedLogging  bool          `json:"enableDetailedLogging"`
}

// MetricsConfig contains metrics collection settings
type MetricsConfig struct {
	Enabled                  bool              `json:"enabled"`
	Provider                 string            `json:"provider"` // prometheus, statsd, etc.
	Endpoint                 string            `json:"endpoint"`
	CollectionInterval       time.Duration     `json:"collectionInterval"`
	EnableHistograms         bool              `json:"enableHistograms"`
	EnableCounters           bool              `json:"enableCounters"`
	EnableGauges             bool              `json:"enableGauges"`
	Namespace                string            `json:"namespace"`
	Labels                   map[string]string `json:"labels"`
	MaxSamples               int               `json:"maxSamples"`
	EnableOperationMetrics   bool              `json:"enableOperationMetrics"`
	EnableTransactionMetrics bool              `json:"enableTransactionMetrics"`
	EnablePatternMetrics     bool              `json:"enablePatternMetrics"`
}

// ReferenceConfig contains reference management settings
type ReferenceConfig struct {
	ValidateOnCreate bool                   `json:"validateOnCreate"`
	ValidateOnUpdate bool                   `json:"validateOnUpdate"`
	CheckIntegrity   bool                   `json:"checkIntegrity"`
	CascadeDelete    bool                   `json:"cascadeDelete"`
	CascadeUpdate    bool                   `json:"cascadeUpdate"`
	MaxCascadeDepth  int                    `json:"maxCascadeDepth"`
	CascadeRules     map[string]CascadeRule `json:"cascadeRules,omitempty"`
	EnableCaching    bool                   `json:"enableCaching"`
	CacheTTL         time.Duration          `json:"cacheTTL"`
	MaxCacheSize     int                    `json:"maxCacheSize"`
	BatchSize        int                    `json:"batchSize"`
}

// CascadeRule defines cascade behavior for specific schema relationships
type CascadeRule struct {
	SourceSchema string        `json:"sourceSchema"`
	TargetSchema string        `json:"targetSchema"`
	Action       CascadeAction `json:"action"`
	MaxDepth     int           `json:"maxDepth,omitempty"`
}

// CascadeAction defines the type of cascade action
type CascadeAction string

const (
	CascadeActionDelete   CascadeAction = "delete"
	CascadeActionUpdate   CascadeAction = "update"
	CascadeActionNullify  CascadeAction = "nullify"
	CascadeActionRestrict CascadeAction = "restrict"
)

// DefaultConfig returns a default configuration
func DefaultConfig(schemaRegistry SchemaRegistry) *Config {
	return &Config{
		SchemaRegistry: schemaRegistry,
		Database:       defaultDatabaseConfig(),
		Query:          defaultQueryConfig(),
		Entity:         defaultEntityConfig(),
		Transaction:    defaultTransactionConfig(),
		Performance:    defaultPerformanceConfig(),
		Logging:        defaultLoggingConfig(),
		Metrics:        defaultMetricsConfig(),
		Reference:      defaultReferenceConfig(),
		DuckDB:         defaultDuckDBConfig(),
	}
}

// -----------------------------------------------------------------------
// Functional Options pattern
// -----------------------------------------------------------------------

// Option is a functional option that mutates a Config.
// Use the With* constructors below to build option values, then pass them to
// NewConfig to obtain a fully-configured Config without touching the struct
// fields directly.
type Option func(*Config)

// NewConfig creates a Config starting from the defaults produced by
// DefaultConfig(nil) and then applies each provided Option in order.
// The schema registry can be set via WithSchemaRegistry.
//
// Example:
//
//	cfg := forma.NewConfig(
//	    forma.WithDatabase(forma.DatabaseConfig{Host: "db.example.com", Port: 5432, MaxConnections: 50}),
//	    forma.WithDuckDB(forma.DuckDBConfig{Enabled: true, DBPath: ":memory:"}),
//	)
func NewConfig(opts ...Option) *Config {
	cfg := DefaultConfig(nil)
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// WithSchemaRegistry sets the schema registry on the config.
func WithSchemaRegistry(sr SchemaRegistry) Option {
	return func(c *Config) { c.SchemaRegistry = sr }
}

// WithDatabase replaces the DatabaseConfig section.
func WithDatabase(db DatabaseConfig) Option {
	return func(c *Config) { c.Database = db }
}

// WithQuery replaces the QueryConfig section.
func WithQuery(q QueryConfig) Option {
	return func(c *Config) { c.Query = q }
}

// WithEntity replaces the EntityConfig section.
func WithEntity(e EntityConfig) Option {
	return func(c *Config) { c.Entity = e }
}

// WithTransaction replaces the TransactionConfig section.
func WithTransaction(t TransactionConfig) Option {
	return func(c *Config) { c.Transaction = t }
}

// WithPerformance replaces the PerformanceConfig section.
func WithPerformance(p PerformanceConfig) Option {
	return func(c *Config) { c.Performance = p }
}

// WithLogging replaces the LoggingConfig section.
func WithLogging(l LoggingConfig) Option {
	return func(c *Config) { c.Logging = l }
}

// WithMetrics replaces the MetricsConfig section.
func WithMetrics(m MetricsConfig) Option {
	return func(c *Config) { c.Metrics = m }
}

// WithReference replaces the ReferenceConfig section.
func WithReference(r ReferenceConfig) Option {
	return func(c *Config) { c.Reference = r }
}

// WithDuckDB replaces the DuckDBConfig section.
func WithDuckDB(d DuckDBConfig) Option {
	return func(c *Config) { c.DuckDB = d }
}

// defaultDatabaseConfig returns default database configuration.
func defaultDatabaseConfig() DatabaseConfig {
	return DatabaseConfig{
		Host:            "localhost",
		Port:            5432,
		Schema:          "public",
		MaxConnections:  25,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
		Timeout:         30 * time.Second,
	}
}

// defaultQueryConfig returns default query configuration.
func defaultQueryConfig() QueryConfig {
	return QueryConfig{
		DefaultTimeout:     30 * time.Second,
		MaxRows:            10000,
		DefaultPageSize:    50,
		MaxPageSize:        100,
		EnableQueryPlan:    true,
		EnableOptimization: true,
		CacheQueryPlans:    true,
		QueryPlanCacheTTL:  1 * time.Hour,
	}
}

// defaultEntityConfig returns default entity configuration.
func defaultEntityConfig() EntityConfig {
	return EntityConfig{
		EnableReferenceValidation: true,
		EnableCascadeDelete:       false,
		BatchSize:                 100,
		CacheEnabled:              true,
		CacheTTL:                  5 * time.Minute,
		MaxEntitySize:             1024 * 1024, // 1MB
		EnableVersioning:          true,
	}
}

// defaultTransactionConfig returns default transaction configuration.
func defaultTransactionConfig() TransactionConfig {
	return TransactionConfig{
		DefaultTimeout:           30 * time.Second,
		MaxTimeout:               5 * time.Minute,
		MaxRetryAttempts:         3,
		RetryAttempts:            3,
		RetryDelay:               100 * time.Millisecond,
		IsolationLevel:           "READ_COMMITTED",
		EnableDeadlockDetection:  true,
		DeadlockCheckInterval:    5 * time.Second,
		DeadlockMaxWaitTime:      30 * time.Second,
		SlowTransactionThreshold: 2 * time.Second,
		MinSuccessRate:           95.0,
		MaxAverageDuration:       1 * time.Second,
		MaxConnectionPoolUsage:   80.0,
	}
}

// defaultPerformanceConfig returns default performance configuration.
func defaultPerformanceConfig() PerformanceConfig {
	return PerformanceConfig{
		EnableMonitoring:          true,
		SlowQueryThreshold:        1 * time.Second,
		SlowOperationThreshold:    2 * time.Second,
		MetricsCollectionInterval: 30 * time.Second,
		BatchSize:                 100,
		MaxBatchSize:              1000,
		Batch:                     defaultBatchConfig(),

		// Unified monitoring defaults
		MaxMetricsHistory:      10000,
		MaxAlertsHistory:       1000,
		MaxRecommendations:     100,
		EnableAlerting:         true,
		EnableRecommendations:  true,
		AlertingInterval:       1 * time.Minute,
		RecommendationInterval: 5 * time.Minute,

		// Memory monitoring defaults
		EnableMemoryMonitoring: true,
		MemoryThreshold:        100 * 1024 * 1024, // 100MB

		// Correlation tracking defaults
		EnableCorrelationTracking: true,
		CorrelationTTL:            1 * time.Hour,
	}
}

// defaultBatchConfig returns default batch configuration.
func defaultBatchConfig() BatchConfig {
	return BatchConfig{
		EnableDynamicSizing:      true,
		EnableParallelProcessing: true,
		EnableBatchStreaming:     true,
		ParallelThreshold:        50,
		StreamingThreshold:       500,
		MaxParallelWorkers:       4,
		StreamingChunkSize:       100,
		StreamingDelay:           10,
		MaxComplexityPerBatch:    500,
		AttributeComplexityScore: 1,
		OptimalChunkSize:         10,
	}
}

// defaultLoggingConfig returns default logging configuration.
func defaultLoggingConfig() LoggingConfig {
	return LoggingConfig{
		Level:                  "info",
		Format:                 "json",
		EnableStructured:       true,
		EnablePerformance:      true,
		EnableQueryLogging:     false,
		LogSlowQueries:         true,
		SlowQueryThreshold:     1 * time.Second,
		MaxLogSize:             100 * 1024 * 1024, // 100MB
		LogRotation:            true,
		SanitizeParameters:     true,
		LogQueries:             false,
		LogErrors:              true,
		LogSecurityEvents:      true,
		LogPerformanceWarnings: true,
		LogAllOperations:       false,
		EnableDetailedLogging:  true,
	}
}

// defaultMetricsConfig returns default metrics configuration.
func defaultMetricsConfig() MetricsConfig {
	return MetricsConfig{
		Enabled:                  true,
		Provider:                 "prometheus",
		CollectionInterval:       30 * time.Second,
		EnableHistograms:         true,
		EnableCounters:           true,
		EnableGauges:             true,
		Namespace:                "dataplane",
		MaxSamples:               10000,
		EnableOperationMetrics:   true,
		EnableTransactionMetrics: true,
		EnablePatternMetrics:     true,
	}
}

// defaultReferenceConfig returns default reference configuration.
func defaultReferenceConfig() ReferenceConfig {
	return ReferenceConfig{
		ValidateOnCreate: true,
		ValidateOnUpdate: true,
		CheckIntegrity:   true,
		CascadeDelete:    false,
		CascadeUpdate:    false,
		MaxCascadeDepth:  5,
		EnableCaching:    true,
		CacheTTL:         5 * time.Minute,
		MaxCacheSize:     1000,
		BatchSize:        100,
	}
}

// Validate validates the configuration
func (c *Config) Validate() error {
	// Add validation logic here
	if c.Database.MaxConnections <= 0 {
		return &ConfigError{Field: "database.maxConnections", Message: "must be greater than 0"}
	}

	if c.Query.DefaultPageSize <= 0 {
		return &ConfigError{Field: "query.defaultPageSize", Message: "must be greater than 0"}
	}

	if c.Query.MaxPageSize < c.Query.DefaultPageSize {
		return &ConfigError{Field: "query.maxPageSize", Message: "must be greater than or equal to defaultPageSize"}
	}

	if c.Performance.BatchSize <= 0 {
		return &ConfigError{Field: "performance.batchSize", Message: "must be greater than 0"}
	}

	if c.Performance.MaxBatchSize < c.Performance.BatchSize {
		return &ConfigError{Field: "performance.maxBatchSize", Message: "must be greater than or equal to batchSize"}
	}

	if err := c.validateDuckDBConfig(); err != nil {
		return err
	}

	return nil
}

// ConfigError represents a configuration validation error
type ConfigError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e *ConfigError) Error() string {
	return "config validation error for field '" + e.Field + "': " + e.Message
}

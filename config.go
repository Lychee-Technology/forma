package forma

import (
	"time"
)

// Config consolidates settings from both modules
type Config struct {
	Database    DatabaseConfig    `json:"database"`
	Query       QueryConfig       `json:"query"`
	Entity      EntityConfig      `json:"entity"`
	Transaction TransactionConfig `json:"transaction"`
	Performance PerformanceConfig `json:"performance"`
	Logging     LoggingConfig     `json:"logging"`
	Metrics     MetricsConfig     `json:"metrics"`
	Reference   ReferenceConfig   `json:"reference"`
}

// DatabaseConfig contains database connection settings
type DatabaseConfig struct {
	Host            string        `json:"host"`
	Port            int           `json:"port"`
	Database        string        `json:"database"`
	Username        string        `json:"username"`
	Password        string        `json:"password"`
	SSLMode         string        `json:"sslMode"`
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
func DefaultConfig() *Config {
	return &Config{
		Database: DatabaseConfig{
			Host:            "localhost",
			Port:            5432,
			MaxConnections:  25,
			MaxIdleConns:    5,
			ConnMaxLifetime: 5 * time.Minute,
			ConnMaxIdleTime: 5 * time.Minute,
			Timeout:         30 * time.Second,
		},
		Query: QueryConfig{
			DefaultTimeout:     30 * time.Second,
			MaxRows:            10000,
			DefaultPageSize:    50,
			MaxPageSize:        100,
			EnableQueryPlan:    true,
			EnableOptimization: true,
			CacheQueryPlans:    true,
			QueryPlanCacheTTL:  1 * time.Hour,
		},
		Entity: EntityConfig{
			EnableReferenceValidation: true,
			EnableCascadeDelete:       false,
			BatchSize:                 100,
			CacheEnabled:              true,
			CacheTTL:                  5 * time.Minute,
			MaxEntitySize:             1024 * 1024, // 1MB
			EnableVersioning:          true,
		},
		Transaction: TransactionConfig{
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
		},
		Performance: PerformanceConfig{
			EnableMonitoring:          true,
			SlowQueryThreshold:        1 * time.Second,
			SlowOperationThreshold:    2 * time.Second,
			MetricsCollectionInterval: 30 * time.Second,
			BatchSize:                 100,
			MaxBatchSize:              1000,
			Batch: BatchConfig{
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
			},

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
		},
		Logging: LoggingConfig{
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
		},
		Metrics: MetricsConfig{
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
		},
		Reference: ReferenceConfig{
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
		},
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

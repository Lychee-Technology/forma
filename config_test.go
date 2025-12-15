package forma

import (
	"testing"
	"time"
)

// mockSchemaRegistry is a mock implementation of SchemaRegistry for testing
type mockSchemaRegistry struct {
	schemas map[string]struct {
		id    int16
		cache SchemaAttributeCache
	}
}

// NewMockSchemaRegistry creates a new mock schema registry for testing
func NewMockSchemaRegistry() SchemaRegistry {
	return &mockSchemaRegistry{
		schemas: make(map[string]struct {
			id    int16
			cache SchemaAttributeCache
		}),
	}
}

// GetSchemaAttributeCacheByName retrieves schema ID and attribute cache by schema name
func (r *mockSchemaRegistry) GetSchemaAttributeCacheByName(name string) (int16, SchemaAttributeCache, error) {
	if schema, exists := r.schemas[name]; exists {
		return schema.id, schema.cache, nil
	}
	return 0, nil, nil
}

// GetSchemaAttributeCacheByID retrieves schema name and attribute cache by schema ID
func (r *mockSchemaRegistry) GetSchemaAttributeCacheByID(id int16) (string, SchemaAttributeCache, error) {
	for name, schema := range r.schemas {
		if schema.id == id {
			return name, schema.cache, nil
		}
	}
	return "", nil, nil
}

// ListSchemas returns a list of all registered schema names
func (r *mockSchemaRegistry) ListSchemas() []string {
	schemas := make([]string, 0, len(r.schemas))
	for name := range r.schemas {
		schemas = append(schemas, name)
	}
	return schemas
}

// GetSchemaByName retrieves schema ID and JSONSchema by schema name
func (r *mockSchemaRegistry) GetSchemaByName(name string) (int16, JSONSchema, error) {
	if schema, exists := r.schemas[name]; exists {
		return schema.id, JSONSchema{ID: schema.id, Name: name}, nil
	}
	return 0, JSONSchema{}, nil
}

// GetSchemaByID retrieves schema name and JSONSchema by schema ID
func (r *mockSchemaRegistry) GetSchemaByID(id int16) (string, JSONSchema, error) {
	for name, schema := range r.schemas {
		if schema.id == id {
			return name, JSONSchema{ID: id, Name: name}, nil
		}
	}
	return "", JSONSchema{}, nil
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig(NewMockSchemaRegistry())

	// Test database defaults
	if config.Database.Host != "localhost" {
		t.Errorf("Expected database host to be 'localhost', got %s", config.Database.Host)
	}
	if config.Database.Port != 5432 {
		t.Errorf("Expected database port to be 5432, got %d", config.Database.Port)
	}
	if config.Database.MaxConnections != 25 {
		t.Errorf("Expected max connections to be 25, got %d", config.Database.MaxConnections)
	}

	// Test query defaults
	if config.Query.DefaultTimeout != 30*time.Second {
		t.Errorf("Expected default timeout to be 30s, got %v", config.Query.DefaultTimeout)
	}
	if config.Query.DefaultPageSize != 50 {
		t.Errorf("Expected default page size to be 50, got %d", config.Query.DefaultPageSize)
	}
	if config.Query.MaxPageSize != 100 {
		t.Errorf("Expected max page size to be 100, got %d", config.Query.MaxPageSize)
	}

	// Test entity defaults
	if !config.Entity.EnableReferenceValidation {
		t.Error("Expected reference validation to be enabled by default")
	}
	if config.Entity.EnableCascadeDelete {
		t.Error("Expected cascade delete to be disabled by default")
	}
	if config.Entity.BatchSize != 100 {
		t.Errorf("Expected batch size to be 100, got %d", config.Entity.BatchSize)
	}

	// Test transaction defaults
	if config.Transaction.DefaultTimeout != 30*time.Second {
		t.Errorf("Expected transaction timeout to be 30s, got %v", config.Transaction.DefaultTimeout)
	}
	if config.Transaction.MaxRetryAttempts != 3 {
		t.Errorf("Expected max retry attempts to be 3, got %d", config.Transaction.MaxRetryAttempts)
	}

	// Test performance defaults
	if !config.Performance.EnableMonitoring {
		t.Error("Expected performance monitoring to be enabled by default")
	}
	if config.Performance.SlowQueryThreshold != 1*time.Second {
		t.Errorf("Expected slow query threshold to be 1s, got %v", config.Performance.SlowQueryThreshold)
	}

	// Test reference defaults
	if !config.Reference.ValidateOnCreate {
		t.Error("Expected reference validation on create to be enabled by default")
	}
	if config.Reference.CascadeDelete {
		t.Error("Expected cascade delete to be disabled by default")
	}
}

func TestConfigValidationDetailed(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectError bool
		errorField  string
	}{
		{
			name:        "valid config",
			config:      DefaultConfig(NewMockSchemaRegistry()),
			expectError: false,
		},
		{
			name: "invalid max connections",
			config: &Config{
				Database:    DatabaseConfig{MaxConnections: 0},
				Query:       QueryConfig{DefaultPageSize: 50, MaxPageSize: 100},
				Performance: PerformanceConfig{BatchSize: 100, MaxBatchSize: 1000},
			},
			expectError: true,
			errorField:  "database.maxConnections",
		},
		{
			name: "invalid page size",
			config: &Config{
				Database:    DatabaseConfig{MaxConnections: 25},
				Query:       QueryConfig{DefaultPageSize: 0, MaxPageSize: 100},
				Performance: PerformanceConfig{BatchSize: 100, MaxBatchSize: 1000},
			},
			expectError: true,
			errorField:  "query.defaultPageSize",
		},
		{
			name: "max page size less than default",
			config: &Config{
				Database:    DatabaseConfig{MaxConnections: 25},
				Query:       QueryConfig{DefaultPageSize: 100, MaxPageSize: 50},
				Performance: PerformanceConfig{BatchSize: 100, MaxBatchSize: 1000},
			},
			expectError: true,
			errorField:  "query.maxPageSize",
		},
		{
			name: "invalid batch size",
			config: &Config{
				Database:    DatabaseConfig{MaxConnections: 25},
				Query:       QueryConfig{DefaultPageSize: 50, MaxPageSize: 100},
				Performance: PerformanceConfig{BatchSize: 0, MaxBatchSize: 1000},
			},
			expectError: true,
			errorField:  "performance.batchSize",
		},
		{
			name: "max batch size less than batch size",
			config: &Config{
				Database:    DatabaseConfig{MaxConnections: 25},
				Query:       QueryConfig{DefaultPageSize: 50, MaxPageSize: 100},
				Performance: PerformanceConfig{BatchSize: 1000, MaxBatchSize: 100},
			},
			expectError: true,
			errorField:  "performance.maxBatchSize",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				if err == nil {
					t.Error("Expected validation error but got none")
				} else if configErr, ok := err.(*ConfigError); ok {
					if configErr.Field != tt.errorField {
						t.Errorf("Expected error field %s, got %s", tt.errorField, configErr.Field)
					}
				} else {
					t.Errorf("Expected ConfigError, got %T", err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no validation error but got: %v", err)
				}
			}
		})
	}
}

func TestConfigError(t *testing.T) {
	err := &ConfigError{
		Field:   "test.field",
		Message: "test message",
	}

	expected := "config validation error for field 'test.field': test message"
	if err.Error() != expected {
		t.Errorf("Expected error message %s, got %s", expected, err.Error())
	}
}

func TestCascadeRuleValidation(t *testing.T) {
	config := DefaultConfig(NewMockSchemaRegistry())

	// Add cascade rules
	config.Reference.CascadeRules = map[string]CascadeRule{
		"user_profile": {
			SourceSchema: "user",
			TargetSchema: "profile",
			Action:       CascadeActionDelete,
			MaxDepth:     3,
		},
	}

	err := config.Validate()
	if err != nil {
		t.Errorf("Expected no error with valid cascade rules, got: %v", err)
	}
}

func TestBatchConfigDefaults(t *testing.T) {
	config := DefaultConfig(NewMockSchemaRegistry())

	batch := config.Performance.Batch
	if !batch.EnableDynamicSizing {
		t.Error("Expected dynamic sizing to be enabled by default")
	}
	if !batch.EnableParallelProcessing {
		t.Error("Expected parallel processing to be enabled by default")
	}
	if batch.ParallelThreshold != 50 {
		t.Errorf("Expected parallel threshold to be 50, got %d", batch.ParallelThreshold)
	}
	if batch.MaxParallelWorkers != 4 {
		t.Errorf("Expected max parallel workers to be 4, got %d", batch.MaxParallelWorkers)
	}
}

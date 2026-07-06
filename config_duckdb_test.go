package forma

import (
	"testing"
	"time"
)

func TestDuckDBBreakerConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg := defaultDuckDBConfig()

	if cfg.CircuitBreakerThreshold != 0 {
		t.Errorf("Expected deprecated circuit breaker threshold to default to 0, got %v", cfg.CircuitBreakerThreshold)
	}
	if cfg.CircuitBreakerFailureThreshold != 5 {
		t.Errorf("Expected circuit breaker failure threshold to be 5, got %d", cfg.CircuitBreakerFailureThreshold)
	}
	if cfg.CircuitBreakerWindow != time.Minute {
		t.Errorf("Expected circuit breaker window to be 1m, got %v", cfg.CircuitBreakerWindow)
	}
	if cfg.CircuitBreakerOpenDuration != time.Minute {
		t.Errorf("Expected circuit breaker open duration to be 1m, got %v", cfg.CircuitBreakerOpenDuration)
	}
}

func TestDuckDBResourceConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg := defaultDuckDBConfig()

	if cfg.MaxParallelism != 4 {
		t.Errorf("Expected max parallelism to default to 4 (parity with the removed template PRAGMA threads=4), got %d", cfg.MaxParallelism)
	}
	if cfg.MemoryLimitMB != 4096 {
		t.Errorf("Expected memory limit to default to 4096MB (parity with the removed template PRAGMA memory_limit='4GB'), got %d", cfg.MemoryLimitMB)
	}
}

func TestDuckDBBreakerConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		duckDB     DuckDBConfig
		errorField string
	}{
		{
			name:       "zero breaker values are allowed for runtime default fallback",
			duckDB:     DuckDBConfig{},
			errorField: "",
		},
		{
			name: "negative failure threshold",
			duckDB: DuckDBConfig{
				CircuitBreakerFailureThreshold: -1,
			},
			errorField: "duckdb.circuitBreakerFailureThreshold",
		},
		{
			name: "negative window",
			duckDB: DuckDBConfig{
				CircuitBreakerWindow: -time.Second,
			},
			errorField: "duckdb.circuitBreakerWindow",
		},
		{
			name: "negative open duration",
			duckDB: DuckDBConfig{
				CircuitBreakerOpenDuration: -time.Second,
			},
			errorField: "duckdb.circuitBreakerOpenDuration",
		},
		{
			name: "negative max parallelism",
			duckDB: DuckDBConfig{
				MaxParallelism: -1,
			},
			errorField: "duckdb.maxParallelism",
		},
		{
			name: "negative memory limit",
			duckDB: DuckDBConfig{
				MemoryLimitMB: -1,
			},
			errorField: "duckdb.memoryLimitMB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := DefaultConfig(NewMockSchemaRegistry())
			config.DuckDB = tt.duckDB

			err := config.Validate()
			if tt.errorField == "" {
				if err != nil {
					t.Fatalf("Expected no validation error, got %v", err)
				}
				return
			}

			if err == nil {
				t.Fatal("Expected validation error, got nil")
			}
			configErr, ok := err.(*ConfigError)
			if !ok {
				t.Fatalf("Expected ConfigError, got %T", err)
			}
			if configErr.Field != tt.errorField {
				t.Errorf("Expected error field %s, got %s", tt.errorField, configErr.Field)
			}
		})
	}
}

package forma

import (
	"errors"
	"strings"
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

// ValidateParquetRead: an enabled engine needs a bucket, and a padded bucket
// is refused whether or not DuckDB is enabled. Config.Validate delegates to
// it and wraps the error with context.
func TestValidateDuckDBConfig_ParquetRead(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		duck      DuckDBConfig
		wantField string
	}{
		"disabled without a bucket":  {DuckDBConfig{}, ""},
		"enabled with a bucket":      {DuckDBConfig{Enabled: true, S3Bucket: "forma-lake"}, ""},
		"enabled without a bucket":   {DuckDBConfig{Enabled: true}, "duckdb.s3Bucket"},
		"padded bucket":              {DuckDBConfig{S3Bucket: "forma-lake\n"}, "duckdb.s3Bucket"},
		"padded bucket when enabled": {DuckDBConfig{Enabled: true, S3Bucket: " forma-lake"}, "duckdb.s3Bucket"},
	} {
		err := tc.duck.ValidateParquetRead()
		if tc.wantField == "" {
			if err != nil {
				t.Errorf("%s: unexpected error %v", name, err)
			}
			continue
		}
		var configErr *ConfigError
		if !errors.As(err, &configErr) || configErr.Field != tc.wantField {
			t.Errorf("%s: expected a ConfigError on %s, got %T (%v)", name, tc.wantField, err, err)
		}
	}

	config := DefaultConfig(NewMockSchemaRegistry())
	config.DuckDB.Enabled = true
	err := config.Validate()
	var configErr *ConfigError
	if !errors.As(err, &configErr) || configErr.Field != "duckdb.s3Bucket" {
		t.Fatalf("Validate must refuse an enabled engine without a bucket, got %T (%v)", err, err)
	}
	if !strings.Contains(err.Error(), "invalid duckdb parquet read configuration") {
		t.Errorf("Expected the delegated error to be wrapped with context, got %q", err.Error())
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

// TestValidateParquetPrefix pins the shared rule the writer tools apply to
// their prefix flags: the field it is given names the setting.
func TestValidateParquetPrefix(t *testing.T) {
	t.Parallel()

	for _, prefix := range []string{"", "data", "delta/v2/", "base_2026-11"} {
		if err := ValidateParquetPrefix("s3-prefix", prefix); err != nil {
			t.Errorf("prefix %q: expected nil, got %v", prefix, err)
		}
	}
	for _, prefix := range []string{"data/*", "data/**", "data?", "data[1]", "a]b[c"} {
		err := ValidateParquetPrefix("s3-prefix", prefix)
		var configErr *ConfigError
		if !errors.As(err, &configErr) || configErr.Field != "s3-prefix" {
			t.Errorf("prefix %q: expected a ConfigError on s3-prefix, got %T (%v)", prefix, err, err)
		}
	}
}

package benchmark

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDefaultConfigValidate(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("DefaultConfig should validate: %v", err)
	}
}

func TestConfigValidateRejectsUnknownWorkload(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Workloads = []string{"does-not-exist"}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("Validate should reject unknown workloads")
	}
}

func TestConfigValidateAcceptsLiveMode(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Mode = ExecutionModeLive
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate should accept live mode: %v", err)
	}
}

func TestConfigValidateRejectsUnknownTierProfile(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TierProfile = "does-not-exist"
	if err := cfg.Validate(); err == nil {
		t.Fatalf("Validate should reject unknown tier profile")
	}
}

func TestConfigValidateRejectsNegativeDuckDBResources(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DuckDBThreads = -1
	if err := cfg.Validate(); err == nil {
		t.Fatalf("Validate should reject negative duckdb threads")
	}

	cfg = DefaultConfig()
	cfg.DuckDBMemoryLimitMB = -1
	if err := cfg.Validate(); err == nil {
		t.Fatalf("Validate should reject negative duckdb memory limit")
	}
}

// TestConfigJSONOmitsZeroDuckDBResources protects BenchmarkID continuity:
// BuildArtifactMetadata hashes the whole Config, so zero-valued resource
// overrides must not appear in the JSON encoding.
func TestConfigJSONOmitsZeroDuckDBResources(t *testing.T) {
	raw, err := json.Marshal(DefaultConfig())
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if strings.Contains(string(raw), "duckdb_threads") || strings.Contains(string(raw), "duckdb_memory_limit_mb") {
		t.Fatalf("zero-valued DuckDB resource overrides must be omitted from JSON, got: %s", raw)
	}
}

func TestDefaultWorkloadNamesIncludesSchemaScopedCoverage(t *testing.T) {
	names := DefaultWorkloadNames()
	foundCustomer := false
	foundSecurity := false
	for _, name := range names {
		if name == "customer-region-page" {
			foundCustomer = true
		}
		if name == "security-symbol-page" {
			foundSecurity = true
		}
	}
	if !foundCustomer || !foundSecurity {
		t.Fatalf("expected default workloads to include schema-scoped customer and security coverage: %v", names)
	}
}

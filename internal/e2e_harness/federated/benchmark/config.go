package benchmark

import "fmt"

// Scale identifies the dataset size preset for benchmark execution.
type Scale string

const (
	ScaleSmall  Scale = "small"
	ScaleMedium Scale = "medium"
	ScaleLarge  Scale = "large"
)

// Distribution identifies the synthetic data distribution used by the benchmark.
type Distribution string

const (
	DistributionUniform       Distribution = "uniform"
	DistributionZipf          Distribution = "zipf"
	DistributionTemporal      Distribution = "temporal"
	DistributionPartitionSkew Distribution = "partition-skew"
	DistributionHotspot       Distribution = "hotspot-overlap"
)

// ExecutionMode controls how far the scaffolded runner goes.
type ExecutionMode string

const (
	ExecutionModeSmoke ExecutionMode = "smoke"
	ExecutionModePlan  ExecutionMode = "plan"
	ExecutionModeLive  ExecutionMode = "live"
)

// Config describes a benchmark run.
type Config struct {
	Mode          ExecutionMode `json:"mode"`
	Scale         Scale         `json:"scale"`
	Distribution  Distribution  `json:"distribution"`
	Iterations    int           `json:"iterations"`
	Concurrency   int           `json:"concurrency"`
	PageSize      int           `json:"page_size"`
	Seed          int64         `json:"seed"`
	TradeCount    int           `json:"trade_count,omitempty"`
	CustomerCount int           `json:"customer_count,omitempty"`
	SecurityCount int           `json:"security_count,omitempty"`
	OverlapRatio  float64       `json:"overlap_ratio,omitempty"`
	DeleteRatio   float64       `json:"delete_ratio,omitempty"`
	TierProfile   string        `json:"tier_profile,omitempty"`
	Workloads     []string      `json:"workloads"`
	// DuckDBThreads / DuckDBMemoryLimitMB override the harness DuckDB
	// resources for live runs (0 = harness default). omitempty keeps the
	// BenchmarkID hash of existing artifacts stable (BuildArtifactMetadata
	// hashes the whole Config).
	DuckDBThreads       int `json:"duckdb_threads,omitempty"`
	DuckDBMemoryLimitMB int `json:"duckdb_memory_limit_mb,omitempty"`
	// TruthPassSampleCap bounds truth-pass oracle construction: when > 0 and
	// a workload's candidate set exceeds the cap, only a seeded deterministic
	// sample of candidates is verified through the engine (spot check) and
	// the expected result is taken from the full reconstructed candidate set.
	// 0 = full truth pass (existing behavior). omitempty keeps the
	// BenchmarkID hash of existing artifacts stable.
	TruthPassSampleCap int `json:"truth_pass_sample_cap,omitempty"`
}

// DefaultConfig returns the benchmark scaffold defaults.
func DefaultConfig() Config {
	return Config{
		Mode:         ExecutionModeSmoke,
		Scale:        ScaleSmall,
		Distribution: DistributionUniform,
		Iterations:   1,
		Concurrency:  1,
		PageSize:     20,
		Seed:         42,
		TierProfile:  DefaultTierMixProfile().Name,
		Workloads:    DefaultWorkloadNames(),
	}
}

// WithDefaults applies defaults to omitted fields.
func (c Config) WithDefaults() Config {
	defaults := DefaultConfig()
	if c.Mode == "" {
		c.Mode = defaults.Mode
	}
	if c.Scale == "" {
		c.Scale = defaults.Scale
	}
	if c.Distribution == "" {
		c.Distribution = defaults.Distribution
	}
	if c.Iterations <= 0 {
		c.Iterations = defaults.Iterations
	}
	if c.Concurrency <= 0 {
		c.Concurrency = defaults.Concurrency
	}
	if c.PageSize <= 0 {
		c.PageSize = defaults.PageSize
	}
	if c.Seed == 0 {
		c.Seed = defaults.Seed
	}
	if c.TierProfile == "" {
		c.TierProfile = defaults.TierProfile
	}
	if len(c.Workloads) == 0 {
		c.Workloads = defaults.Workloads
	}
	return c
}

// Validate ensures the configuration is internally consistent.
func (c Config) Validate() error {
	if !isValidMode(c.Mode) {
		return fmt.Errorf("invalid mode %q", c.Mode)
	}
	if !isValidScale(c.Scale) {
		return fmt.Errorf("invalid scale %q", c.Scale)
	}
	if !isValidDistribution(c.Distribution) {
		return fmt.Errorf("invalid distribution %q", c.Distribution)
	}
	if c.Iterations <= 0 {
		return fmt.Errorf("iterations must be greater than zero")
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be greater than zero")
	}
	if c.PageSize <= 0 {
		return fmt.Errorf("page size must be greater than zero")
	}
	if c.DuckDBThreads < 0 {
		return fmt.Errorf("duckdb threads must be greater than or equal to zero")
	}
	if c.DuckDBMemoryLimitMB < 0 {
		return fmt.Errorf("duckdb memory limit must be greater than or equal to zero")
	}
	if c.TruthPassSampleCap < 0 {
		return fmt.Errorf("truth-pass sample cap must be greater than or equal to zero")
	}
	if c.TierProfile != "" {
		if _, err := ResolveTierMixProfile(c.TierProfile); err != nil {
			return err
		}
	}
	if _, err := ResolveWorkloads(c.Workloads); err != nil {
		return err
	}
	return nil
}

func isValidMode(mode ExecutionMode) bool {
	switch mode {
	case ExecutionModeSmoke, ExecutionModePlan, ExecutionModeLive:
		return true
	default:
		return false
	}
}

func isValidScale(scale Scale) bool {
	switch scale {
	case ScaleSmall, ScaleMedium, ScaleLarge:
		return true
	default:
		return false
	}
}

func isValidDistribution(dist Distribution) bool {
	switch dist {
	case DistributionUniform, DistributionZipf, DistributionTemporal, DistributionPartitionSkew, DistributionHotspot:
		return true
	default:
		return false
	}
}

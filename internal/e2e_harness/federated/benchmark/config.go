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
	Workloads     []string      `json:"workloads"`
}

// DefaultConfig returns the phase-1 benchmark scaffold defaults.
func DefaultConfig() Config {
	return Config{
		Mode:         ExecutionModeSmoke,
		Scale:        ScaleSmall,
		Distribution: DistributionUniform,
		Iterations:   1,
		Concurrency:  1,
		PageSize:     20,
		Seed:         42,
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
	if _, err := ResolveWorkloads(c.Workloads); err != nil {
		return err
	}
	return nil
}

func isValidMode(mode ExecutionMode) bool {
	switch mode {
	case ExecutionModeSmoke, ExecutionModePlan:
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

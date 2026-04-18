package benchmark

import (
	"context"
	"fmt"
	"time"

	forma "github.com/lychee-technology/forma"
)

// RunResult captures the phase-1 scaffold output.
type RunResult struct {
	Config         Config               `json:"config"`
	Generator      GeneratorConfig      `json:"generator"`
	StartedAt      time.Time            `json:"started_at"`
	CompletedAt    time.Time            `json:"completed_at"`
	ValidationOnly bool                 `json:"validation_only"`
	Schemas        []SchemaFixture      `json:"schemas"`
	Workloads      []WorkloadDefinition `json:"workloads"`
	Notes          []string             `json:"notes"`
}

// Runner validates benchmark inputs and prepares the phase-1 execution plan.
type Runner struct {
	config    Config
	registry  forma.SchemaRegistry
	schemas   []SchemaFixture
	workloads []WorkloadDefinition
	genConfig GeneratorConfig
}

// NewRunner builds a runner using the benchmark fixture registry.
func NewRunner(cfg Config) (*Runner, error) {
	resolved := cfg.WithDefaults()
	if err := resolved.Validate(); err != nil {
		return nil, err
	}
	registry, err := LoadFixtureRegistry()
	if err != nil {
		return nil, err
	}
	workloads, err := ResolveWorkloads(resolved.Workloads)
	if err != nil {
		return nil, err
	}
	return &Runner{
		config:    resolved,
		registry:  registry,
		schemas:   DefaultSchemaFixtures(),
		workloads: workloads,
		genConfig: GeneratorConfigFromBenchmark(resolved),
	}, nil
}

// RegisterSchemas loads the benchmark schema entries into a harness-backed registry table.
func (r *Runner) RegisterSchemas(registrar SchemaRegistrar) error {
	return RegisterFixtureSchemas(registrar)
}

// Run executes the scaffolded benchmark flow.
func (r *Runner) Run(ctx context.Context) (*RunResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	startedAt := time.Now()
	if err := r.validateFixtures(); err != nil {
		return nil, err
	}
	result := &RunResult{
		Config:         r.config,
		Generator:      r.genConfig,
		StartedAt:      startedAt,
		ValidationOnly: true,
		Schemas:        append([]SchemaFixture(nil), r.schemas...),
		Workloads:      append([]WorkloadDefinition(nil), r.workloads...),
	}
	switch r.config.Mode {
	case ExecutionModeSmoke:
		result.Notes = []string{
			"validated benchmark configuration",
			"loaded TPC-E-inspired schema fixtures",
			"resolved workload matrix",
			fmt.Sprintf("prepared generator for scale=%s distribution=%s", r.genConfig.Scale, r.genConfig.Distribution),
			"phase-1 scaffold stops before query execution",
		}
	case ExecutionModePlan:
		result.Notes = []string{
			"validated benchmark configuration",
			"loaded TPC-E-inspired schema fixtures",
			fmt.Sprintf("prepared generator for scale=%s distribution=%s", r.genConfig.Scale, r.genConfig.Distribution),
			"built execution plan only",
		}
	default:
		return nil, fmt.Errorf("unsupported execution mode %q", r.config.Mode)
	}
	result.CompletedAt = time.Now()
	return result, nil
}

func (r *Runner) validateFixtures() error {
	for _, fixture := range r.schemas {
		id, _, err := r.registry.GetSchemaAttributeCacheByName(fixture.Name)
		if err != nil {
			return fmt.Errorf("validate fixture %s: %w", fixture.Name, err)
		}
		if id != fixture.ID {
			return fmt.Errorf("fixture %s expected schema ID %d, got %d", fixture.Name, fixture.ID, id)
		}
		if _, _, err := r.registry.GetSchemaByName(fixture.Name); err != nil {
			return fmt.Errorf("load JSON schema for %s: %w", fixture.Name, err)
		}
	}
	return nil
}

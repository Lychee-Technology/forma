package benchmark

import (
	"context"
	"fmt"
	"time"

	forma "github.com/lychee-technology/forma"
	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
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
	Executions     []WorkloadRunResult  `json:"executions,omitempty"`
	Notes          []string             `json:"notes"`
}

// WorkloadRunResult captures one workload execution result.
type WorkloadRunResult struct {
	Name         string        `json:"name"`
	Category     string        `json:"category"`
	Distribution Distribution  `json:"distribution"`
	PageSize     int           `json:"page_size"`
	PageNumber   int           `json:"page_number"`
	Offset       int           `json:"offset"`
	ResultCount  int           `json:"result_count"`
	TotalRecords int64         `json:"total_records"`
	Duration     time.Duration `json:"duration"`
	PlanNotes    []string      `json:"plan_notes,omitempty"`
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

// RunWithHarness executes supported benchmark workloads against a live federated harness.
func (r *Runner) RunWithHarness(ctx context.Context, h *federated.FederatedTestHarness, profile TierMixProfile) (*RunResult, error) {
	if h == nil {
		return nil, fmt.Errorf("federated harness cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	startedAt := time.Now()
	if err := r.validateFixtures(); err != nil {
		return nil, err
	}
	generator, err := NewGenerator(r.genConfig)
	if err != nil {
		return nil, err
	}
	dataset, err := generator.Generate()
	if err != nil {
		return nil, err
	}
	tiered, err := SplitIntoTiers(dataset, profile)
	if err != nil {
		return nil, err
	}
	if err := LoadTieredDataset(ctx, h, tiered); err != nil {
		return nil, err
	}
	executions := make([]WorkloadRunResult, 0, len(r.workloads)*r.config.Iterations)
	for _, workload := range r.workloads {
		if !workload.SupportsDistribution(r.genConfig.Distribution) {
			continue
		}
		for iteration := 0; iteration < r.config.Iterations; iteration++ {
			run, err := r.executeWorkload(ctx, h, workload)
			if err != nil {
				return nil, fmt.Errorf("execute workload %s: %w", workload.Name, err)
			}
			executions = append(executions, run)
		}
	}
	result := &RunResult{
		Config:         r.config,
		Generator:      r.genConfig,
		StartedAt:      startedAt,
		CompletedAt:    time.Now(),
		ValidationOnly: false,
		Schemas:        append([]SchemaFixture(nil), r.schemas...),
		Workloads:      append([]WorkloadDefinition(nil), r.workloads...),
		Executions:     executions,
		Notes: []string{
			"loaded TPC-E-inspired schema fixtures",
			fmt.Sprintf("generated dataset with distribution=%s", r.genConfig.Distribution),
			fmt.Sprintf("loaded tiered dataset profile=%s", profile.Name),
			"executed supported federated query workloads",
		},
	}
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

func (r *Runner) executeWorkload(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition) (WorkloadRunResult, error) {
	pageSize := workload.PageSize
	if pageSize <= 0 {
		pageSize = r.config.PageSize
	}
	opts := &federated.QueryOptions{Limit: pageSize, Offset: workload.DerivedOffset(r.config.PageSize)}
	if workload.UsesSimpleFilter() {
		opts.Filter = &federated.Filter{Conditions: map[string]any{workload.FilterAttribute: workload.FilterValue}}
	}
	result, err := h.ExecuteFederatedQuery(ctx, opts)
	if err != nil {
		return WorkloadRunResult{}, err
	}
	run := WorkloadRunResult{
		Name:         workload.Name,
		Category:     string(workload.Category),
		Distribution: r.genConfig.Distribution,
		PageSize:     pageSize,
		PageNumber:   workload.PageNumber,
		Offset:       opts.Offset,
		ResultCount:  len(result.Records),
		TotalRecords: result.TotalRecords,
		Duration:     result.Duration,
	}
	if result.Plan != nil {
		run.PlanNotes = append([]string(nil), result.Plan.Notes...)
	}
	return run, nil
}

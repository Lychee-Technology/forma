package benchmark

import (
	"context"
	"fmt"
	"time"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
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
	Name         string            `json:"name"`
	Category     string            `json:"category"`
	Distribution Distribution      `json:"distribution"`
	PageSize     int               `json:"page_size"`
	PageNumber   int               `json:"page_number"`
	Offset       int               `json:"offset"`
	ResultCount  int               `json:"result_count"`
	TotalRecords int64             `json:"total_records"`
	Duration     time.Duration     `json:"duration"`
	RowIDs       []string          `json:"row_ids,omitempty"`
	Assertions   []AssertionResult `json:"assertions,omitempty"`
	PlanNotes    []string          `json:"plan_notes,omitempty"`
}

// AssertionResult captures one correctness assertion outcome.
type AssertionResult struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Message string `json:"message,omitempty"`
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
	pageRuns := make(map[string]WorkloadRunResult)
	for _, workload := range r.workloads {
		if !workload.SupportsDistribution(r.genConfig.Distribution) {
			continue
		}
		for iteration := 0; iteration < r.config.Iterations; iteration++ {
			run, records, err := r.executeWorkload(ctx, h, workload)
			if err != nil {
				return nil, fmt.Errorf("execute workload %s: %w", workload.Name, err)
			}
			run.Assertions = append(run.Assertions, validateBasicWorkloadAssertions(workload, run)...)
			run.Assertions = append(run.Assertions, validateResultLevelAssertions(workload, run, records)...)
			if workload.Category == WorkloadCategoryPagination || workload.Category == WorkloadCategoryDeepPage {
				if previous, ok := pageRuns[workload.TargetSchema]; ok {
					run.Assertions = append(run.Assertions, validatePaginationTransition(previous, run)...)
				}
				pageRuns[workload.TargetSchema] = run
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

func (r *Runner) executeWorkload(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition) (WorkloadRunResult, []*internal.PersistentRecord, error) {
	pageSize := workload.PageSize
	if pageSize <= 0 {
		pageSize = r.config.PageSize
	}
	opts := &federated.QueryOptions{Limit: pageSize, Offset: workload.DerivedOffset(r.config.PageSize), SortBy: "tradeTime", SortDesc: true}
	if workload.UsesSimpleFilter() {
		opts.Filter = &federated.Filter{Conditions: map[string]any{workload.FilterAttribute: workload.FilterValue}}
	}
	result, err := h.ExecuteFederatedQuery(ctx, opts)
	if err != nil {
		return WorkloadRunResult{}, nil, err
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
		RowIDs:       persistentRecordIDs(result.Records),
	}
	if result.Plan != nil {
		run.PlanNotes = append([]string(nil), result.Plan.Notes...)
	}
	return run, result.Records, nil
}

func validateBasicWorkloadAssertions(workload WorkloadDefinition, run WorkloadRunResult) []AssertionResult {
	assertions := []AssertionResult{
		{
			Name:    "non-negative-total-records",
			Passed:  run.TotalRecords >= 0,
			Message: fmt.Sprintf("total_records=%d", run.TotalRecords),
		},
		{
			Name:    "page-size-bound",
			Passed:  run.ResultCount <= run.PageSize,
			Message: fmt.Sprintf("result_count=%d page_size=%d", run.ResultCount, run.PageSize),
		},
	}
	if workload.Category == WorkloadCategoryDeepPage {
		assertions = append(assertions, AssertionResult{
			Name:    "deep-page-empty-when-offset-exceeds-total",
			Passed:  run.Offset < int(run.TotalRecords) || run.ResultCount == 0,
			Message: fmt.Sprintf("offset=%d total=%d result_count=%d", run.Offset, run.TotalRecords, run.ResultCount),
		})
	}
	return assertions
}

func validatePaginationTransition(previous, current WorkloadRunResult) []AssertionResult {
	assertions := []AssertionResult{{
		Name:    "non-decreasing-offsets-across-pagination-runs",
		Passed:  current.Offset >= previous.Offset,
		Message: fmt.Sprintf("previous_offset=%d current_offset=%d", previous.Offset, current.Offset),
	}}
	if current.Offset > previous.Offset {
		overlap := hasRowIDOverlap(previous.RowIDs, current.RowIDs)
		assertions = append(assertions, AssertionResult{
			Name:    "no-overlap-across-page-slices",
			Passed:  !overlap,
			Message: fmt.Sprintf("previous_rows=%d current_rows=%d", len(previous.RowIDs), len(current.RowIDs)),
		})
	}
	return assertions
}

func validateResultLevelAssertions(workload WorkloadDefinition, run WorkloadRunResult, records []*internal.PersistentRecord) []AssertionResult {
	assertions := []AssertionResult{validateUniqueRows(run)}
	if workload.UsesSimpleFilter() {
		assertions = append(assertions, validateFilterMatch(workload, records))
	}
	if len(records) > 1 {
		assertions = append(assertions, validateSortOrder(records, "tradeTime", true))
	}
	return assertions
}

func validateUniqueRows(run WorkloadRunResult) AssertionResult {
	seen := make(map[string]struct{}, len(run.RowIDs))
	for _, rowID := range run.RowIDs {
		if _, ok := seen[rowID]; ok {
			return AssertionResult{Name: "unique-row-ids-within-page", Passed: false, Message: rowID}
		}
		seen[rowID] = struct{}{}
	}
	return AssertionResult{Name: "unique-row-ids-within-page", Passed: true, Message: fmt.Sprintf("rows=%d", len(run.RowIDs))}
}

func validateFilterMatch(workload WorkloadDefinition, records []*internal.PersistentRecord) AssertionResult {
	for _, record := range records {
		if !recordMatchesFilter(record, workload.FilterAttribute, workload.FilterValue) {
			return AssertionResult{Name: "filter-results-match-request", Passed: false, Message: fmt.Sprintf("attribute=%s expected=%s row=%s", workload.FilterAttribute, workload.FilterValue, record.RowID)}
		}
	}
	return AssertionResult{Name: "filter-results-match-request", Passed: true, Message: fmt.Sprintf("attribute=%s expected=%s", workload.FilterAttribute, workload.FilterValue)}
}

func validateSortOrder(records []*internal.PersistentRecord, attribute string, desc bool) AssertionResult {
	values := make([]int64, 0, len(records))
	for _, record := range records {
		value, ok := record.Int64Items[attribute]
		if !ok {
			continue
		}
		values = append(values, value)
	}
	if len(values) <= 1 {
		return AssertionResult{Name: "sorted-by-tradeTime-desc", Passed: true, Message: "insufficient comparable rows"}
	}
	if desc {
		for i := 1; i < len(values); i++ {
			if values[i] > values[i-1] {
				return AssertionResult{Name: "sorted-by-tradeTime-desc", Passed: false, Message: fmt.Sprintf("index=%d prev=%d curr=%d", i, values[i-1], values[i])}
			}
		}
	}
	return AssertionResult{Name: "sorted-by-tradeTime-desc", Passed: true, Message: fmt.Sprintf("comparable_rows=%d", len(values))}
}

func recordMatchesFilter(record *internal.PersistentRecord, attribute, expected string) bool {
	switch attribute {
	case "symbol", "exchange", "region", "name":
		return record.TextItems[attribute] == expected
	case "tradeType":
		return fmt.Sprintf("%d", record.Int64Items[attribute]) == expected
	default:
		return true
	}
}

func persistentRecordIDs(records []*internal.PersistentRecord) []string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.RowID.String())
	}
	return ids
}

func hasRowIDOverlap(a, b []string) bool {
	seen := make(map[string]struct{}, len(a))
	for _, rowID := range a {
		seen[rowID] = struct{}{}
	}
	for _, rowID := range b {
		if _, ok := seen[rowID]; ok {
			return true
		}
	}
	return false
}

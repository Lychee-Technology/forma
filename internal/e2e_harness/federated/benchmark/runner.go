package benchmark

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"

	forma "github.com/lychee-technology/forma"
	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
)

// RunResult captures benchmark execution output.
type RunResult struct {
	Config         Config               `json:"config"`
	Generator      GeneratorConfig      `json:"generator"`
	Metadata       ArtifactMetadata     `json:"metadata"`
	Provenance     *RunProvenance       `json:"provenance,omitempty"`
	StartedAt      time.Time            `json:"started_at"`
	CompletedAt    time.Time            `json:"completed_at"`
	ValidationOnly bool                 `json:"validation_only"`
	Passed         bool                 `json:"passed"`
	FailureCount   int                  `json:"failure_count,omitempty"`
	PlanCacheHits  int64                `json:"plan_cache_hits"`
	PlanCacheMiss  int64                `json:"plan_cache_misses"`
	InfraError     string               `json:"infra_error,omitempty"`
	Schemas        []SchemaFixture      `json:"schemas"`
	Workloads      []WorkloadDefinition `json:"workloads"`
	Executions     []WorkloadRunResult  `json:"executions,omitempty"`
	Notes          []string             `json:"notes"`
	OracleModes    map[string]string    `json:"oracle_modes,omitempty"`
}

// WorkloadRunResult captures one workload execution result.
type WorkloadRunResult struct {
	Name           string            `json:"name"`
	Category       string            `json:"category"`
	Distribution   Distribution      `json:"distribution"`
	PageSize       int               `json:"page_size"`
	PageNumber     int               `json:"page_number"`
	Offset         int               `json:"offset"`
	PreferHot      bool              `json:"prefer_hot,omitempty"`
	WorkerID       int               `json:"worker_id,omitempty"`
	GroupID        int               `json:"group_id,omitempty"`
	ResultCount    int               `json:"result_count"`
	TotalRecords   int64             `json:"total_records"`
	Duration       time.Duration     `json:"duration"`
	Passed         bool              `json:"passed"`
	FailureKind    string            `json:"failure_kind,omitempty"`
	OracleMode     string            `json:"oracle_mode,omitempty"`
	FailureCount   int               `json:"failure_count,omitempty"`
	InfraError     string            `json:"infra_error,omitempty"`
	RowIDs         []string          `json:"row_ids,omitempty"`
	Assertions     []AssertionResult `json:"assertions,omitempty"`
	PlanNotes      []string          `json:"plan_notes,omitempty"`
	PerTier        *PerTierMetrics   `json:"per_tier,omitempty"`
	RouteEngine    string            `json:"route_engine,omitempty"`
	RouteReason    string            `json:"route_reason,omitempty"`
	PaginationMode string            `json:"pagination_mode,omitempty"`
}

// PerTierMetrics captures per-engine row counts and pushdown efficiency from the execution plan.
type PerTierMetrics struct {
	PGRows        int64   `json:"pg_rows"`
	DuckDBRows    int64   `json:"duckdb_rows"`
	FinalRows     int64   `json:"final_rows"`
	PushdownRatio float64 `json:"pushdown_ratio"`
	PGDirtyCount  int64   `json:"pg_dirty_count,omitempty"`
	HasPushdown   bool    `json:"has_pushdown"`
	Sources       int     `json:"sources"`
}

const (
	FailureKindInfra       = "infra"
	FailureKindCorrectness = "correctness"
)

// AssertionResult captures one correctness assertion outcome.
type AssertionResult struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Message string `json:"message,omitempty"`
}

// Runner validates benchmark inputs and prepares execution plans.
type Runner struct {
	config    Config
	registry  forma.SchemaRegistry
	schemas   []SchemaFixture
	workloads []WorkloadDefinition
	genConfig GeneratorConfig
	// planCache is shared across every repo/engine the runner constructs so
	// benchmark iterations exercise warm plan-cache hits like production
	// (#142 review finding 1); PlanCacheStats exposes the evidence.
	planCache *queryplan.Cache
}

// PlanCacheStats returns cumulative plan-cache hits and misses across all
// benchmark queries (benchmark artifact evidence for #142).
func (r *Runner) PlanCacheStats() (hits, misses int64) {
	return r.planCache.Stats()
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
		planCache: queryplan.NewCache(4096),
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
		Metadata:       BuildArtifactMetadata(r.config, r.genConfig, r.workloads),
		StartedAt:      startedAt,
		ValidationOnly: true,
		Passed:         true,
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
			"smoke mode stops before query execution",
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
func (r *Runner) RunWithHarness(ctx context.Context, h *federated.FederatedTestHarness, profile TierMixProfile) (*RunResult, error) { //nolint:funlen // #437: benchmark harness split
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
	h.Registry = r.registry
	if err := LoadTieredDataset(ctx, h, tiered); err != nil {
		return nil, err
	}
	executions := make([]WorkloadRunResult, 0, len(r.workloads)*r.config.Iterations*r.config.Concurrency)
	pageRuns := make(map[string]WorkloadRunResult)
	previousRuns := make(map[string]WorkloadRunResult)
	loadedRecords, hotKeys, err := buildLoadedStateSnapshot(ctx, h, tiered)
	if err != nil {
		return nil, fmt.Errorf("build loaded state snapshot: %w", err)
	}
	expectedByWorkload, oracleModes, oracleNotes, err := r.buildExpectedResults(ctx, h, loadedRecords, hotKeys)
	if err != nil {
		return nil, err
	}
	passed := true
	failureCount := 0
	concurrency := r.config.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	type concurrentResult struct {
		run     WorkloadRunResult
		records []*model.PersistentRecord
	}
	for _, workload := range r.workloads {
		if !workload.SupportsDistribution(r.genConfig.Distribution) {
			continue
		}
		for iteration := 0; iteration < r.config.Iterations; iteration++ {
			var wg sync.WaitGroup
			barrier := make(chan struct{})
			resultsChan := make(chan concurrentResult, concurrency)

			for workerID := 0; workerID < concurrency; workerID++ {
				wg.Add(1)
				go func(wid, iter int) {
					defer wg.Done()
					<-barrier

					run, records, err := r.executeWorkloadWithRetry(ctx, h, workload)
					if err != nil {
						run = failedWorkloadRunResult(workload, r.genConfig.Distribution, r.config.PageSize, fmt.Sprintf("execute workload: %v", err))
					}
					run.WorkerID = wid
					run.GroupID = iter
					resultsChan <- concurrentResult{run: run, records: records}
				}(workerID, iteration)
			}

			close(barrier)
			wg.Wait()
			close(resultsChan)

			// Collect all batch results
			batchResults := make([]concurrentResult, 0, concurrency)
			for result := range resultsChan {
				batchResults = append(batchResults, result)
			}

			// Compute concurrency stability assertions across the batch
			batchRuns := make([]WorkloadRunResult, len(batchResults))
			for i, cr := range batchResults {
				batchRuns[i] = cr.run
			}
			concurrencyAssertions := validateConcurrentRunStability(batchRuns)
			routeAssertions := validateConcurrentRouteStability(batchRuns)

			for i, result := range batchResults {
				run := result.run
				semantics := semanticsForWorkload(workload, r.genConfig)
				run.Assertions = append(run.Assertions, validateBasicWorkloadAssertions(workload, run)...)
				run.Assertions = append(run.Assertions, validateResultLevelAssertions(workload, run, result.records, semantics)...)
				run.OracleMode = resolveRunOracleMode(oracleModes, workload)
				if expected, ok := expectedByWorkload[workload.Name]; ok {
					run.Assertions = append(run.Assertions, validateExpectedWorkloadOutcome(run, expected)...)
				}
				if workload.Category == WorkloadCategoryPagination || workload.Category == WorkloadCategoryDeepPage {
					if previous, ok := pageRuns[workload.TargetSchema]; ok {
						run.Assertions = append(run.Assertions, validatePaginationTransition(previous, run)...)
					}
					pageRuns[workload.TargetSchema] = run
				}
				if previous, ok := previousRuns[workload.Name]; ok {
					run.Assertions = append(run.Assertions, validateRepeatedRunStability(previous, run)...)
				}
				previousRuns[workload.Name] = run
				if workload.Category == WorkloadCategoryPushdown {
					run.Assertions = append(run.Assertions, validatePushdownAssertions(run)...)
				}
				// Attach concurrency stability assertions to the first run (representative)
				if i == 0 {
					run.Assertions = append(run.Assertions, concurrencyAssertions...)
					run.Assertions = append(run.Assertions, routeAssertions...)
				}
				run.FailureCount = countFailedAssertions(run.Assertions)
				run.FailureKind = failureKindForRun(run)
				run.Passed = run.FailureCount == 0 && run.InfraError == ""
				if !run.Passed {
					passed = false
					failureCount += maxInt(1, run.FailureCount)
				}
				executions = append(executions, run)
			}
		}
	}
	planCacheHits, planCacheMisses := r.PlanCacheStats()
	result := &RunResult{
		Config:         r.config,
		Generator:      r.genConfig,
		Metadata:       BuildArtifactMetadata(r.config, r.genConfig, r.workloads),
		StartedAt:      startedAt,
		CompletedAt:    time.Now(),
		ValidationOnly: false,
		Passed:         passed,
		PlanCacheHits:  planCacheHits,
		PlanCacheMiss:  planCacheMisses,
		FailureCount:   failureCount,
		Schemas:        append([]SchemaFixture(nil), r.schemas...),
		Workloads:      append([]WorkloadDefinition(nil), r.workloads...),
		Executions:     executions,
		OracleModes:    oracleModes,
	}
	notes := []string{
		"loaded TPC-E-inspired schema fixtures",
		fmt.Sprintf("generated dataset with distribution=%s", r.genConfig.Distribution),
		fmt.Sprintf("loaded tiered dataset profile=%s", profile.Name),
		fmt.Sprintf("loaded-state snapshot rows=%d", len(loadedRecords)),
	}
	notes = append(notes, oracleNotes...)
	notes = append(notes,
		"prefer_hot expresses workload intent and report provenance, not hard execution routing",
		"executed supported federated query workloads",
	)
	result.Notes = notes
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

var maxInfraRetries = 2

var retryBackoffDelay = func(attempt int) time.Duration {
	return time.Duration(attempt) * time.Second
}

func executeWorkloadWithRetry(ctx context.Context, execute func(context.Context) (WorkloadRunResult, []*model.PersistentRecord, error)) (WorkloadRunResult, []*model.PersistentRecord, error) {
	var lastErr error
	for attempt := 0; attempt <= maxInfraRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return WorkloadRunResult{}, nil, ctx.Err()
			case <-time.After(retryBackoffDelay(attempt)):
			}
		}
		run, records, err := execute(ctx)
		if err == nil {
			return run, records, nil
		}
		lastErr = err
	}
	return WorkloadRunResult{}, nil, lastErr
}

func (r *Runner) executeWorkloadWithRetry(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition) (WorkloadRunResult, []*model.PersistentRecord, error) {
	return executeWorkloadWithRetry(ctx, func(ctx context.Context) (WorkloadRunResult, []*model.PersistentRecord, error) {
		return r.executeWorkload(ctx, h, workload)
	})
}

func (r *Runner) executeWorkload(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition) (WorkloadRunResult, []*model.PersistentRecord, error) {
	if workload.ExecutionSource == "service" {
		return r.executeServiceWorkload(ctx, h, workload)
	}

	pageSize := workload.PageSize
	if pageSize <= 0 {
		pageSize = r.config.PageSize
	}
	schemaID, err := workloadSchemaID(workload.TargetSchema)
	if err != nil {
		return WorkloadRunResult{}, nil, err
	}
	previousSchemaID := h.SchemaID
	h.SchemaID = schemaID
	defer func() {
		h.SchemaID = previousSchemaID
	}()
	opts := queryOptionsForWorkloadWithConfig(workload, r.config.PageSize, r.genConfig)
	opts.PreferHot = workloadExecutesPostgresOnly(workload)
	if conditions := workload.ResolvedFilterConditions(); len(conditions) > 0 {
		opts.Filter = &federated.Filter{Conditions: conditions}
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
		PreferHot:    workload.PreferHot,
		ResultCount:  len(result.Records),
		TotalRecords: result.TotalRecords,
		Duration:     result.Duration,
		Passed:       true,
		RowIDs:       persistentRecordIDs(result.Records),
	}
	if result.Plan != nil {
		run.PlanNotes = append([]string(nil), result.Plan.Notes...)
	}
	if engine, reason := routeInfoFromPlanNotes(run.PlanNotes); engine != "" {
		run.RouteEngine = engine
		run.RouteReason = reason
	}
	if workload.PreferHot {
		run.PlanNotes = append(run.PlanNotes, "prefer_hot=true (intent/provenance only; no hard routing override yet)")
		if opts.PreferHot {
			run.PlanNotes = append(run.PlanNotes, "prefer_hot_execution=true (postgres-only override active for tier-mix workload)")
		}
	}
	return run, result.Records, nil
}

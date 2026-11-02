package benchmark

import (
	"context"
	"fmt"
	"sync"
	"time"

	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
	"github.com/lychee-technology/forma/internal/model"
)

// RunWithHarness executes supported benchmark workloads against a live federated harness.
//
// A run has three phases: load the generated dataset into the harness and
// build the oracle's expectations (loadDataset, buildExpectedResults); run
// each workload iteration as one concurrent batch (executeConcurrentBatch);
// then assert every run of the batch against the oracle and the runs before it
// (harnessRunState.recordBatch).
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
	loadedRecords, hotKeys, err := r.loadDataset(ctx, h, profile)
	if err != nil {
		return nil, err
	}
	expectedByWorkload, oracleModes, oracleNotes, err := r.buildExpectedResults(ctx, h, loadedRecords, hotKeys)
	if err != nil {
		return nil, err
	}
	concurrency := r.config.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	state := newHarnessRunState(expectedByWorkload, oracleModes, r.genConfig,
		len(r.workloads)*r.config.Iterations*r.config.Concurrency)
	for _, workload := range r.workloads {
		if !workload.SupportsDistribution(r.genConfig.Distribution) {
			continue
		}
		for iteration := 0; iteration < r.config.Iterations; iteration++ {
			state.recordBatch(workload, r.executeConcurrentBatch(ctx, h, workload, iteration, concurrency))
		}
	}
	result := r.harnessRunResult(startedAt, state)
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

// loadDataset generates the runner's dataset, splits it across tiers by
// profile, loads it into the harness, and returns the loaded-state snapshot
// the oracle reconstructs its expectations from.
func (r *Runner) loadDataset(ctx context.Context, h *federated.FederatedTestHarness, profile TierMixProfile) ([]GeneratedRecord, map[string]struct{}, error) {
	generator, err := NewGenerator(r.genConfig)
	if err != nil {
		return nil, nil, err
	}
	dataset, err := generator.Generate()
	if err != nil {
		return nil, nil, err
	}
	tiered, err := SplitIntoTiers(dataset, profile)
	if err != nil {
		return nil, nil, err
	}
	if err := h.UseRegistry(ctx, r.registry); err != nil {
		return nil, nil, fmt.Errorf("attach benchmark schema registry: %w", err)
	}
	if err := LoadTieredDataset(ctx, h, tiered); err != nil {
		return nil, nil, err
	}
	loadedRecords, hotKeys, err := buildLoadedStateSnapshot(ctx, h, tiered)
	if err != nil {
		return nil, nil, fmt.Errorf("build loaded state snapshot: %w", err)
	}
	return loadedRecords, hotKeys, nil
}

// batchResult is one worker's outcome within a concurrent batch.
type batchResult struct {
	run     WorkloadRunResult
	records []*model.PersistentRecord
}

// executeConcurrentBatch runs workload once on each of concurrency workers,
// released together by a barrier, and returns the results in completion
// order. A worker whose execution fails reports an infra-failed run rather
// than an error, so one bad worker does not discard the batch.
func (r *Runner) executeConcurrentBatch(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition, iteration, concurrency int) []batchResult {
	var wg sync.WaitGroup
	barrier := make(chan struct{})
	resultsChan := make(chan batchResult, concurrency)

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
			resultsChan <- batchResult{run: run, records: records}
		}(workerID, iteration)
	}

	close(barrier)
	wg.Wait()
	close(resultsChan)

	batch := make([]batchResult, 0, concurrency)
	for result := range resultsChan {
		batch = append(batch, result)
	}
	return batch
}

// harnessRunState accumulates the asserted runs of one RunWithHarness call,
// together with the cross-run history the pagination and repeated-run
// assertions compare against.
type harnessRunState struct {
	expectedByWorkload map[string]expectedWorkloadResult
	oracleModes        map[string]string
	genConfig          GeneratorConfig

	// pageRuns holds the latest pagination/deep-page run per target schema;
	// previousRuns the latest run per workload name.
	pageRuns     map[string]WorkloadRunResult
	previousRuns map[string]WorkloadRunResult

	executions   []WorkloadRunResult
	passed       bool
	failureCount int
}

func newHarnessRunState(expectedByWorkload map[string]expectedWorkloadResult, oracleModes map[string]string, genConfig GeneratorConfig, capacity int) *harnessRunState {
	return &harnessRunState{
		expectedByWorkload: expectedByWorkload,
		oracleModes:        oracleModes,
		genConfig:          genConfig,
		pageRuns:           make(map[string]WorkloadRunResult),
		previousRuns:       make(map[string]WorkloadRunResult),
		executions:         make([]WorkloadRunResult, 0, capacity),
		passed:             true,
	}
}

// recordBatch asserts every run of one concurrent batch and appends it to the
// executions. The batch-wide concurrency and route stability assertions are
// attached to the first run only, as its representative.
func (s *harnessRunState) recordBatch(workload WorkloadDefinition, batch []batchResult) {
	batchRuns := make([]WorkloadRunResult, len(batch))
	for i, result := range batch {
		batchRuns[i] = result.run
	}
	concurrencyAssertions := validateConcurrentRunStability(batchRuns)
	routeAssertions := validateConcurrentRouteStability(batchRuns)

	for i, result := range batch {
		run := s.assertRun(workload, result)
		if i == 0 {
			run.Assertions = append(run.Assertions, concurrencyAssertions...)
			run.Assertions = append(run.Assertions, routeAssertions...)
		}
		run.FailureCount = countFailedAssertions(run.Assertions)
		run.FailureKind = failureKindForRun(run)
		run.Passed = run.FailureCount == 0 && run.InfraError == ""
		if !run.Passed {
			s.passed = false
			s.failureCount += maxInt(1, run.FailureCount)
		}
		s.executions = append(s.executions, run)
	}
}

// assertRun attaches the per-run assertions to one run and advances the
// pagination and repeated-run history with it.
func (s *harnessRunState) assertRun(workload WorkloadDefinition, result batchResult) WorkloadRunResult {
	run := result.run
	semantics := semanticsForWorkload(workload, s.genConfig)
	run.Assertions = append(run.Assertions, validateBasicWorkloadAssertions(workload, run)...)
	run.Assertions = append(run.Assertions, validateResultLevelAssertions(workload, run, result.records, semantics)...)
	run.OracleMode = resolveRunOracleMode(s.oracleModes, workload)
	if expected, ok := s.expectedByWorkload[workload.Name]; ok {
		run.Assertions = append(run.Assertions, validateExpectedWorkloadOutcome(run, expected)...)
	}
	if workload.Category == WorkloadCategoryPagination || workload.Category == WorkloadCategoryDeepPage {
		if previous, ok := s.pageRuns[workload.TargetSchema]; ok {
			run.Assertions = append(run.Assertions, validatePaginationTransition(previous, run)...)
		}
		s.pageRuns[workload.TargetSchema] = run
	}
	if previous, ok := s.previousRuns[workload.Name]; ok {
		run.Assertions = append(run.Assertions, validateRepeatedRunStability(previous, run)...)
	}
	s.previousRuns[workload.Name] = run
	if workload.Category == WorkloadCategoryPushdown {
		run.Assertions = append(run.Assertions, validatePushdownAssertions(run)...)
	}
	return run
}

// harnessRunResult assembles the RunResult of a harness run from its state;
// the caller adds the notes.
func (r *Runner) harnessRunResult(startedAt time.Time, state *harnessRunState) *RunResult {
	planCacheHits, planCacheMisses := r.PlanCacheStats()
	return &RunResult{
		Config:         r.config,
		Generator:      r.genConfig,
		Metadata:       BuildArtifactMetadata(r.config, r.genConfig, r.workloads),
		StartedAt:      startedAt,
		CompletedAt:    time.Now(),
		ValidationOnly: false,
		Passed:         state.passed,
		PlanCacheHits:  planCacheHits,
		PlanCacheMiss:  planCacheMisses,
		FailureCount:   state.failureCount,
		Schemas:        append([]SchemaFixture(nil), r.schemas...),
		Workloads:      append([]WorkloadDefinition(nil), r.workloads...),
		Executions:     state.executions,
		OracleModes:    state.oracleModes,
	}
}

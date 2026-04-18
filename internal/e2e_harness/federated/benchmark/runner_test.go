package benchmark

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal"
)

func TestRunnerRunSmoke(t *testing.T) {
	runner, err := NewRunner(DefaultConfig())
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}
	result, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if !result.ValidationOnly {
		t.Fatalf("expected validation-only result")
	}
	if len(result.Workloads) == 0 {
		t.Fatalf("expected resolved workloads")
	}
	if len(result.Schemas) == 0 {
		t.Fatalf("expected loaded schemas")
	}
	if !result.Passed {
		t.Fatalf("expected smoke result to pass")
	}
}

func TestValidateBasicWorkloadAssertionsDetectsUnexpectedEmptyPage(t *testing.T) {
	workload := WorkloadDefinition{Name: "baseline-page-100", Category: WorkloadCategoryPagination, PageNumber: 100, PageSize: 20}
	run := WorkloadRunResult{PageSize: 20, Offset: 40, ResultCount: 0, TotalRecords: 100}
	assertions := validateBasicWorkloadAssertions(workload, run)
	if assertionPassed(assertions, "empty-page-only-when-offset-reaches-total") {
		t.Fatalf("expected empty-page-only-when-offset-reaches-total to fail")
	}
}

func TestValidateBasicWorkloadAssertionsAllowsEmptyDeepPagePastTotal(t *testing.T) {
	workload := WorkloadDefinition{Name: "deep-page-1000", Category: WorkloadCategoryDeepPage, PageNumber: 1000, PageSize: 20}
	run := WorkloadRunResult{PageSize: 20, Offset: 20000, ResultCount: 0, TotalRecords: 50}
	assertions := validateBasicWorkloadAssertions(workload, run)
	if !assertionPassed(assertions, "deep-page-empty-when-offset-exceeds-total") {
		t.Fatalf("expected deep page assertion to pass when offset exceeds total")
	}
	if !assertionPassed(assertions, "empty-page-only-when-offset-reaches-total") {
		t.Fatalf("expected generic empty page assertion to pass when offset exceeds total")
	}
}

func TestValidateResultLevelAssertionsDetectsUnsortedRows(t *testing.T) {
	records := []*internal.PersistentRecord{
		{RowID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Int64Items: map[string]int64{"tradeTime": 10}},
		{RowID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), Int64Items: map[string]int64{"tradeTime": 20}},
	}
	assertions := validateResultLevelAssertions(WorkloadDefinition{Name: "baseline-page-1"}, WorkloadRunResult{RowIDs: []string{records[0].RowID.String(), records[1].RowID.String()}}, records)
	if assertionPassed(assertions, "sorted-by-tradeTime-desc") {
		t.Fatalf("expected sort assertion to fail for ascending rows")
	}
}

func TestQueryOptionsForWorkloadSkipsTradeOrderingForNonTradeSchema(t *testing.T) {
	customerOpts := queryOptionsForWorkload(WorkloadDefinition{Name: "customer-region-page", TargetSchema: "customer", PageSize: 20, PageNumber: 1}, 20)
	if customerOpts.SortBy != "" || customerOpts.SortDesc {
		t.Fatalf("expected customer workload to avoid trade ordering, got %+v", customerOpts)
	}
	tradeOpts := queryOptionsForWorkload(WorkloadDefinition{Name: "baseline-page-1", TargetSchema: "trade", PageSize: 20, PageNumber: 1}, 20)
	if tradeOpts.SortBy != "tradeTime" || !tradeOpts.SortDesc {
		t.Fatalf("expected trade workload to preserve trade ordering, got %+v", tradeOpts)
	}
}

func TestValidateSchemaScopeDetectsCrossSchemaRows(t *testing.T) {
	records := []*internal.PersistentRecord{{RowID: uuid.MustParse("00000000-0000-0000-0000-000000000010"), SchemaID: SchemaIDTrade}}
	assertion := validateSchemaScope(WorkloadDefinition{Name: "customer-region-page", TargetSchema: "customer"}, records)
	if assertion.Passed {
		t.Fatalf("expected schema scope assertion to fail for mismatched schema")
	}
}

func TestFailedWorkloadRunResultMarksInfraFailure(t *testing.T) {
	run := failedWorkloadRunResult(WorkloadDefinition{Name: "baseline-page-1", Category: WorkloadCategoryPagination, PageNumber: 1}, DistributionUniform, 20, "boom")
	if run.Passed {
		t.Fatalf("expected failed workload run to be marked failed")
	}
	if run.InfraError == "" {
		t.Fatalf("expected infra error to be recorded")
	}
	if run.FailureCount != 1 {
		t.Fatalf("expected failure count 1, got %d", run.FailureCount)
	}
}

func TestCountFailedAssertions(t *testing.T) {
	failed := countFailedAssertions([]AssertionResult{{Name: "a", Passed: true}, {Name: "b", Passed: false}, {Name: "c", Passed: false}})
	if failed != 2 {
		t.Fatalf("expected 2 failed assertions, got %d", failed)
	}
}

func assertionPassed(assertions []AssertionResult, name string) bool {
	for _, assertion := range assertions {
		if assertion.Name == name {
			return assertion.Passed
		}
	}
	return false
}

func TestSummarizedWorkloadResultCarriesPassState(t *testing.T) {
	result := &RunResult{
		Passed: false,
		Executions: []WorkloadRunResult{{
			Name:       "q1",
			Passed:     false,
			Duration:   10 * time.Millisecond,
			InfraError: "broken",
			Assertions: []AssertionResult{{Name: "a", Passed: false}},
		}},
	}
	summary := SummarizeRunResult(result)
	if summary.Passed {
		t.Fatalf("expected summary to reflect failed run")
	}
	if summary.InfraFailures != 1 {
		t.Fatalf("expected one infra failure, got %d", summary.InfraFailures)
	}
	if summary.FailureCount != 1 {
		t.Fatalf("expected one total failure, got %d", summary.FailureCount)
	}
}

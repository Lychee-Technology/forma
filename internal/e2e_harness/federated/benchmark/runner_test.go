package benchmark

import (
	"context"
	"strconv"
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
	assertions := validateResultLevelAssertions(WorkloadDefinition{Name: "baseline-page-1"}, WorkloadRunResult{RowIDs: []string{records[0].RowID.String(), records[1].RowID.String()}}, records, workloadSemantics{})
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

func TestQueryOptionsForWorkloadAddsMixedTierWindowTradeTimeRange(t *testing.T) {
	genCfg := GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionTemporal, TimeWindowDays: 30, BaseTime: defaultBaseTime}.WithDefaults()
	opts := queryOptionsForWorkloadWithConfig(WorkloadDefinition{Name: "mixed-tier-window", TargetSchema: "trade", PageSize: 50, PageNumber: 1}, 20, genCfg)
	if opts.TradeTimeStart <= 0 || opts.TradeTimeEnd <= 0 {
		t.Fatalf("expected mixed tier workload to set a trade time window, got %+v", opts)
	}
	if opts.TradeTimeStart >= opts.TradeTimeEnd {
		t.Fatalf("expected ascending trade time window, got %+v", opts)
	}
}

func TestQueryOptionsForWorkloadAddsHotAndColdWindowRanges(t *testing.T) {
	genCfg := GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionTemporal, TimeWindowDays: 30, BaseTime: defaultBaseTime}.WithDefaults()
	hotOpts := queryOptionsForWorkloadWithConfig(WorkloadDefinition{Name: "hot-only-window", TargetSchema: "trade", PageSize: 50, PageNumber: 1}, 20, genCfg)
	coldOpts := queryOptionsForWorkloadWithConfig(WorkloadDefinition{Name: "cold-only-window", TargetSchema: "trade", PageSize: 50, PageNumber: 1}, 20, genCfg)
	if hotOpts.TradeTimeStart <= 0 || hotOpts.TradeTimeEnd <= hotOpts.TradeTimeStart {
		t.Fatalf("expected hot-only window to set a valid range, got %+v", hotOpts)
	}
	if coldOpts.TradeTimeStart <= 0 || coldOpts.TradeTimeEnd <= coldOpts.TradeTimeStart {
		t.Fatalf("expected cold-only window to set a valid range, got %+v", coldOpts)
	}
	if coldOpts.TradeTimeEnd > hotOpts.TradeTimeStart {
		t.Fatalf("expected cold-only window to end before hot-only window starts, cold=%+v hot=%+v", coldOpts, hotOpts)
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
	if run.FailureKind != FailureKindInfra {
		t.Fatalf("expected infra failure kind, got %q", run.FailureKind)
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
			Name:        "q1",
			Passed:      false,
			Duration:    10 * time.Millisecond,
			InfraError:  "broken",
			FailureKind: FailureKindInfra,
			Assertions:  []AssertionResult{{Name: "a", Passed: false}},
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

func TestValidateExpectedWorkloadOutcomeDetectsMismatchedPageRows(t *testing.T) {
	run := WorkloadRunResult{RowIDs: []string{"row-1", "row-3"}, TotalRecords: 2}
	expected := expectedWorkloadResult{RowIDs: []string{"row-1", "row-2"}, TotalRecords: 2}
	assertions := validateExpectedWorkloadOutcome(run, expected)
	if assertionPassed(assertions, "page-row-ids-match-expected") {
		t.Fatalf("expected page-row-ids-match-expected to fail")
	}
	if !assertionPassed(assertions, "total-records-match-expected") {
		t.Fatalf("expected total-records-match-expected to pass")
	}
}

func TestBuildExpectedWorkloadResultsHonorsDeleteShadowing(t *testing.T) {
	rowVisible := deterministicRowID(1, "trade", 1)
	rowDeleted := deterministicRowID(1, "trade", 2)
	dataset := &GeneratedDataset{Records: []GeneratedRecord{
		{
			SchemaID:   SchemaIDTrade,
			SchemaName: "trade",
			RowID:      rowVisible,
			Version:    1,
			ChangedAt:  100,
			Attributes: map[string]any{"symbol": "SYM00001", "tradeTime": "2026-01-01T00:00:00Z"},
		},
		{
			SchemaID:   SchemaIDTrade,
			SchemaName: "trade",
			RowID:      rowDeleted,
			Version:    1,
			ChangedAt:  200,
			Attributes: map[string]any{"symbol": "SYM00002", "tradeTime": "2026-01-01T00:01:00Z"},
		},
		{
			SchemaID:   SchemaIDTrade,
			SchemaName: "trade",
			RowID:      rowDeleted,
			Version:    2,
			ChangedAt:  300,
			DeletedAt:  301,
			Attributes: map[string]any{"symbol": "SYM00002", "tradeTime": "2026-01-01T00:02:00Z"},
		},
	}}
	workloads := []WorkloadDefinition{{Name: "baseline-page-1", TargetSchema: "trade", PageSize: 20, PageNumber: 1}}
	results := buildExpectedWorkloadResults(dataset, workloads, 20)
	expected := results["baseline-page-1"]
	if expected.TotalRecords != 1 {
		t.Fatalf("expected one visible trade after delete shadowing, got %d", expected.TotalRecords)
	}
	if len(expected.RowIDs) != 1 || expected.RowIDs[0] != rowVisible.String() {
		t.Fatalf("unexpected visible row ids: %+v", expected.RowIDs)
	}
}

func TestBuildExpectedWorkloadResultsFromRecordsUsesLoadedTierState(t *testing.T) {
	rowID := deterministicRowID(1, "trade", 1)
	workloads := []WorkloadDefinition{{Name: "hot-selective-page", TargetSchema: "trade", FilterAttribute: "symbol", FilterValue: "SYM00001", PageSize: 20, PageNumber: 1}}
	original := []GeneratedRecord{
		{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: rowID, Version: 1, ChangedAt: 100, Attributes: map[string]any{"symbol": "SYM00001", "tradeTime": "2026-01-01T00:00:00Z"}},
		{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: rowID, Version: 2, ChangedAt: 200, Attributes: map[string]any{"symbol": "SYM99999", "tradeTime": "2026-01-01T00:01:00Z"}},
	}
	results := buildExpectedWorkloadResultsFromRecords([]GeneratedRecord{original[0]}, workloads, 20, DefaultGeneratorConfig())
	expected := results["hot-selective-page"]
	if expected.TotalRecords != 1 {
		t.Fatalf("expected loaded tier state to include visible row, got %+v", expected)
	}
	if len(expected.RowIDs) != 1 || expected.RowIDs[0] != rowID.String() {
		t.Fatalf("unexpected loaded-state row ids: %+v", expected.RowIDs)
	}
	fromDataset := buildExpectedWorkloadResults(&GeneratedDataset{Records: original}, workloads, 20)
	if fromDataset["hot-selective-page"].TotalRecords != 0 {
		t.Fatalf("expected original dataset semantics to exclude row after symbol change, got %+v", fromDataset["hot-selective-page"])
	}
}

func TestBuildExpectedWorkloadResultsFromRecordsExcludesDeletedHotSelectiveRows(t *testing.T) {
	rowID := deterministicRowID(2, "trade", 1)
	workloads := []WorkloadDefinition{{Name: "hot-selective-page", TargetSchema: "trade", FilterAttribute: "symbol", FilterValue: "SYM00001", PageSize: 20, PageNumber: 1}}
	records := []GeneratedRecord{
		{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: rowID, Version: 1, ChangedAt: 100, Attributes: map[string]any{"symbol": "SYM00001", "tradeTime": "2026-01-01T00:00:00Z"}},
		{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: rowID, Version: 2, ChangedAt: 200, DeletedAt: 205, Attributes: map[string]any{"symbol": "SYM00001", "tradeTime": "2026-01-01T00:01:00Z"}},
	}
	results := buildExpectedWorkloadResultsFromRecords(records, workloads, 20, DefaultGeneratorConfig())
	if results["hot-selective-page"].TotalRecords != 0 {
		t.Fatalf("expected deleted latest trade row to be excluded, got %+v", results["hot-selective-page"])
	}
}

func TestExpectedWorkloadResultsCanUseLoadedHotStateSnapshot(t *testing.T) {
	rowID := deterministicRowID(3, "trade", 1)
	workloads := []WorkloadDefinition{{Name: "hot-selective-page", TargetSchema: "trade", FilterAttribute: "symbol", FilterValue: "SYM00001", PageSize: 20, PageNumber: 1}}
	syntheticTiered := []GeneratedRecord{{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: rowID, Version: 2, ChangedAt: 200, Attributes: map[string]any{"symbol": "SYM00001", "tradeTime": "2026-01-01T00:01:00Z"}}}
	loadedHotSnapshot := []GeneratedRecord{{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: rowID, Version: 0, ChangedAt: 200, DeletedAt: 205, Attributes: map[string]any{"symbol": "SYM00001", "tradeTime": "1735689660000"}}}
	fromSynthetic := buildExpectedWorkloadResultsFromRecords(syntheticTiered, workloads, 20, DefaultGeneratorConfig())
	if fromSynthetic["hot-selective-page"].TotalRecords != 1 {
		t.Fatalf("expected synthetic tiered record to match filter, got %+v", fromSynthetic["hot-selective-page"])
	}
	fromLoaded := buildExpectedWorkloadResultsFromRecords(loadedHotSnapshot, workloads, 20, DefaultGeneratorConfig())
	if fromLoaded["hot-selective-page"].TotalRecords != 0 {
		t.Fatalf("expected loaded hot snapshot to exclude deleted row, got %+v", fromLoaded["hot-selective-page"])
	}
}

func TestBuildExpectedWorkloadResultsFromRecordsHonorsMixedTierWindow(t *testing.T) {
	genCfg := GeneratorConfig{Scale: ScaleSmall, Distribution: DistributionTemporal, TimeWindowDays: 30, BaseTime: defaultBaseTime}.WithDefaults()
	semantics := semanticsForWorkload(WorkloadDefinition{Name: "mixed-tier-window", TargetSchema: "trade"}, genCfg)
	inside := deterministicRowID(7, "trade", 1)
	before := deterministicRowID(7, "trade", 2)
	after := deterministicRowID(7, "trade", 3)
	workloads := []WorkloadDefinition{{Name: "mixed-tier-window", TargetSchema: "trade", PageSize: 50, PageNumber: 1}}
	records := []GeneratedRecord{
		{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: inside, Version: 1, ChangedAt: semantics.TradeTimeStart + 1000, Attributes: map[string]any{"tradeTime": strconv.FormatInt(semantics.TradeTimeStart+1000, 10)}},
		{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: before, Version: 1, ChangedAt: semantics.TradeTimeStart - 1000, Attributes: map[string]any{"tradeTime": strconv.FormatInt(semantics.TradeTimeStart-1000, 10)}},
		{SchemaID: SchemaIDTrade, SchemaName: "trade", RowID: after, Version: 1, ChangedAt: semantics.TradeTimeEnd + 1000, Attributes: map[string]any{"tradeTime": strconv.FormatInt(semantics.TradeTimeEnd+1000, 10)}},
	}
	results := buildExpectedWorkloadResultsFromRecords(records, workloads, 20, genCfg)
	expected := results["mixed-tier-window"]
	if expected.TotalRecords != 1 {
		t.Fatalf("expected one in-window record, got %+v", expected)
	}
	if len(expected.RowIDs) != 1 || expected.RowIDs[0] != inside.String() {
		t.Fatalf("unexpected in-window row ids: %+v", expected.RowIDs)
	}
}

func TestGeneratedRecordMatchesFilterForWorkloadSupportsMultipleConditions(t *testing.T) {
	workload := WorkloadDefinition{FilterConditions: map[string]any{"symbol": "SYM00001", "exchange": "NYSE"}}
	record := GeneratedRecord{SchemaName: "trade", Attributes: map[string]any{"symbol": "SYM00001", "exchange": "NYSE"}}
	if !generatedRecordMatchesFilterForWorkload(record, workload) {
		t.Fatalf("expected record to satisfy all filter conditions")
	}
	record.Attributes["exchange"] = "NASDAQ"
	if generatedRecordMatchesFilterForWorkload(record, workload) {
		t.Fatalf("expected record to fail when one filter condition does not match")
	}
}

func TestValidateFilterMatchSupportsMultipleConditions(t *testing.T) {
	workload := WorkloadDefinition{FilterConditions: map[string]any{"symbol": "SYM00001", "exchange": "NYSE"}}
	passing := []*internal.PersistentRecord{{TextItems: map[string]string{"symbol": "SYM00001", "exchange": "NYSE"}}}
	if assertion := validateFilterMatch(workload, passing); !assertion.Passed {
		t.Fatalf("expected multi-condition filter assertion to pass: %+v", assertion)
	}
	failing := []*internal.PersistentRecord{{TextItems: map[string]string{"symbol": "SYM00001", "exchange": "NASDAQ"}}}
	if assertion := validateFilterMatch(workload, failing); assertion.Passed {
		t.Fatalf("expected multi-condition filter assertion to fail")
	}
}

func TestValidateTradeTimeWindowDetectsOutOfWindowRows(t *testing.T) {
	semantics := workloadSemantics{TradeTimeStart: 100, TradeTimeEnd: 200}
	records := []*internal.PersistentRecord{{RowID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Int64Items: map[string]int64{"tradeTime": 99}}}
	assertion := validateTradeTimeWindow(records, semantics)
	if assertion.Passed {
		t.Fatalf("expected trade time window assertion to fail")
	}
}

func TestFailureKindForRunDistinguishesInfraAndCorrectness(t *testing.T) {
	if kind := failureKindForRun(WorkloadRunResult{InfraError: "boom"}); kind != FailureKindInfra {
		t.Fatalf("expected infra failure kind, got %q", kind)
	}
	if kind := failureKindForRun(WorkloadRunResult{Assertions: []AssertionResult{{Name: "a", Passed: false}}}); kind != FailureKindCorrectness {
		t.Fatalf("expected correctness failure kind, got %q", kind)
	}
	if kind := failureKindForRun(WorkloadRunResult{Assertions: []AssertionResult{{Name: "a", Passed: true}}}); kind != "" {
		t.Fatalf("expected empty failure kind for successful run, got %q", kind)
	}
}

func TestValidateRepeatedRunStabilityDetectsChangingRows(t *testing.T) {
	previous := WorkloadRunResult{FailureKind: "", TotalRecords: 5, RowIDs: []string{"a", "b"}}
	current := WorkloadRunResult{FailureKind: "", TotalRecords: 5, RowIDs: []string{"a", "c"}}
	assertions := validateRepeatedRunStability(previous, current)
	if assertionPassed(assertions, "repeated-run-page-row-ids-stable") {
		t.Fatalf("expected repeated-run-page-row-ids-stable to fail")
	}
	if !assertionPassed(assertions, "repeated-run-total-records-stable") {
		t.Fatalf("expected repeated-run-total-records-stable to pass")
	}
}

func TestWorkloadResolvedOracleModeDefaultsToLoadedState(t *testing.T) {
	if mode := (WorkloadDefinition{Name: "baseline-page-1"}).ResolvedOracleMode(); mode != OracleModeLoadedState {
		t.Fatalf("expected default oracle mode to be loaded-state, got %q", mode)
	}
	if mode := (WorkloadDefinition{Name: "generic-trade-filter", Category: WorkloadCategoryFilter, TargetSchema: "trade"}).ResolvedOracleMode(); mode != OracleModeTruthPass {
		t.Fatalf("expected trade filter workload to default to truth-pass oracle, got %q", mode)
	}
	if mode := (WorkloadDefinition{Name: "generic-customer-filter", Category: WorkloadCategoryFilter, TargetSchema: "customer"}).ResolvedOracleMode(); mode != OracleModeLoadedState {
		t.Fatalf("expected non-trade filter workload to default to loaded-state oracle, got %q", mode)
	}
	if mode := (WorkloadDefinition{Name: "hot-selective-page", OracleMode: OracleModeTruthPass}).ResolvedOracleMode(); mode != OracleModeTruthPass {
		t.Fatalf("expected explicit truth-pass oracle mode, got %q", mode)
	}
}

func TestDefaultWorkloadsMarkSelectiveWorkloadsAsTruthPass(t *testing.T) {
	resolved, err := ResolveWorkloads([]string{"hot-selective-page", "hot-low-selectivity-page", "eav-selective-page", "mixed-hot-eav-page", "baseline-page-1"})
	if err != nil {
		t.Fatalf("ResolveWorkloads failed: %v", err)
	}
	modes := map[string]OracleMode{}
	for _, workload := range resolved {
		modes[workload.Name] = workload.ResolvedOracleMode()
	}
	if modes["hot-selective-page"] != OracleModeTruthPass {
		t.Fatalf("expected hot-selective-page to use truth-pass oracle, got %q", modes["hot-selective-page"])
	}
	if modes["eav-selective-page"] != OracleModeTruthPass {
		t.Fatalf("expected eav-selective-page to use truth-pass oracle, got %q", modes["eav-selective-page"])
	}
	if modes["hot-low-selectivity-page"] != OracleModeTruthPass {
		t.Fatalf("expected hot-low-selectivity-page to use truth-pass oracle, got %q", modes["hot-low-selectivity-page"])
	}
	if modes["mixed-hot-eav-page"] != OracleModeTruthPass {
		t.Fatalf("expected mixed-hot-eav-page to use truth-pass oracle, got %q", modes["mixed-hot-eav-page"])
	}
	if modes["baseline-page-1"] != OracleModeLoadedState {
		t.Fatalf("expected baseline-page-1 to use loaded-state oracle, got %q", modes["baseline-page-1"])
	}
}

func TestDefaultSelectiveWorkloadsRelyOnInferredOracleMode(t *testing.T) {
	resolved, err := ResolveWorkloads([]string{"hot-selective-page", "hot-low-selectivity-page", "eav-selective-page", "mixed-hot-eav-page"})
	if err != nil {
		t.Fatalf("ResolveWorkloads failed: %v", err)
	}
	for _, workload := range resolved {
		if workload.OracleMode != "" {
			t.Fatalf("expected workload %s to rely on inferred oracle mode, got explicit mode %q", workload.Name, workload.OracleMode)
		}
		if mode := workload.ResolvedOracleMode(); mode != OracleModeTruthPass {
			t.Fatalf("expected workload %s to resolve to truth-pass, got %q", workload.Name, mode)
		}
	}
}

func TestInferredOracleModeUsesWorkloadClassInsteadOfName(t *testing.T) {
	cases := []struct {
		name     string
		workload WorkloadDefinition
		expect   OracleMode
	}{
		{
			name:     "trade filter defaults to truth pass",
			workload: WorkloadDefinition{Name: "future-trade-filter", Category: WorkloadCategoryFilter, TargetSchema: "trade", FilterConditions: map[string]any{"symbol": "SYM12345"}},
			expect:   OracleModeTruthPass,
		},
		{
			name:     "trade pagination defaults to loaded state",
			workload: WorkloadDefinition{Name: "future-trade-page", Category: WorkloadCategoryPagination, TargetSchema: "trade"},
			expect:   OracleModeLoadedState,
		},
		{
			name:     "non trade filter defaults to loaded state",
			workload: WorkloadDefinition{Name: "future-security-filter", Category: WorkloadCategoryFilter, TargetSchema: "security", FilterConditions: map[string]any{"symbol": "SYM12345"}},
			expect:   OracleModeLoadedState,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if mode := tc.workload.InferredOracleMode(); mode != tc.expect {
				t.Fatalf("expected inferred oracle mode %q, got %q", tc.expect, mode)
			}
			if mode := tc.workload.ResolvedOracleMode(); mode != tc.expect {
				t.Fatalf("expected resolved oracle mode %q, got %q", tc.expect, mode)
			}
		})
	}
}

func TestWorkloadResolvedFilterConditionsPrefersExplicitMap(t *testing.T) {
	workload := WorkloadDefinition{FilterAttribute: "symbol", FilterValue: "SYM00001", FilterConditions: map[string]any{"symbol": "SYM00002", "exchange": "NYSE"}}
	conditions := workload.ResolvedFilterConditions()
	if len(conditions) != 2 || conditions["symbol"] != "SYM00002" || conditions["exchange"] != "NYSE" {
		t.Fatalf("expected explicit filter conditions to win, got %+v", conditions)
	}
	simple := WorkloadDefinition{FilterAttribute: "symbol", FilterValue: "SYM00001"}
	conditions = simple.ResolvedFilterConditions()
	if len(conditions) != 1 || conditions["symbol"] != "SYM00001" {
		t.Fatalf("expected simple filter fallback, got %+v", conditions)
	}
}

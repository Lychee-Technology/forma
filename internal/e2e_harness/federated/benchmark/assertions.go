package benchmark

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
)

func validateExpectedWorkloadOutcome(run WorkloadRunResult, expected expectedWorkloadResult) []AssertionResult {
	assertions := []AssertionResult{{
		Name:    "total-records-match-expected",
		Passed:  run.TotalRecords == expected.TotalRecords,
		Message: fmt.Sprintf("actual=%d expected=%d", run.TotalRecords, expected.TotalRecords),
	}}
	actualRows := append([]string(nil), run.RowIDs...)
	expectedRows := append([]string(nil), expected.RowIDs...)
	assertions = append(assertions, AssertionResult{
		Name:    "page-row-ids-match-expected",
		Passed:  stringSlicesEqual(actualRows, expectedRows),
		Message: fmt.Sprintf("actual=%v expected=%v", actualRows, expectedRows),
	})
	return assertions
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func failureKindForRun(run WorkloadRunResult) string {
	if run.InfraError != "" {
		return FailureKindInfra
	}
	if countFailedAssertions(run.Assertions) > 0 {
		return FailureKindCorrectness
	}
	return ""
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
		{
			Name:    "result-count-within-total-records",
			Passed:  int64(run.ResultCount) <= run.TotalRecords,
			Message: fmt.Sprintf("result_count=%d total_records=%d", run.ResultCount, run.TotalRecords),
		},
		{
			Name:    "empty-page-only-when-offset-reaches-total",
			Passed:  run.ResultCount > 0 || run.Offset >= int(run.TotalRecords) || run.TotalRecords == 0,
			Message: fmt.Sprintf("offset=%d total=%d result_count=%d", run.Offset, run.TotalRecords, run.ResultCount),
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

func validateRepeatedRunStability(previous, current WorkloadRunResult) []AssertionResult {
	assertions := []AssertionResult{{
		Name:    "repeated-run-failure-kind-stable",
		Passed:  previous.FailureKind == current.FailureKind,
		Message: fmt.Sprintf("previous=%s current=%s", previous.FailureKind, current.FailureKind),
	}}
	assertions = append(assertions, AssertionResult{
		Name:    "repeated-run-total-records-stable",
		Passed:  previous.TotalRecords == current.TotalRecords,
		Message: fmt.Sprintf("previous=%d current=%d", previous.TotalRecords, current.TotalRecords),
	})
	assertions = append(assertions, AssertionResult{
		Name:    "repeated-run-page-row-ids-stable",
		Passed:  stringSlicesEqual(previous.RowIDs, current.RowIDs),
		Message: fmt.Sprintf("previous=%v current=%v", previous.RowIDs, current.RowIDs),
	})
	return assertions
}

func validateConcurrentRunStability(runs []WorkloadRunResult) []AssertionResult {
	if len(runs) < 2 {
		return nil
	}
	reference := runs[0]
	var assertions []AssertionResult
	allPassed := true
	for i := 1; i < len(runs); i++ {
		allPassed = allPassed && reference.TotalRecords == runs[i].TotalRecords
	}
	assertions = append(assertions, AssertionResult{
		Name:    "concurrent-run-total-records-stable",
		Passed:  allPassed,
		Message: fmt.Sprintf("reference=%d workers=%d", reference.TotalRecords, len(runs)),
	})

	allPassed = true
	for i := 1; i < len(runs); i++ {
		allPassed = allPassed && stringSlicesEqual(reference.RowIDs, runs[i].RowIDs)
	}
	assertions = append(assertions, AssertionResult{
		Name:    "concurrent-run-page-row-ids-stable",
		Passed:  allPassed,
		Message: fmt.Sprintf("reference_rows=%d workers=%d", len(reference.RowIDs), len(runs)),
	})

	allPassed = true
	for i := 1; i < len(runs); i++ {
		allPassed = allPassed && reference.FailureKind == runs[i].FailureKind
	}
	assertions = append(assertions, AssertionResult{
		Name:    "concurrent-run-failure-kind-stable",
		Passed:  allPassed,
		Message: fmt.Sprintf("reference=%s workers=%d", reference.FailureKind, len(runs)),
	})

	return assertions
}

func validateConcurrentRouteStability(runs []WorkloadRunResult) []AssertionResult {
	if len(runs) < 2 {
		return nil
	}
	reference := runs[0]
	if reference.RouteEngine == "" {
		return nil
	}
	divergentEngines := make(map[string]int)
	for _, run := range runs {
		if run.RouteEngine != "" {
			divergentEngines[run.RouteEngine]++
		}
	}
	stable := len(divergentEngines) <= 1
	parts := make([]string, 0, len(divergentEngines))
	for engine, count := range divergentEngines {
		parts = append(parts, fmt.Sprintf("%s=%d", engine, count))
	}
	assertions := []AssertionResult{{
		Name:    "concurrent-run-route-engine-stable",
		Passed:  stable,
		Message: fmt.Sprintf("engines: %s", partsToStr(parts)),
	}}
	return assertions
}

func partsToStr(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += ", " + parts[i]
	}
	return result
}

func validateResultLevelAssertions(workload WorkloadDefinition, run WorkloadRunResult, records []*model.PersistentRecord, semantics workloadSemantics) []AssertionResult {
	assertions := []AssertionResult{validateUniqueRows(run), validateSchemaScope(workload, records)}
	if len(workload.ResolvedFilterConditions()) > 0 {
		assertions = append(assertions, validateFilterMatch(workload, records))
	}
	if semantics.TradeTimeStart > 0 || semantics.TradeTimeEnd > 0 {
		assertions = append(assertions, validateTradeTimeWindow(records, semantics))
	}
	if workload.TargetSchema == "trade" && len(records) > 1 {
		assertions = append(assertions, validateSortOrder(records, "tradeTime", true))
	}
	return assertions
}

func validateSchemaScope(workload WorkloadDefinition, records []*model.PersistentRecord) AssertionResult {
	expectedSchemaID, err := workloadSchemaID(workload.TargetSchema)
	if err != nil {
		return AssertionResult{Name: "schema-scoped-results-match-target", Passed: false, Message: err.Error()}
	}
	for _, record := range records {
		if record.SchemaID != expectedSchemaID {
			return AssertionResult{Name: "schema-scoped-results-match-target", Passed: false, Message: fmt.Sprintf("expected_schema=%d actual_schema=%d row=%s", expectedSchemaID, record.SchemaID, record.RowID)}
		}
	}
	return AssertionResult{Name: "schema-scoped-results-match-target", Passed: true, Message: fmt.Sprintf("schema=%s rows=%d", workload.TargetSchema, len(records))}
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

func validateFilterMatch(workload WorkloadDefinition, records []*model.PersistentRecord) AssertionResult {
	for _, record := range records {
		for key, expected := range workload.ResolvedFilterConditions() {
			if !recordMatchesFilter(record, key, fmt.Sprint(expected)) {
				return AssertionResult{Name: "filter-results-match-request", Passed: false, Message: fmt.Sprintf("attribute=%s expected=%v row=%s", key, expected, record.RowID)}
			}
		}
	}
	return AssertionResult{Name: "filter-results-match-request", Passed: true, Message: fmt.Sprintf("conditions=%v", workload.ResolvedFilterConditions())}
}

func validateTradeTimeWindow(records []*model.PersistentRecord, semantics workloadSemantics) AssertionResult {
	for _, record := range records {
		tradeTime, ok := record.Int64Items["tradeTime"]
		if !ok {
			return AssertionResult{Name: "tradeTime-window-match-request", Passed: false, Message: fmt.Sprintf("missing tradeTime row=%s", record.RowID)}
		}
		if semantics.TradeTimeStart > 0 && tradeTime < semantics.TradeTimeStart {
			return AssertionResult{Name: "tradeTime-window-match-request", Passed: false, Message: fmt.Sprintf("row=%s tradeTime=%d start=%d", record.RowID, tradeTime, semantics.TradeTimeStart)}
		}
		if semantics.TradeTimeEnd > 0 && tradeTime > semantics.TradeTimeEnd {
			return AssertionResult{Name: "tradeTime-window-match-request", Passed: false, Message: fmt.Sprintf("row=%s tradeTime=%d end=%d", record.RowID, tradeTime, semantics.TradeTimeEnd)}
		}
	}
	return AssertionResult{Name: "tradeTime-window-match-request", Passed: true, Message: fmt.Sprintf("rows=%d start=%d end=%d", len(records), semantics.TradeTimeStart, semantics.TradeTimeEnd)}
}

func validateSortOrder(records []*model.PersistentRecord, attribute string, desc bool) AssertionResult {
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

// benchmarkTradeFilterColumns and benchmarkTradeFilterEAVAttrs map trade filter
// attributes to their storage layout (see schemas/trade_attributes.json):
// service-path records are rebuilt via transform.ToPersistentRecord, which
// stores column-bound attributes under entity_main column names and EAV-only
// attributes as OtherAttributes entries.
var benchmarkTradeFilterColumns = map[string]string{
	"symbol": "text_01",
	"region": "text_02",
}

var benchmarkTradeFilterEAVAttrs = map[string]int16{
	"exchange":     8,
	"orderChannel": 12,
}

func recordMatchesFilter(record *model.PersistentRecord, attribute, expected string) bool {
	switch attribute {
	case "symbol", "exchange", "region", "name", "orderChannel":
		value, ok := benchmarkRecordTextValue(record, attribute)
		return ok && value == expected
	case "tradeType":
		if value, ok := record.Int64Items[attribute]; ok {
			return fmt.Sprintf("%d", value) == expected
		}
		if value, ok := record.Int16Items["smallint_01"]; ok {
			return fmt.Sprintf("%d", value) == expected
		}
		return false
	default:
		// Fail closed: an unmapped filter attribute means the oracle cannot
		// verify the row, and silently passing would mask filter regressions.
		return false
	}
}

// benchmarkRecordTextValue reads a benchmark text attribute from either record
// shape: harness-path records carry attribute names directly, service-path
// records carry the storage layout.
func benchmarkRecordTextValue(record *model.PersistentRecord, attribute string) (string, bool) {
	if value, ok := record.TextItems[attribute]; ok {
		return value, true
	}
	if column, ok := benchmarkTradeFilterColumns[attribute]; ok {
		if value, ok := record.TextItems[column]; ok {
			return value, true
		}
	}
	if attrID, ok := benchmarkTradeFilterEAVAttrs[attribute]; ok {
		for _, eav := range record.OtherAttributes {
			if eav.AttrID == attrID && eav.ValueText != nil {
				return *eav.ValueText, true
			}
		}
	}
	return "", false
}

func persistentRecordIDs(records []*model.PersistentRecord) []string {
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

func countFailedAssertions(assertions []AssertionResult) int {
	failed := 0
	for _, assertion := range assertions {
		if !assertion.Passed {
			failed++
		}
	}
	return failed
}

func extractPerTierMetrics(plan *model.ExecutionPlan) *PerTierMetrics {
	if plan == nil {
		return nil
	}
	metrics := &PerTierMetrics{Sources: len(plan.Sources)}
	for _, src := range plan.Sources {
		switch src.Engine {
		case "postgres":
			if src.ActualRows > 0 {
				metrics.PGRows += src.ActualRows
			}
			if src.Reason == "dirty id set fetched" {
				metrics.PGDirtyCount = src.ActualRows
			}
			if src.PredicatePushdown {
				metrics.HasPushdown = true
			}
		case "duckdb":
			if src.ActualRows > 0 {
				metrics.DuckDBRows += src.ActualRows
			}
		}
	}
	if metrics.PGRows > 0 || metrics.DuckDBRows > 0 {
		metrics.FinalRows = metrics.PGRows + metrics.DuckDBRows
		if metrics.FinalRows > 0 {
			metrics.PushdownRatio = float64(metrics.PGRows) / float64(metrics.FinalRows)
		}
	}
	return metrics
}

func validatePushdownAssertions(run WorkloadRunResult) []AssertionResult {
	if run.PerTier == nil {
		return nil
	}
	var assertions []AssertionResult
	pt := run.PerTier

	if pt.Sources > 0 {
		assertions = append(assertions, AssertionResult{
			Name:    "pushdown-plan-sources-present",
			Passed:  true,
			Message: fmt.Sprintf("sources=%d", pt.Sources),
		})
	} else {
		assertions = append(assertions, AssertionResult{
			Name:    "pushdown-plan-sources-present",
			Passed:  false,
			Message: "execution plan has no sources",
		})
	}

	if pt.PGRows >= 0 {
		assertions = append(assertions, AssertionResult{
			Name:    "pushdown-pg-rows-tracked",
			Passed:  true,
			Message: fmt.Sprintf("pg_rows=%d", pt.PGRows),
		})
	}

	if pt.HasPushdown {
		assertions = append(assertions, AssertionResult{
			Name:    "pushdown-active",
			Passed:  true,
			Message: "at least one source has pushdown enabled",
		})
	}

	if pt.PushdownRatio > 0 && pt.PushdownRatio <= 1 {
		assertions = append(assertions, AssertionResult{
			Name:    "pushdown-efficiency-reasonable",
			Passed:  true,
			Message: fmt.Sprintf("pushdown_ratio=%.3f", pt.PushdownRatio),
		})
	}

	return assertions
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

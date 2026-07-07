package benchmark

import (
	"math"
	"sort"
	"time"
)

// SummarizeRunResult aggregates durations and assertion outcomes.
func SummarizeRunResult(result *RunResult) SummaryReport {
	summary := SummaryReport{AssertionStats: make(map[string]AssertionStat)}
	if result == nil {
		return summary
	}
	summary.Metadata = metadataForResult(result)
	summary.OracleModes = cloneStringMap(result.OracleModes)
	summary.OracleProvenance = summarizeOracleProvenance(result)
	if result.Provenance != nil {
		pc := *result.Provenance
		if pc.StartedAt.IsZero() {
			pc.StartedAt = result.StartedAt
		}
		if pc.CompletedAt.IsZero() {
			pc.CompletedAt = result.CompletedAt
		}
		if pc.Concurrency == 0 {
			pc.Concurrency = result.Config.Concurrency
		}
		summary.Provenance = &pc
	} else if !result.StartedAt.IsZero() {
		summary.Provenance = &RunProvenance{
			StartedAt:    result.StartedAt,
			CompletedAt:  result.CompletedAt,
			Mode:         string(result.Config.Mode),
			Scale:        string(result.Config.Scale),
			Distribution: string(result.Config.Distribution),
			TierProfile:  result.Config.TierProfile,
			Concurrency:  result.Config.Concurrency,
		}
	}
	if len(result.Executions) == 0 {
		summary.Passed = result != nil && result.Passed
		summary.FailureCount = resultFailureCount(result)
		summary.Workloads = summarizeWorkloads(result)
		summary.Stability = summarizeStability(summary.Workloads)
		return summary
	}
	summary.Passed = result.Passed
	summary.FailureCount = resultFailureCount(result)
	summary.Workloads = summarizeWorkloads(result)
	summary.Stability = summarizeStability(summary.Workloads)
	durations := make([]time.Duration, 0, len(result.Executions))
	for _, execution := range result.Executions {
		durations = append(durations, execution.Duration)
		if execution.InfraError != "" {
			summary.InfraFailures++
		}
		if execution.FailureKind == FailureKindCorrectness {
			summary.CorrectnessFailures++
		}
		for _, assertion := range execution.Assertions {
			stat := summary.AssertionStats[assertion.Name]
			if assertion.Passed {
				stat.Passed++
			} else {
				stat.Failed++
			}
			summary.AssertionStats[assertion.Name] = stat
		}
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	summary.ExecutionCount = len(durations)
	summary.Min = durations[0]
	summary.P50 = percentileDuration(durations, 0.50)
	summary.P95 = percentileDuration(durations, 0.95)
	summary.P99 = percentileDuration(durations, 0.99)
	summary.Max = durations[len(durations)-1]
	var total time.Duration
	for _, duration := range durations {
		total += duration
	}
	summary.TotalDuration = total
	summary.Avg = total / time.Duration(len(durations))
	if total > 0 {
		summary.QPS = float64(len(durations)) / total.Seconds()
	}
	return summary
}

func percentileDuration(values []time.Duration, percentile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := int(math.Ceil(percentile*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func resultFailureCount(result *RunResult) int {
	if result == nil {
		return 0
	}
	if result.FailureCount > 0 {
		return result.FailureCount
	}
	failed := 0
	for _, execution := range result.Executions {
		failed += maxInt(execution.FailureCount, countFailedAssertions(execution.Assertions))
	}
	return failed
}

// CompareSummaryReports builds a machine-readable diff between two summaries.
func CompareSummaryReports(baseline, candidate SummaryReport) DiffReport {
	return DiffReport{
		BaselineMetadata:  baseline.Metadata,
		CandidateMetadata: candidate.Metadata,
		Summary: SummaryDiff{
			PassedChanged:            baseline.Passed != candidate.Passed,
			FailureCountDelta:        candidate.FailureCount - baseline.FailureCount,
			CorrectnessFailuresDelta: candidate.CorrectnessFailures - baseline.CorrectnessFailures,
			InfraFailuresDelta:       candidate.InfraFailures - baseline.InfraFailures,
			ExecutionCountDelta:      candidate.ExecutionCount - baseline.ExecutionCount,
			QPSDelta:                 candidate.QPS - baseline.QPS,
			AvgLatencyDelta:          candidate.Avg - baseline.Avg,
			P95LatencyDelta:          candidate.P95 - baseline.P95,
			P99LatencyDelta:          candidate.P99 - baseline.P99,
			TotalDurationDelta:       candidate.TotalDuration - baseline.TotalDuration,
		},
		Workloads: compareWorkloadSummaries(baseline.Workloads, candidate.Workloads),
	}
}

func summarizeWorkloads(result *RunResult) []WorkloadSummary {
	if result == nil || len(result.Executions) == 0 {
		return nil
	}
	workloadDefs := make(map[string]WorkloadDefinition, len(result.Workloads))
	for _, workload := range result.Workloads {
		workloadDefs[workload.Name] = workload
	}
	grouped := make(map[string][]WorkloadRunResult)
	for _, execution := range result.Executions {
		grouped[execution.Name] = append(grouped[execution.Name], execution)
	}
	names := make([]string, 0, len(grouped))
	for name := range grouped {
		names = append(names, name)
	}
	sort.Strings(names)
	workloads := make([]WorkloadSummary, 0, len(names))
	for _, name := range names {
		runs := grouped[name]
		workload := WorkloadSummary{Name: name, Passed: true, AssertionStats: make(map[string]AssertionStat)}
		if def, ok := workloadDefs[name]; ok {
			workload.Category = string(def.Category)
			workload.TargetSchema = def.TargetSchema
			workload.ExecutionSource = def.ExecutionSource
			workload.OracleMode = string(def.ResolvedOracleMode())
			workload.PreferHot = def.PreferHot
			workload.Distribution = runs[0].Distribution
			workload.PageSize = def.PageSize
		}
		if mode, ok := result.OracleModes[name]; ok && mode != "" {
			workload.OracleMode = mode
		}
		durations := make([]time.Duration, 0, len(runs))
		var totalDuration time.Duration
		var totalResultCount int
		var totalRecords int64
		for _, run := range runs {
			durations = append(durations, run.Duration)
			totalDuration += run.Duration
			totalResultCount += run.ResultCount
			totalRecords += run.TotalRecords
			workload.ExecutionCount++
			workload.FailureCount += maxInt(run.FailureCount, countFailedAssertions(run.Assertions))
			if run.InfraError != "" {
				workload.InfraFailures++
			}
			if run.FailureKind == FailureKindCorrectness {
				workload.CorrectnessFailures++
			}
			if !run.Passed {
				workload.Passed = false
			}
			if run.PageSize > 0 && workload.PageSize == 0 {
				workload.PageSize = run.PageSize
			}
			if run.Offset > workload.MaxOffset {
				workload.MaxOffset = run.Offset
			}
			if workload.OracleMode == "" && run.OracleMode != "" {
				workload.OracleMode = run.OracleMode
			}
			if run.PreferHot {
				workload.PreferHot = true
			}
			if run.RouteEngine != "" {
				if workload.RouteEngineCounts == nil {
					workload.RouteEngineCounts = make(map[string]int)
					workload.RouteReasonCounts = make(map[string]int)
				}
				workload.RouteEngineCounts[run.RouteEngine]++
				if run.RouteReason != "" {
					workload.RouteReasonCounts[run.RouteReason]++
				}
			}
			for _, assertion := range run.Assertions {
				stat := workload.AssertionStats[assertion.Name]
				if assertion.Passed {
					stat.Passed++
				} else {
					stat.Failed++
				}
				workload.AssertionStats[assertion.Name] = stat
			}
		}
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		workload.Min = durations[0]
		workload.P50 = percentileDuration(durations, 0.50)
		workload.P95 = percentileDuration(durations, 0.95)
		workload.P99 = percentileDuration(durations, 0.99)
		workload.Max = durations[len(durations)-1]
		workload.TotalDuration = totalDuration
		workload.Avg = totalDuration / time.Duration(len(durations))
		if totalDuration > 0 {
			workload.QPS = float64(len(durations)) / totalDuration.Seconds()
		}
		workload.AvgResultCount = float64(totalResultCount) / float64(len(runs))
		workload.AvgTotalRecords = float64(totalRecords) / float64(len(runs))
		workloads = append(workloads, workload)
	}
	return workloads
}

func summarizeOracleProvenance(result *RunResult) []OracleProvenance {
	if result == nil {
		return nil
	}
	byMode := make(map[string][]string)
	if len(result.Workloads) > 0 {
		for _, workload := range result.Workloads {
			mode := string(workload.ResolvedOracleMode())
			if sampled, ok := result.OracleModes[workload.Name]; ok && sampled != "" {
				mode = sampled
			}
			byMode[mode] = append(byMode[mode], workload.Name)
		}
	} else {
		for _, execution := range result.Executions {
			if execution.OracleMode == "" {
				continue
			}
			byMode[execution.OracleMode] = append(byMode[execution.OracleMode], execution.Name)
		}
	}
	if len(byMode) == 0 {
		return nil
	}
	modes := make([]string, 0, len(byMode))
	for mode := range byMode {
		modes = append(modes, mode)
	}
	sort.Strings(modes)
	provenance := make([]OracleProvenance, 0, len(modes))
	for _, mode := range modes {
		workloads := append([]string(nil), byMode[mode]...)
		sort.Strings(workloads)
		provenance = append(provenance, OracleProvenance{Mode: mode, Workloads: workloads})
	}
	return provenance
}

func summarizeStability(workloads []WorkloadSummary) StabilitySummary {
	var summary StabilitySummary
	seenUnstable := make(map[string]struct{})
	for _, workload := range workloads {
		failureKind := workload.AssertionStats["repeated-run-failure-kind-stable"]
		totalRecords := workload.AssertionStats["repeated-run-total-records-stable"]
		pageRowIDs := workload.AssertionStats["repeated-run-page-row-ids-stable"]
		checked := failureKind.Passed+failureKind.Failed > 0 || totalRecords.Passed+totalRecords.Failed > 0 || pageRowIDs.Passed+pageRowIDs.Failed > 0
		if !checked {
			continue
		}
		summary.Enabled = true
		summary.WorkloadsChecked++
		summary.FailureKindFailures += failureKind.Failed
		summary.TotalRecordsFailures += totalRecords.Failed
		summary.PageRowIDFailures += pageRowIDs.Failed
		if failureKind.Failed > 0 || totalRecords.Failed > 0 || pageRowIDs.Failed > 0 {
			if _, ok := seenUnstable[workload.Name]; !ok {
				seenUnstable[workload.Name] = struct{}{}
				summary.UnstableWorkloads = append(summary.UnstableWorkloads, workload.Name)
			}
		}
	}
	sort.Strings(summary.UnstableWorkloads)
	return summary
}
func compareWorkloadSummaries(baseline, candidate []WorkloadSummary) []WorkloadDiff {
	baselineByName := make(map[string]WorkloadSummary, len(baseline))
	candidateByName := make(map[string]WorkloadSummary, len(candidate))
	nameSet := make(map[string]struct{}, len(baseline)+len(candidate))
	for _, workload := range baseline {
		baselineByName[workload.Name] = workload
		nameSet[workload.Name] = struct{}{}
	}
	for _, workload := range candidate {
		candidateByName[workload.Name] = workload
		nameSet[workload.Name] = struct{}{}
	}
	names := make([]string, 0, len(nameSet))
	for name := range nameSet {
		names = append(names, name)
	}
	sort.Strings(names)
	diffs := make([]WorkloadDiff, 0, len(names))
	for _, name := range names {
		base, baseOK := baselineByName[name]
		cand, candOK := candidateByName[name]
		targetSchema := base.TargetSchema
		if targetSchema == "" {
			targetSchema = cand.TargetSchema
		}
		routeEngineChanged := false
		if len(base.RouteEngineCounts) > 0 && len(cand.RouteEngineCounts) > 0 {
			for engine := range base.RouteEngineCounts {
				if _, ok := cand.RouteEngineCounts[engine]; !ok {
					routeEngineChanged = true
					break
				}
			}
			if !routeEngineChanged {
				for engine := range cand.RouteEngineCounts {
					if _, ok := base.RouteEngineCounts[engine]; !ok {
						routeEngineChanged = true
						break
					}
				}
			}
		} else if len(base.RouteEngineCounts) != len(cand.RouteEngineCounts) {
			routeEngineChanged = true
		}
		diffs = append(diffs, WorkloadDiff{
			Name:                     name,
			TargetSchema:             targetSchema,
			MissingInBaseline:        !baseOK,
			MissingInCandidate:       !candOK,
			PassedChanged:            base.Passed != cand.Passed,
			FailureCountDelta:        cand.FailureCount - base.FailureCount,
			CorrectnessFailuresDelta: cand.CorrectnessFailures - base.CorrectnessFailures,
			InfraFailuresDelta:       cand.InfraFailures - base.InfraFailures,
			ExecutionCountDelta:      cand.ExecutionCount - base.ExecutionCount,
			QPSDelta:                 cand.QPS - base.QPS,
			AvgLatencyDelta:          cand.Avg - base.Avg,
			P95LatencyDelta:          cand.P95 - base.P95,
			P99LatencyDelta:          cand.P99 - base.P99,
			AvgResultCountDelta:      cand.AvgResultCount - base.AvgResultCount,
			AvgTotalRecordsDelta:     cand.AvgTotalRecords - base.AvgTotalRecords,
			RouteEngineChanged:       routeEngineChanged,
		})
	}
	return diffs
}

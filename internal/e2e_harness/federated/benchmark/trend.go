package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	TrendSeverityHardStop = "hard_stop"
	TrendSeverityReview   = "review"

	TrendKindCorrectness   = "correctness_regression"
	TrendKindInstability   = "instability_regression"
	TrendKindP95Latency    = "p95_latency_regression"
	TrendKindQPSRegression = "qps_regression"

	TrendClassStable      = "stable"
	TrendClassRegression  = "regression"
	TrendClassImprovement = "improvement"
	TrendClassDrift       = "drift"
	TrendClassMethodology = "methodology_change"

	TrendStatusPass               = "pass"
	TrendStatusRegression         = "regression"
	TrendStatusHardStopRegression = "hard_stop_regression"
	TrendStatusDrift              = "baseline_drift"
	TrendStatusMethodology        = "methodology_change"
)

// ReadTrendHistory scans a directory tree for benchmark-summary.json files
// and returns them sorted by timestamp, oldest first.
func ReadTrendHistory(historyDir string) ([]TrendRun, error) {
	var runs []TrendRun
	err := filepath.Walk(historyDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if info.Name() != "benchmark-summary.json" {
			return nil
		}
		summary, readErr := ReadSummaryReport(path)
		if readErr != nil {
			return nil
		}
		// Trend needs timestamps: summaries without provenance (manual or
		// legacy artifacts) are skipped, never dereferenced.
		if summary.Provenance == nil {
			return nil
		}
		startedAt := summary.Provenance.StartedAt
		if startedAt.IsZero() {
			startedAt = summary.Provenance.CompletedAt
		}
		if startedAt.IsZero() {
			return nil
		}
		runs = append(runs, TrendRun{
			Path:      path,
			Summary:   summary,
			StartedAt: startedAt,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk history directory: %w", err)
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].StartedAt.Before(runs[j].StartedAt) })
	return runs, nil
}

// buildComparabilityIdentity builds a string key that groups comparable benchmark runs.
// The identity combines mode, scale, distribution, tier profile, and workload set.
// Oracle modes are intentionally excluded so that oracle-mode changes are surfaced
// as methodology changes rather than splitting runs into separate comparability groups.
func buildComparabilityIdentity(summary SummaryReport) string {
	if summary.Provenance == nil {
		return summary.Metadata.BenchmarkID
	}
	names := make([]string, 0, len(summary.Workloads))
	for _, w := range summary.Workloads {
		names = append(names, w.Name)
	}
	sort.Strings(names)
	identity := fmt.Sprintf("%s|%s|%s|%s|%s",
		summary.Provenance.Mode,
		summary.Provenance.Scale,
		summary.Provenance.Distribution,
		summary.Provenance.TierProfile,
		strings.Join(names, ","))
	// Concurrent runs form their own comparability group so they never mix
	// into the sequential trend baseline; C<=1 keeps the legacy identity so
	// historical artifacts stay comparable.
	if summary.Provenance.Concurrency > 1 {
		identity = fmt.Sprintf("%s|c%d", identity, summary.Provenance.Concurrency)
	}
	return identity
}

// findCandidateRun returns the specified candidate path or the newest comparable run.
func findCandidateRun(runs []TrendRun, candidatePath string, comparabilityKey string) (TrendRun, []TrendRun, error) {
	var comparable []TrendRun
	for _, r := range runs {
		if buildComparabilityIdentity(r.Summary) == comparabilityKey {
			comparable = append(comparable, r)
		}
	}
	if len(comparable) == 0 {
		return TrendRun{}, nil, fmt.Errorf("no comparable runs found for identity %s", comparabilityKey)
	}
	if candidatePath != "" {
		for _, r := range comparable {
			if r.Path == candidatePath {
				return r, comparable, nil
			}
		}
		return TrendRun{}, nil, fmt.Errorf("candidate path %s not found in comparable runs", candidatePath)
	}
	return comparable[len(comparable)-1], comparable, nil
}

// selectBaseAndDriftWindows selects baseline and drift windows from comparable runs,
// excluding the candidate.
func selectBaseAndDriftWindows(comparable []TrendRun, candidate TrendRun, baselineWindow, driftWindow int) (baseline, drift []TrendRun) {
	var before []TrendRun
	for _, r := range comparable {
		if r.StartedAt.Before(candidate.StartedAt) {
			before = append(before, r)
		}
	}
	sort.Slice(before, func(i, j int) bool { return before[i].StartedAt.After(before[j].StartedAt) })
	if len(before) > baselineWindow {
		before = before[:baselineWindow]
	}
	sort.Slice(before, func(i, j int) bool { return before[i].StartedAt.Before(before[j].StartedAt) })
	baseline = before
	if driftWindow <= 0 || len(baseline) < baselineWindow {
		return baseline, nil
	}
	older := make([]TrendRun, 0)
	for _, r := range comparable {
		if r.StartedAt.Before(candidate.StartedAt) && !containsRun(baseline, r) {
			older = append(older, r)
		}
	}
	sort.Slice(older, func(i, j int) bool { return older[i].StartedAt.After(older[j].StartedAt) })
	if len(older) > driftWindow {
		older = older[:driftWindow]
	}
	sort.Slice(older, func(i, j int) bool { return older[i].StartedAt.Before(older[j].StartedAt) })
	return baseline, older
}

func containsRun(runs []TrendRun, target TrendRun) bool {
	for _, r := range runs {
		if r.Path == target.Path || r.StartedAt.Equal(target.StartedAt) {
			return true
		}
	}
	return false
}

// detectMethodologyChanges checks for oracle mode or workload set changes.
func detectMethodologyChanges(baseline, candidate SummaryReport) []string {
	var changes []string
	if len(baseline.OracleModes) != len(candidate.OracleModes) {
		changes = append(changes, "oracle mode count changed")
		return changes
	}
	for k, v := range baseline.OracleModes {
		if cv, ok := candidate.OracleModes[k]; !ok || cv != v {
			changes = append(changes, fmt.Sprintf("oracle mode for workload %s changed", k))
		}
	}
	baseNames := make(map[string]bool)
	for _, w := range baseline.Workloads {
		baseNames[w.Name] = true
	}
	for _, w := range candidate.Workloads {
		if !baseNames[w.Name] {
			changes = append(changes, fmt.Sprintf("new workload %s not present in baseline", w.Name))
		}
	}
	candNames := make(map[string]bool)
	for _, w := range candidate.Workloads {
		candNames[w.Name] = true
	}
	for _, w := range baseline.Workloads {
		if !candNames[w.Name] {
			changes = append(changes, fmt.Sprintf("workload %s removed in candidate", w.Name))
		}
	}
	return changes
}

// isWorkloadInstabilityChanged returns whether a workload's instability status changed
// between baseline and candidate. It checks changes in CorrectnessFailures (>0) as
// a proxy for hard-stop instability regressions.
func isWorkloadInstabilityChanged(base, cand WorkloadSummary) bool {
	baseUnstable := containsString(base.AssertionStats, func(s AssertionStat) bool { return s.Failed > 0 })
	candUnstable := containsString(cand.AssertionStats, func(s AssertionStat) bool { return s.Failed > 0 })
	return baseUnstable != candUnstable
}

func containsString(stats map[string]AssertionStat, predicate func(AssertionStat) bool) bool {
	for _, s := range stats {
		if predicate(s) {
			return true
		}
	}
	return false
}

// analyzeWorkloadTrends computes per-workload trends across baseline and candidate.
func analyzeWorkloadTrends(baselineRuns []TrendRun, candidate SummaryReport, protected map[string]bool) []WorkloadTrend {
	if len(baselineRuns) == 0 {
		return nil
	}
	baselineByWorkload := make(map[string][]WorkloadSummary)
	for _, r := range baselineRuns {
		for _, w := range r.Summary.Workloads {
			baselineByWorkload[w.Name] = append(baselineByWorkload[w.Name], w)
		}
	}
	var trends []WorkloadTrend
	for _, cw := range candidate.Workloads {
		bws, ok := baselineByWorkload[cw.Name]
		if !ok || !protected[cw.Name] && len(bws) == 0 {
			continue
		}
		if !protected[cw.Name] {
			continue
		}
		avgP95 := medianP95(bws)
		avgQPS := medianQPS(bws)
		p95Delta := cw.P95 - avgP95
		qpsDelta := cw.QPS - avgQPS
		var p95DeltaPct, qpsDeltaPct float64
		if avgP95 > 0 {
			p95DeltaPct = float64(p95Delta) / float64(avgP95) * 100
		}
		if avgQPS > 0 {
			qpsDeltaPct = qpsDelta / avgQPS * 100
		}
		instabilityChanged := false
		if len(bws) > 0 {
			instabilityChanged = isWorkloadInstabilityChanged(bws[0], cw)
		}
		correctnessDelta := 0
		correctnessChanged := false
		if len(bws) > 0 {
			correctnessDelta = cw.CorrectnessFailures - bws[0].CorrectnessFailures
			correctnessChanged = correctnessDelta != 0
		}
		classification := TrendClassStable
		if correctnessChanged && correctnessDelta > 0 {
			classification = TrendClassRegression
		} else if instabilityChanged {
			classification = TrendClassRegression
		} else if p95DeltaPct >= 10 {
			classification = TrendClassRegression
		} else if qpsDeltaPct <= -10 {
			classification = TrendClassRegression
		} else if p95DeltaPct <= -10 {
			classification = TrendClassImprovement
		}
		trends = append(trends, WorkloadTrend{
			Name:               cw.Name,
			TargetSchema:       cw.TargetSchema,
			BaselineP95:        avgP95,
			CandidateP95:       cw.P95,
			P95Delta:           p95Delta,
			P95DeltaPercent:    p95DeltaPct,
			BaselineQPS:        avgQPS,
			CandidateQPS:       cw.QPS,
			QPSDelta:           qpsDelta,
			QPSDeltaPercent:    qpsDeltaPct,
			CorrectnessChanged: correctnessChanged,
			CorrectnessDelta:   correctnessDelta,
			InstabilityChanged: instabilityChanged,
			Classification:     classification,
		})
	}
	sort.Slice(trends, func(i, j int) bool { return trends[i].Name < trends[j].Name })
	return trends
}

func medianP95(workloads []WorkloadSummary) time.Duration {
	if len(workloads) == 0 {
		return 0
	}
	durs := make([]time.Duration, len(workloads))
	for i, w := range workloads {
		durs[i] = w.P95
	}
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	mid := len(durs) / 2
	if len(durs)%2 == 0 {
		return (durs[mid-1] + durs[mid]) / 2
	}
	return durs[mid]
}

func medianQPS(workloads []WorkloadSummary) float64 {
	if len(workloads) == 0 {
		return 0
	}
	qpsVals := make([]float64, len(workloads))
	for i, w := range workloads {
		qpsVals[i] = w.QPS
	}
	sort.Float64s(qpsVals)
	mid := len(qpsVals) / 2
	if len(qpsVals)%2 == 0 {
		return (qpsVals[mid-1] + qpsVals[mid]) / 2
	}
	return qpsVals[mid]
}

func medianP95FromTrendRuns(runs []TrendRun) time.Duration {
	var all []time.Duration
	for _, r := range runs {
		for _, w := range r.Summary.Workloads {
			all = append(all, w.P95)
		}
	}
	if len(all) == 0 {
		return 0
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	mid := len(all) / 2
	if len(all)%2 == 0 {
		return (all[mid-1] + all[mid]) / 2
	}
	return all[mid]
}

// detectRegressions analyzes workload trends and flags regression candidates.
func detectRegressions(trends []WorkloadTrend, candidate SummaryReport) []RegressionCandidate {
	var regressions []RegressionCandidate
	candByName := make(map[string]WorkloadSummary, len(candidate.Workloads))
	for _, w := range candidate.Workloads {
		candByName[w.Name] = w
	}
	for _, t := range trends {
		cw := candByName[t.Name]
		if t.CorrectnessChanged && t.CorrectnessDelta > 0 {
			regressions = append(regressions, RegressionCandidate{
				WorkloadName:   t.Name,
				TargetSchema:   t.TargetSchema,
				Kind:           TrendKindCorrectness,
				BaselineValue:  float64(cw.CorrectnessFailures - t.CorrectnessDelta),
				CandidateValue: float64(cw.CorrectnessFailures),
				Delta:          float64(t.CorrectnessDelta),
				Severity:       TrendSeverityHardStop,
				Description:    fmt.Sprintf("correctness failures increased from %d to %d", cw.CorrectnessFailures-t.CorrectnessDelta, cw.CorrectnessFailures),
			})
		}
		if t.InstabilityChanged {
			regressions = append(regressions, RegressionCandidate{
				WorkloadName: t.Name,
				TargetSchema: t.TargetSchema,
				Kind:         TrendKindInstability,
				Severity:     TrendSeverityHardStop,
				Description:  "workload repeated-run stability status changed",
			})
		}
		if t.P95DeltaPercent >= 10 && t.Classification == TrendClassRegression {
			regressions = append(regressions, RegressionCandidate{
				WorkloadName:   t.Name,
				TargetSchema:   t.TargetSchema,
				Kind:           TrendKindP95Latency,
				BaselineValue:  float64(t.BaselineP95),
				CandidateValue: float64(t.CandidateP95),
				Delta:          float64(t.P95Delta),
				Threshold:      10,
				Severity:       TrendSeverityReview,
				Description:    fmt.Sprintf("p95 increased by %.1f%% from %s to %s", t.P95DeltaPercent, t.BaselineP95, t.CandidateP95),
			})
		}
		if t.QPSDeltaPercent <= -10 && t.Classification == TrendClassRegression {
			regressions = append(regressions, RegressionCandidate{
				WorkloadName:   t.Name,
				TargetSchema:   t.TargetSchema,
				Kind:           TrendKindQPSRegression,
				BaselineValue:  t.BaselineQPS,
				CandidateValue: t.CandidateQPS,
				Delta:          t.QPSDelta,
				Threshold:      -10,
				Severity:       TrendSeverityReview,
				Description:    fmt.Sprintf("qps dropped by %.1f%% from %.2f to %.2f", -t.QPSDeltaPercent, t.BaselineQPS, t.CandidateQPS),
			})
		}
	}
	return regressions
}

// detectBaselineDrift checks whether the baseline window has drifted from the
// drift window.
func detectBaselineDrift(baseline, drift []TrendRun) *DriftSignal {
	if len(drift) == 0 || len(baseline) == 0 {
		return nil
	}
	baselineP95 := medianP95FromTrendRuns(baseline)
	driftP95 := medianP95FromTrendRuns(drift)
	if baselineP95 <= 0 || driftP95 <= 0 {
		return nil
	}
	deltaPct := float64(baselineP95-driftP95) / float64(driftP95) * 100
	if deltaPct >= 10 {
		return &DriftSignal{
			Detected:    true,
			Description: fmt.Sprintf("baseline p95 is %.1f%% slower than drift window (%s vs %s)", deltaPct, baselineP95, driftP95),
			Evidence:    []string{fmt.Sprintf("baseline_median_p95=%s drift_median_p95=%s delta_pct=%.1f", baselineP95, driftP95, deltaPct)},
		}
	}
	return nil
}

// AnalyzeTrend is the main entry point for longitudinal trend analysis.
func AnalyzeTrend(historyDir, candidatePath string, baselineWindow, driftWindow int, protectedWorkloads []string) (TrendReport, error) {
	runs, err := ReadTrendHistory(historyDir)
	if err != nil {
		return TrendReport{}, err
	}
	if len(runs) == 0 {
		return TrendReport{}, fmt.Errorf("no historical runs found in %s", historyDir)
	}
	protectedSet := make(map[string]bool, len(protectedWorkloads))
	for _, name := range protectedWorkloads {
		protectedSet[name] = true
	}
	if candidatePath == "" {
		candidatePath = runs[len(runs)-1].Path
	}
	candidateSummary := runs[len(runs)-1].Summary
	if candidatePath != runs[len(runs)-1].Path {
		for _, r := range runs {
			if r.Path == candidatePath {
				candidateSummary = r.Summary
				break
			}
		}
	}
	compKey := buildComparabilityIdentity(candidateSummary)
	candidate, comparable, err := findCandidateRun(runs, candidatePath, compKey)
	if err != nil {
		return TrendReport{}, err
	}
	baseline, drift := selectBaseAndDriftWindows(comparable, candidate, baselineWindow, driftWindow)
	methodologyChanges := make([]string, 0)
	if len(baseline) > 0 {
		methodologyChanges = detectMethodologyChanges(baseline[0].Summary, candidate.Summary)
	}
	trends := analyzeWorkloadTrends(baseline, candidate.Summary, protectedSet)
	regressions := detectRegressions(trends, candidate.Summary)
	driftSignal := detectBaselineDrift(baseline, drift)
	status := TrendStatusPass
	hasHardStop := false
	for _, reg := range regressions {
		if reg.Severity == TrendSeverityHardStop {
			hasHardStop = true
			break
		}
	}
	if hasHardStop {
		status = TrendStatusHardStopRegression
	} else if len(methodologyChanges) > 0 {
		status = TrendStatusMethodology
	} else if driftSignal != nil && driftSignal.Detected {
		status = TrendStatusDrift
	} else if len(regressions) > 0 {
		status = TrendStatusRegression
	}
	stableWindows := len(baseline) == baselineWindow && driftWindow > 0 && len(drift) < driftWindow
	return TrendReport{
		Candidate:          candidate,
		BaselineWindow:     baseline,
		DriftWindow:        drift,
		ComparabilityKey:   compKey,
		MethodologyChanges: methodologyChanges,
		Regressions:        regressions,
		Drift:              driftSignal,
		WorkloadTrends:     trends,
		ProtectedWorkloads: protectedWorkloads,
		WindowsToStable:    stableWindows,
		Status:             status,
	}, nil
}

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
	bench "github.com/lychee-technology/forma/internal/e2e_harness/federated/benchmark"
)

type benchmarkPreset struct {
	Name          string       `json:"name"`
	Description   string       `json:"description"`
	RuntimeClass  string       `json:"runtime_class"`
	BaselineDir   string       `json:"baseline_dir"`
	CISafe        bool         `json:"ci_safe"`
	ExpectedUsage string       `json:"expected_usage"`
	Config        bench.Config `json:"config"`
}

var (
	newBenchmarkRunner = bench.NewRunner
	runValidationMode  = func(ctx context.Context, runner *bench.Runner) (*bench.RunResult, error) {
		return runner.Run(ctx)
	}
	runLiveMode = func(ctx context.Context, runner *bench.Runner, cfg bench.Config) (*bench.RunResult, error) {
		profile, err := bench.ResolveTierMixProfile(cfg.TierProfile)
		if err != nil {
			return nil, err
		}
		h, err := federated.NewFederatedTestHarness(ctx,
			federated.WithDuckDBResources(cfg.DuckDBThreads, cfg.DuckDBMemoryLimitMB, 0))
		if err != nil {
			return nil, fmt.Errorf("create federated harness: %w", err)
		}
		result, runErr := runner.RunWithHarness(ctx, h, profile)
		cleanupErr := h.Cleanup(ctx)
		if runErr != nil {
			if cleanupErr != nil {
				return nil, fmt.Errorf("%w; cleanup federated harness: %v", runErr, cleanupErr)
			}
			return nil, runErr
		}
		if cleanupErr != nil {
			return nil, fmt.Errorf("cleanup federated harness: %w", cleanupErr)
		}
		return result, nil
	}
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(runBenchmarkMain(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func runBenchmarkMain(ctx context.Context, args []string, out, errOut io.Writer) int {
	if len(args) == 0 {
		printUsage(out)
		return 1
	}
	switch args[0] {
	case "describe":
		return runDescribe(out)
	case "compare":
		return runCompare(args[1:], out, errOut)
	case "baseline":
		return runBaseline(ctx, args[1:], out, errOut)
	case "run":
		return runBenchmark(ctx, args[1:], out, errOut)
	case "trend":
		return runTrend(args[1:], out, errOut)
	case "concurrency-report":
		return runConcurrencyReport(args[1:], out, errOut)
	default:
		fmt.Fprintf(errOut, "unknown command %q\n", args[0])
		printUsage(out)
		return 1
	}
}

func printUsage(out io.Writer) {
	fmt.Fprintln(out, "Usage: benchmark <command> [options]")
	fmt.Fprintln(out, "")
	fmt.Fprintln(out, "Commands:")
	fmt.Fprintln(out, "  describe   Print benchmark schemas, workloads, and defaults")
	fmt.Fprintln(out, "  compare    Compare two benchmark summary artifacts")
	fmt.Fprintln(out, "  baseline   Capture a baseline artifact set for a preset")
	fmt.Fprintln(out, "  run        Execute benchmark validation or live runtime")
	fmt.Fprintln(out, "  trend      Analyze longitudinal benchmark trends and regressions")
	fmt.Fprintln(out, "  concurrency-report  Aggregate per-concurrency summaries into one p50/p95/p99 matrix")
}

func runDescribe(out io.Writer) int {
	payload := struct {
		SchemaDir    string                     `json:"schema_dir"`
		Defaults     bench.Config               `json:"defaults"`
		Generator    bench.GeneratorConfig      `json:"generator_defaults"`
		Presets      []benchmarkPreset          `json:"presets"`
		ScalePresets []bench.ScalePreset        `json:"scale_presets"`
		TierProfiles []bench.TierMixProfile     `json:"tier_profiles"`
		Schemas      []bench.SchemaFixture      `json:"schemas"`
		Workloads    []bench.WorkloadDefinition `json:"workloads"`
	}{
		SchemaDir:    bench.FixturesDir(),
		Defaults:     bench.DefaultConfig(),
		Generator:    bench.DefaultGeneratorConfig(),
		Presets:      defaultBenchmarkPresets(),
		ScalePresets: bench.DefaultScalePresets(),
		TierProfiles: bench.DefaultTierMixProfiles(),
		Schemas:      bench.DefaultSchemaFixtures(),
		Workloads:    bench.DefaultWorkloads(),
	}
	return writeJSON(out, payload)
}

func runBenchmark(ctx context.Context, args []string, out, errOut io.Writer) int {
	config, outputs, exitCode := parseRunConfig(args, errOut)
	if exitCode != 0 {
		return exitCode
	}
	return executeBenchmarkRun(ctx, config, outputs, out, errOut)
}

func runBaseline(ctx context.Context, args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(errOut)
	preset := flags.String("preset", "small", "Baseline preset: ci-smoke, small, small-live, medium, medium-live, or heavy-plan")
	outputDir := flags.String("output-dir", ".artifacts/benchmark", "Directory to store baseline captures")
	distribution := flags.String("distribution", string(bench.DistributionUniform), "Data distribution preset")
	compareTo := flags.String("compare-to", "", "Optional path to an existing benchmark-summary.json for diff output")
	channel := flags.String("channel", "", "Provenance channel: ci, nightly, manual")
	gitSHA := flags.String("git-sha", "", "Provenance git commit SHA")
	gitRef := flags.String("git-ref", "", "Provenance git ref")
	label := flags.String("label", "", "Provenance human-readable label")
	concurrency := flags.Int("concurrency", 0, "Override preset concurrency (0 = preset value)")
	duckDBThreads := flags.Int("duckdb-threads", 0, "Override DuckDB threads for live runs (0 = harness default)")
	duckDBMemoryMB := flags.Int("duckdb-memory-mb", 0, "Override DuckDB memory limit in MB for live runs (0 = harness default)")
	if err := flags.Parse(args); err != nil {
		return 1
	}
	config, dirName, err := baselinePresetConfig(*preset, bench.Distribution(*distribution))
	if err != nil {
		fmt.Fprintf(errOut, "baseline setup failed: %v\n", err)
		return 1
	}
	if *concurrency > 0 {
		config.Concurrency = *concurrency
	}
	config.DuckDBThreads = *duckDBThreads
	config.DuckDBMemoryLimitMB = *duckDBMemoryMB
	// Concurrent baselines get their own artifact directory (-c{N}); C<=1
	// keeps the preset directory so existing capture paths (and the
	// make benchmark-regression contract) are untouched.
	if config.Concurrency > 1 {
		dirName = fmt.Sprintf("%s-c%d", dirName, config.Concurrency)
	}
	outputs := runOutputs{
		baselineDir: filepath.Join(*outputDir, dirName),
		channel:     *channel,
		gitSHA:      *gitSHA,
		gitRef:      *gitRef,
		label:       *label,
	}
	exitCode := executeBenchmarkRun(ctx, config, outputs, out, errOut)
	if *compareTo == "" {
		return exitCode
	}
	if outputs.baselineDir == "" {
		return exitCode
	}
	diffExitCode := compareSummaryFiles(*compareTo, filepath.Join(outputs.baselineDir, "benchmark-summary.json"), out, errOut)
	if diffExitCode != 0 {
		return diffExitCode
	}
	return exitCode
}

func runCompare(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(errOut)
	baseline := flags.String("baseline", "", "Path to the baseline benchmark-summary.json")
	candidate := flags.String("candidate", "", "Path to the candidate benchmark-summary.json")
	diffOut := flags.String("diff-out", "", "Optional path for machine-readable diff output")
	if err := flags.Parse(args); err != nil {
		return 1
	}
	if strings.TrimSpace(*baseline) == "" || strings.TrimSpace(*candidate) == "" {
		fmt.Fprintln(errOut, "compare requires -baseline and -candidate")
		return 1
	}
	return compareSummaryFiles(*baseline, *candidate, out, errOut, *diffOut)
}

func compareSummaryFiles(baselinePath, candidatePath string, out, errOut io.Writer, diffOut ...string) int {
	baseline, err := bench.ReadSummaryReport(baselinePath)
	if err != nil {
		fmt.Fprintf(errOut, "failed to read baseline summary: %v\n", err)
		return 1
	}
	candidate, err := bench.ReadSummaryReport(candidatePath)
	if err != nil {
		fmt.Fprintf(errOut, "failed to read candidate summary: %v\n", err)
		return 1
	}
	diff := bench.CompareSummaryReports(baseline, candidate)
	if len(diffOut) > 0 && strings.TrimSpace(diffOut[0]) != "" {
		if err := bench.WriteDiffReport(diffOut[0], &diff); err != nil {
			fmt.Fprintf(errOut, "failed to write diff report: %v\n", err)
			return 1
		}
	}
	fmt.Fprintln(errOut, bench.FormatDiffSummary(diff))
	return writeJSON(out, diff)
}

// runConcurrencyReport aggregates one benchmark-summary.json per concurrency
// level into a single publishable markdown/JSON artifact (#104).
func runConcurrencyReport(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("concurrency-report", flag.ContinueOnError)
	flags.SetOutput(errOut)
	inputs := flags.String("inputs", "", "Comma-separated benchmark-summary.json paths (one per concurrency level)")
	inputDir := flags.String("input-dir", "", "Directory tree scanned for benchmark-summary.json files")
	mdOut := flags.String("md-out", "", "Path for the markdown report (omit to print to stdout)")
	jsonOut := flags.String("json-out", "", "Optional path for the JSON report")
	if err := flags.Parse(args); err != nil {
		return 1
	}

	var summaries []bench.SummaryReport
	if *inputDir != "" {
		collected, err := bench.CollectConcurrencySummaries(*inputDir)
		if err != nil {
			fmt.Fprintf(errOut, "concurrency-report failed: %v\n", err)
			return 1
		}
		summaries = append(summaries, collected...)
	}
	for _, path := range strings.Split(*inputs, ",") {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		summary, err := bench.ReadSummaryReport(path)
		if err != nil {
			fmt.Fprintf(errOut, "concurrency-report failed: %v\n", err)
			return 1
		}
		summaries = append(summaries, summary)
	}
	if len(summaries) == 0 {
		fmt.Fprintln(errOut, "concurrency-report requires -inputs or -input-dir")
		return 1
	}

	report, err := bench.BuildConcurrencyReport(summaries)
	if err != nil {
		fmt.Fprintf(errOut, "concurrency-report failed: %v\n", err)
		return 1
	}
	if err := bench.WriteConcurrencyReport(*jsonOut, *mdOut, report); err != nil {
		fmt.Fprintf(errOut, "concurrency-report failed: %v\n", err)
		return 1
	}
	if *mdOut == "" {
		fmt.Fprint(out, bench.FormatConcurrencyMarkdown(report))
	} else {
		fmt.Fprintf(out, "concurrency report written to %s\n", *mdOut)
	}
	if !report.Comparable {
		fmt.Fprintln(errOut, "warning: runs are not directly comparable; see report warnings")
	}
	return 0
}

func runTrend(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("trend", flag.ContinueOnError)
	flags.SetOutput(errOut)
	historyDir := flags.String("history-dir", "", "Directory containing historical benchmark-summary.json artifacts")
	candidate := flags.String("candidate", "", "Optional path to a specific candidate benchmark-summary.json")
	baselineWindow := flags.Int("baseline-window", 5, "Number of recent comparable runs to use as baseline")
	driftWindow := flags.Int("drift-window", 5, "Number of older comparable runs for drift detection")
	protectedWorkloads := flags.String("protected-workloads", "", "Comma-separated protected workload names (defaults to standard set)")
	jsonOut := flags.String("json-out", "", "Optional path for JSON trend report output")
	mdOut := flags.String("md-out", "", "Optional path for Markdown trend report output")
	if err := flags.Parse(args); err != nil {
		return 1
	}
	if strings.TrimSpace(*historyDir) == "" {
		fmt.Fprintln(errOut, "trend requires -history-dir")
		return 1
	}
	var protected []string
	if strings.TrimSpace(*protectedWorkloads) != "" {
		protected = parseWorkloads(*protectedWorkloads)
	} else {
		protected = bench.DefaultProtectedWorkloads()
	}
	report, err := bench.AnalyzeTrend(*historyDir, *candidate, *baselineWindow, *driftWindow, protected)
	if err != nil {
		fmt.Fprintf(errOut, "trend analysis failed: %v\n", err)
		return 1
	}
	if *jsonOut != "" {
		if err := bench.WriteTrendReport(*jsonOut, &report); err != nil {
			fmt.Fprintf(errOut, "failed to write trend report: %v\n", err)
			return 1
		}
	}
	if *mdOut != "" {
		if err := bench.WriteTrendReport(*mdOut, &report); err != nil {
			fmt.Fprintf(errOut, "failed to write trend markdown: %v\n", err)
			return 1
		}
	}
	fmt.Fprintln(errOut, bench.FormatTrendSummary(report))
	exitCode := writeJSON(out, report)
	if exitCode != 0 {
		return exitCode
	}
	if report.Status == bench.TrendStatusHardStopRegression || report.Status == bench.TrendStatusRegression {
		return 1
	}
	return 0
}

type runOutputs struct {
	jsonOut     string
	mdOut       string
	baselineDir string
	channel     string
	gitSHA      string
	gitRef      string
	label       string
}

func parseRunConfig(args []string, errOut io.Writer) (bench.Config, runOutputs, int) {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(errOut)
	mode := flags.String("mode", string(bench.ExecutionModeSmoke), "Execution mode: smoke, plan, or live")
	scale := flags.String("scale", string(bench.ScaleSmall), "Dataset scale: small, medium, or large")
	distribution := flags.String("distribution", string(bench.DistributionUniform), "Data distribution preset")
	iterations := flags.Int("iterations", 1, "Number of iterations to schedule")
	concurrency := flags.Int("concurrency", 1, "Number of concurrent workers to schedule")
	pageSize := flags.Int("page-size", 20, "Default page size for workload planning")
	tierProfile := flags.String("tier-profile", bench.DefaultTierMixProfile().Name, "Tier mix profile: balanced, high-hot, long-history, or explicit profile name")
	seed := flags.Int64("seed", 42, "Deterministic benchmark seed")
	tradeCount := flags.Int("trade-count", 0, "Override generated trade row count")
	duckDBThreads := flags.Int("duckdb-threads", 0, "Override DuckDB threads for live runs (0 = harness default)")
	duckDBMemoryMB := flags.Int("duckdb-memory-mb", 0, "Override DuckDB memory limit in MB for live runs (0 = harness default)")
	customerCount := flags.Int("customer-count", 0, "Override generated customer row count")
	securityCount := flags.Int("security-count", 0, "Override generated security row count")
	overlapRatio := flags.Float64("overlap-ratio", 0, "Override overlap ratio")
	deleteRatio := flags.Float64("delete-ratio", 0, "Override delete ratio")
	jsonOut := flags.String("json-out", "", "Optional path for JSON benchmark output")
	mdOut := flags.String("md-out", "", "Optional path for Markdown benchmark output")
	baselineDir := flags.String("baseline-dir", "", "Optional directory for baseline capture outputs")
	workloads := flags.String("workloads", "", "Comma-separated workload names (defaults to all)")
	channel := flags.String("channel", "", "Provenance channel: ci, nightly, manual")
	gitSHA := flags.String("git-sha", "", "Provenance git commit SHA")
	gitRef := flags.String("git-ref", "", "Provenance git ref")
	label := flags.String("label", "", "Provenance human-readable label")
	if err := flags.Parse(args); err != nil {
		return bench.Config{}, runOutputs{}, 1
	}
	cfg := bench.Config{
		Mode:          bench.ExecutionMode(*mode),
		Scale:         bench.Scale(*scale),
		Distribution:  bench.Distribution(*distribution),
		Iterations:    *iterations,
		Concurrency:   *concurrency,
		PageSize:      *pageSize,
		Seed:          *seed,
		TierProfile:   *tierProfile,
		TradeCount:    *tradeCount,
		CustomerCount: *customerCount,
		SecurityCount: *securityCount,
		OverlapRatio:  *overlapRatio,
		DeleteRatio:   *deleteRatio,
		Workloads:     parseWorkloads(*workloads),

		DuckDBThreads:       *duckDBThreads,
		DuckDBMemoryLimitMB: *duckDBMemoryMB,
	}.WithDefaults()
	outputs := runOutputs{jsonOut: *jsonOut, mdOut: *mdOut, baselineDir: *baselineDir}
	outputs.channel = *channel
	outputs.gitSHA = *gitSHA
	outputs.gitRef = *gitRef
	outputs.label = *label
	return cfg, outputs, 0
}

func executeBenchmarkRun(ctx context.Context, cfg bench.Config, outputs runOutputs, out, errOut io.Writer) int {
	runner, err := newBenchmarkRunner(cfg)
	if err != nil {
		fmt.Fprintf(errOut, "benchmark setup failed: %v\n", err)
		return 1
	}
	var result *bench.RunResult
	switch cfg.Mode {
	case bench.ExecutionModeLive:
		result, err = runLiveMode(ctx, runner, cfg)
	default:
		result, err = runValidationMode(ctx, runner)
	}
	if err != nil {
		fmt.Fprintf(errOut, "benchmark run failed: %v\n", err)
		return 1
	}
	if outputs.channel != "" || outputs.gitSHA != "" || outputs.gitRef != "" || outputs.label != "" {
		result.Provenance = &bench.RunProvenance{
			StartedAt:    result.StartedAt,
			CompletedAt:  result.CompletedAt,
			Channel:      outputs.channel,
			GitSHA:       outputs.gitSHA,
			GitRef:       outputs.gitRef,
			Label:        outputs.label,
			Mode:         string(result.Config.Mode),
			Scale:        string(result.Config.Scale),
			Distribution: string(result.Config.Distribution),
			TierProfile:  result.Config.TierProfile,
			Concurrency:  result.Config.Concurrency,
		}
	}
	if outputs.jsonOut != "" {
		if err := bench.WriteJSONReport(outputs.jsonOut, result); err != nil {
			fmt.Fprintf(errOut, "failed to write JSON report: %v\n", err)
			return 1
		}
	}
	if outputs.mdOut != "" {
		if err := bench.WriteMarkdownReport(outputs.mdOut, result); err != nil {
			fmt.Fprintf(errOut, "failed to write Markdown report: %v\n", err)
			return 1
		}
	}
	if outputs.baselineDir != "" {
		if err := bench.WriteBaselineCapture(outputs.baselineDir, result); err != nil {
			fmt.Fprintf(errOut, "failed to write baseline capture: %v\n", err)
			return 1
		}
	}
	fmt.Fprintln(errOut, bench.FormatConsoleSummary(result))
	exitCode := writeJSON(out, result)
	if exitCode != 0 {
		return exitCode
	}
	if !result.Passed {
		return 1
	}
	return 0
}

func baselinePresetConfig(rawPreset string, distribution bench.Distribution) (bench.Config, string, error) {
	preset, err := resolveBenchmarkPreset(rawPreset, distribution)
	if err != nil {
		return bench.Config{}, "", err
	}
	return preset.Config, preset.BaselineDir, nil
}

func defaultBenchmarkPresets() []benchmarkPreset {
	return []benchmarkPreset{
		{
			Name:          "ci-smoke",
			Description:   "Cheap CI-safe smoke validation with artifact capture.",
			RuntimeClass:  "fast",
			BaselineDir:   "ci-smoke-uniform",
			CISafe:        true,
			ExpectedUsage: "pull requests and quick local validation",
			Config:        bench.Config{Mode: bench.ExecutionModeSmoke, Scale: bench.ScaleSmall, Distribution: bench.DistributionUniform, Iterations: 1, PageSize: 20, Seed: 42, Workloads: []string{"baseline-page-1", "hot-selective-page"}}.WithDefaults(),
		},
		{
			Name:          "small-live",
			Description:   "Live small-scale baseline subset for routine benchmark evidence.",
			RuntimeClass:  "medium",
			BaselineDir:   "small-live-hotspot-overlap",
			CISafe:        false,
			ExpectedUsage: "local or controlled review baseline capture",
			Config:        bench.Config{Mode: bench.ExecutionModeLive, Scale: bench.ScaleSmall, Distribution: bench.DistributionHotspot, Iterations: 2, PageSize: 20, Seed: 42, TierProfile: bench.DefaultTierMixProfile().Name, Workloads: []string{"baseline-page-1", "customer-region-page", "security-symbol-page", "hot-selective-page", "eav-selective-page", "mixed-tier-window", "hot-only-window"}}.WithDefaults(),
		},
		{
			Name:          "medium-live",
			Description:   "Controlled medium-scale live baseline subset for regression review.",
			RuntimeClass:  "medium",
			BaselineDir:   "medium-live-zipf",
			CISafe:        false,
			ExpectedUsage: "manual or scheduled baseline capture",
			Config:        bench.Config{Mode: bench.ExecutionModeLive, Scale: bench.ScaleMedium, Distribution: bench.DistributionZipf, Iterations: 2, PageSize: 20, Seed: 42, TierProfile: bench.DefaultTierMixProfile().Name, Workloads: []string{"baseline-page-1", "hot-selective-page", "eav-selective-page", "mixed-tier-window", "hot-only-window", "cold-only-window", "deep-page-1000"}}.WithDefaults(),
		},
		{
			Name:          "heavy-plan",
			Description:   "Heavy planning-only workload set for manual or nightly review.",
			RuntimeClass:  "heavy",
			BaselineDir:   "heavy-plan-hotspot-overlap",
			CISafe:        false,
			ExpectedUsage: "manual or nightly planning review only",
			Config:        bench.Config{Mode: bench.ExecutionModePlan, Scale: bench.ScaleLarge, Distribution: bench.DistributionHotspot, Iterations: 3, PageSize: 20, Seed: 42, TierProfile: bench.DefaultTierMixProfile().Name, Workloads: bench.DefaultWorkloadNames()}.WithDefaults(),
		},
	}
}

func resolveBenchmarkPreset(rawPreset string, distribution bench.Distribution) (benchmarkPreset, error) {
	presetName := strings.ToLower(strings.TrimSpace(rawPreset))
	if presetName == "small" {
		presetName = "small-live"
	}
	if presetName == "medium" {
		presetName = "medium-live"
	}
	for _, preset := range defaultBenchmarkPresets() {
		if preset.Name != presetName {
			continue
		}
		resolved := preset
		if distribution != "" {
			resolved.Config.Distribution = distribution
			resolved.BaselineDir = fmt.Sprintf("%s-%s", resolved.Name, distribution)
		}
		return resolved, nil
	}
	return benchmarkPreset{}, fmt.Errorf("unknown baseline preset %q", rawPreset)
}

func parseWorkloads(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name != "" {
			result = append(result, name)
		}
	}
	return result
}

func writeJSON(out io.Writer, payload any) int {
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to encode JSON: %v\n", err)
		return 1
	}
	fmt.Fprintln(out, string(encoded))
	return 0
}

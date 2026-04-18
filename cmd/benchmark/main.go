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
		h, err := federated.NewFederatedTestHarness(ctx)
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
	case "baseline":
		return runBaseline(ctx, args[1:], out, errOut)
	case "run":
		return runBenchmark(ctx, args[1:], out, errOut)
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
	fmt.Fprintln(out, "  baseline   Capture a baseline artifact set for a preset")
	fmt.Fprintln(out, "  run        Execute benchmark validation or live runtime")
}

func runDescribe(out io.Writer) int {
	payload := struct {
		SchemaDir    string                     `json:"schema_dir"`
		Defaults     bench.Config               `json:"defaults"`
		Generator    bench.GeneratorConfig      `json:"generator_defaults"`
		ScalePresets []bench.ScalePreset        `json:"scale_presets"`
		TierProfiles []bench.TierMixProfile     `json:"tier_profiles"`
		Schemas      []bench.SchemaFixture      `json:"schemas"`
		Workloads    []bench.WorkloadDefinition `json:"workloads"`
	}{
		SchemaDir:    bench.FixturesDir(),
		Defaults:     bench.DefaultConfig(),
		Generator:    bench.DefaultGeneratorConfig(),
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
	preset := flags.String("preset", "small", "Baseline preset: small or medium")
	outputDir := flags.String("output-dir", ".artifacts/benchmark", "Directory to store baseline captures")
	distribution := flags.String("distribution", string(bench.DistributionUniform), "Data distribution preset")
	if err := flags.Parse(args); err != nil {
		return 1
	}
	config, dirName, err := baselinePresetConfig(*preset, bench.Distribution(*distribution))
	if err != nil {
		fmt.Fprintf(errOut, "baseline setup failed: %v\n", err)
		return 1
	}
	outputs := runOutputs{baselineDir: filepath.Join(*outputDir, dirName)}
	return executeBenchmarkRun(ctx, config, outputs, out, errOut)
}

type runOutputs struct {
	jsonOut     string
	mdOut       string
	baselineDir string
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
	customerCount := flags.Int("customer-count", 0, "Override generated customer row count")
	securityCount := flags.Int("security-count", 0, "Override generated security row count")
	overlapRatio := flags.Float64("overlap-ratio", 0, "Override overlap ratio")
	deleteRatio := flags.Float64("delete-ratio", 0, "Override delete ratio")
	jsonOut := flags.String("json-out", "", "Optional path for JSON benchmark output")
	mdOut := flags.String("md-out", "", "Optional path for Markdown benchmark output")
	baselineDir := flags.String("baseline-dir", "", "Optional directory for baseline capture outputs")
	workloads := flags.String("workloads", "", "Comma-separated workload names (defaults to all)")
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
	}.WithDefaults()
	outputs := runOutputs{jsonOut: *jsonOut, mdOut: *mdOut, baselineDir: *baselineDir}
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
	return writeJSON(out, result)
}

func baselinePresetConfig(rawPreset string, distribution bench.Distribution) (bench.Config, string, error) {
	preset := strings.ToLower(strings.TrimSpace(rawPreset))
	if distribution == "" {
		distribution = bench.DistributionUniform
	}
	switch preset {
	case "small":
		return bench.Config{Mode: bench.ExecutionModeSmoke, Scale: bench.ScaleSmall, Distribution: distribution, Iterations: 5, PageSize: 20, Seed: 42}.WithDefaults(), fmt.Sprintf("small-%s", distribution), nil
	case "medium":
		return bench.Config{Mode: bench.ExecutionModePlan, Scale: bench.ScaleMedium, Distribution: distribution, Iterations: 10, PageSize: 20, Seed: 42}.WithDefaults(), fmt.Sprintf("medium-%s", distribution), nil
	default:
		return bench.Config{}, "", fmt.Errorf("unknown baseline preset %q", rawPreset)
	}
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

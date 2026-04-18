package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	bench "github.com/lychee-technology/forma/internal/e2e_harness/federated/benchmark"
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
	fmt.Fprintln(out, "  run        Execute the scaffolded benchmark runner")
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
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(errOut)
	mode := flags.String("mode", string(bench.ExecutionModeSmoke), "Execution mode: smoke or plan")
	scale := flags.String("scale", string(bench.ScaleSmall), "Dataset scale: small, medium, or large")
	distribution := flags.String("distribution", string(bench.DistributionUniform), "Data distribution preset")
	iterations := flags.Int("iterations", 1, "Number of iterations to schedule")
	concurrency := flags.Int("concurrency", 1, "Number of concurrent workers to schedule")
	pageSize := flags.Int("page-size", 20, "Default page size for workload planning")
	seed := flags.Int64("seed", 42, "Deterministic benchmark seed")
	workloads := flags.String("workloads", "", "Comma-separated workload names (defaults to all)")
	if err := flags.Parse(args); err != nil {
		return 1
	}
	cfg := bench.Config{
		Mode:         bench.ExecutionMode(*mode),
		Scale:        bench.Scale(*scale),
		Distribution: bench.Distribution(*distribution),
		Iterations:   *iterations,
		Concurrency:  *concurrency,
		PageSize:     *pageSize,
		Seed:         *seed,
		Workloads:    parseWorkloads(*workloads),
	}.WithDefaults()
	runner, err := bench.NewRunner(cfg)
	if err != nil {
		fmt.Fprintf(errOut, "benchmark setup failed: %v\n", err)
		return 1
	}
	result, err := runner.Run(ctx)
	if err != nil {
		fmt.Fprintf(errOut, "benchmark run failed: %v\n", err)
		return 1
	}
	return writeJSON(out, result)
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

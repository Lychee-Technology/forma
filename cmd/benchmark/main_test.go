package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	bench "github.com/lychee-technology/forma/internal/e2e_harness/federated/benchmark"
)

func TestRunBenchmarkMainDescribe(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	exitCode := runBenchmarkMain(context.Background(), []string{"describe"}, &out, &errOut)
	if exitCode != 0 {
		t.Fatalf("describe returned exit code %d: %s", exitCode, errOut.String())
	}
	if out.Len() == 0 {
		t.Fatalf("describe should emit JSON output")
	}
}

func TestRunBenchmarkMainSmoke(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	exitCode := runBenchmarkMain(context.Background(), []string{"run", "-mode", "smoke"}, &out, &errOut)
	if exitCode != 0 {
		t.Fatalf("run returned exit code %d: %s", exitCode, errOut.String())
	}
	if out.Len() == 0 {
		t.Fatalf("run should emit JSON output")
	}
	if errOut.Len() == 0 {
		t.Fatalf("run should emit console summary to stderr")
	}
}

func TestRunBenchmarkMainBaseline(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	var errOut bytes.Buffer
	exitCode := runBenchmarkMain(context.Background(), []string{"baseline", "-preset", "ci-smoke", "-output-dir", dir}, &out, &errOut)
	if exitCode != 0 {
		t.Fatalf("baseline returned exit code %d: %s", exitCode, errOut.String())
	}
	for _, name := range []string{"benchmark-result.json", "benchmark-result.md", "benchmark-summary.json"} {
		matches, err := filepath.Glob(filepath.Join(dir, "ci-smoke*", name))
		if err != nil || len(matches) == 0 {
			t.Fatalf("expected baseline artifact %s to exist", name)
		}
		if _, err := os.Stat(matches[0]); err != nil {
			t.Fatalf("expected baseline artifact %s to exist: %v", name, err)
		}
	}
}

func TestResolveBenchmarkPresetSupportsAliasesAndPresets(t *testing.T) {
	small, err := resolveBenchmarkPreset("small", bench.DistributionUniform)
	if err != nil {
		t.Fatalf("resolveBenchmarkPreset small failed: %v", err)
	}
	if small.Name != "small-live" || small.Config.Mode != bench.ExecutionModeLive {
		t.Fatalf("expected small alias to resolve to small-live live preset, got %+v", small)
	}
	ci, err := resolveBenchmarkPreset("ci-smoke", bench.DistributionUniform)
	if err != nil {
		t.Fatalf("resolveBenchmarkPreset ci-smoke failed: %v", err)
	}
	if !ci.CISafe || ci.Config.Mode != bench.ExecutionModeSmoke {
		t.Fatalf("expected ci-smoke preset to be CI-safe smoke, got %+v", ci)
	}
}

func TestRunBenchmarkMainDescribeIncludesPresets(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	exitCode := runBenchmarkMain(context.Background(), []string{"describe"}, &out, &errOut)
	if exitCode != 0 {
		t.Fatalf("describe returned exit code %d: %s", exitCode, errOut.String())
	}
	if !bytes.Contains(out.Bytes(), []byte(`"presets"`)) {
		t.Fatalf("expected describe output to include presets: %s", out.String())
	}
}

func TestRunBenchmarkMainLiveUsesLiveExecutor(t *testing.T) {
	originalLiveMode := runLiveMode
	originalValidationMode := runValidationMode
	defer func() {
		runLiveMode = originalLiveMode
		runValidationMode = originalValidationMode
	}()

	var liveCalled bool
	runValidationMode = func(context.Context, *bench.Runner) (*bench.RunResult, error) {
		t.Fatalf("validation mode should not be called for live execution")
		return nil, nil
	}
	runLiveMode = func(_ context.Context, _ *bench.Runner, cfg bench.Config) (*bench.RunResult, error) {
		liveCalled = true
		if cfg.Mode != bench.ExecutionModeLive {
			t.Fatalf("expected live mode config, got %q", cfg.Mode)
		}
		if cfg.TierProfile != "high-hot" {
			t.Fatalf("expected tier profile override to be preserved, got %q", cfg.TierProfile)
		}
		return &bench.RunResult{
			Config:         cfg,
			Generator:      bench.GeneratorConfig{Scale: cfg.Scale, Distribution: cfg.Distribution},
			ValidationOnly: false,
			Passed:         true,
			Notes:          []string{"stub live execution"},
		}, nil
	}

	var out bytes.Buffer
	var errOut bytes.Buffer
	exitCode := runBenchmarkMain(context.Background(), []string{"run", "-mode", "live", "-tier-profile", "high-hot", "-workloads", "baseline-page-1"}, &out, &errOut)
	if exitCode != 0 {
		t.Fatalf("live run returned exit code %d: %s", exitCode, errOut.String())
	}
	if !liveCalled {
		t.Fatalf("expected live executor to be called")
	}
	if out.Len() == 0 {
		t.Fatalf("live run should emit JSON output")
	}
	if errOut.Len() == 0 {
		t.Fatalf("live run should emit console summary to stderr")
	}
}

func TestRunBenchmarkMainReturnsNonZeroForFailedBenchmarkResult(t *testing.T) {
	originalValidationMode := runValidationMode
	defer func() {
		runValidationMode = originalValidationMode
	}()
	runValidationMode = func(context.Context, *bench.Runner) (*bench.RunResult, error) {
		return &bench.RunResult{
			Passed:         false,
			FailureCount:   1,
			ValidationOnly: true,
			Generator:      bench.GeneratorConfig{Scale: bench.ScaleSmall, Distribution: bench.DistributionUniform},
			Notes:          []string{"failed benchmark result"},
		}, nil
	}

	var out bytes.Buffer
	var errOut bytes.Buffer
	exitCode := runBenchmarkMain(context.Background(), []string{"run", "-mode", "smoke"}, &out, &errOut)
	if exitCode == 0 {
		t.Fatalf("expected non-zero exit code for failed benchmark result")
	}
	if out.Len() == 0 {
		t.Fatalf("expected JSON output even on failed benchmark result")
	}
}

func TestRunBenchmarkMainCompare(t *testing.T) {
	dir := t.TempDir()
	baseline := bench.SummaryReport{Metadata: bench.ArtifactMetadata{BenchmarkID: "bench-a"}, Passed: true, QPS: 10, Avg: 10}
	candidate := bench.SummaryReport{Metadata: bench.ArtifactMetadata{BenchmarkID: "bench-b"}, Passed: false, FailureCount: 1, QPS: 8, Avg: 15}
	baselinePath := filepath.Join(dir, "baseline-summary.json")
	candidatePath := filepath.Join(dir, "candidate-summary.json")
	diffPath := filepath.Join(dir, "diff.json")
	writeSummaryFixture(t, baselinePath, baseline)
	writeSummaryFixture(t, candidatePath, candidate)

	var out bytes.Buffer
	var errOut bytes.Buffer
	exitCode := runBenchmarkMain(context.Background(), []string{"compare", "-baseline", baselinePath, "-candidate", candidatePath, "-diff-out", diffPath}, &out, &errOut)
	if exitCode != 0 {
		t.Fatalf("compare returned exit code %d: %s", exitCode, errOut.String())
	}
	if out.Len() == 0 {
		t.Fatalf("compare should emit JSON output")
	}
	if _, err := os.Stat(diffPath); err != nil {
		t.Fatalf("expected diff output to exist: %v", err)
	}
	if errOut.Len() == 0 {
		t.Fatalf("compare should emit console diff summary")
	}
}

func writeSummaryFixture(t *testing.T, path string, summary bench.SummaryReport) {
	t.Helper()
	data, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("marshal summary fixture: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write summary fixture: %v", err)
	}
}

package main

import (
	"bytes"
	"context"
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
	exitCode := runBenchmarkMain(context.Background(), []string{"baseline", "-preset", "small", "-output-dir", dir}, &out, &errOut)
	if exitCode != 0 {
		t.Fatalf("baseline returned exit code %d: %s", exitCode, errOut.String())
	}
	for _, name := range []string{"benchmark-result.json", "benchmark-result.md", "benchmark-summary.json"} {
		matches, err := filepath.Glob(filepath.Join(dir, "small-*", name))
		if err != nil || len(matches) == 0 {
			t.Fatalf("expected baseline artifact %s to exist", name)
		}
		if _, err := os.Stat(matches[0]); err != nil {
			t.Fatalf("expected baseline artifact %s to exist: %v", name, err)
		}
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

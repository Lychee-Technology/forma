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

// TestRunBenchmarkMainBaselineTierProfileOverride verifies -tier-profile on
// the baseline subcommand overrides the preset's tier mix profile (#100
// final-review Fix 4). ci-smoke is smoke mode (bench.Runner.Run, not
// RunWithHarness) so this runs fast with no Docker/live harness required.
func TestRunBenchmarkMainBaselineTierProfileOverride(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	var errOut bytes.Buffer
	exitCode := runBenchmarkMain(context.Background(), []string{
		"baseline",
		"-preset", "ci-smoke",
		"-tier-profile", "high-hot-40-20-40",
		"-output-dir", dir,
	}, &out, &errOut)
	if exitCode != 0 {
		t.Fatalf("baseline returned exit code %d: %s", exitCode, errOut.String())
	}
	matches, err := filepath.Glob(filepath.Join(dir, "ci-smoke*", "benchmark-result.json"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("expected baseline artifact benchmark-result.json to exist, err=%v matches=%v", err, matches)
	}
	data, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("failed to read benchmark-result.json: %v", err)
	}
	var captured struct {
		Config struct {
			TierProfile string `json:"tier_profile"`
		} `json:"config"`
	}
	if err := json.Unmarshal(data, &captured); err != nil {
		t.Fatalf("failed to unmarshal benchmark-result.json: %v", err)
	}
	if captured.Config.TierProfile != "high-hot-40-20-40" {
		t.Fatalf("expected tier_profile override high-hot-40-20-40, got %q", captured.Config.TierProfile)
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

func TestResolveBenchmarkPresetHeavyLive(t *testing.T) {
	preset, err := resolveBenchmarkPreset("heavy", bench.DistributionHotspot)
	if err != nil {
		t.Fatalf("resolveBenchmarkPreset heavy failed: %v", err)
	}
	if preset.Name != "heavy-live" {
		t.Fatalf("expected heavy alias to resolve to heavy-live, got %q", preset.Name)
	}
	if preset.Config.Mode != bench.ExecutionModeLive || preset.Config.Scale != bench.ScaleLarge {
		t.Fatalf("expected live/large heavy-live config, got %+v", preset.Config)
	}
	if preset.Config.TruthPassSampleCap != 10000 {
		t.Fatalf("expected heavy-live truth-pass sample cap 10000, got %d", preset.Config.TruthPassSampleCap)
	}
	if preset.Config.DuckDBMemoryLimitMB != 8192 {
		t.Fatalf("expected heavy-live DuckDB memory 8192MB, got %d", preset.Config.DuckDBMemoryLimitMB)
	}
	if len(preset.Config.Workloads) != len(bench.DefaultWorkloadNames()) {
		t.Fatalf("expected the full workload matrix, got %d workloads", len(preset.Config.Workloads))
	}
	if preset.CISafe {
		t.Fatal("heavy-live must not be CI-safe")
	}
	if preset.BaselineDir != "heavy-live-hotspot-overlap" {
		t.Fatalf("unexpected baseline dir %q", preset.BaselineDir)
	}
}

// TestResolveBenchmarkPresetHeavyLiveDefaultsToPresetDistribution verifies
// that omitting -distribution on the baseline subcommand no longer silently
// overrides a preset's own distribution with DistributionUniform (#100 PR
// #157 Fix A). Before the fix, runBaseline's -distribution flag defaulted to
// "uniform", so resolveBenchmarkPreset("heavy-live", "uniform") always
// replaced heavy-live's hotspot-overlap distribution.
func TestResolveBenchmarkPresetHeavyLiveDefaultsToPresetDistribution(t *testing.T) {
	preset, err := resolveBenchmarkPreset("heavy-live", "")
	if err != nil {
		t.Fatalf("resolveBenchmarkPreset heavy-live failed: %v", err)
	}
	if preset.Config.Distribution != bench.DistributionHotspot {
		t.Fatalf("omitted -distribution must keep preset hotspot-overlap, got %q", preset.Config.Distribution)
	}
	if preset.BaselineDir != "heavy-live-hotspot-overlap" {
		t.Fatalf("omitted -distribution must keep preset baseline dir, got %q", preset.BaselineDir)
	}
}

func TestApplyDuckDBOverridesKeepsPresetValues(t *testing.T) {
	cfg := bench.Config{DuckDBThreads: 4, DuckDBMemoryLimitMB: 8192}
	got := applyDuckDBOverrides(cfg, 0, 0)
	if got.DuckDBThreads != 4 || got.DuckDBMemoryLimitMB != 8192 {
		t.Fatalf("zero flags must keep preset resources, got %+v", got)
	}
	got = applyDuckDBOverrides(cfg, 8, 16384)
	if got.DuckDBThreads != 8 || got.DuckDBMemoryLimitMB != 16384 {
		t.Fatalf("positive flags must override preset resources, got %+v", got)
	}
}

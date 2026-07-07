package main

import (
	"fmt"
	"strings"

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
		{
			Name:          "heavy-live",
			Description:   "Full live workload matrix at large scale for capacity-aware baseline capture.",
			RuntimeClass:  "heavy",
			BaselineDir:   "heavy-live-hotspot-overlap",
			CISafe:        false,
			ExpectedUsage: "manual capacity-aware baseline capture only (idle machine, hours)",
			Config:        bench.Config{Mode: bench.ExecutionModeLive, Scale: bench.ScaleLarge, Distribution: bench.DistributionHotspot, Iterations: 2, PageSize: 20, Seed: 42, TierProfile: bench.DefaultTierMixProfile().Name, Workloads: bench.DefaultWorkloadNames(), TruthPassSampleCap: 10000, DuckDBMemoryLimitMB: 8192}.WithDefaults(),
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
	if presetName == "heavy" {
		presetName = "heavy-live"
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

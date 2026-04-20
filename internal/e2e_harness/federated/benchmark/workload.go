package benchmark

import (
	"fmt"
	"strings"
)

// WorkloadCategory groups benchmark workloads by intent.
type WorkloadCategory string

// OracleMode declares how expected benchmark results are derived.
type OracleMode string

const (
	WorkloadCategoryPagination WorkloadCategory = "pagination"
	WorkloadCategoryFilter     WorkloadCategory = "filter"
	WorkloadCategoryDeepPage   WorkloadCategory = "deep-pagination"
	WorkloadCategoryTierMix    WorkloadCategory = "tier-mix"

	OracleModeLoadedState OracleMode = "loaded-state"
	OracleModeTruthPass   OracleMode = "truth-pass"
)

// WorkloadDefinition declares a benchmark workload.
type WorkloadDefinition struct {
	Name                  string           `json:"name"`
	Description           string           `json:"description"`
	Category              WorkloadCategory `json:"category"`
	TargetSchema          string           `json:"target_schema"`
	FilterAttribute       string           `json:"filter_attribute,omitempty"`
	FilterValue           string           `json:"filter_value,omitempty"`
	FilterConditions      map[string]any   `json:"filter_conditions,omitempty"`
	PageSize              int              `json:"page_size"`
	PageNumber            int              `json:"page_number"`
	SupportsDistributions []Distribution   `json:"supports_distributions"`
	PreferHot             bool             `json:"prefer_hot,omitempty"`
	UsesEAVFilter         bool             `json:"uses_eav_filter,omitempty"`
	LargePageJump         bool             `json:"large_page_jump,omitempty"`
	OracleMode            OracleMode       `json:"oracle_mode,omitempty"`
}

// DefaultWorkloads returns the initial declarative workload matrix.
func DefaultWorkloads() []WorkloadDefinition {
	return []WorkloadDefinition{
		{
			Name:                  "baseline-page-1",
			Description:           "Unfiltered first page ordered by trade time descending.",
			Category:              WorkloadCategoryPagination,
			TargetSchema:          "trade",
			PageSize:              20,
			PageNumber:            1,
			SupportsDistributions: allDistributions(),
			OracleMode:            OracleModeLoadedState,
		},
		{
			Name:                  "customer-region-page",
			Description:           "Customer region filter to validate schema-scoped non-trade execution.",
			Category:              WorkloadCategoryFilter,
			TargetSchema:          "customer",
			FilterAttribute:       "region",
			FilterValue:           "NA",
			PageSize:              20,
			PageNumber:            1,
			SupportsDistributions: allDistributions(),
			OracleMode:            OracleModeLoadedState,
		},
		{
			Name:                  "security-symbol-page",
			Description:           "Security symbol filter to validate schema-scoped reference lookups.",
			Category:              WorkloadCategoryFilter,
			TargetSchema:          "security",
			FilterAttribute:       "symbol",
			FilterValue:           "SYM00001",
			PageSize:              20,
			PageNumber:            1,
			SupportsDistributions: allDistributions(),
			OracleMode:            OracleModeLoadedState,
		},
		{
			Name:                  "hot-selective-page",
			Description:           "High-selectivity hot-column filter with pagination on trade rows.",
			Category:              WorkloadCategoryFilter,
			TargetSchema:          "trade",
			FilterAttribute:       "symbol",
			FilterValue:           "SYM00001",
			FilterConditions:      map[string]any{"symbol": "SYM00001"},
			PageSize:              20,
			PageNumber:            1,
			SupportsDistributions: allDistributions(),
		},
		{
			Name:                  "hot-low-selectivity-page",
			Description:           "Lower-selectivity hot-column filter with pagination on trade rows.",
			Category:              WorkloadCategoryFilter,
			TargetSchema:          "trade",
			FilterAttribute:       "tradeType",
			FilterValue:           "0",
			FilterConditions:      map[string]any{"tradeType": 0},
			PageSize:              20,
			PageNumber:            1,
			SupportsDistributions: allDistributions(),
			PreferHot:             true,
		},
		{
			Name:                  "eav-selective-page",
			Description:           "EAV-backed filter with paginated trade results.",
			Category:              WorkloadCategoryFilter,
			TargetSchema:          "trade",
			FilterAttribute:       "exchange",
			FilterValue:           "NYSE",
			FilterConditions:      map[string]any{"exchange": "NYSE"},
			PageSize:              20,
			PageNumber:            1,
			SupportsDistributions: allDistributions(),
			UsesEAVFilter:         true,
		},
		{
			Name:                  "mixed-hot-eav-page",
			Description:           "Mixed hot and EAV filter workload with paginated trade results.",
			Category:              WorkloadCategoryFilter,
			TargetSchema:          "trade",
			FilterAttribute:       "symbol",
			FilterValue:           "SYM00001",
			FilterConditions:      map[string]any{"symbol": "SYM00001", "exchange": "NYSE"},
			PageSize:              20,
			PageNumber:            1,
			SupportsDistributions: allDistributions(),
			PreferHot:             true,
			UsesEAVFilter:         true,
		},
		{
			Name:                  "mixed-tier-window",
			Description:           "Time-window query expected to touch cold and hot tiers.",
			Category:              WorkloadCategoryTierMix,
			TargetSchema:          "trade",
			PageSize:              50,
			PageNumber:            1,
			SupportsDistributions: []Distribution{DistributionTemporal, DistributionHotspot, DistributionUniform},
			OracleMode:            OracleModeLoadedState,
		},
		{
			Name:                  "hot-only-window",
			Description:           "Recent time-window query expected to stay within hot rows.",
			Category:              WorkloadCategoryTierMix,
			TargetSchema:          "trade",
			PageSize:              50,
			PageNumber:            1,
			SupportsDistributions: []Distribution{DistributionTemporal, DistributionHotspot, DistributionUniform},
			PreferHot:             true,
			OracleMode:            OracleModeLoadedState,
		},
		{
			Name:                  "cold-only-window",
			Description:           "Historical time-window query expected to stay within cold tiers.",
			Category:              WorkloadCategoryTierMix,
			TargetSchema:          "trade",
			PageSize:              50,
			PageNumber:            1,
			SupportsDistributions: []Distribution{DistributionTemporal, DistributionHotspot, DistributionUniform},
			OracleMode:            OracleModeLoadedState,
		},
		{
			Name:                  "deep-page-1000",
			Description:           "Deep pagination baseline at page 1,000 using LIMIT/OFFSET semantics.",
			Category:              WorkloadCategoryDeepPage,
			TargetSchema:          "trade",
			PageSize:              20,
			PageNumber:            1000,
			SupportsDistributions: allDistributions(),
			LargePageJump:         true,
			OracleMode:            OracleModeLoadedState,
		},
		{
			Name:                  "deep-page-100000",
			Description:           "Large page jump benchmark at page 100,000.",
			Category:              WorkloadCategoryDeepPage,
			TargetSchema:          "trade",
			PageSize:              20,
			PageNumber:            100000,
			SupportsDistributions: allDistributions(),
			LargePageJump:         true,
			OracleMode:            OracleModeLoadedState,
		},
	}
}

// SupportsDistribution reports whether a workload can run for a distribution.
func (w WorkloadDefinition) SupportsDistribution(dist Distribution) bool {
	for _, supported := range w.SupportsDistributions {
		if supported == dist {
			return true
		}
	}
	return false
}

// DerivedOffset returns the offset implied by page size and page number.
func (w WorkloadDefinition) DerivedOffset(defaultPageSize int) int {
	pageSize := w.PageSize
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	pageNumber := w.PageNumber
	if pageNumber <= 1 {
		return 0
	}
	return (pageNumber - 1) * pageSize
}

// UsesSimpleFilter reports whether the workload is representable via the current harness filter model.
func (w WorkloadDefinition) UsesSimpleFilter() bool {
	return strings.TrimSpace(w.FilterAttribute) != ""
}

// ResolvedFilterConditions returns the filter map used by executable workloads.
func (w WorkloadDefinition) ResolvedFilterConditions() map[string]any {
	if len(w.FilterConditions) > 0 {
		conditions := make(map[string]any, len(w.FilterConditions))
		for key, value := range w.FilterConditions {
			conditions[key] = value
		}
		return conditions
	}
	if strings.TrimSpace(w.FilterAttribute) == "" {
		return nil
	}
	return map[string]any{w.FilterAttribute: w.FilterValue}
}

// InferredOracleMode returns the workload-class default oracle mode.
func (w WorkloadDefinition) InferredOracleMode() OracleMode {
	// Trade filter workloads use a truth-pass oracle because loaded-state-only
	// reconstruction can diverge from executable federated filter semantics.
	if w.Category == WorkloadCategoryFilter && w.TargetSchema == "trade" {
		return OracleModeTruthPass
	}
	return OracleModeLoadedState
}

// ResolvedOracleMode returns the explicit oracle override or the inferred default.
func (w WorkloadDefinition) ResolvedOracleMode() OracleMode {
	if w.OracleMode == "" {
		return w.InferredOracleMode()
	}
	return w.OracleMode
}

// DefaultWorkloadNames returns the full default workload set.
func DefaultWorkloadNames() []string {
	workloads := DefaultWorkloads()
	names := make([]string, 0, len(workloads))
	for _, workload := range workloads {
		names = append(names, workload.Name)
	}
	return names
}

// ResolveWorkloads resolves named workloads from the default matrix.
func ResolveWorkloads(names []string) ([]WorkloadDefinition, error) {
	all := DefaultWorkloads()
	index := make(map[string]WorkloadDefinition, len(all))
	for _, workload := range all {
		index[workload.Name] = workload
	}
	resolved := make([]WorkloadDefinition, 0, len(names))
	for _, name := range names {
		workload, ok := index[name]
		if !ok {
			return nil, fmt.Errorf("unknown workload %q", name)
		}
		resolved = append(resolved, workload)
	}
	return resolved, nil
}

func allDistributions() []Distribution {
	return []Distribution{
		DistributionUniform,
		DistributionZipf,
		DistributionTemporal,
		DistributionPartitionSkew,
		DistributionHotspot,
	}
}

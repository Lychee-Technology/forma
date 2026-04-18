package benchmark

import "fmt"

// WorkloadCategory groups benchmark workloads by intent.
type WorkloadCategory string

const (
	WorkloadCategoryPagination WorkloadCategory = "pagination"
	WorkloadCategoryFilter     WorkloadCategory = "filter"
	WorkloadCategoryDeepPage   WorkloadCategory = "deep-pagination"
	WorkloadCategoryTierMix    WorkloadCategory = "tier-mix"
)

// WorkloadDefinition declares a benchmark workload.
type WorkloadDefinition struct {
	Name                   string           `json:"name"`
	Description            string           `json:"description"`
	Category               WorkloadCategory `json:"category"`
	TargetSchema           string           `json:"target_schema"`
	PageSize               int              `json:"page_size"`
	PageNumber             int              `json:"page_number"`
	SupportsDistributions  []Distribution   `json:"supports_distributions"`
	PreferHot              bool             `json:"prefer_hot,omitempty"`
	UsesEAVFilter          bool             `json:"uses_eav_filter,omitempty"`
	LargePageJump          bool             `json:"large_page_jump,omitempty"`
	Phase1ExecutionStubbed bool             `json:"phase1_execution_stubbed"`
}

// DefaultWorkloads returns the initial declarative workload matrix.
func DefaultWorkloads() []WorkloadDefinition {
	return []WorkloadDefinition{
		{
			Name:                   "baseline-page-1",
			Description:            "Unfiltered first page ordered by trade time descending.",
			Category:               WorkloadCategoryPagination,
			TargetSchema:           "trade",
			PageSize:               20,
			PageNumber:             1,
			SupportsDistributions:  allDistributions(),
			Phase1ExecutionStubbed: true,
		},
		{
			Name:                   "hot-selective-page",
			Description:            "High-selectivity hot-column filter with pagination on trade rows.",
			Category:               WorkloadCategoryFilter,
			TargetSchema:           "trade",
			PageSize:               20,
			PageNumber:             1,
			SupportsDistributions:  allDistributions(),
			Phase1ExecutionStubbed: true,
		},
		{
			Name:                   "eav-selective-page",
			Description:            "EAV-backed filter with paginated trade results.",
			Category:               WorkloadCategoryFilter,
			TargetSchema:           "trade",
			PageSize:               20,
			PageNumber:             1,
			SupportsDistributions:  allDistributions(),
			UsesEAVFilter:          true,
			Phase1ExecutionStubbed: true,
		},
		{
			Name:                   "mixed-tier-window",
			Description:            "Time-window query expected to touch cold and hot tiers.",
			Category:               WorkloadCategoryTierMix,
			TargetSchema:           "trade",
			PageSize:               50,
			PageNumber:             1,
			SupportsDistributions:  []Distribution{DistributionTemporal, DistributionHotspot, DistributionUniform},
			Phase1ExecutionStubbed: true,
		},
		{
			Name:                   "deep-page-1000",
			Description:            "Deep pagination baseline at page 1,000 using LIMIT/OFFSET semantics.",
			Category:               WorkloadCategoryDeepPage,
			TargetSchema:           "trade",
			PageSize:               20,
			PageNumber:             1000,
			SupportsDistributions:  allDistributions(),
			LargePageJump:          true,
			Phase1ExecutionStubbed: true,
		},
		{
			Name:                   "deep-page-100000",
			Description:            "Large page jump benchmark at page 100,000.",
			Category:               WorkloadCategoryDeepPage,
			TargetSchema:           "trade",
			PageSize:               20,
			PageNumber:             100000,
			SupportsDistributions:  allDistributions(),
			LargePageJump:          true,
			Phase1ExecutionStubbed: true,
		},
	}
}

// DefaultWorkloadNames returns the full phase-1 workload set.
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

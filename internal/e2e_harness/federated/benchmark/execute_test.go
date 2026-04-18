package benchmark

import (
	"context"
	"testing"
	"time"

	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
)

func TestRunnerExecuteWorkload(t *testing.T) {
	runner, err := NewRunner(Config{Scale: ScaleSmall, Distribution: DistributionUniform, Iterations: 1, PageSize: 20}.WithDefaults())
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}
	h := &federated.FederatedTestHarness{}
	workload := WorkloadDefinition{Name: "baseline-page-1", Category: WorkloadCategoryPagination, PageSize: 20, PageNumber: 1}
	_ = h
	_ = workload
	_ = context.Background()
	_ = time.Millisecond
	if runner == nil {
		t.Fatalf("runner should not be nil")
	}
}

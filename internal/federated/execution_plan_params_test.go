package federated

import (
	"testing"

	"github.com/lychee-technology/forma/internal/model"
)

// TestRecordTranslation_CapturesParams pins #173: when execution-plan
// capture is on, the rendered DuckDB source carries its bind parameters in
// string form so diagnostic artifacts can replay the exact query.
func TestRecordTranslation_CapturesParams(t *testing.T) {
	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	planCtx := newDuckDBExecutionPlanContext(opts)

	planCtx.recordTranslation("SELECT 1 WHERE a = ? AND b = ?", []any{"x", int64(42)}, 3,
		&model.FederatedAttributeQuery{UseMainAsAnchor: true})

	sources := opts.ExecutionPlan.Sources
	if len(sources) != 1 {
		t.Fatalf("sources = %d, want 1", len(sources))
	}
	got := sources[0].Params
	want := []string{"x", "42"}
	if len(got) != len(want) {
		t.Fatalf("params = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("params[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestRecordTranslation_NoCaptureWhenDisabled pins that Params (and the
// source entry itself) are only recorded under IncludeExecutionPlan.
func TestRecordTranslation_NoCaptureWhenDisabled(t *testing.T) {
	opts := &model.FederatedQueryOptions{}
	planCtx := newDuckDBExecutionPlanContext(opts)

	planCtx.recordTranslation("SELECT 1", []any{"x"}, 1, nil)

	if opts.ExecutionPlan != nil {
		t.Fatalf("execution plan recorded despite capture disabled: %+v", opts.ExecutionPlan)
	}
}

func TestFormatPlanParams_Empty(t *testing.T) {
	if got := formatPlanParams(nil); got != nil {
		t.Fatalf("formatPlanParams(nil) = %v, want nil", got)
	}
}

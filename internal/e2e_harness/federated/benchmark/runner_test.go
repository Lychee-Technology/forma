package benchmark

import (
	"context"
	"testing"
)

func TestRunnerRunSmoke(t *testing.T) {
	runner, err := NewRunner(DefaultConfig())
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}
	result, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if !result.ValidationOnly {
		t.Fatalf("expected validation-only result")
	}
	if len(result.Workloads) == 0 {
		t.Fatalf("expected resolved workloads")
	}
	if len(result.Schemas) == 0 {
		t.Fatalf("expected loaded schemas")
	}
}

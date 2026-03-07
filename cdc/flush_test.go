package cdc

import (
	"context"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestRunOnceRequiresSchemaRegistry(t *testing.T) {
	err := RunOnce(context.Background(), Config{}, nil, false, zap.NewNop(), nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "schema registry is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunnerRunOnceRequiresSchemaRegistry(t *testing.T) {
	runner := NewRunner(zap.NewNop())
	err := runner.RunOnce(context.Background(), Config{}, nil, false, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "schema registry is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunToolMain_UnknownCommand(t *testing.T) {
	var out bytes.Buffer
	exitCode := runToolMain(context.Background(), []string{"nope"}, &out)
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}
	if !strings.Contains(out.String(), `unknown command "nope"`) {
		t.Fatalf("expected unknown command output, got %q", out.String())
	}
}

func TestRunToolMain_ExitBehavior(t *testing.T) {
	var out bytes.Buffer
	exitCode := runToolMain(context.Background(), []string{"cdc-flush"}, &out)
	if exitCode != 1 {
		t.Fatalf("expected exit code 1 for cdc-flush failure, got %d", exitCode)
	}

	out.Reset()
	exitCode = runToolMain(context.Background(), []string{"generate-attributes"}, &out)
	if exitCode != 0 {
		t.Fatalf("expected exit code 0 for generate-attributes failure, got %d", exitCode)
	}
	if !strings.Contains(out.String(), "either -schema or -schema-file must be provided") {
		t.Fatalf("expected generate-attributes error output, got %q", out.String())
	}
}

func TestRunToolMain_Success(t *testing.T) {
	tempDir := t.TempDir()
	schemaPath := filepath.Join(tempDir, "schema.json")
	outputPath := filepath.Join(tempDir, "schema_attributes.json")
	if err := os.WriteFile(schemaPath, []byte(`{"type":"object","properties":{"name":{"type":"string"}}}`), 0o644); err != nil {
		t.Fatalf("write schema: %v", err)
	}

	var out bytes.Buffer
	exitCode := runToolMain(context.Background(), []string{
		"generate-attributes",
		"-schema-file", schemaPath,
		"-out", outputPath,
		"-init",
	}, &out)
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d output=%q", exitCode, out.String())
	}
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("expected output file: %v", err)
	}
}

func TestRunToolMain_ValidateSchemaConsistencySuccess(t *testing.T) {
	old := runValidateSchemaConsistencyFn
	defer func() { runValidateSchemaConsistencyFn = old }()

	called := false
	runValidateSchemaConsistencyFn = func(ctx context.Context, args []string) error {
		called = true
		if len(args) != 1 || args[0] != "-h" {
			t.Fatalf("unexpected args: %v", args)
		}
		return nil
	}

	var out bytes.Buffer
	exitCode := runToolMain(context.Background(), []string{"validate-schema-consistency", "-h"}, &out)
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d output=%q", exitCode, out.String())
	}
	if !called {
		t.Fatal("expected validate-schema-consistency command to run")
	}
}

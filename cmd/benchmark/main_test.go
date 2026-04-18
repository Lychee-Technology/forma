package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
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

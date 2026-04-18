package main

import (
	"bytes"
	"context"
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

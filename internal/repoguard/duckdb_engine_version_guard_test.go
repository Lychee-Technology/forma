package repoguard

import (
	"io/fs"
	"strings"
	"testing"
)

// TestCIAssertsDuckDBEngineVersionOnAmd64 pins the runtime engine-version
// assertion to CI's e2e job, the amd64 job that runs DuckDB. The test job
// runs the same assertion as part of the unit suite, but on arm64 only, and
// duckdb-go loads a separate native library per platform (#750). Comment
// lines are skipped so prose never satisfies the guard.
func TestCIAssertsDuckDBEngineVersionOnAmd64(t *testing.T) {
	wf, err := fs.ReadFile(repo, ".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("read ci.yml: %v", err)
	}
	job := workflowJobBlock(t, string(wf), "e2e")
	var hasAmd64Runner, hasVersionStep bool
	for _, line := range strings.Split(job, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		if trimmed == "runs-on: ubuntu-latest" {
			hasAmd64Runner = true
		}
		if strings.Contains(trimmed, "./internal/duckdbinit") && strings.Contains(trimmed, "TestRunningDuckDBEngineIsThePinnedVersion") {
			hasVersionStep = true
		}
	}
	if !hasAmd64Runner {
		t.Error("ci.yml e2e job no longer runs on ubuntu-latest (amd64): move the engine-version assertion to a job that does (#750)")
	}
	if !hasVersionStep {
		t.Error("ci.yml e2e job does not run TestRunningDuckDBEngineIsThePinnedVersion: no amd64 CI job would check which DuckDB engine it loaded (#750)")
	}
}

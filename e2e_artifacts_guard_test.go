package forma

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// TestProductionHarnessJobsUploadArtifactsOnFailure pins #718: a failing
// production-harness test dumps its diagnostics under .artifacts/e2e/
// (internal/e2e_harness/production/artifacts.go), and a hosted runner
// discards them with the job. Every job that runs the harness must therefore
// upload that tree in a later step gated on failure(): a step placed before
// the harness run never sees its failure, and an ungated one would upload on
// every run. Jobs are found by what they run, not listed, so a new harness
// job is covered too. Comment lines are skipped so prose never satisfies the
// guard, as in the sibling workflow guards.
func TestProductionHarnessJobsUploadArtifactsOnFailure(t *testing.T) {
	for _, tc := range []struct {
		workflow string
		known    []string // jobs that must be found, so the guard cannot lose its targets
	}{
		{".github/workflows/ci.yml", []string{"e2e"}},
		{".github/workflows/nightly-e2e.yml", []string{"production-race"}},
	} {
		wf, err := os.ReadFile(tc.workflow)
		if err != nil {
			t.Fatalf("read %s: %v", tc.workflow, err)
		}
		var found []string
		for _, job := range workflowJobNames(string(wf)) {
			steps := workflowJobSteps(workflowJobBlock(t, string(wf), job))
			harness := slices.IndexFunc(steps, runsProductionHarness)
			if harness == -1 {
				continue
			}
			found = append(found, job)
			if !slices.ContainsFunc(steps[harness+1:], uploadsE2EArtifactsOnFailure) {
				t.Errorf("%s job %q runs the production e2e harness but no later step uploads .artifacts/e2e/ with `if: failure()`: a failing run's diagnostics die with the runner (#718)", tc.workflow, job)
			}
		}
		for _, job := range tc.known {
			if !slices.Contains(found, job) {
				t.Errorf("%s job %q no longer runs the production e2e harness as this guard detects it (found %v): the #718 upload guard has lost its target", tc.workflow, job, found)
			}
		}
	}
}

func runsProductionHarness(step []string) bool {
	return slices.ContainsFunc(step, func(line string) bool {
		return strings.Contains(line, "make test-e2e-production") ||
			strings.Contains(line, "./internal/e2e_harness/production/")
	})
}

func uploadsE2EArtifactsOnFailure(step []string) bool {
	var uploads, onFailure, path bool
	for _, line := range step {
		line = strings.TrimPrefix(line, "- ")
		switch {
		case strings.HasPrefix(line, "uses: actions/upload-artifact@"):
			uploads = true
		case line == "if: failure()":
			onFailure = true
		case strings.HasPrefix(line, "path: .artifacts/e2e"):
			path = true
		}
	}
	return uploads && onFailure && path
}

// workflowJobNames lists the job keys under the top-level `jobs:` key: the
// lines indented exactly two spaces, the shape workflowJobBlock cuts by.
func workflowJobNames(text string) []string {
	var names []string
	inJobs := false
	for _, line := range strings.Split(text, "\n") {
		if line == "jobs:" {
			inJobs = true
			continue
		}
		if !inJobs || strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if !strings.HasPrefix(line, " ") {
			break // the next top-level key
		}
		if !strings.HasPrefix(line, "   ") && strings.HasSuffix(line, ":") {
			names = append(names, strings.TrimSuffix(strings.TrimSpace(line), ":"))
		}
	}
	return names
}

// workflowJobSteps splits a job block into its steps, each a list of trimmed
// non-comment lines. A step starts at a `- ` item indented six spaces, the
// shape every job in this repository's workflows uses.
func workflowJobSteps(block string) [][]string {
	var steps [][]string
	for _, line := range strings.Split(block, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasPrefix(line, "      - ") {
			steps = append(steps, nil)
		}
		if len(steps) > 0 {
			steps[len(steps)-1] = append(steps[len(steps)-1], trimmed)
		}
	}
	return steps
}

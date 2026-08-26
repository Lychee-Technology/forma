package forma

import (
	"os"
	"strings"
	"testing"
)

// TestDuckDBExtensionCacheCoversExposedJobs pins the CI half of the #487 fix:
// DuckDB extension INSTALLs download from the network, and on a cold runner a
// slow download can eat a connection-init step's deadline — the fail-open
// init then logs-and-skips that extension. Every workflow job that opens
// DuckDB connections must therefore restore the local extension repository
// (~/.duckdb/extensions) from the actions cache, keeping INSTALL a local
// no-op on the hot path. Comment lines are skipped so prose never satisfies a
// positive guard (#482/#485 hardening, same as the sibling ci.yml guards).
func TestDuckDBExtensionCacheCoversExposedJobs(t *testing.T) {
	for _, tc := range []struct {
		workflow string
		jobs     []string
	}{
		{".github/workflows/ci.yml", []string{"test", "e2e", "k6-smoke"}},
		{".github/workflows/nightly-e2e.yml", []string{"federated-full", "production-race"}},
	} {
		wf, err := os.ReadFile(tc.workflow)
		if err != nil {
			t.Fatalf("read %s: %v", tc.workflow, err)
		}
		for _, job := range tc.jobs {
			block := workflowJobBlock(t, string(wf), job)
			var hasCache, hasPath bool
			for _, line := range strings.Split(block, "\n") {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "#") {
					continue
				}
				if strings.HasPrefix(trimmed, "uses: actions/cache@") {
					hasCache = true
				}
				if strings.Contains(trimmed, "~/.duckdb") {
					hasPath = true
				}
			}
			if !hasCache || !hasPath {
				t.Errorf("%s job %q has no actions/cache step covering ~/.duckdb: a cold runner re-downloads DuckDB extensions inside the connection-init deadline again (#487)", tc.workflow, job)
			}
		}
	}
}

// workflowJobBlock cuts one job's body out of a workflow file by indentation:
// the block starts after the two-space `<job>:` key under the top-level
// `jobs:` key and ends at the next non-blank line indented two spaces or less
// (the next job, or a later top-level key). Deliberately not a YAML parser —
// same rationale as ciLintJobBlock (#482), which delegates here. If the
// workflow's job indentation ever changes shape, this fails loudly via
// t.Fatal rather than passing on an empty block.
func workflowJobBlock(t *testing.T, text, job string) string {
	t.Helper()
	lines := strings.Split(text, "\n")
	inJobs := false
	start := -1
	for i, line := range lines {
		if line == "jobs:" {
			inJobs = true
			continue
		}
		if !inJobs {
			continue
		}
		if start == -1 {
			if line == "  "+job+":" {
				start = i + 1
			}
			continue
		}
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(line, "    ") {
			return strings.Join(lines[start:i], "\n")
		}
	}
	if start == -1 {
		t.Fatalf("workflow has no `%s:` job under `jobs:`: this guard scopes its check to that job and has lost its target", job)
	}
	return strings.Join(lines[start:], "\n")
}

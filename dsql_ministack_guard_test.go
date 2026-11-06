package forma

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

const ministackScript = "scripts/test_dsql_ministack.sh"

// TestMiniStackLaneRecipes pins the one entry point local runs and CI share
// (#739) to the script, under $(GOENV) so the suites run on the pinned
// toolchain, and its teardown to the same script.
func TestMiniStackLaneRecipes(t *testing.T) {
	mk, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	run := strings.Join(recipeLines(t, string(mk), "test-dsql-ministack"), "\n")
	if !strings.Contains(run, "$(GOENV) ./"+ministackScript+" run") {
		t.Errorf("test-dsql-ministack does not run the lane under $(GOENV): %q", run)
	}
	down := strings.Join(recipeLines(t, string(mk), "test-dsql-ministack-down"), "\n")
	if !strings.Contains(down, ministackScript+" down") {
		t.Errorf("test-dsql-ministack-down does not run the lane's teardown: %q", down)
	}
}

// TestCIRunsTheMiniStackLane pins the job (#739): the shared target, bounded,
// with its logs uploaded on failure, and nothing that lets it fail unnoticed.
func TestCIRunsTheMiniStackLane(t *testing.T) {
	wf, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("read ci.yml: %v", err)
	}
	var lines []string
	for _, line := range strings.Split(workflowJobBlock(t, string(wf), "dsql-ministack"), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "#") {
			lines = append(lines, line)
		}
	}
	block := strings.Join(lines, "\n")
	for _, want := range []string{"run: make test-dsql-ministack", "timeout-minutes:", "if: failure()", ".artifacts/dsql-ministack/"} {
		if !strings.Contains(block, want) {
			t.Errorf("the dsql-ministack job has no %q", want)
		}
	}
	if strings.Contains(block, "continue-on-error") {
		t.Error("the dsql-ministack job may fail unnoticed: it sets continue-on-error")
	}
}

// TestMiniStackLanePinsStrictMode pins what makes a pass mean something: both
// images by digest, since the proxy's rules change between releases, strict
// mode, without which a cluster has no SQL data plane, and the gate running
// before any suite.
func TestMiniStackLanePinsStrictMode(t *testing.T) {
	script, err := os.ReadFile(ministackScript)
	if err != nil {
		t.Fatalf("read %s: %v", ministackScript, err)
	}
	text := string(script)
	for _, variable := range []string{"MINISTACK_IMAGE", "BACKEND_IMAGE"} {
		if !regexp.MustCompile(`(?m)^` + variable + `="[^"@]+:[^"@]+@sha256:[0-9a-f]{64}"$`).MatchString(text) {
			t.Errorf("%s does not pin %s to a tag and digest", ministackScript, variable)
		}
	}
	if !strings.Contains(text, "-e DSQL_STRICT=1") {
		t.Errorf("%s does not start MiniStack with DSQL_STRICT=1: its clusters would be metadata-only", ministackScript)
	}
	gate := strings.Index(text, "run_suite gate ")
	suite := strings.Index(text, "run_suite entity ")
	if gate == -1 || suite == -1 || gate > suite {
		t.Errorf("%s must run the strict-proxy gate before any suite", ministackScript)
	}
}

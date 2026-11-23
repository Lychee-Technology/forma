package repoguard

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"
)

const ministackScript = "scripts/test_dsql_ministack.sh"

// TestMiniStackLaneRecipes pins the one entry point local runs and CI share
// (#739) to the script, under $(GOENV) so the suites run on the pinned
// toolchain, and its teardown to the same script.
func TestMiniStackLaneRecipes(t *testing.T) {
	mk, err := fs.ReadFile(repo, "Makefile")
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
	wf, err := fs.ReadFile(repo, ".github/workflows/ci.yml")
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
	script, err := fs.ReadFile(repo, ministackScript)
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

// TestMiniStackLaneRunsTheLedgerUpgradeSuite pins the generation-ledger
// upgrade suite in the lane (#738): run by name under a floor of passes, with
// MiniStack's S3 named for its parquet tier, which the suite fails without on
// this target. A suite dropped from the lane, or one whose -run pattern stops
// matching, would otherwise pass unnoticed.
func TestMiniStackLaneRunsTheLedgerUpgradeSuite(t *testing.T) {
	script, err := fs.ReadFile(repo, ministackScript)
	if err != nil {
		t.Fatalf("read %s: %v", ministackScript, err)
	}
	text := string(script)
	suite := regexp.MustCompile(`(?m)^\s*run_suite ledger ([0-9]+) .*\./cmd/tools -run '\^TestLedgerUpgrade'$`).FindStringSubmatch(text)
	if suite == nil || suite[1] == "0" {
		t.Errorf("%s does not run the ledger upgrade suite under a nonzero floor", ministackScript)
	}
	if !strings.Contains(text, "export FORMA_DSQL_S3_ENDPOINT") {
		t.Errorf("%s does not name MiniStack's S3 for the ledger suite's parquet tier", ministackScript)
	}
}

// TestMiniStackLaneRunsThePgxReadSuites pins the pgx read paths that
// replaced DuckDB's postgres extension in the lane (#691): the CDC export's
// reads in the cdc suite and the federated hot leg's in a suite of their
// own, each by name under a nonzero floor of passes, the Protocol V2
// publication contract's compare-and-set suite (#888), and the V2 cdc-flush
// and cdc-init writers built on it (#899).
func TestMiniStackLaneRunsThePgxReadSuites(t *testing.T) {
	script, err := fs.ReadFile(repo, ministackScript)
	if err != nil {
		t.Fatalf("read %s: %v", ministackScript, err)
	}
	text := string(script)
	for name, suite := range map[string]*regexp.Regexp{
		"the CDC export's reads":        regexp.MustCompile(`(?m)^\s*run_suite cdc ([0-9]+) .*\./internal/cdc -run '\^\([^']*\|TestDSQLExport\)'$`),
		"the federated hot leg's reads": regexp.MustCompile(`(?m)^\s*run_suite federated ([0-9]+) .*\./internal/federated -run '\^TestDSQLHotLeg'$`),
		"the publication contract":      regexp.MustCompile(`(?m)^\s*run_suite publication ([0-9]+) .*\./internal/publication$`),
		"the V2 cdc writers":            regexp.MustCompile(`(?m)^\s*run_suite cdcv2 ([0-9]+) .*\./internal/cdcv2$`),
	} {
		if m := suite.FindStringSubmatch(text); m == nil || m[1] == "0" {
			t.Errorf("%s does not run %s under a nonzero floor", ministackScript, name)
		}
	}
}

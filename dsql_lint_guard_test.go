package forma

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// recipeLines answers the non-comment lines of a Makefile target's recipe.
func recipeLines(t *testing.T, makefile, target string) []string {
	t.Helper()
	var lines []string
	in := false
	for _, line := range strings.Split(makefile, "\n") {
		switch {
		case strings.HasPrefix(line, target+":"):
			in = true
			continue
		case !in:
			continue
		case !strings.HasPrefix(line, "\t"):
			return lines
		}
		if !strings.HasPrefix(strings.TrimSpace(line), "#") {
			lines = append(lines, line)
		}
	}
	return lines
}

// TestLintDSQLRecipeRunsTheGate pins `make lint-dsql` (#740) to the gate
// command under $(GOENV), the single entry point CI and `make all` share.
func TestLintDSQLRecipeRunsTheGate(t *testing.T) {
	mk, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	for _, line := range recipeLines(t, string(mk), "lint-dsql") {
		if strings.Contains(line, "$(GOENV)") && strings.Contains(line, "go run ./cmd/dsqllint") {
			return
		}
	}
	t.Error("Makefile's lint-dsql recipe has no `$(GOENV) go run ./cmd/dsqllint` line: the DSQL lint gate would not run (#740)")
}

// TestMakeAllRunsLintDSQL pins the local build verification half of #740:
// `make all` runs the DSQL lint gate, so local verification and CI run the
// same entry point.
func TestMakeAllRunsLintDSQL(t *testing.T) {
	mk, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	allLine := regexp.MustCompile(`(?m)^all:(.*)$`).FindStringSubmatch(string(mk))
	if allLine == nil || !strings.Contains(" "+allLine[1]+" ", " lint-dsql ") {
		t.Errorf("Makefile's all target does not depend on lint-dsql: %q (#740)", allLine)
	}
}

// makeRule is one Makefile target: its prerequisites and its recipe.
type makeRule struct {
	prerequisites []string
	recipe        []string
}

var makeTargetLine = regexp.MustCompile(`^([A-Za-z0-9_.-]+):(?:[ \t]+(.*))?$`)

// parseMakeRules answers the rules of makefile, skipping comment lines.
func parseMakeRules(makefile string) map[string]*makeRule {
	rules := make(map[string]*makeRule)
	var current *makeRule
	for _, line := range strings.Split(makefile, "\n") {
		if strings.HasPrefix(line, "\t") {
			if current != nil && !strings.HasPrefix(strings.TrimSpace(line), "#") {
				current.recipe = append(current.recipe, line)
			}
			continue
		}
		current = nil
		if m := makeTargetLine.FindStringSubmatch(line); m != nil {
			current = &makeRule{prerequisites: strings.Fields(m[2])}
			rules[m[1]] = current
		}
	}
	return rules
}

// reachesLintDSQL answers how target reaches the DSQL lint gate through its
// prerequisites or recipes, or "" when it does not.
func reachesLintDSQL(rules map[string]*makeRule, target string, seen map[string]bool) string {
	if seen[target] {
		return ""
	}
	seen[target] = true
	if target == "lint-dsql" {
		return target
	}
	rule, ok := rules[target]
	if !ok {
		return ""
	}
	for _, line := range rule.recipe {
		if strings.Contains(line, "lint-dsql") || strings.Contains(line, "dsqllint") {
			return target + " recipe: " + strings.TrimSpace(line)
		}
	}
	for _, p := range rule.prerequisites {
		if path := reachesLintDSQL(rules, p, seen); path != "" {
			return target + " -> " + path
		}
	}
	return ""
}

// TestBuildAllDoesNotRunLintDSQL pins that `make build-all` stays a compile: it
// must keep working offline once modules are cached, and the DSQL gate
// downloads its pinned linter on first use (#740). The walk follows every
// prerequisite and recipe of build-all, so an indirect route also fails.
func TestBuildAllDoesNotRunLintDSQL(t *testing.T) {
	mk, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	rules := parseMakeRules(string(mk))
	if _, ok := rules["build-all"]; !ok {
		t.Fatal("Makefile has no build-all target: this guard reads nothing")
	}
	if path := reachesLintDSQL(rules, "build-all", map[string]bool{}); path != "" {
		t.Errorf("make build-all runs the DSQL lint gate: %s (#740)", path)
	}

	// The walk itself must see both routes, or the guard above proves nothing.
	for name, makefile := range map[string]string{
		"prerequisite": "build-all: a\na: lint-dsql\nlint-dsql:\n\t@go run ./cmd/dsqllint\n",
		"recipe":       "build-all: a\na:\n\t@$(MAKE) lint-dsql\n",
	} {
		if reachesLintDSQL(parseMakeRules(makefile), "build-all", map[string]bool{}) == "" {
			t.Errorf("reachesLintDSQL misses the %s route", name)
		}
	}
}

// TestCILintJobRunsLintDSQL pins the CI half: the lint job runs
// `make lint-dsql` itself, not an open-coded copy that could drift from the
// local gate. Comment lines never satisfy the guard.
func TestCILintJobRunsLintDSQL(t *testing.T) {
	ci, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("read .github/workflows/ci.yml: %v", err)
	}
	run := regexp.MustCompile(`^run:\s*make lint-dsql\s*$`)
	for _, line := range strings.Split(ciLintJobBlock(t, string(ci)), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "#") && run.MatchString(trimmed) {
			return
		}
	}
	t.Error("ci.yml's lint job has no `run: make lint-dsql` directive: the DSQL lint gate would not be a required check (#740)")
}

// TestNoBuildStepRewritesSQL pins that the DSQL gate stays lint-only: no
// Makefile or CI line invokes dsql-lint's --fix, which rewrites SQL.
func TestNoBuildStepRewritesSQL(t *testing.T) {
	for _, path := range []string{"Makefile", ".github/workflows/ci.yml", ".github/workflows/nightly-e2e.yml"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for i, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			if strings.Contains(line, "dsql") && strings.Contains(line, "--fix") {
				t.Errorf("%s:%d runs dsql-lint --fix, which rewrites SQL; the build must only lint (#740)", path, i+1)
			}
		}
	}
}

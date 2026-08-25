package forma

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestLintRecipeCoversInfraModule pins the #444 fix: infra/ is a separate Go
// module, so the root `golangci-lint run` never descends into it — the funlen
// gate (#319) was module-wide, not repo-wide, and NewServerless sat at 180
// code lines ungated. The lint recipe must therefore carry a second run line
// executed inside infra/. Matching "cd infra" on a golangci-lint run line is
// the minimal shape that proves the linter runs in the module's own root;
// the sibling guards in toolchain_guard_test.go already hold that same line
// to the $(GOENV) prefix and --build-tags flag, since they scan every
// golangci-lint run line.
func TestLintRecipeCoversInfraModule(t *testing.T) {
	mk, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	for _, line := range strings.Split(string(mk), "\n") {
		if strings.Contains(line, "golangci-lint run") && strings.Contains(line, "cd infra") {
			return
		}
	}
	t.Error("Makefile has no golangci-lint run line executed inside infra/: the infra module would silently drop out of linting again (#444)")
}

// TestMakefileHasCheckInfraTarget pins the build+vet half of #444: infra/ had
// no CI coverage of any kind — never built, vetted, or tested. The check-infra
// recipe must build and vet the module under $(GOENV) so the toolchain pin
// (#448) reaches it; CI's side of the wiring is guarded separately by
// TestCILintJobChecksInfraModule.
func TestMakefileHasCheckInfraTarget(t *testing.T) {
	mk, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	text := string(mk)
	if !strings.Contains(text, "\ncheck-infra:") {
		t.Fatal("Makefile has no check-infra target: infra/ would go back to being never built or vetted (#444)")
	}
	var hasBuild, hasVet bool
	for _, line := range strings.Split(text, "\n") {
		if !strings.Contains(line, "cd infra") || !strings.Contains(line, "$(GOENV)") {
			continue
		}
		if strings.Contains(line, "go build ./...") {
			hasBuild = true
		}
		if strings.Contains(line, "go vet ./...") {
			hasVet = true
		}
	}
	if !hasBuild {
		t.Error("no `cd infra && $(GOENV) ... go build ./...` line in Makefile: check-infra must build the module under the pinned toolchain (#444)")
	}
	if !hasVet {
		t.Error("no `cd infra && $(GOENV) ... go vet ./...` line in Makefile: check-infra must vet the module under the pinned toolchain (#444)")
	}
}

// TestCILintJobChecksInfraModule pins the CI side of #444's build+vet gate:
// the lint job must run `make check-infra`, the single definition of the
// infra build+vet wiring (same #454 rationale as TestCILintJobRunsMakeLint —
// an open-coded copy would drop GOENV and the toolchain pin). Comment lines
// are skipped so prose never satisfies the guard.
func TestCILintJobChecksInfraModule(t *testing.T) {
	ci, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("read .github/workflows/ci.yml: %v", err)
	}
	runCheckInfra := regexp.MustCompile(`^run:\s*make check-infra\s*$`)
	for _, line := range strings.Split(ciLintJobBlock(t, string(ci)), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		if runCheckInfra.MatchString(trimmed) {
			return
		}
	}
	t.Error("ci.yml's lint job has no `run: make check-infra` directive: the infra module would go back to having no CI build/vet coverage (#444)")
}

package forma

import (
	"os"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// TestGatesRunUnderPinnedToolchain fails with a named diagnosis when the
// test binary was compiled by a toolchain whose major.minor differs from
// go.mod's go directive (#448). Without it the drift surfaces as unrelated
// packages going red — ~46 spurious golangci-lint typecheck errors and a
// schemavalidate assertion on changed stdlib error text under go1.27 —
// because GOTOOLCHAIN=auto upgrades to a newer local toolchain but never
// downgrades to the pinned one. The Makefile pins GOTOOLCHAIN for every
// go-based gate (fmt-check excepted — it calls the local gofmt binary);
// this test goes red if the pin leaves GOENV. It also ties CI
// to the pin in one direction: CI's test job runs go test under setup-go's
// GO_VERSION with no GOTOOLCHAIN set, so a GO_VERSION on a newer major.minor
// than go.mod's directive fails here, while a go.mod-only bump auto-upgrades
// CI's toolchain and passes, and patch-level divergence is tolerated. The
// test observes only the binary that compiled it — the lint gate's wiring is
// pinned separately by TestLintRecipeInheritsGOENV.
func TestGatesRunUnderPinnedToolchain(t *testing.T) {
	mod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	// The directive must carry a full x.y.z so this guard captures a stable
	// major.minor to compare against; go.mod's own convention writes the patch
	// ("go 1.26.0"). The Makefile pins GOTOOLCHAIN=go1.26+auto — a fixed floor,
	// no longer sed-derived from this directive — so the go1.26 floor selects
	// go1.26.0 without adopting a newer local Go, while +auto still follows a
	// higher go/toolchain directive here if one is added. Requiring the patch
	// segment keeps the major.minor capture below unambiguous, while capturing
	// only major.minor so patch-level toolchain divergence stays tolerated.
	match := regexp.MustCompile(`(?m)^go (\d+\.\d+)\.\d+`).FindSubmatch(mod)
	if match == nil {
		t.Fatal("no full x.y.z go directive found in go.mod: this pinned-toolchain guard needs the patch segment to capture major.minor (#448)")
	}
	want := "go" + string(match[1])
	got := runtime.Version()
	if got != want && !strings.HasPrefix(got, want+".") {
		t.Fatalf("test binary built by %s, but go.mod pins %s.x (#448): run gates through make, which sets GOTOOLCHAIN, or export GOTOOLCHAIN yourself", got, want)
	}
}

// TestLintRecipeInheritsGOENV pins the other half of the #448 wiring: the
// lint gate. golangci-lint spawns its own `go list`, which reads GOTOOLCHAIN
// from the environment, so the Makefile's lint run line must lead with
// $(GOENV). TestGatesRunUnderPinnedToolchain cannot see this gate, and the
// branch that introduced the pin shipped exactly this omission until review
// caught it (commit 78f003a) — a $(GOENV) buried inside the GOPATH command
// substitution does not reach the linter process, so a prefix is required.
func TestLintRecipeInheritsGOENV(t *testing.T) {
	mk, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	found := false
	for _, line := range strings.Split(string(mk), "\n") {
		if !strings.Contains(line, "golangci-lint run") {
			continue
		}
		found = true
		if !strings.HasPrefix(strings.TrimLeft(line, "\t@"), "$(GOENV) ") {
			t.Errorf("lint recipe line %q does not lead with $(GOENV): golangci-lint would run its go list on the ambient toolchain, reviving the spurious typecheck errors of #448", line)
		}
	}
	if !found {
		t.Fatal("no golangci-lint run line found in Makefile: the lint-pin guard has lost its target")
	}
}

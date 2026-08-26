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
	// Capture the go directive's major.minor to compare against the running
	// toolchain. The patch segment is optional — "go 1.26" and "go 1.26.0" both
	// match: GOTOOLCHAIN is a fixed go1.26.0+auto floor in the Makefile, no
	// longer sed-derived from this directive, so the directive no longer needs a
	// patch. (The floor itself must keep its patch — go1.26.0, not go1.26 — since
	// a bare go1.26 is a language version, not a toolchain name.) The go1.26.0
	// floor selects go1.26.0 without adopting a newer local Go, while +auto still
	// follows a higher go/toolchain directive here if one is added. Only
	// major.minor is captured, so patch-level toolchain divergence stays
	// tolerated.
	match := regexp.MustCompile(`(?m)^go (\d+\.\d+)(?:\.\d+)?$`).FindSubmatch(mod)
	if match == nil {
		t.Fatal("no go x.y[.z] directive found in go.mod: this pinned-toolchain guard reads major.minor from it (#448)")
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
	// A per-module run line may lead with `cd <dir> && ` (#444) — golangci-lint
	// v1.64.8 has no chdir flag, and a leading cd does not prevent the
	// following $(GOENV) assignments from reaching the linter process. Strip
	// that prefix, then require $(GOENV) exactly as before.
	cdPrefix := regexp.MustCompile(`^cd \S+ && `)
	for _, line := range strings.Split(string(mk), "\n") {
		if !strings.Contains(line, "golangci-lint run") {
			continue
		}
		found = true
		trimmed := cdPrefix.ReplaceAllString(strings.TrimLeft(line, "\t@"), "")
		if !strings.HasPrefix(trimmed, "$(GOENV) ") {
			t.Errorf("lint recipe line %q does not lead with $(GOENV): golangci-lint would run its go list on the ambient toolchain, reviving the spurious typecheck errors of #448", line)
		}
	}
	if !found {
		t.Fatal("no golangci-lint run line found in Makefile: the lint-pin guard has lost its target")
	}
}

// TestLintRecipeCoversE2ETag pins the #436 fix: the Makefile's lint recipe
// must pass --build-tags e2e, or the 96 //go:build e2e files silently drop
// out of linting again — golangci-lint only loads packages matching the
// active build tags, so without the flag those files are never analyzed
// (that gap hid 83 pre-existing violations). CI inherits this line via
// `make lint` (#454), so this one guard covers both gates.
func TestLintRecipeCoversE2ETag(t *testing.T) {
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
		if !strings.Contains(line, "--build-tags e2e") {
			t.Errorf("lint recipe line %q lacks --build-tags e2e: the e2e-tagged files would silently stop being linted (#436)", line)
		}
	}
	if !found {
		t.Fatal("no golangci-lint run line found in Makefile: the e2e-tag guard has lost its target")
	}
}

// TestCILintJobRunsMakeLint pins the CI side of the #448 wiring (#454): the
// lint job must invoke `make lint` rather than open-coding the golangci-lint
// install+run. An open-coded invocation carries none of the Makefile's GOENV
// (GOTOOLCHAIN pin, GOCACHE, GOFLAGS), so local/CI parity would hold only
// while setup-go's GO_VERSION coincidentally equals the Makefile's toolchain
// floor — the same coincidence #448 proved unreliable. Asserting the absence
// of "golangci-lint" in the whole workflow catches both halves of a
// regression: re-adding the install line or the run line.
func TestCILintJobRunsMakeLint(t *testing.T) {
	ci, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("read .github/workflows/ci.yml: %v", err)
	}
	text := string(ci)
	// The positive half must match a real run directive, not prose, and only
	// inside the lint job (#482 review, both rounds): ci.yml's explanatory
	// comment also says "make lint", and another job legitimately running the
	// same command must not satisfy a guard about the lint job's wiring.
	// Comment lines are skipped explicitly so prose never counts, whatever
	// shape the directive pattern takes in the future.
	runMakeLint := regexp.MustCompile(`^run:\s*make lint\s*$`)
	found := false
	for _, line := range strings.Split(ciLintJobBlock(t, text), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		if runMakeLint.MatchString(trimmed) {
			found = true
			break
		}
	}
	if !found {
		t.Error("ci.yml's lint job has no `run: make lint` directive: the lint job must run the Makefile recipe so the golangci-lint pin and GOENV have one definition (#454)")
	}
	if strings.Contains(text, "golangci-lint") {
		t.Error("ci.yml mentions golangci-lint directly: the install+run belong to the Makefile lint recipe alone — an open-coded copy drops the GOTOOLCHAIN pin and re-splits the version pin (#454)")
	}
}

// ciLintJobBlock cuts the `lint:` job's body out of ci.yml. The indentation
// cut lives in workflowJobBlock (duckdb_ext_cache_guard_test.go), which is
// deliberately not a YAML parser — a yaml dependency buys nothing here (#482
// review round 1), and the sibling guards in this file are plain text scans
// for the same reason.
func ciLintJobBlock(t *testing.T, text string) string {
	t.Helper()
	return workflowJobBlock(t, text, "lint")
}

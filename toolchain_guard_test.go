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
// packages going red — 46 spurious golangci-lint typecheck errors and a
// schemavalidate assertion on changed stdlib error text under go1.27 —
// because GOTOOLCHAIN=auto upgrades to a newer local toolchain but never
// downgrades to the pinned one. The Makefile pins GOTOOLCHAIN for every
// gate; this test is what goes red first if that wiring is ever removed.
func TestGatesRunUnderPinnedToolchain(t *testing.T) {
	mod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	match := regexp.MustCompile(`(?m)^go (\d+\.\d+)`).FindSubmatch(mod)
	if match == nil {
		t.Fatal("no go directive found in go.mod")
	}
	want := "go" + string(match[1])
	got := runtime.Version()
	if got != want && !strings.HasPrefix(got, want+".") {
		t.Fatalf("test binary built by %s, but go.mod pins %s.x (#448): run gates through make, which sets GOTOOLCHAIN, or export GOTOOLCHAIN yourself", got, want)
	}
}

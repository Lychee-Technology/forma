package forma

import (
	"os"
	"strings"
	"testing"
)

// TestNightlyRacesProductionSuite pins the #410 ruling: the nightly workflow
// must run the production e2e harness under -race. CI's PR gate races only
// the unit suite, and the gate's e2e jobs cannot carry -race — the detector's
// slowdown would invalidate the wall-clock regression tripwires #434
// calibrated for race-free runs — so this scheduled run is the only recurring
// -race signal over the harness's real concurrent flush/compaction/read
// paths. A plain text scan, like the sibling guards in
// toolchain_guard_test.go: a yaml dependency buys nothing here.
func TestNightlyRacesProductionSuite(t *testing.T) {
	wf, err := os.ReadFile(".github/workflows/nightly-e2e.yml")
	if err != nil {
		t.Fatalf("read .github/workflows/nightly-e2e.yml: %v", err)
	}
	found := false
	for _, line := range strings.Split(string(wf), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || !strings.HasPrefix(trimmed, "run:") {
			continue
		}
		if !strings.Contains(trimmed, "./internal/e2e_harness/production/") {
			continue
		}
		found = true
		for _, flag := range []string{"-tags=e2e", "-race"} {
			if !strings.Contains(trimmed, flag) {
				t.Errorf("nightly production run line %q lacks %s: the production suite would silently lose its only recurring -race signal (#410)", trimmed, flag)
			}
		}
	}
	if !found {
		t.Fatal("no run directive for ./internal/e2e_harness/production/ found in nightly-e2e.yml: the -race guard has lost its target (#410)")
	}
}

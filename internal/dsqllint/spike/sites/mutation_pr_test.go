package sites

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestMutationsPRScanner replays every adversarial mutation against PR
// #801's own inventory guard (TestInventoryCoversRepository) in the
// disposable worktree, for comparison with the typed inventory.
func TestMutationsPRScanner(t *testing.T) {
	dir := os.Getenv("SPIKE_MUT_DIR")
	if dir == "" {
		t.Skip("SPIKE_MUT_DIR not set")
	}
	reset(t, dir)
	out := map[string]string{}
	for _, mu := range mutations {
		if only := os.Getenv("SPIKE_MUT_ONLY"); only != "" && !strings.HasPrefix(mu.Name, only) {
			continue
		}
		if mu.Group != "adversarial" {
			continue
		}
		apply(t, dir, mu)
		cmd := exec.Command("go", "test", "-count=1", "-run", "^TestInventoryCoversRepository$", "./internal/dsqllint")
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOFLAGS=-buildvcs=false")
		b, err := cmd.CombinedOutput()
		switch {
		case err == nil:
			out[mu.Name] = "passes (not flagged)"
		case strings.Contains(string(b), "no inventory entry classifies it"):
			out[mu.Name] = "flagged"
		default:
			out[mu.Name] = "fails: " + firstLine(string(b))
		}
		t.Logf("%-50s %s", mu.Name, out[mu.Name])
		reset(t, dir)
	}
	j, _ := json.MarshalIndent(out, "", "  ")
	if p := os.Getenv("SPIKE_MUT_OUT"); p != "" {
		_ = os.WriteFile(p, j, 0o644)
	}
}

func firstLine(s string) string {
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, "inventory") || strings.Contains(l, "Error") {
			return strings.TrimSpace(l)
		}
	}
	return strings.TrimSpace(strings.SplitN(s, "\n", 2)[0])
}

package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// knownHTTPEntryPoints is the floor for TestMetricsInstalledInEveryHTTPEntryPoint,
// in the spirit of knownManagerBuilders: the walk discovers directories, this
// list only makes a walk that finds nothing fail loudly.
var knownHTTPEntryPoints = []string{"lambda", "server"}

// TestMetricsInstalledInEveryHTTPEntryPoint guards the #423 fix at its root:
// every cmd/ tree that builds an httpapi.Server must also install the metrics
// emitter, so the telemetry hook cannot go back to being registered on the
// targets someone remembered and inert on the rest. Same source-level scan,
// same limits and same reasons as TestEntityConfigFromEnvWiredInEveryWriteEntryPoint.
func TestMetricsInstalledInEveryHTTPEntryPoint(t *testing.T) {
	const cmdDir = ".."
	missing, served, err := scanHTTPEntryPointsForMetrics(cmdDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range missing {
		t.Errorf("cmd/%s builds an httpapi.Server but never calls bootstrap.InstallMetrics, "+
			"so every telemetry counter is inert on that deployment target (#423)", name)
	}
	for _, name := range knownHTTPEntryPoints {
		if !served[name] {
			t.Errorf("the walk did not classify cmd/%s as an HTTP entry point; it is reading "+
				"the wrong tree or matching nothing, so its green would be meaningless", name)
		}
	}
}

// TestScanHTTPEntryPointsForMetrics_ReportsAnUnwiredTree proves the guard can
// fail: a synthetic cmd tree that builds a server without installing metrics
// is reported, and one that does both is not.
func TestScanHTTPEntryPointsForMetrics_ReportsAnUnwiredTree(t *testing.T) {
	root := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		dir := filepath.Join(root, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("unwired", "package main\nfunc main() { _ = httpapi.NewServer(nil, httpapi.Options{}) }\n")
	write("wired", "package main\nfunc main() { _, _ = bootstrap.InstallMetrics(cfg, nil, nil); _ = httpapi.NewServer(nil, httpapi.Options{}) }\n")
	write("neither", "package main\nfunc main() {}\n")

	missing, served, err := scanHTTPEntryPointsForMetrics(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0] != "unwired" {
		t.Fatalf("expected only the unwired tree reported, got %v", missing)
	}
	if !served["wired"] || served["neither"] {
		t.Fatalf("unexpected classification: %v", served)
	}
}

// scanHTTPEntryPointsForMetrics walks every child of cmdDir and returns the
// children that build an httpapi.Server without calling
// bootstrap.InstallMetrics, plus the set of children that build one at all.
func scanHTTPEntryPointsForMetrics(cmdDir string) (missing []string, served map[string]bool, err error) {
	entries, err := os.ReadDir(cmdDir)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read cmd directory %s: %w", cmdDir, err)
	}
	served = map[string]bool{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		found, err := treeContains(filepath.Join(cmdDir, entry.Name()), "httpapi.NewServer(", "bootstrap.InstallMetrics(")
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan cmd/%s: %w", entry.Name(), err)
		}
		if !found[0] {
			continue
		}
		served[entry.Name()] = true
		if !found[1] {
			missing = append(missing, entry.Name())
		}
	}
	return missing, served, nil
}

// treeContains reports, per needle, whether any non-test Go file in dir's
// subtree contains it.
func treeContains(dir string, needles ...string) ([]bool, error) {
	found := make([]bool, len(needles))
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", path, err)
		}
		for i, needle := range needles {
			if strings.Contains(string(source), needle) {
				found[i] = true
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk %s: %w", dir, err)
	}
	return found, nil
}

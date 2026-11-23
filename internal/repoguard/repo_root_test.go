package repoguard

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeGoMod(t *testing.T, dir, module string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create %s: %v", dir, err)
	}
	body := "module " + module + "\n\ngo 1.26\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(body), 0o644); err != nil {
		t.Fatalf("write %s/go.mod: %v", dir, err)
	}
}

// TestFindRepoRootSkipsOtherModules pins the walk past a nested module's
// go.mod: infra/ is a module of its own, and a guard started there must still
// read the repository root.
func TestFindRepoRootSkipsOtherModules(t *testing.T) {
	root := t.TempDir()
	writeGoMod(t, root, modulePath)
	writeGoMod(t, filepath.Join(root, "infra"), modulePath+"/infra")
	start := filepath.Join(root, "infra", "stack")
	if err := os.MkdirAll(start, 0o755); err != nil {
		t.Fatalf("create %s: %v", start, err)
	}

	got, err := findRepoRoot(start)
	if err != nil {
		t.Fatalf("findRepoRoot(%s): %v", start, err)
	}
	if got != root {
		t.Errorf("findRepoRoot(%s) = %s, want %s", start, got, root)
	}
}

// TestFindRepoRootRefusesAnotherRepository pins the loud failure: a tree whose
// go.mod declares some other module is never taken for this repository.
func TestFindRepoRootRefusesAnotherRepository(t *testing.T) {
	root := t.TempDir()
	writeGoMod(t, root, "example.com/other")

	_, err := findRepoRoot(root)
	if err == nil || !strings.Contains(err.Error(), "no go.mod declaring module "+modulePath) {
		t.Fatalf("findRepoRoot(%s) error = %v, want one naming the missing %s module", root, err, modulePath)
	}
}

// TestRepoIsTheRepositoryRoot checks what TestMain resolved: the guards read
// the tree that holds this package, not the package directory.
func TestRepoIsTheRepositoryRoot(t *testing.T) {
	if _, err := fs.Stat(repo, "internal/repoguard/repo_test.go"); err != nil {
		t.Fatalf("repo does not hold this package: %v", err)
	}
	mod, err := fs.ReadFile(repo, "go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	if !strings.Contains(string(mod), "module "+modulePath+"\n") {
		t.Errorf("go.mod at the resolved root does not declare module %s", modulePath)
	}
}

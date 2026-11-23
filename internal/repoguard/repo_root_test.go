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
	if got, err := declaredModule("go.mod", mod); err != nil || got != modulePath {
		t.Errorf("go.mod at the resolved root declares module %q (%v), want %s", got, err, modulePath)
	}
	if !filepath.IsAbs(repoRoot) {
		t.Errorf("repoRoot = %q, want an absolute path", repoRoot)
	}
}

// TestFindRepoRootReadsGoModSyntax pins that the module declaration is read
// as the go command reads it: a trailing comment, a quoted path and a module
// block all declare the same module, and a module whose path merely starts
// with this one's does not.
func TestFindRepoRootReadsGoModSyntax(t *testing.T) {
	for _, tc := range []struct {
		name, gomod string
		match       bool
	}{
		{"inline comment", "module " + modulePath + " // canonical import path\n\ngo 1.26\n", true},
		{"double-quoted", "module \"" + modulePath + "\"\n", true},
		{"module block", "module (\n\t" + modulePath + "\n)\n", true},
		{"leading comment", "// the forma module\nmodule " + modulePath + "\n", true},
		{"longer path", "module " + modulePath + "/infra // nested\n", false},
		{"commented out", "// module " + modulePath + "\nmodule example.com/other\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte(tc.gomod), 0o644); err != nil {
				t.Fatalf("write go.mod: %v", err)
			}
			got, err := findRepoRoot(root)
			switch {
			case tc.match && (err != nil || got != root):
				t.Errorf("findRepoRoot(%q) = %q, %v; want %s", tc.gomod, got, err, root)
			case !tc.match && err == nil:
				t.Errorf("findRepoRoot(%q) = %q, want no match", tc.gomod, got)
			}
		})
	}
}

// TestFindRepoRootFailsOnAMalformedGoMod keeps a go.mod the go command would
// reject from being passed over in silence on the way up.
func TestFindRepoRootFailsOnAMalformedGoMod(t *testing.T) {
	root := t.TempDir()
	writeGoMod(t, root, modulePath)
	nested := filepath.Join(root, "nested")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("create %s: %v", nested, err)
	}
	if err := os.WriteFile(filepath.Join(nested, "go.mod"), []byte("module a b\n"), 0o644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}

	if got, err := findRepoRoot(nested); err == nil || !strings.Contains(err.Error(), "parse ") {
		t.Fatalf("findRepoRoot(%s) = %q, %v; want a parse error", nested, got, err)
	}
}

// TestGuardsIgnoreTheWorkingDirectory runs what used to read the working
// directory from somewhere else: the size guard's package directory and the
// MiniStack fixture's fake tools both come from the resolved root.
func TestGuardsIgnoreTheWorkingDirectory(t *testing.T) {
	t.Chdir(t.TempDir())

	dir, err := packageFS()
	if err != nil {
		t.Fatalf("open %s: %v", packageDir, err)
	}
	if _, err := fs.Stat(dir, "repo_test.go"); err != nil {
		t.Errorf("package directory does not hold this package: %v", err)
	}

	f := newLaneFixture(t)
	for _, tool := range []string{"docker", "curl", "go"} {
		// Stat follows the symlink, so a target that moved fails here.
		if _, err := os.Stat(filepath.Join(f.root, "bin", tool)); err != nil {
			t.Errorf("fake %s does not resolve: %v", tool, err)
		}
	}
}

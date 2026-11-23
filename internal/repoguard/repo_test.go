package repoguard

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/mod/modfile"
)

// modulePath is the module whose repository the guards check.
const modulePath = "github.com/lychee-technology/forma"

// packageDir is this package's directory, relative to the repository root.
const packageDir = "internal/repoguard"

// repoRoot is the repository root, and repo the same tree as a file system,
// both set by TestMain before any test runs. Every guard reads the repository
// through them, with slash-separated paths relative to the root, so a guard's
// result and its diagnostics never depend on the directory the test binary
// runs in: go test runs it in the package directory, but a compiled guard
// binary may be launched from anywhere inside the repository. Nothing in this
// package reads a path relative to the working directory.
var (
	repoRoot string
	repo     fs.FS
)

func TestMain(m *testing.M) {
	root, err := resolveRepoRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "repoguard: %v\n", err)
		os.Exit(1)
	}
	repoRoot = root
	repo = os.DirFS(root)
	os.Exit(m.Run())
}

// resolveRepoRoot finds the repository root from the working directory: the
// package directory under go test, or wherever a compiled guard binary was
// launched inside the repository.
func resolveRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve the working directory: %w", err)
	}
	return findRepoRoot(wd)
}

// declaredModule answers the module path a go.mod declares, parsed with the
// go command's own go.mod grammar: a comment after the path, a quoted path or
// a parenthesized module block is the same declaration to the go command, so
// it is here too. It answers "" when the file declares no module, and an
// error when the file is not valid go.mod syntax.
func declaredModule(name string, gomod []byte) (string, error) {
	f, err := modfile.ParseLax(name, gomod, nil)
	if err != nil {
		return "", err
	}
	if f.Module == nil {
		return "", nil
	}
	return f.Module.Mod.Path, nil
}

// findRepoRoot walks up from start to the nearest directory whose go.mod
// declares modulePath. A go.mod of another module on the way (infra/ holds
// one) is passed over, and no match at all is an error: a guard that read
// some other tree would pass or fail for the wrong reasons.
func findRepoRoot(start string) (string, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", start, err)
	}
	for {
		gomod := filepath.Join(dir, "go.mod")
		data, err := os.ReadFile(gomod)
		switch {
		case err == nil:
			module, err := declaredModule(gomod, data)
			if err != nil {
				return "", fmt.Errorf("parse %s: %w", gomod, err)
			}
			if module == modulePath {
				return dir, nil
			}
		case !errors.Is(err, fs.ErrNotExist):
			return "", fmt.Errorf("read %s: %w", gomod, err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod declaring module %s at or above %s", modulePath, start)
		}
		dir = parent
	}
}

// packageFS is this package's directory, read from the repository root.
func packageFS() (fs.FS, error) {
	return fs.Sub(repo, packageDir)
}

// repoPath answers the absolute OS path of a slash-separated path relative to
// the repository root, for what has to leave Go's fs.FS: a symlink target, a
// file a subprocess runs.
func repoPath(rel string) string {
	return filepath.Join(repoRoot, filepath.FromSlash(rel))
}

// parseRepoFile parses a Go file of the repository. Positions carry the
// root-relative path, as they did when the guards ran from the root.
func parseRepoFile(fset *token.FileSet, path string) (*ast.File, error) {
	src, err := fs.ReadFile(repo, path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	// The caller names the file in its own context; a parse error already
	// carries the position.
	return parser.ParseFile(fset, path, src, 0)
}

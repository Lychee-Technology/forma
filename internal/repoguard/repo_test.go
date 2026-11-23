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
	"regexp"
	"strings"
	"testing"
)

// modulePath is the module whose repository the guards check.
const modulePath = "github.com/lychee-technology/forma"

// repo is the repository root, set by TestMain before any test runs. Every
// guard reads the repository through it, with slash-separated paths relative
// to the root, so a guard's result and its diagnostics never depend on the
// directory the test binary runs in.
var repo fs.FS

func TestMain(m *testing.M) {
	root, err := resolveRepoRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "repoguard: %v\n", err)
		os.Exit(1)
	}
	repo = os.DirFS(root)
	os.Exit(m.Run())
}

// resolveRepoRoot finds the repository root from the package directory, where
// go test runs a package's tests.
func resolveRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve the working directory: %w", err)
	}
	return findRepoRoot(wd)
}

var moduleDirective = regexp.MustCompile(`(?m)^module\s+(\S+)\s*$`)

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
		data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		switch {
		case err == nil:
			if m := moduleDirective.FindSubmatch(data); m != nil && strings.Trim(string(m[1]), `"`) == modulePath {
				return dir, nil
			}
		case !errors.Is(err, fs.ErrNotExist):
			return "", fmt.Errorf("read %s: %w", filepath.Join(dir, "go.mod"), err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod declaring module %s at or above %s", modulePath, start)
		}
		dir = parent
	}
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

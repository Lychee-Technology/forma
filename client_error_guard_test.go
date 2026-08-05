package forma

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoBareSentinelWraps is a source-level guard for #313's deny-by-default
// contract. A client error must be built with InvalidInputf/NotFoundf/
// Conflictf (or re-wrapped with WrapPublicf) so it carries a published
// message; a bare fmt.Errorf("…: %w", forma.ErrInvalidInput) or an
// errors.Join with a sentinel still earns its 4xx status but answers an
// opaque redacted body. The boundary makes that safe; this guard makes it
// discoverable — the runtime failure mode is "the caller's 400 became
// useless", which a feature author without a body assertion never notices.
//
// Idiom follows TestWriteErrorAlwaysCarriesALiteral4xxStatus
// (internal/httpapi/error_leak_test.go): fail loudly on a vacuous pass,
// report every offender rather than the first, and state the blind spots.
//
// Blind spots, deliberately accepted: the guard sees fmt.Errorf and
// errors.Join argument lists only. A hand-rolled type whose Unwrap returns a
// sentinel is invisible here — the boundary's deny shape covers it at
// runtime. Test files are excluded: tests may build bare wraps on purpose to
// pin the deny shape itself.
func TestNoBareSentinelWraps(t *testing.T) {
	sentinels := map[string]bool{
		"ErrInvalidInput": true,
		"ErrNotFound":     true,
		"ErrConflict":     true,
	}
	skipDirs := map[string]bool{
		".git": true, ".gocache": true, "build": true,
		"node_modules": true, "testdata": true, "vendor": true,
	}

	referencesSentinel := func(e ast.Expr) bool {
		switch v := e.(type) {
		case *ast.SelectorExpr:
			// forma.ErrInvalidInput from any package (the X identifier is not
			// checked: an aliased import must not slip through).
			return sentinels[v.Sel.Name]
		case *ast.Ident:
			// Bare ErrInvalidInput inside package forma itself.
			return sentinels[v.Name]
		}
		return false
	}

	isWrapCall := func(call *ast.CallExpr) bool {
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok {
			return false
		}
		return (pkg.Name == "fmt" && sel.Sel.Name == "Errorf") ||
			(pkg.Name == "errors" && sel.Sel.Name == "Join")
	}

	var offenders []string
	scanned := 0
	fset := token.NewFileSet()

	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		scanned++
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isWrapCall(call) {
				return true
			}
			for _, arg := range call.Args {
				if referencesSentinel(arg) {
					offenders = append(offenders, fset.Position(call.Pos()).String())
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking the module: %v", err)
	}

	// A vacuous pass is a broken guard, not a clean sweep: this test runs from
	// the module root and must see the whole tree.
	if scanned < 50 {
		t.Fatalf("guard scanned only %d non-test Go files; the walk is broken", scanned)
	}

	if len(offenders) > 0 {
		t.Fatalf("found %d bare sentinel wrap(s); build these with forma.InvalidInputf/NotFoundf/Conflictf "+
			"(or forma.WrapPublicf) so the 4xx body carries a published message (#313):\n%s",
			len(offenders), strings.Join(offenders, "\n"))
	}
}

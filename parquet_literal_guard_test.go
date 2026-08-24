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

// TestParquetPathRendersAreEscaped is the #456 forward guard: every SQL render
// that interpolates a parquet path into a single-quoted literal must wrap the
// value in sqlutil.EscapeLiteral, so a path containing a quote cannot alter SQL
// structure. It enumerates the render shapes by their format string rather than
// by a hand-maintained site list, so a NEW read_parquet/glob/DESCRIBE/scan
// site is covered the moment it is added.
//
// Scanned shapes (fmt.Sprintf format string literals) in internal/federated
// and internal/sqlgen:
//   - contains `read_parquet('%s'`  (drainParquet, describeParquetColumns)
//   - contains `glob('%s'`          (globParquetPaths)
//   - equals   `'%s'`               (formatDuckDBPathList, drainGuardedParquet
//     via BuildParquetScanSource)
//
// For each, the argument feeding that %s must be a call to
// sqlutil.EscapeLiteral(...).
//
// Idiom follows TestNoBareSentinelWraps: fail loudly on a vacuous scan, report
// every offender, state the blind spots. Blind spots: only direct fmt.Sprintf
// calls with a single string-literal format are inspected; a path quoted by
// some other construction is invisible here and is covered by the behavioral
// escape tests instead. Test files are excluded.
func TestParquetPathRendersAreEscaped(t *testing.T) {
	dirs := []string{"internal/federated", "internal/sqlgen"}

	isEscapeLiteralCall := func(e ast.Expr) bool {
		call, ok := e.(*ast.CallExpr)
		if !ok {
			return false
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		return sel.Sel.Name == "EscapeLiteral"
	}

	// isPathRenderFormat reports whether a format string literal renders a
	// parquet path into a single-quoted literal, and thus needs its %s arg
	// escaped.
	isPathRenderFormat := func(format string) bool {
		return strings.Contains(format, "read_parquet('%s'") ||
			strings.Contains(format, "glob('%s'") ||
			format == "'%s'"
	}

	var offenders []string
	scanned := 0
	fset := token.NewFileSet()

	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				return fmt.Errorf("parse %s: %w", path, perr)
			}
			scanned++
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Sprintf" {
					return true
				}
				if len(call.Args) < 2 {
					return true
				}
				lit, ok := call.Args[0].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				format := strings.Trim(lit.Value, "`\"")
				if !isPathRenderFormat(format) {
					return true
				}
				if !isEscapeLiteralCall(call.Args[1]) {
					offenders = append(offenders, fmt.Sprintf("%s: Sprintf(%q, ...) path arg not sqlutil.EscapeLiteral", fset.Position(call.Pos()), format))
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", dir, err)
		}
	}

	if scanned < 10 {
		t.Fatalf("guard scanned only %d non-test Go files; the walk is broken", scanned)
	}
	if len(offenders) > 0 {
		t.Fatalf("found %d unescaped parquet-path render(s); wrap the path arg in sqlutil.EscapeLiteral (#456):\n%s",
			len(offenders), strings.Join(offenders, "\n"))
	}
}

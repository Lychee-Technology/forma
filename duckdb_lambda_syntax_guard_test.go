package forma

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// deprecatedLambdaArrow matches a DuckDB single-arrow lambda passed as a
// function argument: `, x -> body` or `, (x, i) -> body`. DuckDB 2.0 refuses
// that form by default; every generated lambda must use `lambda x: body` /
// `lambda x, i: body` (#751). The leading comma anchors the match to an
// argument list, which keeps prose such as "text -> date" out; the body
// must not open with a quote or '>', which keeps the JSON operators
// (`col -> '$.a'`, `col ->> 'k'`) out.
var deprecatedLambdaArrow = regexp.MustCompile(
	`,\s*(\(\s*[A-Za-z_]\w*(\s*,\s*[A-Za-z_]\w*)*\s*\)|[A-Za-z_]\w*)\s*->\s*[^>'"\s]`)

// lambdaArrowProbes are the deliberate single-arrow lambdas: each proves
// that a strict-syntax test database really refuses the old form. A file
// listed here may hold exactly that many matches and no more.
var lambdaArrowProbes = map[string]int{
	"internal/cdc/duckdb_list_syntax_test.go":         1,
	"internal/federated/strict_lambda_dsn_test.go":    1,
	"internal/sqlgen/duckdb_lambda_semantics_test.go": 1,
}

// TestNoDeprecatedDuckDBLambdaArrow is the #751 forward guard: no Go string
// literal in the module, production or test, carries a single-arrow DuckDB
// lambda, so neither generated SQL nor an exact-SQL expectation can bring
// the deprecated syntax back before (or after) the DuckDB 2.0 driver bump.
//
// Blind spots: only Go string literals are scanned, each on its own, so an
// arrow assembled across concatenated literals or held outside Go source is
// invisible here; the tests that execute generated SQL on a database
// refusing the single arrow cover those at run time. A lambda whose
// parameter list is not preceded by a comma (the first argument of a
// function) is out of the pattern; DuckDB's list functions all take the
// lambda after the list.
func TestNoDeprecatedDuckDBLambdaArrow(t *testing.T) {
	found := map[string][]string{}
	scanned := 0
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != "." && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor" || name == "build") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", path, perr)
		}
		scanned++
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, uerr := strconv.Unquote(lit.Value)
			if uerr != nil {
				t.Fatalf("unquote %s: %v", fset.Position(lit.Pos()), uerr)
			}
			for _, m := range deprecatedLambdaArrow.FindAllString(value, -1) {
				key := filepath.ToSlash(path)
				found[key] = append(found[key], fset.Position(lit.Pos()).String()+": "+m)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk module: %v", err)
	}
	if scanned < 100 {
		t.Fatalf("scanned only %d Go files: the walk is not covering the module", scanned)
	}

	var offenders []string
	for path, hits := range found {
		if len(hits) == lambdaArrowProbes[path] {
			continue
		}
		offenders = append(offenders, hits...)
	}
	for path, want := range lambdaArrowProbes {
		if got := len(found[path]); got != want {
			t.Errorf("%s: want %d single-arrow refusal probe(s), found %d; update lambdaArrowProbes", path, want, got)
		}
	}
	sort.Strings(offenders)
	for _, o := range offenders {
		t.Errorf("single-arrow DuckDB lambda (refused by DuckDB 2.0; write `lambda x: ...` / `lambda x, i: ...`): %s", o)
	}
}

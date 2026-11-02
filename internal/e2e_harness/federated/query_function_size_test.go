package federated

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// maxGuardedFunctionLines is coding-standard.md §7's refactoring trigger, which
// is deliberately tighter than AGENTS.md's 100-line hard limit: the point is to
// catch a function on its way past 80, not once it is already unmaintainable.
const maxGuardedFunctionLines = 80

type guardedFunction struct {
	name  string
	lines int
}

// measureFunctionLines reports the span of every function body in one source
// file, from its func keyword through its closing brace. A doc comment sits
// before FuncDecl.Pos() and so is not counted — the guard measures code, not
// the explanation of it.
func measureFunctionLines(filename string, source []byte) ([]guardedFunction, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, source, 0)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", filename, err)
	}

	var functions []guardedFunction
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		functions = append(functions, guardedFunction{
			name:  fn.Name.Name,
			lines: fset.Position(fn.End()).Line - fset.Position(fn.Pos()).Line + 1,
		})
	}
	return functions, nil
}

// listFunctionGuardedSourceFiles is the file-size guard's listing of the
// package's non-test files, taken as it stands rather than filtered by a name
// pattern: a file whose name stops matching a pattern drops out of the guard in
// silence (#369). Until #431 the function guard watched only query*.go, because
// NewFederatedTestHarness was over the trigger and would have made a
// whole-package guard unsatisfiable on arrival.
func listFunctionGuardedSourceFiles() ([]string, error) {
	files, err := sizeguard.List(os.DirFS("."), sizeguard.ExcludeTests)
	if err != nil {
		return nil, fmt.Errorf("list function-guarded source files: %w", err)
	}
	return files, nil
}

func TestMeasureFunctionLinesSpansFuncKeywordToClosingBrace(t *testing.T) {
	source := []byte(`package fixture

// Doc comments precede the func keyword and must not be counted.
// This second line would inflate the measurement if they were.
func measured() int {
	x := 1
	return x
}

func declared()
`)

	functions, err := measureFunctionLines("fixture.go", source)
	if err != nil {
		t.Fatalf("measure function lines: %v", err)
	}
	if len(functions) != 1 {
		t.Fatalf("expected only the function with a body, got %v", functions)
	}
	if functions[0].name != "measured" {
		t.Fatalf("measured the wrong function: %q", functions[0].name)
	}
	if functions[0].lines != 4 {
		t.Fatalf("measured %d lines, want 4 (func keyword through closing brace)", functions[0].lines)
	}
}

// oversizedFunctions reports every function in the named files that is over
// the refactoring trigger, as "file: function has N lines".
func oversizedFunctions(names []string) ([]string, error) {
	var oversized []string
	for _, name := range names {
		source, err := os.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		functions, err := measureFunctionLines(name, source)
		if err != nil {
			return nil, fmt.Errorf("measure %s: %w", name, err)
		}
		for _, fn := range functions {
			if fn.lines > maxGuardedFunctionLines {
				oversized = append(oversized, fmt.Sprintf("%s: %s has %d lines", name, fn.name, fn.lines))
			}
		}
	}
	return oversized, nil
}

// TestGuardedFunctionsStayWithinRefactoringTrigger keeps the package's
// functions from accumulating back into oversized ones. ExecuteFederatedQuery
// reached 94 lines and NewFederatedTestHarness 141 because nothing was
// watching; this is what watches (#324, #431).
func TestGuardedFunctionsStayWithinRefactoringTrigger(t *testing.T) {
	names, err := listFunctionGuardedSourceFiles()
	if err != nil {
		t.Fatalf("list function-guarded source files: %v", err)
	}
	oversized, err := oversizedFunctions(names)
	if err != nil {
		t.Fatalf("measure function-guarded source files: %v", err)
	}
	for _, problem := range oversized {
		t.Errorf("%s, exceeds the %d-line refactoring trigger", problem, maxGuardedFunctionLines)
	}
}

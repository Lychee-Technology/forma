package federated

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
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

// listFunctionGuardedSourceFiles scans a narrower set than the file-size guard,
// which since #369 covers the whole package: two functions outside query*.go are
// over the trigger and would make this guard unsatisfiable on arrival —
// NewFederatedTestHarness in harness.go at 131 lines, past even AGENTS.md's
// 100-line hard limit (#431), and loadExternalFederatedConfigFromEnv in
// helpers.go at 84. Each needs a refactor this guard does not own, so the scope
// widens one file group at a time; only once both are under the trigger can this
// guard drop its pattern list and share listGuardedSourceFiles.
func listFunctionGuardedSourceFiles() ([]string, error) {
	return listNonTestSources("query*.go")
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

func TestListFunctionGuardedSourceFilesScopesToQuerySources(t *testing.T) {
	files, err := listFunctionGuardedSourceFiles()
	if err != nil {
		t.Fatalf("list function-guarded source files: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected function-guarded source files")
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			t.Errorf("test file %s included in function guard", name)
		}
		if !strings.HasPrefix(name, "query") {
			t.Errorf("%s is outside the query*.go scope this guard can hold", name)
		}
	}
}

// TestGuardedFunctionsStayWithinRefactoringTrigger keeps the query assembly
// entry points from accumulating back into oversized functions. ExecuteFederatedQuery
// reached 94 lines because nothing was watching; this is what watches (#324).
func TestGuardedFunctionsStayWithinRefactoringTrigger(t *testing.T) {
	names, err := listFunctionGuardedSourceFiles()
	if err != nil {
		t.Fatalf("list function-guarded source files: %v", err)
	}

	for _, name := range names {
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		functions, err := measureFunctionLines(name, source)
		if err != nil {
			t.Fatalf("measure %s: %v", name, err)
		}
		for _, fn := range functions {
			if fn.lines > maxGuardedFunctionLines {
				t.Errorf("%s: %s has %d lines, exceeds the %d-line refactoring trigger",
					name, fn.name, fn.lines, maxGuardedFunctionLines)
			}
		}
	}
}

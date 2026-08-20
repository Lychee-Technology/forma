package federated

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
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

// listFunctionGuardedSourceFiles deliberately does not reuse
// listGuardedSourceFiles: that one also covers harness*.go, where
// NewFederatedTestHarness is already past the 100-line hard limit, so pulling it
// in would make this guard unsatisfiable without a harness.go refactor this
// change does not own. Widening the scope is the follow-up's job (#324).
func listFunctionGuardedSourceFiles() ([]string, error) {
	candidates, err := filepath.Glob("query*.go")
	if err != nil {
		return nil, fmt.Errorf("glob function-guarded source files: %w", err)
	}

	var files []string
	for _, name := range candidates {
		if !strings.HasSuffix(name, "_test.go") {
			files = append(files, name)
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no function-guarded source files matched query*.go")
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

package federated

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
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

// listFunctionGuardedSourceFiles scans a narrower set than the file-size guard,
// which since #369 covers the whole package: functions outside query*.go are
// still over the trigger, and would make this guard unsatisfiable on arrival.
// Which ones, and how long, is not written here: it rots the moment one is
// touched. TestFunctionGuardScopeIsStillBlocked enforces the reason instead, and
// fails once nothing outside this scope is over the trigger, which is the signal
// to drop the query prefix and take the file guard's listing as it stands.
func listFunctionGuardedSourceFiles() ([]string, error) {
	sources, err := sizeguard.List(os.DirFS("."), sizeguard.ExcludeTests)
	if err != nil {
		return nil, fmt.Errorf("list function-guarded source files: %w", err)
	}

	var files []string
	for _, name := range sources {
		if strings.HasPrefix(name, "query") {
			files = append(files, name)
		}
	}
	if len(files) == 0 {
		return nil, errors.New("no function-guarded source files matched query*.go")
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

// TestGuardedFunctionsStayWithinRefactoringTrigger keeps the query assembly
// entry points from accumulating back into oversized functions. ExecuteFederatedQuery
// reached 94 lines because nothing was watching; this is what watches (#324).
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

// TestFunctionGuardScopeIsStillBlocked is the self-retiring tripwire for the
// guard's narrow scope (#447). It holds while some non-test file outside
// query*.go has a function over the trigger, which is the only reason the
// function guard does not watch the whole package as the file guard does. Once
// none does, it fails, so whoever finishes the last refactor learns here that
// the scope can widen rather than from a comment in a neighbouring file.
func TestFunctionGuardScopeIsStillBlocked(t *testing.T) {
	sources, err := sizeguard.List(os.DirFS("."), sizeguard.ExcludeTests)
	if err != nil {
		t.Fatalf("list package source files: %v", err)
	}
	guarded, err := listFunctionGuardedSourceFiles()
	if err != nil {
		t.Fatalf("list function-guarded source files: %v", err)
	}
	inScope := make(map[string]bool, len(guarded))
	for _, name := range guarded {
		inScope[name] = true
	}
	var unguarded []string
	for _, name := range sources {
		if !inScope[name] {
			unguarded = append(unguarded, name)
		}
	}

	blockers, err := oversizedFunctions(unguarded)
	if err != nil {
		t.Fatalf("measure unguarded source files: %v", err)
	}
	if len(blockers) == 0 {
		t.Fatalf("no function outside the function guard's query*.go scope is over the %d-line trigger: "+
			"the function guard can now widen. Make listFunctionGuardedSourceFiles return the file guard's "+
			"sizeguard.List(os.DirFS(\".\"), sizeguard.ExcludeTests) listing, drop the query*.go scope test, "+
			"and delete this tripwire", maxGuardedFunctionLines)
	}
	for _, blocker := range blockers {
		t.Logf("still blocking a whole-package function guard: %s", blocker)
	}
}

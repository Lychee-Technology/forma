// Package sizeguard is the one implementation of the per-package file-size
// guards that hold coding-standard.md's 500-line cap. Before #449 each guarded
// package carried its own copy of the counting rule, the listing and the size
// loop, so a change to the rule had five edit sites; a guarded package now
// holds a single test that calls Check with its scope.
//
// Check reads the working directory, and relies on go test to make that the
// package under test: go test runs each package's test binary with the
// package's source directory as its working directory. A guard therefore
// watches exactly the directory of the package it sits in, and not its
// subdirectories, which are separate packages with guards of their own. Call
// Check only from a test in the package whose files it should watch.
package sizeguard

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"
)

// maxLines is coding-standard.md's source-file cap.
const maxLines = 500

// Scope says which Go files in a package directory a guard watches. Only the
// two constants below are scopes: List fails on any other value, which a
// conversion such as Scope(2) can still build.
type Scope int

const (
	// IncludeTests watches every Go file, _test.go included. It is the norm:
	// the violations the guards were first written for were test files (#320).
	IncludeTests Scope = iota
	// ExcludeTests watches non-test files only. It is for a package that still
	// holds an over-cap test file, and each use names the issue that retires it.
	ExcludeTests
)

// valid reports whether s is a declared scope. An undeclared value would
// otherwise fall through watches as ExcludeTests and drop every test file from
// the guard in silence.
func (s Scope) valid() bool {
	return s == IncludeTests || s == ExcludeTests
}

func (s Scope) watches(name string) bool {
	return s == IncludeTests || !isTest(name)
}

func isTest(name string) bool {
	return strings.HasSuffix(name, "_test.go")
}

// Check fails t once for every file in the working directory that is over the
// cap, and once for every way the listing disagrees with the directory.
func Check(t testing.TB, scope Scope) {
	t.Helper()
	check(t, os.DirFS("."), scope)
}

func check(t testing.TB, fsys fs.FS, scope Scope) {
	t.Helper()
	problems, err := audit(fsys, scope)
	if err != nil {
		t.Fatalf("file-size guard: %v", err)
	}
	for _, problem := range problems {
		t.Error(problem)
	}
}

// List returns the Go files in the root of fsys that scope watches. It globs
// *.go and filters by scope alone, never by a list of name patterns: a file
// whose name stops matching a pattern drops out of a pattern-based guard in
// silence (#369). A scope that is neither IncludeTests nor ExcludeTests is an
// error.
func List(fsys fs.FS, scope Scope) ([]string, error) {
	if !scope.valid() {
		return nil, fmt.Errorf("unknown scope %d: want IncludeTests or ExcludeTests", scope)
	}
	candidates, err := fs.Glob(fsys, "*.go")
	if err != nil {
		return nil, fmt.Errorf("glob guarded files: %w", err)
	}

	files := make([]string, 0, len(candidates))
	for _, name := range candidates {
		if scope.watches(name) {
			files = append(files, name)
		}
	}
	if len(files) == 0 {
		return nil, errors.New("no guarded files matched *.go")
	}
	return files, nil
}

// audit is the whole guard: list, cross-check the listing, then measure every
// listed file. List rejects an undeclared scope, so the steps after it only
// ever see IncludeTests or ExcludeTests.
func audit(fsys fs.FS, scope Scope) ([]string, error) {
	names, err := List(fsys, scope)
	if err != nil {
		return nil, err
	}

	problems, err := scopeProblems(fsys, scope, names)
	if err != nil {
		return nil, err
	}
	oversized, err := violations(fsys, names)
	if err != nil {
		return nil, err
	}
	return append(problems, oversized...), nil
}

// scopeProblems checks a listing against an independent read of the directory
// (#369): every Go file on disk that scope watches must be listed. Test files
// are pinned in both directions. An IncludeTests listing must hold at least one,
// which the guard's own file always provides, so the check holds even if the
// listing and the directory read were ever to drop tests together. An
// ExcludeTests listing must hold none.
func scopeProblems(fsys fs.FS, scope Scope, names []string) ([]string, error) {
	var problems []string
	listed := make(map[string]bool, len(names))
	listedTests := 0
	for _, name := range names {
		listed[name] = true
		if isTest(name) {
			listedTests++
			if scope == ExcludeTests {
				problems = append(problems, fmt.Sprintf("test file %s included in a guard that excludes tests", name))
			}
		}
	}
	if scope == IncludeTests && listedTests == 0 {
		problems = append(problems, "no _test.go file in the listing: the guard has stopped covering tests")
	}

	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read package directory: %w", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || !scope.watches(name) {
			continue
		}
		if !listed[name] {
			problems = append(problems, fmt.Sprintf("%s is outside the file-size guard", name))
		}
	}
	return problems, nil
}

// violations measures every name it is given and describes each file over the
// cap. It is the guard's only consumption step and filters nothing, so a file
// the listing holds cannot escape measurement (#449).
func violations(fsys fs.FS, names []string) ([]string, error) {
	var found []string
	for _, name := range names {
		source, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("read guarded file %s: %w", name, err)
		}
		if lines := countLines(source); lines > maxLines {
			found = append(found, fmt.Sprintf(
				"%s has %d lines, exceeds the %d-line source-file limit", name, lines, maxLines))
		}
	}
	return found, nil
}

// countLines counts lines the way wc -l does, plus a final line that has no
// terminating newline, which wc -l would miss.
func countLines(source []byte) int {
	if len(source) == 0 {
		return 0
	}

	lines := bytes.Count(source, []byte{'\n'})
	if source[len(source)-1] != '\n' {
		return lines + 1
	}
	return lines
}

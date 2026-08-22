package federated

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCountSourceLinesIncludesFinalUnterminatedLine(t *testing.T) {
	source := append(bytes.Repeat([]byte("x\n"), 500), 'x')
	if got := countSourceLines(source); got != 501 {
		t.Fatalf("countSourceLines = %d, want 501", got)
	}
}

// TestListGuardedSourceFilesCoversEveryNonTestSource pins the guard's scope to
// the whole package: every non-test source file is watched, and no test file is.
// Listing the directory independently is what makes the first half real, and it
// is what a pattern list cannot satisfy — a file whose name stops matching a
// pattern drops out of a pattern-based guard in silence (#369).
func TestListGuardedSourceFilesCoversEveryNonTestSource(t *testing.T) {
	files, err := listGuardedSourceFiles()
	if err != nil {
		t.Fatalf("list guarded source files: %v", err)
	}

	guarded := make(map[string]bool, len(files))
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			t.Fatalf("test file %s included in source guard", name)
		}
		guarded[name] = true
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	sources := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		sources++
		if !guarded[name] {
			t.Errorf("%s is outside the file-size guard", name)
		}
	}
	if sources == 0 {
		t.Fatal("expected package source files")
	}
}

func countSourceLines(source []byte) int {
	if len(source) == 0 {
		return 0
	}

	lines := bytes.Count(source, []byte{'\n'})
	if source[len(source)-1] != '\n' {
		return lines + 1
	}
	return lines
}

// listNonTestSources collects the non-test files matching the given patterns.
// The two size guards apply the same rule to different scopes, so the scope is
// the argument and the rule lives here once.
func listNonTestSources(patterns ...string) ([]string, error) {
	var files []string
	for _, pattern := range patterns {
		candidates, err := filepath.Glob(pattern)
		if err != nil {
			return nil, fmt.Errorf("glob guarded source files %q: %w", pattern, err)
		}
		for _, name := range candidates {
			if !strings.HasSuffix(name, "_test.go") {
				files = append(files, name)
			}
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no guarded source files matched %s", strings.Join(patterns, " or "))
	}
	return files, nil
}

// listGuardedSourceFiles scopes the file-size guard to the whole package rather
// than to a list of name patterns. Every non-test file is watched, so a new one
// arrives guarded and a renamed one cannot slip out (#369).
func listGuardedSourceFiles() ([]string, error) {
	return listNonTestSources("*.go")
}

// TestGuardedSourcesStayWithinFileSizeLimit prevents any concern in this harness
// package — query assembly, seeding, assertions, infrastructure — from
// accumulating back into an oversized source file (#220, #369).
func TestGuardedSourcesStayWithinFileSizeLimit(t *testing.T) {
	names, err := listGuardedSourceFiles()
	if err != nil {
		t.Fatalf("list guarded source files: %v", err)
	}

	for _, name := range names {
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}

		if lines := countSourceLines(source); lines > 500 {
			t.Errorf("%s has %d lines, exceeds the 500-line source-file limit", name, lines)
		}
	}
}

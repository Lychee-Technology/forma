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

func TestListGuardedSourceFilesExcludesTests(t *testing.T) {
	files, err := listGuardedSourceFiles()
	if err != nil {
		t.Fatalf("list guarded source files: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected guarded source files")
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			t.Fatalf("test file %s included in source guard", name)
		}
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

func listGuardedSourceFiles() ([]string, error) {
	return listNonTestSources("query*.go", "harness*.go")
}

// TestGuardedSourcesStayWithinFileSizeLimit prevents query assembly and harness
// infrastructure concerns from accumulating back into oversized source files (#220).
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

package httpapi

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

func TestListPackageSourceFilesExcludesTests(t *testing.T) {
	files, err := listPackageSourceFiles()
	if err != nil {
		t.Fatalf("list package source files: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected package source files")
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

func listPackageSourceFiles() ([]string, error) {
	candidates, err := filepath.Glob("*.go")
	if err != nil {
		return nil, fmt.Errorf("glob package source files: %w", err)
	}

	files := make([]string, 0, len(candidates))
	for _, name := range candidates {
		if !strings.HasSuffix(name, "_test.go") {
			files = append(files, name)
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no package source files matched *.go")
	}
	return files, nil
}

// TestHTTPAPISourcesStayWithinFileSizeLimit prevents handler and helper
// concerns from accumulating back into an oversized source file (#220).
func TestHTTPAPISourcesStayWithinFileSizeLimit(t *testing.T) {
	names, err := listPackageSourceFiles()
	if err != nil {
		t.Fatalf("list package source files: %v", err)
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

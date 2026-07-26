package federated

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourceLineCountIncludesFinalUnterminatedLine(t *testing.T) {
	source := append(bytes.Repeat([]byte("x\n"), 500), 'x')
	if got := countSourceLines(source); got != 501 {
		t.Fatalf("countSourceLines = %d, want 501", got)
	}
}

func TestListQuerySourceFilesExcludesTests(t *testing.T) {
	files, err := listQuerySourceFiles()
	if err != nil {
		t.Fatalf("list query source files: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected query source files")
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

func listQuerySourceFiles() ([]string, error) {
	candidates, err := filepath.Glob("query*.go")
	if err != nil {
		return nil, fmt.Errorf("glob query source files: %w", err)
	}

	files := make([]string, 0, len(candidates))
	for _, name := range candidates {
		if !strings.HasSuffix(name, "_test.go") {
			files = append(files, name)
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no query source files matched query*.go")
	}
	return files, nil
}

// TestQuerySourcesStayWithinFileSizeLimit prevents query assembly concerns
// from accumulating back into an oversized source file (#220).
func TestQuerySourcesStayWithinFileSizeLimit(t *testing.T) {
	names, err := listQuerySourceFiles()
	if err != nil {
		t.Fatalf("list query source files: %v", err)
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

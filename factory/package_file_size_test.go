package factory

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestCountSourceLinesIncludesFinalUnterminatedLine(t *testing.T) {
	source := append(bytes.Repeat([]byte("x\n"), 500), 'x')
	if got := countSourceLines(source); got != 501 {
		t.Fatalf("countSourceLines = %d, want 501", got)
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

// listGuardedFiles watches every Go file in the package directory, tests
// included — unlike the httpapi and federated guards, which exclude tests
// because their packages carry grandfathered oversized test files. The
// violations this guard exists for were test files (#320, #271, #401), so
// tests are in scope. Glob *.go with no filtering cannot silently narrow
// the way a pattern list can (#369): every Go file in the directory matches.
func listGuardedFiles() ([]string, error) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		return nil, fmt.Errorf("glob guarded files: %w", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no guarded files matched *.go")
	}
	return files, nil
}

// TestFactoryPackageFilesStayWithinFileSizeLimit keeps every file in the
// package — sources and tests alike — under the 500-line cap from
// coding-standard.md; factory_test.go was split once already and must not
// accrete back (#320).
func TestFactoryPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	names, err := listGuardedFiles()
	if err != nil {
		t.Fatalf("list guarded files: %v", err)
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

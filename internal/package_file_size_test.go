package internal

import (
	"bytes"
	"errors"
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
// included — unlike the httpapi and federated guards, which exclude tests: the
// federated package carries grandfathered oversized test files (#369). The
// violations this guard exists for were test files (#320, #271, #401), so
// tests are in scope. Glob *.go with no filtering cannot silently narrow
// the way a pattern list can (#369): every Go file in the directory matches.
func listGuardedFiles() ([]string, error) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		return nil, fmt.Errorf("glob guarded files: %w", err)
	}
	if len(files) == 0 {
		return nil, errors.New("no guarded files matched *.go")
	}
	return files, nil
}

// TestListGuardedFilesCoversEveryGoFileIncludingTests pins the guard's scope by
// listing the package directory independently: every Go file on disk must be in
// the listing, and the _test.go assertion is inverted relative to the httpapi
// and federated guards — here a test file must be present, not absent (#320).
// The listing is also required to hold at least one test file, so the inversion
// stays pinned even if ReadDir and Glob were ever to drift together.
func TestListGuardedFilesCoversEveryGoFileIncludingTests(t *testing.T) {
	files, err := listGuardedFiles()
	if err != nil {
		t.Fatalf("list guarded files: %v", err)
	}

	guarded := make(map[string]bool, len(files))
	listedTests := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			listedTests++
		}
		guarded[name] = true
	}
	if listedTests == 0 {
		t.Error("no _test.go file in the listing: the guard has stopped covering tests")
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	found := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}
		found++
		if !guarded[name] {
			t.Errorf("%s is outside the file-size guard", name)
		}
	}
	if found == 0 {
		t.Fatal("expected package Go files")
	}
}

// TestInternalPackageFilesStayWithinFileSizeLimit keeps every file in the
// package — sources and tests alike — under the 500-line cap from
// coding-standard.md, so split files cannot silently accrete back (#320).
func TestInternalPackageFilesStayWithinFileSizeLimit(t *testing.T) {
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

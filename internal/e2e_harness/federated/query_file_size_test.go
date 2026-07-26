package federated

import (
	"bytes"
	"os"
	"testing"
)

func TestSourceLineCountIncludesFinalUnterminatedLine(t *testing.T) {
	source := append(bytes.Repeat([]byte("x\n"), 500), 'x')
	if got := sourceLineCount(source); got != 501 {
		t.Fatalf("sourceLineCount = %d, want 501", got)
	}
}

func sourceLineCount(source []byte) int {
	if len(source) == 0 {
		return 0
	}

	lines := bytes.Count(source, []byte{'\n'})
	if source[len(source)-1] != '\n' {
		return lines + 1
	}
	return lines
}

// TestQuerySourcesStayWithinFileSizeLimit prevents query assembly concerns
// from accumulating back into an oversized source file (#220).
func TestQuerySourcesStayWithinFileSizeLimit(t *testing.T) {
	for _, name := range []string{
		"query.go",
		"query_build.go",
		"query_hot.go",
		"query_postgres_build.go",
	} {
		source, err := os.ReadFile(name)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}

		if lines := sourceLineCount(source); lines > 500 {
			t.Errorf("%s has %d lines, exceeds the 500-line source-file limit", name, lines)
		}
	}
}

package dsqllint

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// fakeTool writes a stand-in dsql-lint that records its arguments, prints
// stdout and exits with code.
func fakeTool(t *testing.T, stdout string, code int) (tool, argsFile string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the fake dsql-lint is a shell script")
	}
	dir := t.TempDir()
	argsFile = filepath.Join(dir, "args")
	out := filepath.Join(dir, "out.json")
	if err := os.WriteFile(out, []byte(stdout), 0o644); err != nil {
		t.Fatalf("write fake output: %v", err)
	}
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = --version ]; then echo 'dsql-lint " + ToolVersion + "'; exit 0; fi\n" +
		"echo \"$@\" > '" + argsFile + "'\n" +
		"cat '" + out + "'\n" +
		"exit " + string(rune('0'+code)) + "\n"
	tool = filepath.Join(dir, "dsql-lint")
	if err := os.WriteFile(tool, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake tool: %v", err)
	}
	return tool, argsFile
}

func TestRunLintIsLintOnlyAndHonoursExitCodes(t *testing.T) {
	tool, argsFile := fakeTool(t, lintJSON, 1)
	byFile, err := runLint(context.Background(), tool, []string{"u.sql", "clean.sql"})
	if err != nil || len(byFile["u.sql"]) != 2 {
		t.Fatalf("runLint = %v, %v", byFile, err)
	}
	args, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("read recorded args: %v", err)
	}
	if got := strings.TrimSpace(string(args)); got != "--format json u.sql clean.sql" {
		t.Fatalf("dsql-lint ran with %q, want lint-only JSON output and no --fix", got)
	}

	usage, _ := fakeTool(t, "error: unexpected argument", 2)
	if _, err := runLint(context.Background(), usage, []string{"u.sql"}); err == nil || !strings.Contains(err.Error(), "exited 2") {
		t.Fatalf("a usage error must fail the gate, got %v", err)
	}
}

func TestEnsureToolVerifiesTheOverrideVersion(t *testing.T) {
	tool, _ := fakeTool(t, "", 0)
	if got, err := EnsureTool(context.Background(), t.TempDir(), tool); err != nil || got != tool {
		t.Fatalf("EnsureTool(override) = %q, %v", got, err)
	}
	wrong := filepath.Join(t.TempDir(), "dsql-lint")
	if err := os.WriteFile(wrong, []byte("#!/bin/sh\necho 'dsql-lint 0.0.1'\n"), 0o755); err != nil {
		t.Fatalf("write wrong tool: %v", err)
	}
	if _, err := EnsureTool(context.Background(), t.TempDir(), wrong); err == nil || !strings.Contains(err.Error(), "the gate pins") {
		t.Fatalf("a dsql-lint of another version must be refused, got %v", err)
	}
}

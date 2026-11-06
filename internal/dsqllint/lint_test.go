package dsqllint

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit holds coding-standard.md's 500-line
// cap for this package, sources and tests alike.
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.IncludeTests)
}

const lintJSON = `{
  "schema_version": 1,
  "files": [
    {"file": "u.sql", "error": null, "diagnostics": [
      {"rule": "multi_ddl_transaction", "line": 3, "message": "Transaction contains 2 DDL statements.", "suggestion": "Split.", "statement_preview": "BEGIN;"},
      {"rule": "index_async", "line": 9, "message": "CREATE INDEX without ASYNC", "suggestion": "Use ASYNC.", "statement_preview": "CREATE INDEX i ON t (a);"}
    ]},
    {"file": "clean.sql", "error": null, "diagnostics": []}
  ],
  "summary": {"errors": 1, "warnings": 1, "fixed": 0}
}`

const cleanJSON = `{"schema_version": 1, "files": [{"file": "clean.sql", "error": null, "diagnostics": []}]}`

// realToolJSON is the shape the pinned dsql-lint prints, fields the gate does not
// read included: they must not stop it being accepted.
const realToolJSON = `{
  "schema_version": 1,
  "files": [
    {"file": "u.sql", "diagnostics": [
      {"rule": "truncate", "line": 6, "message": "TRUNCATE is not supported in DSQL.", "suggestion": "Use DELETE.", "fix_result": {"status": "unfixable"}, "statement_preview": "TRUNCATE t;"}
    ], "error": null, "output_file": null, "fixed_sql": null},
    {"file": "clean.sql", "diagnostics": [], "error": null, "output_file": null, "fixed_sql": null}
  ],
  "summary": {"errors": 1, "warnings": 0, "fixed": 0}
}`

// TestParseLintOutputRejectsMalformedResults holds the required schema 1 fields:
// a file result or a diagnostic missing one is not a file linted clean. Each
// exit code agrees with the document's diagnostics, so only the field check can
// refuse it.
func TestParseLintOutputRejectsMalformedResults(t *testing.T) {
	doc := func(files string) string { return `{"schema_version": 1, "files": ` + files + `}` }
	withDiagnostic := func(d string) string {
		return doc(`[{"file": "a.sql", "error": null, "diagnostics": [` + d + `]}]`)
	}
	tests := []struct {
		name   string
		exit   int
		stdout string
		want   string
	}{
		{"files omitted", 0, `{"schema_version": 1}`, "no files array"},
		{"files null", 0, doc(`null`), "no files array"},
		{"null file result", 0, doc(`[null]`), "no file name"},
		{"file omitted", 0, doc(`[{"error": null, "diagnostics": []}]`), "no file name"},
		{"file null", 0, doc(`[{"file": null, "error": null, "diagnostics": []}]`), "no file name"},
		{"file empty", 0, doc(`[{"file": "", "error": null, "diagnostics": []}]`), "no file name"},
		{"diagnostics omitted", 0, doc(`[{"file": "a.sql", "error": null}]`), "a.sql has no diagnostics array"},
		{"diagnostics null", 0, doc(`[{"file": "a.sql", "error": null, "diagnostics": null}]`), "a.sql has no diagnostics array"},
		{"diagnostics not an array", 0, doc(`[{"file": "a.sql", "error": null, "diagnostics": {}}]`), "parse dsql-lint JSON"},
		{"error omitted", 0, doc(`[{"file": "a.sql", "diagnostics": []}]`), "a.sql has no error field"},
		{"error not a string", 0, doc(`[{"file": "a.sql", "error": 5, "diagnostics": []}]`), "neither null nor a string"},
		{"error object", 0, doc(`[{"file": "a.sql", "error": {}, "diagnostics": []}]`), "neither null nor a string"},
		{"empty error string", 0, doc(`[{"file": "a.sql", "error": "", "diagnostics": []}]`), "could not lint a.sql"},
		{"null diagnostic", 1, withDiagnostic(`null`), "diagnostic 1 has no rule"},
		{"rule omitted", 1, withDiagnostic(`{"line": 1, "message": "m"}`), "diagnostic 1 has no rule"},
		{"rule empty", 1, withDiagnostic(`{"rule": "", "line": 1, "message": "m"}`), "diagnostic 1 has no rule"},
		{"line omitted", 1, withDiagnostic(`{"rule": "r", "message": "m"}`), "diagnostic 1 (r) has no line"},
		{"line zero", 1, withDiagnostic(`{"rule": "r", "line": 0, "message": "m"}`), "diagnostic 1 (r) has no line"},
		{"line negative", 1, withDiagnostic(`{"rule": "r", "line": -1, "message": "m"}`), "diagnostic 1 (r) has no line"},
		{"message omitted", 1, withDiagnostic(`{"rule": "r", "line": 1}`), "diagnostic 1 (r) has no message"},
		{"second diagnostic malformed", 1, withDiagnostic(`{"rule": "r", "line": 1, "message": "m"}, {"line": 2, "message": "m"}`), "diagnostic 2 has no rule"},
		{"malformed beside a finding", 1, doc(`[{"file": "a.sql", "error": null, "diagnostics": [{"rule": "r", "line": 1, "message": "m"}]}, {"file": "b.sql", "error": null}]`), "b.sql has no diagnostics array"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseLintOutput(tt.exit, []byte(tt.stdout), []string{"a.sql", "b.sql"})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ParseLintOutput error = %v, want one containing %q", err, tt.want)
			}
		})
	}
}

func TestParseLintOutputReadsTheRealToolsShape(t *testing.T) {
	byFile, err := ParseLintOutput(1, []byte(realToolJSON), []string{"u.sql", "clean.sql"})
	if err != nil {
		t.Fatalf("ParseLintOutput: %v", err)
	}
	if got := byFile["u.sql"]; len(got) != 1 || got[0].Rule != "truncate" || got[0].Line != 6 {
		t.Errorf("u.sql diagnostics = %+v", got)
	}
	if got, ok := byFile["clean.sql"]; !ok || len(got) != 0 {
		t.Errorf("clean.sql diagnostics = %+v, present %v; want an empty result", got, ok)
	}
}

func TestParseLintOutputFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		exit   int
		stdout string
		files  []string
		want   string
	}{
		{"usage error", 2, "", []string{"a.sql"}, "exited 2"},
		{"fix-mode exit", 3, cleanJSON, []string{"clean.sql"}, "exited 3"},
		{"killed", -1, "", []string{"a.sql"}, "exited -1"},
		{"not json", 1, "boom", []string{"a.sql"}, "parse dsql-lint JSON"},
		{"empty output", 0, "", []string{"a.sql"}, "parse dsql-lint JSON"},
		{"unknown schema", 0, `{"schema_version": 2, "files": []}`, nil, "schema_version 2"},
		{"no schema", 0, `{"files": []}`, nil, "schema_version 0"},
		{"file error", 1, `{"schema_version": 1, "files": [{"file": "a.sql", "error": "No such file", "diagnostics": []}]}`, []string{"a.sql"}, "could not lint a.sql"},
		{"missing file", 0, cleanJSON, []string{"clean.sql", "other.sql"}, "no result for other.sql"},
		{"unasked file", 0, cleanJSON, []string{"other.sql"}, "not asked to lint"},
		{"two results for a file", 0, `{"schema_version": 1, "files": [{"file": "a.sql", "error": null, "diagnostics": []}, {"file": "a.sql", "error": null, "diagnostics": []}]}`, []string{"a.sql"}, "two results"},
		{"exit 1 without diagnostics", 1, cleanJSON, []string{"clean.sql"}, "without diagnostics"},
		{"exit 0 with diagnostics", 0, lintJSON, []string{"u.sql", "clean.sql"}, "disagree"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseLintOutput(tt.exit, []byte(tt.stdout), tt.files)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ParseLintOutput error = %v, want one containing %q", err, tt.want)
			}
		})
	}
	byFile, err := ParseLintOutput(1, []byte(lintJSON), []string{"u.sql", "clean.sql"})
	if err != nil || len(byFile["u.sql"]) != 2 || len(byFile["clean.sql"]) != 0 {
		t.Fatalf("ParseLintOutput = %v, %v", byFile, err)
	}
	if _, err := ParseLintOutput(0, []byte(cleanJSON), []string{"clean.sql"}); err != nil {
		t.Fatalf("a clean run must pass: %v", err)
	}
}

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

// TestRunLintIsLintOnlyAndHonoursExitCodes captures the tool's argv: the gate
// asks for JSON and never passes --fix.
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
	if _, err := runLint(context.Background(), filepath.Join(t.TempDir(), "absent"), []string{"u.sql"}); err == nil {
		t.Fatal("a tool that cannot start must fail the gate")
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

func wheelWith(t *testing.T, name string, body []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create(name)
	if err != nil {
		t.Fatalf("zip create: %v", err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatalf("zip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

func sha(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestExtractBinaryVerifiesThePin(t *testing.T) {
	good := wheelWith(t, binaryInWheel, []byte("ELF"))
	if got, err := extractBinary(good, sha(good)); err != nil || string(got) != "ELF" {
		t.Fatalf("extractBinary = %q, %v", got, err)
	}
	if _, err := extractBinary(good, strings.Repeat("0", 64)); err == nil || !strings.Contains(err.Error(), "refusing an unverified download") {
		t.Fatalf("a hash mismatch must be refused, got %v", err)
	}
	other := wheelWith(t, "dsql_lint-9.9.9.data/scripts/dsql-lint", []byte("ELF"))
	if _, err := extractBinary(other, sha(other)); err == nil || !strings.Contains(err.Error(), "no "+binaryInWheel) {
		t.Fatalf("a wheel of another version must be refused, got %v", err)
	}
}

func TestWheelsPinEveryCIPlatform(t *testing.T) {
	for _, platform := range []string{"linux/amd64", "linux/arm64"} {
		w, ok := wheels[platform]
		if !ok {
			t.Fatalf("no pinned dsql-lint wheel for %s, which CI runs on", platform)
		}
		if !strings.Contains(w.URL, "dsql_lint-"+ToolVersion+"-") || len(w.SHA256) != 64 {
			t.Errorf("%s wheel %+v does not pin dsql-lint %s with a sha256", platform, w, ToolVersion)
		}
	}
}

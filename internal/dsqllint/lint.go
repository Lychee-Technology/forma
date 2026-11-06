package dsqllint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// lintOutput is dsql-lint's --format json document (schema_version 1).
type lintOutput struct {
	SchemaVersion int        `json:"schema_version"`
	Files         []lintFile `json:"files"`
}

// lintFile is one file's result. Unmarshal leaves a field the document omits at
// its zero value, so these types keep what a required field's absence looks
// like apart from its legitimate empty value: see check.
type lintFile struct {
	File string `json:"file"`
	// Diagnostics is nil when omitted or null, and empty, not nil, for [].
	Diagnostics []Diagnostic `json:"diagnostics"`
	// Error is nil when omitted, and holds the JSON text otherwise, null included.
	Error json.RawMessage `json:"error"`
}

// check answers why this result does not say the file was linted: a required
// field is missing, or the tool could not lint it.
func (f lintFile) check() error {
	if f.File == "" {
		return errors.New("dsql-lint output has a file result with no file name")
	}
	if f.Error == nil {
		return fmt.Errorf("dsql-lint result for %s has no error field: a malformed result is not a clean file", f.File)
	}
	var lintErr *string
	if err := json.Unmarshal(f.Error, &lintErr); err != nil {
		return fmt.Errorf("dsql-lint result for %s has an error field that is neither null nor a string: %w", f.File, err)
	}
	if lintErr != nil {
		return fmt.Errorf("dsql-lint could not lint %s: %s", f.File, *lintErr)
	}
	if f.Diagnostics == nil {
		return fmt.Errorf("dsql-lint result for %s has no diagnostics array: a malformed result is not a clean file", f.File)
	}
	for i, d := range f.Diagnostics {
		if err := d.check(); err != nil {
			return fmt.Errorf("dsql-lint result for %s: diagnostic %d %w", f.File, i+1, err)
		}
	}
	return nil
}

// Diagnostic is one dsql-lint finding.
type Diagnostic struct {
	Rule             string `json:"rule"`
	Line             int    `json:"line"`
	Message          string `json:"message"`
	Suggestion       string `json:"suggestion"`
	StatementPreview string `json:"statement_preview"`
}

// check answers which field the gate reads is missing. Suggestion and
// StatementPreview are not required: the gate decides nothing on them.
func (d Diagnostic) check() error {
	if d.Rule == "" {
		return errors.New("has no rule")
	}
	if d.Line < 1 {
		return fmt.Errorf("(%s) has no line: got %d", d.Rule, d.Line)
	}
	if d.Message == "" {
		return fmt.Errorf("(%s) has no message", d.Rule)
	}
	return nil
}

// ParseLintOutput interprets one dsql-lint run over files. Exit 0 must come
// with no diagnostics and exit 1 with at least one; anything else (a usage
// error, exit 1 without diagnostics, which is an I/O error, a file error, an
// unknown schema, a file result or diagnostic missing a required field, a
// result for a file not asked about or none for one that was) fails closed.
func ParseLintOutput(exitCode int, stdout []byte, files []string) (map[string][]Diagnostic, error) {
	if exitCode != 0 && exitCode != 1 {
		return nil, fmt.Errorf("dsql-lint exited %d (2 is a usage error): %s", exitCode, strings.TrimSpace(string(stdout)))
	}
	var out lintOutput
	if err := json.Unmarshal(stdout, &out); err != nil {
		return nil, fmt.Errorf("parse dsql-lint JSON output (exit %d): %w", exitCode, err)
	}
	if out.SchemaVersion != 1 {
		return nil, fmt.Errorf("dsql-lint JSON schema_version %d, the gate reads 1: review the pinned tool upgrade", out.SchemaVersion)
	}
	if out.Files == nil {
		return nil, errors.New("dsql-lint JSON output has no files array")
	}
	asked := make(map[string]bool, len(files))
	for _, f := range files {
		asked[f] = true
	}
	byFile := make(map[string][]Diagnostic, len(out.Files))
	total := 0
	for _, f := range out.Files {
		if err := f.check(); err != nil {
			return nil, err
		}
		if !asked[f.File] {
			return nil, fmt.Errorf("dsql-lint output has a result for %s, which it was not asked to lint", f.File)
		}
		if _, dup := byFile[f.File]; dup {
			return nil, fmt.Errorf("dsql-lint output has two results for %s", f.File)
		}
		byFile[f.File] = f.Diagnostics
		total += len(f.Diagnostics)
	}
	for _, f := range files {
		if _, ok := byFile[f]; !ok {
			return nil, fmt.Errorf("dsql-lint output has no result for %s", f)
		}
	}
	if exitCode == 1 && total == 0 {
		return nil, errors.New("dsql-lint exited 1 without diagnostics: an I/O or tool error")
	}
	if exitCode == 0 && total > 0 {
		return nil, fmt.Errorf("dsql-lint exited 0 with %d diagnostics: its exit code and output disagree", total)
	}
	return byFile, nil
}

// runLint runs `dsql-lint --format json` over paths. It never passes --fix:
// the gate reads SQL, it does not rewrite it.
func runLint(ctx context.Context, tool string, paths []string) (map[string][]Diagnostic, error) {
	cmd := exec.CommandContext(ctx, tool, append([]string{"--format", "json"}, paths...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	exitCode := 0
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			return nil, fmt.Errorf("run %s: %w", tool, err)
		}
		exitCode = exitErr.ExitCode()
	}
	byFile, err := ParseLintOutput(exitCode, stdout.Bytes(), paths)
	if err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, fmt.Errorf("%w (stderr: %s)", err, msg)
		}
		return nil, fmt.Errorf("read the dsql-lint result: %w", err)
	}
	return byFile, nil
}

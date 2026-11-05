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

type lintFile struct {
	File        string       `json:"file"`
	Diagnostics []Diagnostic `json:"diagnostics"`
	Error       *string      `json:"error"`
}

// Diagnostic is one dsql-lint finding.
type Diagnostic struct {
	Rule             string `json:"rule"`
	Line             int    `json:"line"`
	Message          string `json:"message"`
	Suggestion       string `json:"suggestion"`
	StatementPreview string `json:"statement_preview"`
}

// ParseLintOutput interprets one dsql-lint run over files. Exit 0 must come
// with no diagnostics and exit 1 with at least one; anything else (a usage
// error, exit 1 without diagnostics, which is an I/O error, a file error, an
// unknown schema, a result for a file not asked about or none for one that
// was) fails closed.
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
	asked := make(map[string]bool, len(files))
	for _, f := range files {
		asked[f] = true
	}
	byFile := make(map[string][]Diagnostic, len(out.Files))
	total := 0
	for _, f := range out.Files {
		if f.Error != nil {
			return nil, fmt.Errorf("dsql-lint could not lint %s: %s", f.File, *f.Error)
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

package dsqllint

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
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

// Finding is a diagnostic traced to the generator of its statement.
type Finding struct {
	File   string
	Origin string
	Diagnostic
}

// ParseLintOutput interprets one dsql-lint run over files. Exit 0 must
// come with no diagnostics and exit 1 with at least one; anything else (a
// usage error, exit 1 without diagnostics, which is an I/O error, a file
// error, an unknown schema, a missing file) fails closed.
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
	byFile := make(map[string][]Diagnostic, len(out.Files))
	total := 0
	for _, f := range out.Files {
		if f.Error != nil {
			return nil, fmt.Errorf("dsql-lint could not lint %s: %s", f.File, *f.Error)
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
		return nil, fmt.Errorf("dsql-lint exited 1 without diagnostics: an I/O or tool error")
	}
	if exitCode == 0 && total > 0 {
		return nil, fmt.Errorf("dsql-lint exited 0 with %d diagnostics: its exit code and output disagree", total)
	}
	return byFile, nil
}

// Origins maps each statement line of a corpus file to the origin comment
// before it.
type Origins struct {
	lines   []int
	origins []string
	// Statements counts the executable statements under the origin
	// comments, derived from the SQL itself, not from the labels.
	Statements int
}

// ReadOrigins reads a corpus file's origin comments and counts the SQL
// statements under them. It fails closed on a label with no executable SQL
// after it (only comments, blank lines or semicolons), which would be
// counted yet lint nothing, and on SQL before the first label, which no
// diagnostic could be traced from.
func ReadOrigins(r io.Reader) (Origins, error) {
	var o Origins
	var body strings.Builder
	flush := func() error {
		n := corpus.CountStatements(body.String())
		body.Reset()
		switch {
		case len(o.origins) == 0 && n > 0:
			return fmt.Errorf("corpus file holds %d statement(s) before its first origin comment", n)
		case len(o.origins) > 0 && n == 0:
			return fmt.Errorf("origin %s on line %d labels no executable SQL statement", o.origins[len(o.origins)-1], o.lines[len(o.lines)-1])
		}
		o.Statements += n
		return nil
	}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for n := 1; scanner.Scan(); n++ {
		rest, ok := strings.CutPrefix(scanner.Text(), corpus.OriginPrefix)
		if !ok {
			body.WriteString(scanner.Text())
			body.WriteByte('\n')
			continue
		}
		if err := flush(); err != nil {
			return Origins{}, err
		}
		o.lines = append(o.lines, n)
		o.origins = append(o.origins, strings.TrimSpace(rest))
	}
	if err := scanner.Err(); err != nil {
		return Origins{}, fmt.Errorf("read corpus origins: %w", err)
	}
	if err := flush(); err != nil {
		return Origins{}, err
	}
	return o, nil
}

// At answers the origin of the statement at line: that of the last origin
// comment before it.
func (o Origins) At(line int) string {
	i := sort.SearchInts(o.lines, line) // first comment at or after line
	if i == 0 {
		return ""
	}
	return o.origins[i-1]
}

// Set answers the distinct origins.
func (o Origins) Set() map[string]bool {
	set := make(map[string]bool, len(o.origins))
	for _, origin := range o.origins {
		set[origin] = true
	}
	return set
}

// Known is one known-findings line: a finding the gate accepts because an
// issue tracks it.
type Known struct {
	Origin, Rule, Issue, Note string
	Line                      int
}

func (k Known) key() string { return k.Origin + " " + k.Rule }

// ParseKnown reads known-findings lines of the form
//
//	<origin> <rule> <#issue> <note>
//
// Each accepts every diagnostic of that rule on statements of that origin,
// and nothing else: there is no way to disable a rule globally.
func ParseKnown(r io.Reader) ([]Known, error) {
	var known []Known
	seen := make(map[string]int)
	scanner := bufio.NewScanner(r)
	for n := 1; scanner.Scan(); n++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 4 || !strings.HasPrefix(f[2], "#") || len(f[2]) < 2 {
			return nil, fmt.Errorf("known findings line %d: want <origin> <rule> <#issue> <note>, got %q", n, line)
		}
		k := Known{Origin: f[0], Rule: f[1], Issue: f[2], Note: strings.Join(f[3:], " "), Line: n}
		if prev, ok := seen[k.key()]; ok {
			return nil, fmt.Errorf("known findings line %d: %s is already listed on line %d", n, k.key(), prev)
		}
		seen[k.key()] = n
		known = append(known, k)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read known findings: %w", err)
	}
	return known, nil
}

// Verdict splits findings into those a known entry accepts and those it
// does not, and names the known entries nothing matched any more.
type Verdict struct {
	Accepted   []Finding
	Unexpected []Finding
	Stale      []Known
}

// Judge compares findings with known.
func Judge(findings []Finding, known []Known) Verdict {
	byKey := make(map[string]Known, len(known))
	for _, k := range known {
		byKey[k.key()] = k
	}
	matched := make(map[string]bool)
	var v Verdict
	for _, f := range findings {
		key := f.Origin + " " + f.Rule
		if _, ok := byKey[key]; ok {
			matched[key] = true
			v.Accepted = append(v.Accepted, f)
			continue
		}
		v.Unexpected = append(v.Unexpected, f)
	}
	for _, k := range known {
		if !matched[k.key()] {
			v.Stale = append(v.Stale, k)
		}
	}
	return v
}

// IssueFor answers the tracking issue of an accepted finding.
func IssueFor(f Finding, known []Known) string {
	for _, k := range known {
		if k.Origin == f.Origin && k.Rule == f.Rule {
			return k.Issue
		}
	}
	return ""
}

// FormatFinding renders f for a CI log: where, which generator, which rule,
// what to do.
func FormatFinding(f Finding) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s:%d: [%s] %s\n", f.File, f.Line, f.Rule, f.Message)
	fmt.Fprintf(&b, "    generated by: %s\n", f.Origin)
	if f.Suggestion != "" {
		fmt.Fprintf(&b, "    suggestion:   %s\n", f.Suggestion)
	}
	if f.StatementPreview != "" {
		fmt.Fprintf(&b, "    statement:    %s\n", oneLine(f.StatementPreview, 200))
	}
	return b.String()
}

func oneLine(s string, limit int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > limit {
		return s[:limit] + "..."
	}
	return s
}

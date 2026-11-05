package dsqllint

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

// Known is one known-findings line: exactly Count diagnostics of Rule on
// statements labelled Label, accepted because Issue tracks them.
type Known struct {
	Label, Rule, Issue, Reason string
	Count                      int
	Line                       int
}

func (k Known) key() findingKey { return findingKey{label: k.Label, rule: k.Rule} }

// findingKey is what a known finding is matched on until #806 re-keys it to a
// verified site.
type findingKey struct{ label, rule string }

func (k findingKey) String() string { return k.label + " " + k.rule }

var (
	ruleName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	issueRef = regexp.MustCompile(`^#[1-9][0-9]*$`)
)

// ParseKnown reads known-findings lines of the form
//
//	<label> <rule> <count> #<issue> -- <reason>
//
// A label is a whole path/file.go:Symbol location, so an entry cannot use a
// wildcard, and nothing can disable a rule globally.
func ParseKnown(r io.Reader) ([]Known, error) {
	var known []Known
	seen := make(map[findingKey]int)
	scanner := bufio.NewScanner(r)
	for n := 1; scanner.Scan(); n++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, err := parseKnownLine(line)
		if err != nil {
			return nil, fmt.Errorf("known findings line %d: %w", n, err)
		}
		k.Line = n
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

func parseKnownLine(line string) (Known, error) {
	const shape = "want <label> <rule> <count> #<issue> -- <reason>"
	f := strings.Fields(line)
	if len(f) < 6 || f[4] != "--" {
		return Known{}, fmt.Errorf("%s, got %q", shape, line)
	}
	count, err := strconv.Atoi(f[2])
	switch {
	case !corpus.LabelPattern.MatchString(f[0]):
		return Known{}, fmt.Errorf("label %q is not a path/file.go:Symbol location: %s", f[0], shape)
	case !ruleName.MatchString(f[1]):
		return Known{}, fmt.Errorf("rule %q is not a dsql-lint rule name: %s", f[1], shape)
	case err != nil || count < 1:
		return Known{}, fmt.Errorf("count %q must be a positive number: %s", f[2], shape)
	case !issueRef.MatchString(f[3]):
		return Known{}, fmt.Errorf("%q does not name a tracking issue: %s", f[3], shape)
	}
	return Known{Label: f[0], Rule: f[1], Count: count, Issue: f[3], Reason: strings.Join(f[5:], " ")}, nil
}

// CountMismatch is a known entry whose findings are not exactly as many as it
// lists. Got 0 means the entry is stale.
type CountMismatch struct {
	Known    Known
	Got      int
	Findings []Finding
}

// Verdict compares findings with the known list.
type Verdict struct {
	// Accepted are the findings of entries whose count matched exactly.
	Accepted []Finding
	// Mismatched are the entries whose count did not. An extra diagnostic
	// cannot be absorbed by an existing entry, and a fixed one forces the
	// entry to shrink.
	Mismatched []CountMismatch
	// Unexpected are the findings no entry names: a new rule, or a known rule
	// from another label.
	Unexpected []Finding
}

// Passed reports whether every finding is known and every count exact.
func (v Verdict) Passed() bool { return len(v.Mismatched) == 0 && len(v.Unexpected) == 0 }

// Judge compares findings with known, key by key and by exact count.
func Judge(findings []Finding, known []Known) Verdict {
	byKey := make(map[findingKey][]Finding)
	listed := make(map[findingKey]bool, len(known))
	for _, k := range known {
		listed[k.key()] = true
	}
	var v Verdict
	for _, f := range findings {
		key := findingKey{label: f.Record.Label, rule: f.Rule}
		if !listed[key] {
			v.Unexpected = append(v.Unexpected, f)
			continue
		}
		byKey[key] = append(byKey[key], f)
	}
	for _, k := range known {
		got := byKey[k.key()]
		if len(got) != k.Count {
			v.Mismatched = append(v.Mismatched, CountMismatch{Known: k, Got: len(got), Findings: got})
			continue
		}
		v.Accepted = append(v.Accepted, got...)
	}
	return v
}

// issueFor answers the tracking issue of an accepted finding.
func issueFor(f Finding, known []Known) string {
	for _, k := range known {
		if k.Label == f.Record.Label && k.Rule == f.Rule {
			return k.Issue
		}
	}
	return ""
}

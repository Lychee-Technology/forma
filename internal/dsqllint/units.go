package dsqllint

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

// Finding is a diagnostic traced to the record of its statement.
type Finding struct {
	// File is the linted unit, as a report names it.
	File   string
	Record corpus.Record
	Diagnostic
}

// unit is one corpus unit: its records, and the SQL file linted from them.
type unit struct {
	name    string
	records []corpus.Record
	// sqlPath is where writeSQL put the unit's statements, starts the line
	// each record begins on there, and end the line after the last.
	sqlPath string
	starts  []int
	end     int
}

// loadUnits reads every unit file in dir, in name order. Anything else in the
// directory, or no unit at all, fails closed.
func loadUnits(dir string) ([]*unit, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("list corpus units: %w", err)
	}
	var units []*unit
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), corpus.UnitFileSuffix)
		if !ok {
			return nil, fmt.Errorf("the corpus directory holds %s, which is not a unit's records file", e.Name())
		}
		records, err := corpus.ReadUnit(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("load corpus unit %s: %w", name, err)
		}
		units = append(units, &unit{name: name, records: records})
	}
	if len(units) == 0 {
		return nil, fmt.Errorf("the corpus export wrote no unit to %s: refusing to lint nothing", dir)
	}
	sort.Slice(units, func(i, j int) bool { return units[i].name < units[j].name })
	return units, nil
}

// checkNotEmpty answers a problem per record whose SQL holds no statement.
func checkNotEmpty(ctx context.Context, checker *emptinessChecker, units []*unit) ([]error, error) {
	var problems []error
	for _, u := range units {
		for _, r := range u.records {
			empty, err := checker.isEmpty(ctx, r.SQL)
			if err != nil {
				return nil, fmt.Errorf("corpus unit %s record %d: %w", u.name, r.Seq, err)
			}
			if empty {
				problems = append(problems, fmt.Errorf("corpus unit %s record %d (%s, asserted) holds no statement: its SQL is empty, whitespace or comments, so it would be counted and lint nothing: %q", u.name, r.Seq, r.Label, r.SQL))
			}
		}
	}
	return problems, nil
}

// writeSQL writes the unit's statements to a file in dir for dsql-lint, each
// starting on its own line, and notes which line that is. A statement ends
// with its own line holding the terminator, because a statement may end in a
// line comment that would swallow one on the same line. The empty statement
// this adds after a statement that already has a semicolon is harmless:
// dsql-lint ignores it.
func (u *unit) writeSQL(dir string) error {
	var b strings.Builder
	line := 1
	u.starts = make([]int, len(u.records))
	for i, r := range u.records {
		u.starts[i] = line
		text := r.SQL + "\n;\n"
		b.WriteString(text)
		line += strings.Count(text, "\n")
	}
	u.end = line
	u.sqlPath = filepath.Join(dir, u.name+".sql")
	if err := os.WriteFile(u.sqlPath, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("write the SQL of corpus unit %s: %w", u.name, err)
	}
	return nil
}

// display names the unit in a report, independent of the directory it was
// linted in.
func (u *unit) display() string { return "corpus/" + u.name + ".sql" }

// recordAt answers the record whose statement holds line.
func (u *unit) recordAt(line int) (corpus.Record, bool) {
	if line < 1 || line >= u.end {
		return corpus.Record{}, false
	}
	i := sort.SearchInts(u.starts, line+1) - 1
	return u.records[i], true
}

// lintUnits runs dsql-lint once over every unit, lint-only, and traces each
// diagnostic to its record. A diagnostic on a line no record holds fails
// closed. Findings are ordered by unit, then record, then rule, because
// dsql-lint does not report a file's diagnostics in line order.
func lintUnits(ctx context.Context, tool string, units []*unit) ([]Finding, error) {
	paths := make([]string, len(units))
	for i, u := range units {
		paths[i] = u.sqlPath
	}
	byFile, err := runLint(ctx, tool, paths)
	if err != nil {
		return nil, fmt.Errorf("lint the corpus: %w", err)
	}
	var findings []Finding
	for _, u := range units {
		var unitFindings []Finding
		for _, d := range byFile[u.sqlPath] {
			r, ok := u.recordAt(d.Line)
			if !ok {
				return nil, fmt.Errorf("dsql-lint reported %s on line %d of %s, which holds no record", d.Rule, d.Line, u.display())
			}
			unitFindings = append(unitFindings, Finding{File: u.display(), Record: r, Diagnostic: d})
		}
		sort.SliceStable(unitFindings, func(i, j int) bool {
			a, b := unitFindings[i], unitFindings[j]
			if a.Record.Seq != b.Record.Seq {
				return a.Record.Seq < b.Record.Seq
			}
			if a.Rule != b.Rule {
				return a.Rule < b.Rule
			}
			return a.Line < b.Line
		})
		findings = append(findings, unitFindings...)
	}
	return findings, nil
}

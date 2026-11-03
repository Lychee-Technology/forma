package dsqllint

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// target is one file the gate lints: a corpus unit, whose statements carry
// origin comments, or a checked-in SQL file, which is its own origin.
type target struct {
	path    string
	origins Origins
	static  string
}

// display names t in a report: the corpus unit, or the checked-in file.
func (t target) display() string {
	if t.static != "" {
		return t.static
	}
	return "corpus/" + filepath.Base(t.path)
}

func (t target) originAt(line int) string {
	if t.static != "" {
		return t.static
	}
	if o := t.origins.At(line); o != "" {
		return o
	}
	return "<unlabelled statement>"
}

type targets struct {
	files      []target
	origins    map[string]bool
	units      int
	statements int
}

// collectTargets reads the rendered corpus in dir and adds the rendered
// checked-in SQL files. No unit, or a unit without a statement, fails
// closed.
func collectTargets(root, dir string, entries []Entry) (targets, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil {
		return targets{}, fmt.Errorf("list corpus units: %w", err)
	}
	sort.Strings(paths)
	t := targets{origins: make(map[string]bool)}
	for _, p := range paths {
		o, err := readOriginsFile(p)
		if err != nil {
			return targets{}, err
		}
		if o.Statements == 0 {
			return targets{}, fmt.Errorf("corpus unit %s has no labelled statement", filepath.Base(p))
		}
		t.files = append(t.files, target{path: p, origins: o})
		t.units++
		t.statements += o.Statements
		for origin := range o.Set() {
			t.origins[origin] = true
		}
	}
	if t.units == 0 {
		return targets{}, fmt.Errorf("the corpus export wrote no unit to %s: refusing to lint nothing", dir)
	}
	for _, e := range entries {
		if e.Class == ClassRendered && e.Symbol() == "" {
			t.files = append(t.files, target{path: filepath.Join(root, e.File()), static: e.Location})
			t.origins[e.Location] = true
		}
	}
	return t, nil
}

func readOriginsFile(path string) (Origins, error) {
	f, err := os.Open(path)
	if err != nil {
		return Origins{}, fmt.Errorf("open corpus unit: %w", err)
	}
	defer func() { _ = f.Close() }()
	o, err := ReadOrigins(f)
	if err != nil {
		return Origins{}, fmt.Errorf("%s: %w", path, err)
	}
	return o, nil
}

// lintTargets runs dsql-lint once over every target, lint-only, and traces
// each diagnostic to its origin.
func lintTargets(ctx context.Context, tool string, t targets) ([]Finding, error) {
	paths := make([]string, len(t.files))
	for i, f := range t.files {
		paths[i] = f.path
	}
	byFile, err := runLint(ctx, tool, paths)
	if err != nil {
		return nil, err
	}
	var findings []Finding
	for _, f := range t.files {
		for _, d := range byFile[f.path] {
			findings = append(findings, Finding{File: f.display(), Origin: f.originAt(d.Line), Diagnostic: d})
		}
	}
	return findings, nil
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
		return nil, err
	}
	return byFile, nil
}

// expectPrefix marks a rule a regression fixture must trigger.
const expectPrefix = "-- expect: "

// selfTest lints each regression fixture on its own and requires exactly
// the rules its expect comments name: an invalid fixture proves the pinned
// tool still rejects what it must, a valid one that it accepts what it
// must. A tool upgrade that changes either fails here first.
func selfTest(ctx context.Context, cfg Config) []error {
	paths, err := filepath.Glob(filepath.Join(cfg.Root, RegressionDir, "*.sql"))
	if err != nil || len(paths) == 0 {
		return []error{fmt.Errorf("no regression fixtures in %s (%v)", RegressionDir, err)}
	}
	sort.Strings(paths)
	var problems []error
	var sawInvalid, sawValid bool
	for _, p := range paths {
		want, err := expectedRules(p)
		if err != nil {
			problems = append(problems, err)
			continue
		}
		sawInvalid = sawInvalid || len(want) > 0
		sawValid = sawValid || len(want) == 0
		byFile, err := runLint(ctx, cfg.Tool, []string{p})
		if err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", filepath.Base(p), err))
			continue
		}
		got := map[string]bool{}
		for _, d := range byFile[p] {
			got[d.Rule] = true
		}
		if g, w := strings.Join(sortedKeys(got), ","), strings.Join(sortedKeys(want), ","); g != w {
			problems = append(problems, fmt.Errorf("%s: dsql-lint reported rules [%s], the fixture expects [%s]", filepath.Base(p), g, w))
		}
	}
	if !sawInvalid || !sawValid {
		problems = append(problems, fmt.Errorf("%s needs at least one fixture with expect comments and one without", RegressionDir))
	}
	return problems
}

func expectedRules(path string) (map[string]bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open regression fixture: %w", err)
	}
	defer func() { _ = f.Close() }()
	want := map[string]bool{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if rule, ok := strings.CutPrefix(strings.TrimSpace(scanner.Text()), expectPrefix); ok {
			want[strings.TrimSpace(rule)] = true
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return want, nil
}

// printSummary writes the coverage inventory and every finding.
func printSummary(out io.Writer, entries []Entry, t targets, v Verdict, known []Known) {
	counts := map[Class]int{}
	for _, e := range entries {
		counts[e.Class]++
	}
	fmt.Fprintf(out, "DSQL lint gate (dsql-lint %s, lint-only)\n", ToolVersion)
	fmt.Fprintf(out, "Inventory: %d rendered, %d uncovered, %d postgres-only, %d duckdb, %d not-dsql (%s)\n",
		counts[ClassRendered], counts[ClassUncovered], counts[ClassPostgresOnly], counts[ClassDuckDB], counts[ClassNotDSQL], InventoryPath)
	fmt.Fprintf(out, "Linted: %d corpus units, %d statements from %d generators\n", t.units, t.statements, len(t.origins))
	fmt.Fprintln(out, "Not linted (DSQL-reachable, uncovered):")
	for _, e := range entries {
		if e.Class == ClassUncovered {
			fmt.Fprintf(out, "  %s: %s\n", e.Location, e.Note)
		}
	}
	if len(v.Accepted) > 0 {
		fmt.Fprintf(out, "\nKnown findings (%d, accepted by %s):\n", len(v.Accepted), KnownPath)
		for _, f := range v.Accepted {
			fmt.Fprintf(out, "%s    tracked by:   %s\n", FormatFinding(f), IssueFor(f, known))
		}
	}
	if len(v.Unexpected) > 0 {
		fmt.Fprintf(out, "\nNEW findings (%d): fix the generator, or if the SQL is intentionally not DSQL SQL, reclassify it in %s:\n", len(v.Unexpected), InventoryPath)
		for _, f := range v.Unexpected {
			fmt.Fprint(out, FormatFinding(f))
		}
	}
	for _, k := range v.Stale {
		fmt.Fprintf(out, "\nSTALE known finding (%s line %d): %s %s no longer occurs; remove it (and check %s)\n", KnownPath, k.Line, k.Origin, k.Rule, k.Issue)
	}
	if len(v.Unexpected) == 0 && len(v.Stale) == 0 {
		fmt.Fprintln(out, "\nDSQL lint gate passed. A clean dsql-lint result is not proof of DSQL compatibility; see docs/dsql-lint.md.")
	}
}

package dsqllint

import (
	"fmt"
	"io"
	"strings"

	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

// corpusSummary is the counts a gate run reports.
type corpusSummary struct {
	units, records, executed, rendered, labels int
}

func summarize(units []*unit) corpusSummary {
	s := corpusSummary{units: len(units)}
	labels := make(map[string]bool)
	for _, u := range units {
		for _, r := range u.records {
			s.records++
			labels[r.Label] = true
			if r.Kind == corpus.KindExecuted {
				s.executed++
			} else {
				s.rendered++
			}
		}
	}
	s.labels = len(labels)
	return s
}

// FormatFinding renders f for a CI log: where, which rule, the corpus origin
// label and how much it can be trusted, what to do, and the statement.
func FormatFinding(f Finding) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s:%d: [%s] %s\n", f.File, f.Line, f.Rule, f.Message)
	fmt.Fprintf(&b, "    origin:       %s (asserted by the corpus test, not verified, until #806)\n", f.Record.Label)
	fmt.Fprintf(&b, "    record:       %s via %s, record %d of the unit\n", f.Record.Kind, f.Record.Mechanism, f.Record.Seq)
	if f.Suggestion != "" {
		fmt.Fprintf(&b, "    suggestion:   %s\n", f.Suggestion)
	}
	fmt.Fprintf(&b, "    statement:    %s\n", strings.Join(strings.Fields(f.Record.SQL), " "))
	return b.String()
}

// printSummary writes what was linted, what was not, and every finding. It
// holds no path or time, so two runs over one corpus print the same bytes.
func printSummary(out io.Writer, s corpusSummary, v Verdict, known []Known) {
	fmt.Fprintf(out, "DSQL lint gate (dsql-lint %s, lint-only)\n", ToolVersion)
	fmt.Fprintf(out, "Linted: %d corpus units, %d statements (%d executed, %d rendered) from %d origin labels\n", s.units, s.records, s.executed, s.rendered, s.labels)
	fmt.Fprintln(out, "Not checked: SQL outside the corpus. The gate has no SQL inventory yet (#804, #805), and origin labels are asserted (#806).")
	if len(v.Accepted) > 0 {
		fmt.Fprintf(out, "\nKnown findings (%d, accepted by %s):\n", len(v.Accepted), KnownPath)
		for _, f := range v.Accepted {
			fmt.Fprintf(out, "%s    tracked by:   %s\n", FormatFinding(f), issueFor(f, known))
		}
	}
	for _, m := range v.Mismatched {
		fmt.Fprintf(out, "\nKNOWN FINDING COUNT CHANGED (%s line %d): %s lists %d %s, the corpus has %d. Fix what changed, or update the count and check %s.\n",
			KnownPath, m.Known.Line, m.Known.Label, m.Known.Count, m.Known.Rule, m.Got, m.Known.Issue)
		for _, f := range m.Findings {
			fmt.Fprint(out, FormatFinding(f))
		}
	}
	if len(v.Unexpected) > 0 {
		fmt.Fprintf(out, "\nNEW findings (%d): fix the code that produces the statement, or list it in %s under the issue that tracks it:\n", len(v.Unexpected), KnownPath)
		for _, f := range v.Unexpected {
			fmt.Fprint(out, FormatFinding(f))
		}
	}
	if v.Passed() {
		fmt.Fprintln(out, "\nDSQL lint gate passed. A clean dsql-lint result is not proof of DSQL compatibility; see docs/dsql-lint.md.")
		return
	}
	fmt.Fprintln(out, "\nDSQL lint gate failed; see docs/dsql-lint.md for what to do.")
}

func report(out io.Writer, what string, problems []error) error {
	fmt.Fprintf(out, "DSQL lint gate: %d %s problem(s):\n", len(problems), what)
	for _, p := range problems {
		fmt.Fprintf(out, "  - %v\n", p)
	}
	return ErrGateFailed
}

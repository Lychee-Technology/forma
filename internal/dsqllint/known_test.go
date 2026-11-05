package dsqllint

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

func TestParseKnownRefusesWhatIsNotAnExactTrackedEntry(t *testing.T) {
	for name, text := range map[string]string{
		"no issue":        "a.go:F index_async 1 needs-an-issue -- note\n",
		"bare number":     "a.go:F index_async 1 690 -- note\n",
		"no count":        "a.go:F index_async #690 -- note\n",
		"zero count":      "a.go:F index_async 0 #690 -- note\n",
		"negative count":  "a.go:F index_async -1 #690 -- note\n",
		"wildcard count":  "a.go:F index_async * #690 -- note\n",
		"no reason":       "a.go:F index_async 1 #690 --\n",
		"no separator":    "a.go:F index_async 1 #690 note\n",
		"wildcard label":  "* index_async 1 #690 -- note\n",
		"wildcard symbol": "a.go:* index_async 1 #690 -- note\n",
		"wildcard rule":   "a.go:F * 1 #690 -- note\n",
		"duplicate":       "a.go:F index_async 1 #690 -- x\na.go:F index_async 2 #691 -- y\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseKnown(strings.NewReader(text)); err == nil {
				t.Errorf("ParseKnown(%q) accepted it", text)
			}
		})
	}
	known, err := ParseKnown(strings.NewReader("# c\n\na.go:F index_async 2 #690 -- init-db indexes\n"))
	if err != nil || len(known) != 1 {
		t.Fatalf("ParseKnown = %+v, %v", known, err)
	}
	if k := known[0]; k.Label != "a.go:F" || k.Rule != "index_async" || k.Count != 2 || k.Issue != "#690" || k.Reason != "init-db indexes" || k.Line != 3 {
		t.Fatalf("ParseKnown = %+v", k)
	}
}

func finding(label, rule string) Finding {
	return Finding{File: "corpus/u.sql", Record: corpus.Record{Label: label, SQL: "SELECT 1"}, Diagnostic: Diagnostic{Rule: rule, Line: 1}}
}

// TestJudgeMatchesExactCounts is T-29, label-keyed: one more, one fewer, a new
// rule on a known label, or a known rule on another label all fail.
func TestJudgeMatchesExactCounts(t *testing.T) {
	known := []Known{{Label: "a.go:F", Rule: "index_async", Count: 2, Issue: "#690"}}
	two := []Finding{finding("a.go:F", "index_async"), finding("a.go:F", "index_async")}

	if v := Judge(two, known); !v.Passed() || len(v.Accepted) != 2 {
		t.Fatalf("exactly the listed findings must pass: %+v", v)
	}
	tests := []struct {
		name           string
		findings       []Finding
		wantGot        int // the count reported as mismatched, -1 for none
		wantUnexpected int
	}{
		{"one more", append(append([]Finding{}, two...), finding("a.go:F", "index_async")), 3, 0},
		{"one fewer", two[:1], 1, 0},
		{"none left", nil, 0, 0},
		{"a new rule on the label", append(append([]Finding{}, two...), finding("a.go:F", "serial_type")), -1, 1},
		{"the rule from another label", append(append([]Finding{}, two...), finding("a.go:G", "index_async")), -1, 1},
		{"moved to another label", []Finding{finding("a.go:F", "index_async"), finding("a.go:G", "index_async")}, 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := Judge(tt.findings, known)
			if v.Passed() {
				t.Fatalf("Judge passed %+v", tt.findings)
			}
			if len(v.Unexpected) != tt.wantUnexpected {
				t.Errorf("Unexpected = %+v, want %d", v.Unexpected, tt.wantUnexpected)
			}
			if tt.wantGot < 0 {
				if len(v.Mismatched) != 0 {
					t.Errorf("Mismatched = %+v, want none", v.Mismatched)
				}
				return
			}
			if len(v.Mismatched) != 1 || v.Mismatched[0].Got != tt.wantGot {
				t.Errorf("Mismatched = %+v, want one entry with Got %d", v.Mismatched, tt.wantGot)
			}
			if len(v.Accepted) != 0 {
				t.Errorf("a mismatched entry accepted %+v", v.Accepted)
			}
		})
	}
}

// TestFormatFindingShowsTheStatementAndTheAssertedOrigin is the interim form of
// #740's criterion 7: a diagnostic names the statement and the corpus label,
// and says the label is asserted until #806.
func TestFormatFindingShowsTheStatementAndTheAssertedOrigin(t *testing.T) {
	report := FormatFinding(Finding{
		File:       "corpus/u.sql",
		Record:     corpus.Record{Kind: corpus.KindExecuted, Mechanism: corpus.MechanismPgxmock, Seq: 4, Label: "cmd/tools/init_db.go:eavIndexDDL", SQL: "CREATE INDEX i\n   ON t (a)"},
		Diagnostic: Diagnostic{Rule: "index_async", Line: 9, Message: "CREATE INDEX without ASYNC", Suggestion: "Use ASYNC."},
	})
	for _, want := range []string{
		"corpus/u.sql:9: [index_async] CREATE INDEX without ASYNC",
		"origin:       cmd/tools/init_db.go:eavIndexDDL (asserted by the corpus test, not verified, until #806)",
		"executed via pgxmock, record 4",
		"suggestion:   Use ASYNC.",
		"statement:    CREATE INDEX i ON t (a)",
	} {
		if !strings.Contains(report, want) {
			t.Errorf("FormatFinding lacks %q:\n%s", want, report)
		}
	}
}

func TestParseFloorAndCheckFloor(t *testing.T) {
	for name, text := range map[string]string{
		"empty":     "# nothing\n",
		"no min":    "a.go:F\n",
		"zero":      "a.go:F 0\n",
		"bad label": "F 1\n",
		"duplicate": "a.go:F 1\na.go:F 2\n",
	} {
		if _, err := ParseFloor(strings.NewReader(text)); err == nil {
			t.Errorf("%s: ParseFloor(%q) accepted it", name, text)
		}
	}
	floors, err := ParseFloor(strings.NewReader("a.go:F 2\na.go:G 1\n"))
	if err != nil {
		t.Fatalf("ParseFloor: %v", err)
	}
	u := &unit{records: []corpus.Record{{Label: "a.go:F"}, {Label: "a.go:F"}, {Label: "a.go:F"}, {Label: "a.go:New"}}}
	problems := checkFloor(floors, []*unit{u})
	if len(problems) != 1 || !strings.Contains(problems[0].Error(), "holds 0 record(s) of a.go:G, the floor is 1") {
		t.Fatalf("checkFloor = %v: only the lost label must fail, not more records or a new label", problems)
	}
}

func TestRecordAtTracesEveryLineOfAStatement(t *testing.T) {
	u := &unit{name: "u", records: []corpus.Record{
		{Label: "a.go:One", SQL: "SELECT 1"},
		{Label: "a.go:Two", SQL: "-- leading\n\nSELECT\n2"},
		{Label: "a.go:Three", SQL: "SELECT 3 -- trailing"},
	}}
	dir := t.TempDir()
	if err := u.writeSQL(dir); err != nil {
		t.Fatalf("writeSQL: %v", err)
	}
	text, err := os.ReadFile(filepath.Join(dir, "u.sql"))
	if err != nil {
		t.Fatalf("read SQL: %v", err)
	}
	want := "SELECT 1\n;\n-- leading\n\nSELECT\n2\n;\nSELECT 3 -- trailing\n;\n"
	if string(text) != want {
		t.Fatalf("unit SQL = %q, want %q: a terminator must not join a trailing comment", text, want)
	}
	for line, label := range map[int]string{1: "a.go:One", 2: "a.go:One", 3: "a.go:Two", 6: "a.go:Two", 7: "a.go:Two", 8: "a.go:Three", 9: "a.go:Three"} {
		if r, ok := u.recordAt(line); !ok || r.Label != label {
			t.Errorf("recordAt(%d) = %q, %v, want %q", line, r.Label, ok, label)
		}
	}
	for _, line := range []int{-1, 0, 10, 100} {
		if r, ok := u.recordAt(line); ok {
			t.Errorf("recordAt(%d) = %q, want none: no record holds it", line, r.Label)
		}
	}
}

// TestCheckedInBaselines holds the checked-in data to #740's baseline: 15
// known diagnostics, all tracked by #690, and a corpus floor of 106 statements
// from 64 generators.
func TestCheckedInBaselines(t *testing.T) {
	root := filepath.Join("..", "..")
	known, floors, err := loadInputs(root)
	if err != nil {
		t.Fatalf("loadInputs: %v", err)
	}
	diagnostics := 0
	for _, k := range known {
		diagnostics += k.Count
		if k.Issue != "#690" {
			t.Errorf("%s: known finding tracked by %s, the baseline is all #690", k.key(), k.Issue)
		}
	}
	if diagnostics != 15 {
		t.Errorf("known findings list %d diagnostics, the baseline is 15", diagnostics)
	}
	statements := 0
	for _, f := range floors {
		statements += f.Min
	}
	if len(floors) < 64 || statements < 106 {
		t.Errorf("the corpus floor holds %d statements from %d labels, below PR #801's 106 from 64", statements, len(floors))
	}
}

// TestRegressionFixturesNameADDLRestriction holds the invalid fixture to the
// criterion that it proves a DDL restriction is caught on every run.
func TestRegressionFixturesNameADDLRestriction(t *testing.T) {
	want, err := expectedRules(filepath.Join("testdata", "regression", "invalid.sql"))
	if err != nil {
		t.Fatalf("expectedRules: %v", err)
	}
	for _, rule := range []string{"multi_ddl_transaction", "index_async"} {
		if !want[rule] {
			t.Errorf("invalid.sql does not expect %s", rule)
		}
	}
	valid, err := expectedRules(filepath.Join("testdata", "regression", "valid.sql"))
	if err != nil {
		t.Fatalf("expectedRules: %v", err)
	}
	if len(valid) != 0 {
		t.Errorf("valid.sql expects %v, want no rule", valid)
	}
}

package dsqllint

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// repoRoot is the repository root seen from this package's directory.
const repoRoot = "../.."

// TestInventoryCoversRepository is the guard that keeps new SQL from reaching
// Aurora DSQL unclassified (#740): every non-test Go symbol holding a
// SQL-shaped literal, and every checked-in .sql file, must have an inventory
// entry, and every entry must still name something that exists. make test
// runs it, so the guard holds even where the dsql-lint download is not
// available.
func TestInventoryCoversRepository(t *testing.T) {
	entries, known, err := loadInputs(repoRoot)
	if err != nil {
		t.Fatalf("load gate inputs: %v", err)
	}
	for _, p := range checkAll(repoRoot, entries, known) {
		t.Error(p)
	}
}

func TestParseInventoryRefusesUnexplainedOrAmbiguousEntries(t *testing.T) {
	tests := []struct {
		name, text, want string
	}{
		{"missing note", "rendered a/b.go:F\n", "want <class> <location> <note>"},
		{"unknown class", "ignored a/b.go:F because\n", "unknown class"},
		{"bad location", "uncovered a/b.go because\n", "is not path/file.go:Symbol"},
		{"rendered wildcard", "rendered a/b.go:* all of it\n", "must name one symbol"},
		{"duplicate", "uncovered a/b.go:F x\nduckdb a/b.go:F y\n", "already listed on line 1"},
		{"empty", "# only a comment\n", "no entries"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseInventory(strings.NewReader(tt.text))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ParseInventory error = %v, want one containing %q", err, tt.want)
			}
		})
	}
}

// scanFixture writes a tiny repository and scans it. It is not a git
// checkout, so Scan walks it.
func scanFixture(t *testing.T, files map[string]string) Snapshot {
	t.Helper()
	root := t.TempDir()
	for rel, text := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
	snap, err := Scan(root)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return snap
}

const fixtureGo = `package p

import "fmt"

const tableDDL = ` + "`CREATE TABLE IF NOT EXISTS %s (a INT)`" + `

type repo struct{}

func (r *repo) load(table string) string {
	return fmt.Sprintf("SELECT a FROM %s WHERE a = $1", table)
}

func helper() string { return "begin tx: not SQL" }

func upd(t string) string { return fmt.Sprintf("UPDATE %s cl SET a = 1", t) }
`

func TestScanFindsSQLBySymbol(t *testing.T) {
	snap := scanFixture(t, map[string]string{
		"p/p.go":            fixtureGo,
		"p/p_test.go":       "package p\nconst q = `SELECT 1 FROM t`\n",
		"p/testdata/x.sql":  "SELECT 1;",
		"scripts/check.sql": "SELECT 1;",
	})
	for _, want := range []string{"p/p.go:tableDDL", "p/p.go:repo.load", "p/p.go:upd"} {
		if _, ok := snap.Sites[want]; !ok {
			t.Errorf("Scan missed SQL in %s; sites = %v", want, snap.Sites)
		}
	}
	if _, ok := snap.Sites["p/p.go:helper"]; ok {
		t.Error("lower-case prose is not SQL, but helper was reported")
	}
	if len(snap.Sites) != 3 {
		t.Errorf("Scan reported %d sites, want 3 (test files are not scanned): %v", len(snap.Sites), snap.Sites)
	}
	if !snap.Symbols["p/p.go:helper"] {
		t.Error("Symbols must list every top-level symbol, helper included")
	}
	if !snap.SQLFiles["scripts/check.sql"] || snap.SQLFiles["p/testdata/x.sql"] {
		t.Errorf("SQLFiles = %v, want scripts/check.sql and no testdata file", snap.SQLFiles)
	}
}

func TestCheckInventoryReportsEveryDiscrepancy(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": fixtureGo, "q/q.go": fixtureGo})
	entries := []Entry{
		{Class: ClassRendered, Location: "p/p.go:tableDDL", Line: 1},
		{Class: ClassRendered, Location: "p/p.go:helper", Line: 2},     // rendered needs no literal
		{Class: ClassUncovered, Location: "p/p.go:repo.load", Line: 3}, // fine
		// p/p.go:upd is missing
		{Class: ClassNotDSQL, Location: "p/p.go:gone", Line: 4},
		{Class: ClassDuckDB, Location: "q/q.go:*", Line: 5},
		{Class: ClassDuckDB, Location: "q/q.go:upd", Line: 6}, // also under the wildcard
		{Class: ClassNotDSQL, Location: "scripts/none.sql", Line: 7},
	}
	var got []string
	for _, p := range CheckInventory(entries, snap) {
		got = append(got, p.Error())
	}
	joined := strings.Join(got, "\n")
	for _, want := range []string{
		"p/p.go:upd holds SQL (line 15) that no inventory entry classifies",
		"p/p.go:gone does not exist",
		"q/q.go:upd is listed both on its own and under q/q.go:*",
		"SQL file scripts/none.sql does not exist",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("CheckInventory problems lack %q:\n%s", want, joined)
		}
	}
	if len(got) != 4 {
		t.Errorf("CheckInventory reported %d problems, want 4:\n%s", len(got), joined)
	}
}

func TestCheckInventoryRefusesAnExclusionWhoseSymbolHoldsNoSQL(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": fixtureGo})
	entries := []Entry{
		{Class: ClassRendered, Location: "p/p.go:tableDDL"},
		{Class: ClassRendered, Location: "p/p.go:repo.load"},
		{Class: ClassRendered, Location: "p/p.go:upd"},
		{Class: ClassPostgresOnly, Location: "p/p.go:helper", Line: 9},
	}
	problems := CheckInventory(entries, snap)
	if len(problems) != 1 || !strings.Contains(problems[0].Error(), "p/p.go:helper holds no SQL any more") {
		t.Fatalf("CheckInventory = %v, want the stale postgres-only entry refused", problems)
	}
}

package dsqllint

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// emptyProbes hold no statement: dsql-lint exits 0 on each, as it does on a
// valid statement, so only this check can refuse them.
var emptyProbes = []string{
	"",
	" ",
	"\n\t ",
	"  \n\t\v\f\n",
	";",
	";;;",
	" ; ; ",
	"-- only a comment",
	"-- one\n-- two\n",
	"/* block */",
	"/* outer /* nested */ outer */",
	"-- line\n/* and /* nested */ block */ ;\n",
}

// nonEmptyProbes hold a statement, or text only dsql-lint should judge: DuckDB
// has no parser for several of them, and its parser error must not count as
// empty.
var nonEmptyProbes = []string{
	"SELECT 1",
	"select 1;",
	"  SELECT 1  ",
	"-- leading\nSELECT 1",
	"/* leading */ SELECT 1",
	"SELECT 1 -- trailing",
	"/* a /* b */ c */ SELECT 1",
	"SELECT 'a;b'",
	"SELECT '-- not a comment'",
	"SELECT '/* not a comment */'",
	`SELECT "a;b" FROM t`,
	"SELECT $body$ x; y $body$",
	"SELECT $$;$$",
	"SELECT E'it\\'s'",
	"SELECT E'it\\'s'; SELECT 1",
	"SELECT 1; SELECT 2",
	"CREATE TABLE a (id INT); CREATE TABLE b (id INT)",
	"CREATE TABLE ok (id BIGINT PRIMARY KEY)",
	"CREATE INDEX ASYNC i ON t (a)",
	"CREATE INDEX CONCURRENTLY i ON t (a)",
	"begin isolation level repeatable read read only",
	"BEGIN",
	"COMMIT",
	"SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
	"INSERT INTO t (a) VALUES ($1) ON CONFLICT (a) DO UPDATE SET a = $1",
	"SELEC 1",
}

// TestEmptinessCheckClassifiesTheProbes is T-25: only DuckDB's empty-query
// answer counts, and a DuckDB parser error still proceeds to dsql-lint.
func TestEmptinessCheckClassifiesTheProbes(t *testing.T) {
	ctx := context.Background()
	checker, err := newEmptinessChecker(ctx)
	if err != nil {
		t.Fatalf("newEmptinessChecker: %v", err)
	}
	defer checker.close()
	for want, probes := range map[bool][]string{true: emptyProbes, false: nonEmptyProbes} {
		for _, probe := range probes {
			got, err := checker.isEmpty(ctx, probe)
			if err != nil {
				t.Errorf("isEmpty(%q): %v", probe, err)
			} else if got != want {
				t.Errorf("isEmpty(%q) = %v, want %v", probe, got, want)
			}
		}
	}
}

// TestEmptinessCheckCannotReachOutside pins that a multi-statement record,
// whose earlier statements the driver executes while preparing it, stays inside
// the in-memory database.
func TestEmptinessCheckCannotReachOutside(t *testing.T) {
	ctx := context.Background()
	checker, err := newEmptinessChecker(ctx)
	if err != nil {
		t.Fatalf("newEmptinessChecker: %v", err)
	}
	defer checker.close()
	path := filepath.Join(t.TempDir(), "escaped.csv")
	empty, err := checker.isEmpty(ctx, "COPY (SELECT 1) TO '"+path+"'; SELECT 1")
	if err != nil {
		t.Fatalf("isEmpty: %v", err)
	}
	if empty {
		t.Error("a statement counted as empty")
	}
	if _, err := os.Stat(path); err == nil {
		t.Errorf("the check wrote %s", path)
	}
}

func TestEmptinessCheckStopsOnACancelledContext(t *testing.T) {
	checker, err := newEmptinessChecker(context.Background())
	if err != nil {
		t.Fatalf("newEmptinessChecker: %v", err)
	}
	defer checker.close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := checker.isEmpty(ctx, "SELECT 1"); err == nil {
		t.Fatal("a cancelled check must not answer")
	}
}

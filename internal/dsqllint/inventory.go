// Package dsqllint is the static Aurora DSQL compatibility gate (#740): it
// renders the SQL Forma's generators produce, lints it with AWS's pinned
// dsql-lint, and keeps an inventory of every place in the code that holds
// SQL so that new SQL cannot reach DSQL without being classified.
//
// dsql-lint is an early-release static checker. A clean run is not proof that
// a live Aurora DSQL cluster accepts the SQL or behaves as PostgreSQL does;
// see docs/dsql-lint.md.
package dsqllint

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// Class says what the gate does with an inventory location.
type Class string

const (
	// ClassRendered: a TestDSQLCorpus test renders this generator's SQL and
	// the gate lints it. Every rendered location must appear as an origin in
	// the corpus, and every corpus origin must be a rendered location.
	ClassRendered Class = "rendered"
	// ClassUncovered: SQL that can reach DSQL but is not linted. The note
	// says why (inline dynamic SQL, a parser limitation) and what tracks it.
	ClassUncovered Class = "uncovered"
	// ClassPostgresOnly: SQL that runs only under the PostgreSQL engine,
	// such as advisory locks. It must stay out of the corpus.
	ClassPostgresOnly Class = "postgres-only"
	// ClassDuckDB: SQL sent to DuckDB, never to PostgreSQL or DSQL.
	ClassDuckDB Class = "duckdb"
	// ClassNotDSQL: anything else that is never sent to a DSQL database:
	// test harnesses, offline tools, error text that only looks like SQL.
	ClassNotDSQL Class = "not-dsql"
)

func (c Class) valid() bool {
	switch c {
	case ClassRendered, ClassUncovered, ClassPostgresOnly, ClassDuckDB, ClassNotDSQL:
		return true
	}
	return false
}

// Entry is one inventory line.
type Entry struct {
	Class Class
	// Location is "path/file.go:Symbol" (a function, method as
	// Type.Method, or package-level const/var), "path/file.go:*" for every
	// symbol of a file, or "path/file.sql" for a checked-in SQL file.
	Location string
	Note     string
	Line     int
}

// File is the path part of the location.
func (e Entry) File() string {
	file, _, _ := strings.Cut(e.Location, ":")
	return file
}

// Symbol is the symbol part of the location: "" for a SQL file, "*" for a
// whole Go file.
func (e Entry) Symbol() string {
	_, sym, _ := strings.Cut(e.Location, ":")
	return sym
}

var locationShape = regexp.MustCompile(`^[A-Za-z0-9_./-]+(\.go:(\*|[A-Za-z_][A-Za-z0-9_.]*)|\.sql)$`)

// ParseInventory reads inventory lines of the form
//
//	<class> <location> <note>
//
// Blank lines and lines starting with # are skipped. Every entry needs a
// note: an exclusion without a stated scope and rationale is refused.
func ParseInventory(r io.Reader) ([]Entry, error) {
	var entries []Entry
	seen := make(map[string]int)
	scanner := bufio.NewScanner(r)
	for n := 1; scanner.Scan(); n++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			return nil, fmt.Errorf("inventory line %d: want <class> <location> <note>, got %q", n, line)
		}
		e := Entry{Class: Class(fields[0]), Location: fields[1], Note: strings.Join(fields[2:], " "), Line: n}
		if !e.Class.valid() {
			return nil, fmt.Errorf("inventory line %d: unknown class %q", n, fields[0])
		}
		if !locationShape.MatchString(e.Location) {
			return nil, fmt.Errorf("inventory line %d: location %q is not path/file.go:Symbol, path/file.go:* or path/file.sql", n, e.Location)
		}
		if e.Class == ClassRendered && e.Symbol() == "*" {
			return nil, fmt.Errorf("inventory line %d: a rendered entry must name one symbol, not %s", n, e.Location)
		}
		if prev, ok := seen[e.Location]; ok {
			return nil, fmt.Errorf("inventory line %d: %s is already listed on line %d", n, e.Location, prev)
		}
		seen[e.Location] = n
		entries = append(entries, e)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read inventory: %w", err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("inventory has no entries")
	}
	return entries, nil
}

// CheckInventory compares entries with what Scan found and answers every
// discrepancy: SQL in code or a .sql file no entry classifies, an entry whose
// location no longer exists, an entry that no longer holds SQL, and a symbol
// listed both on its own and under its file's wildcard.
func CheckInventory(entries []Entry, snap Snapshot) []error {
	byLocation := make(map[string]Entry, len(entries))
	for _, e := range entries {
		byLocation[e.Location] = e
	}
	var problems []error
	for _, site := range snap.sortedSites() {
		_, exact := byLocation[site]
		_, wildcard := byLocation[fileOf(site)+":*"]
		switch {
		case exact && wildcard:
			problems = append(problems, fmt.Errorf("%s is listed both on its own and under %s:*; keep one", site, fileOf(site)))
		case !exact && !wildcard:
			problems = append(problems, fmt.Errorf("%s holds SQL (line %d) that no inventory entry classifies: add it to %s as rendered (with a TestDSQLCorpus export), uncovered, postgres-only, duckdb or not-dsql",
				site, snap.Sites[site], InventoryPath))
		}
	}
	for _, e := range entries {
		if err := checkEntryExists(e, snap); err != nil {
			problems = append(problems, err)
		}
	}
	return problems
}

func checkEntryExists(e Entry, snap Snapshot) error {
	switch sym := e.Symbol(); {
	case sym == "":
		if !snap.SQLFiles[e.File()] {
			return fmt.Errorf("inventory line %d: SQL file %s does not exist", e.Line, e.File())
		}
	case sym == "*":
		if !snap.hasSiteIn(e.File()) {
			return fmt.Errorf("inventory line %d: %s holds no SQL any more; remove the entry", e.Line, e.Location)
		}
	case !snap.Symbols[e.Location]:
		return fmt.Errorf("inventory line %d: %s does not exist; update or remove the entry", e.Line, e.Location)
	case e.Class != ClassRendered:
		// A rendered entry may name a symbol without a SQL literal, such as
		// the code that opens a transaction; the corpus check proves it is
		// rendered. Any other entry exists only because its symbol holds SQL.
		if _, ok := snap.Sites[e.Location]; !ok {
			return fmt.Errorf("inventory line %d: %s holds no SQL any more; remove the entry", e.Line, e.Location)
		}
	}
	return nil
}

func fileOf(location string) string {
	file, _, _ := strings.Cut(location, ":")
	return file
}

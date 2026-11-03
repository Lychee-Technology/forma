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
	// test harnesses, offline tools, error text that only looks like SQL,
	// a method that only shares a database method's name.
	ClassNotDSQL Class = "not-dsql"
	// ClassPassthrough: code that sends, forwards or declares SQL another
	// inventory location builds (an executor of a builder, a wrapper, an
	// interface) and builds none itself. The note names the builder.
	ClassPassthrough Class = "passthrough"
)

func (c Class) valid() bool {
	switch c {
	case ClassRendered, ClassUncovered, ClassPostgresOnly, ClassDuckDB, ClassNotDSQL, ClassPassthrough:
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
	// Gap is the gap group of an uncovered entry.
	Gap string
}

// Gap is a declared group of uncovered entries: one coverage gap, and the
// tracking that lists its entries.
type Gap struct {
	Name, Tracking, Note string
	Line                 int
}

// Inventory is the parsed inventory file.
type Inventory struct {
	Entries []Entry
	Gaps    []Gap
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

// ParseInventory reads inventory lines of the forms
//
//	<class> <location> <note>
//	gap <name> <tracking> <note>
//
// Blank lines and lines starting with # are skipped. Every entry needs a
// note: an exclusion without a stated scope and rationale is refused. An
// uncovered entry's note starts with [<name>], the gap group it belongs to;
// a gap line declares that group and what tracks closing it, and must have
// at least one entry.
func ParseInventory(r io.Reader) (Inventory, error) {
	var inv Inventory
	seen := make(map[string]int)
	gaps := make(map[string]int)
	scanner := bufio.NewScanner(r)
	for n := 1; scanner.Scan(); n++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if fields[0] == "gap" {
			g, err := parseGap(n, fields)
			if err != nil {
				return Inventory{}, err
			}
			if prev, ok := gaps[g.Name]; ok {
				return Inventory{}, fmt.Errorf("inventory line %d: gap %s is already declared on line %d", n, g.Name, prev)
			}
			gaps[g.Name] = n
			inv.Gaps = append(inv.Gaps, g)
			continue
		}
		e, err := parseEntry(n, fields, line)
		if err != nil {
			return Inventory{}, err
		}
		if prev, ok := seen[e.Location]; ok {
			return Inventory{}, fmt.Errorf("inventory line %d: %s is already listed on line %d", n, e.Location, prev)
		}
		seen[e.Location] = n
		inv.Entries = append(inv.Entries, e)
	}
	if err := scanner.Err(); err != nil {
		return Inventory{}, fmt.Errorf("read inventory: %w", err)
	}
	if len(inv.Entries) == 0 {
		return Inventory{}, fmt.Errorf("inventory has no entries")
	}
	if err := checkGaps(inv, gaps); err != nil {
		return Inventory{}, err
	}
	return inv, nil
}

var (
	gapName    = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	gapLabel   = regexp.MustCompile(`^\[([a-z][a-z0-9-]*)\]$`)
	issueShape = regexp.MustCompile(`^#[0-9]+$`)
)

func parseGap(n int, fields []string) (Gap, error) {
	if len(fields) < 4 {
		return Gap{}, fmt.Errorf("inventory line %d: want gap <name> <tracking> <note>", n)
	}
	g := Gap{Name: fields[1], Tracking: fields[2], Note: strings.Join(fields[3:], " "), Line: n}
	if !gapName.MatchString(g.Name) {
		return Gap{}, fmt.Errorf("inventory line %d: gap name %q must be lower-case letters, digits and dashes", n, g.Name)
	}
	if !issueShape.MatchString(g.Tracking) && g.Tracking != TrackingFollowUp {
		return Gap{}, fmt.Errorf("inventory line %d: gap %s tracking %q must be an issue (#123) or %s", n, g.Name, g.Tracking, TrackingFollowUp)
	}
	return g, nil
}

// TrackingFollowUp marks a gap tracked by the DSQL lint coverage follow-up
// issue, which lists every such gap by name with its entries.
const TrackingFollowUp = "dsql-lint-coverage-follow-up"

func parseEntry(n int, fields []string, line string) (Entry, error) {
	if len(fields) < 3 {
		return Entry{}, fmt.Errorf("inventory line %d: want <class> <location> <note>, got %q", n, line)
	}
	e := Entry{Class: Class(fields[0]), Location: fields[1], Note: strings.Join(fields[2:], " "), Line: n}
	if !e.Class.valid() {
		return Entry{}, fmt.Errorf("inventory line %d: unknown class %q", n, fields[0])
	}
	if !locationShape.MatchString(e.Location) {
		return Entry{}, fmt.Errorf("inventory line %d: location %q is not path/file.go:Symbol, path/file.go:* or path/file.sql", n, e.Location)
	}
	if e.Class == ClassRendered && e.Symbol() == "*" {
		return Entry{}, fmt.Errorf("inventory line %d: a rendered entry must name one symbol, not %s", n, e.Location)
	}
	if e.Class == ClassUncovered {
		m := gapLabel.FindStringSubmatch(fields[2])
		if m == nil || len(fields) < 4 {
			return Entry{}, fmt.Errorf("inventory line %d: an uncovered entry's note starts with its [gap] and says why, got %q", n, e.Note)
		}
		e.Gap = m[1]
	}
	return e, nil
}

// checkGaps holds the gap declarations and the uncovered entries to each
// other: every uncovered entry names a declared gap, and every gap has an
// entry, so a gap closed by rendering its last entry is removed.
func checkGaps(inv Inventory, declared map[string]int) error {
	used := make(map[string]bool)
	for _, e := range inv.Entries {
		if e.Class != ClassUncovered {
			continue
		}
		if _, ok := declared[e.Gap]; !ok {
			return fmt.Errorf("inventory line %d: gap [%s] of %s is not declared by a gap line", e.Line, e.Gap, e.Location)
		}
		used[e.Gap] = true
	}
	for _, g := range inv.Gaps {
		if !used[g.Name] {
			return fmt.Errorf("inventory line %d: gap %s has no uncovered entry any more; remove it and update %s", g.Line, g.Name, g.Tracking)
		}
	}
	return nil
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
			problems = append(problems, fmt.Errorf("%s %s (line %d) and no inventory entry classifies it: add it to %s as rendered (with a TestDSQLCorpus export), uncovered (with its [gap]), passthrough, postgres-only, duckdb or not-dsql",
				site, snap.Sites[site].Why, snap.Sites[site].Line, InventoryPath))
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

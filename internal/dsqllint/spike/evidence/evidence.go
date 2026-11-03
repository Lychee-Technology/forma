// Package evidence is the EXPERIMENTAL static-to-dynamic evidence join of
// the DSQL typed call-site spike (#740): it holds the typed site inventory,
// a classification manifest and the attributed corpus records to each
// other, and fails closed on every mismatch.
package evidence

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"forma-dsql-spike/attr"
	"forma-dsql-spike/sites"
)

// Class is a manifest classification.
type Class string

const (
	DSQL         Class = "dsql"          // must have corpus evidence; linted
	PostgresOnly Class = "postgres-only" // must never appear in the DSQL corpus
	DuckDB       Class = "duckdb"        // must never appear in the DSQL corpus
	Uncovered    Class = "uncovered"     // DSQL-reachable, not exercised; needs #issue
	Boundary     Class = "boundary"      // COMMIT/ROLLBACK; evidence optional
)

// Entry is one manifest line: <class> <site-id> <reason>.
type Entry struct {
	Class  Class
	Site   string
	Reason string
	Line   int
}

// Manifest is the parsed classification file.
type Manifest struct {
	Entries   []Entry
	Unshipped map[string]string // main package -> reason
}

var issueRef = regexp.MustCompile(`#[0-9]+\b`)

// Parse reads manifest lines of the forms
//
//	<class> <site-id> <reason>
//	unshipped-main <import-path> <reason>
func Parse(r io.Reader) (Manifest, []error) {
	m := Manifest{Unshipped: map[string]string{}}
	var errs []error
	seen := map[string]int{}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 3 {
			errs = append(errs, fmt.Errorf("manifest line %d: want <class> <site> <reason>", n))
			continue
		}
		reason := strings.Join(f[2:], " ")
		if f[0] == "unshipped-main" {
			m.Unshipped[f[1]] = reason
			continue
		}
		e := Entry{Class: Class(f[0]), Site: f[1], Reason: reason, Line: n}
		switch e.Class {
		case DSQL, PostgresOnly, DuckDB, Boundary:
		case Uncovered:
			if !issueRef.MatchString(reason) {
				errs = append(errs, fmt.Errorf("manifest line %d: uncovered %s needs a tracking issue (#123) in its reason", n, e.Site))
			}
		default:
			errs = append(errs, fmt.Errorf("manifest line %d: unknown class %q", n, f[0]))
		}
		if prev, ok := seen[e.Site]; ok {
			errs = append(errs, fmt.Errorf("manifest line %d: %s already classified on line %d", n, e.Site, prev))
		}
		seen[e.Site] = n
		m.Entries = append(m.Entries, e)
	}
	return m, errs
}

// Report is the outcome of one join, with the three coverage levels kept
// apart.
type Report struct {
	Problems []string `json:"problems"`
	// Static inventory coverage: discovered sites that are classified.
	SitesTotal      int `json:"sites_total"`
	SitesClassified int `json:"sites_classified"`
	// Dynamic execution coverage: dsql sites with at least one record.
	DSQLSites          int `json:"dsql_sites"`
	DSQLWithEvidence   int `json:"dsql_sites_with_evidence"`
	RecordsTotal       int `json:"records_total"`
	RecordsAttributed  int `json:"records_attributed"`
	RecordsInDSQLSites int `json:"records_in_dsql_sites"`
}

// Join holds inventory, manifest, main packages and attributed records to
// each other.
func Join(inv *sites.Result, m Manifest, mains, entries []string, outcomes []attr.Outcome) Report {
	var r Report
	bad := func(format string, a ...any) { r.Problems = append(r.Problems, fmt.Sprintf(format, a...)) }
	classOf := map[string]Class{}
	for _, e := range m.Entries {
		classOf[e.Site] = e.Class
	}
	live := map[string]bool{}
	r.SitesTotal = len(inv.Sites)
	for _, s := range inv.Sites {
		live[s.ID] = true
		if _, ok := classOf[s.ID]; ok {
			r.SitesClassified++
		} else {
			bad("unclassified site %s (%s:%d)", s.ID, s.File, s.Line)
		}
	}
	for _, e := range m.Entries {
		if !live[e.Site] {
			bad("stale manifest entry line %d: %s no longer exists", e.Line, e.Site)
		}
	}
	for _, f := range inv.Findings {
		bad("unsupported pattern %s at %s: %s", f.Kind, f.Pos, f.Detail)
	}
	shipped := map[string]bool{}
	for _, e := range entries {
		shipped[e] = true
	}
	for _, mp := range mains {
		if !shipped[mp] && m.Unshipped[mp] == "" {
			bad("main package %s is neither a shipped entry point nor listed unshipped", mp)
		}
	}
	evidence := map[string]int{}
	r.RecordsTotal = len(outcomes)
	for _, o := range outcomes {
		switch o.Status {
		case "attributed":
			r.RecordsAttributed++
		case "ambiguous":
			bad("record %q (%s) attributes to several sites: %s", o.SQL, o.Test, strings.Join(o.Candidates, ", "))
			continue
		default:
			bad("record %q (%s) attributes to no site", o.SQL, o.Test)
			continue
		}
		evidence[o.SiteID]++
		switch c := classOf[o.SiteID]; c {
		case DSQL:
			r.RecordsInDSQLSites++
		case Boundary:
		case "":
			bad("record %q comes from unclassified site %s", o.SQL, o.SiteID)
		default:
			bad("record %q comes from %s site %s: it must not be in the DSQL corpus", o.SQL, c, o.SiteID)
		}
	}
	for _, e := range m.Entries {
		if e.Class != DSQL {
			continue
		}
		r.DSQLSites++
		if evidence[e.Site] > 0 {
			r.DSQLWithEvidence++
		} else if live[e.Site] {
			bad("dsql site %s has no corpus evidence", e.Site)
		}
	}
	sort.Strings(r.Problems)
	return r
}

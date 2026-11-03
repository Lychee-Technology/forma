// Package attr is the EXPERIMENTAL runtime origin attribution of the DSQL
// typed call-site spike (#740): it maps the stack captured when a corpus
// statement reached a recording driver to a static site of the inventory.
package attr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"forma-dsql-spike/sites"
)

// Frame is one captured stack frame.
type Frame struct {
	Func string `json:"func"`
	File string `json:"file"`
	Line int    `json:"line"`
}

// Record is one captured corpus statement.
type Record struct {
	Test      string  `json:"test"`
	Label     string  `json:"label"`
	SQL       string  `json:"sql"`
	Mechanism string  `json:"mechanism"`
	Frames    []Frame `json:"frames"`
}

// Outcome is the attribution of one record.
type Outcome struct {
	Record     Record   `json:"-"`
	Test       string   `json:"test"`
	Label      string   `json:"label"`
	SQL        string   `json:"sql"`
	Mechanism  string   `json:"mechanism"`
	Status     string   `json:"status"` // attributed | ambiguous | failed
	SiteID     string   `json:"site_id,omitempty"`
	Candidates []string `json:"candidates,omitempty"`
	Frame      string   `json:"frame,omitempty"`
	site       *sites.Site
}

// Site answers the attributed site, if any.
func (o Outcome) Site() *sites.Site { return o.site }

// ReadRecords reads a records.jsonl file.
func ReadRecords(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open records: %w", err)
	}
	defer func() { _ = f.Close() }()
	var out []Record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<24)
	for sc.Scan() {
		var r Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, fmt.Errorf("decode record: %w", err)
		}
		out = append(out, r)
	}
	return out, sc.Err()
}

// Attribute maps each record to the innermost module, non-test frame that
// lies inside a site's call span. Frames outside the module (drivers,
// mocks, runtime), in _test.go files, or at no site (wrappers forwarding
// SQL, helpers) are passed over. More than one site on the matching line
// is ambiguous, unless exactly one of them sends the record's SQL as a
// literal.
func Attribute(recs []Record, inv *sites.Result, root string) []Outcome {
	byFile := map[string][]*sites.Site{}
	for i := range inv.Sites {
		s := &inv.Sites[i]
		byFile[s.File] = append(byFile[s.File], s)
	}
	out := make([]Outcome, 0, len(recs))
	for _, r := range recs {
		o := Outcome{Record: r, Test: r.Test, Label: r.Label, SQL: r.SQL, Mechanism: r.Mechanism, Status: "failed"}
		for _, f := range r.Frames {
			rel, err := filepath.Rel(root, f.File)
			if err != nil || strings.HasPrefix(rel, "..") || strings.HasSuffix(rel, "_test.go") {
				continue
			}
			var hits []*sites.Site
			for _, s := range byFile[filepath.ToSlash(rel)] {
				if s.Line <= f.Line && f.Line <= s.EndLine {
					hits = append(hits, s)
				}
			}
			if len(hits) == 0 {
				continue
			}
			o.Frame = fmt.Sprintf("%s:%d %s", rel, f.Line, f.Func)
			if len(hits) > 1 {
				if lit := byLiteral(hits, r.SQL); lit != nil {
					hits = []*sites.Site{lit}
				}
			}
			if len(hits) == 1 {
				o.Status, o.SiteID, o.site = "attributed", hits[0].ID, hits[0]
			} else {
				o.Status = "ambiguous"
				for _, h := range hits {
					o.Candidates = append(o.Candidates, h.ID)
				}
			}
			break
		}
		out = append(out, o)
	}
	return out
}

func byLiteral(hits []*sites.Site, sql string) *sites.Site {
	var found *sites.Site
	for _, h := range hits {
		if h.ArgKind != "literal" {
			continue
		}
		if v, err := strconv.Unquote(h.SQLArg); err == nil && v == sql {
			if found != nil {
				return nil
			}
			found = h
		}
	}
	return found
}

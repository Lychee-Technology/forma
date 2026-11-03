// Command attribute is the EXPERIMENTAL attribution check of the DSQL typed
// call-site spike (#740): it maps captured corpus stacks to inventory sites
// and scores them against the test's asserted label.
//
//	go run ./cmd/attribute -sites sites.json -records records.jsonl -root <forma> [-out outcomes.json]
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"forma-dsql-spike/attr"
	"forma-dsql-spike/sites"
)

func main() {
	sitesPath := flag.String("sites", "", "inventory JSON from sqlsites")
	recPath := flag.String("records", "", "records.jsonl from a SPIKE_ATTR_OUT run")
	root := flag.String("root", "", "forma checkout the stacks were captured in")
	out := flag.String("out", "", "write outcomes JSON here")
	flag.Parse()

	b, err := os.ReadFile(*sitesPath)
	if err != nil {
		fail(err)
	}
	var inv sites.Result
	if err := json.Unmarshal(b, &inv); err != nil {
		fail(err)
	}
	recs, err := attr.ReadRecords(*recPath)
	if err != nil {
		fail(err)
	}
	outcomes := attr.Attribute(recs, &inv, *root)
	type row struct {
		attr.Outcome
		Check string `json:"label_check"`
	}
	var rows []row
	status, check := map[string]int{}, map[string]int{}
	for _, o := range outcomes {
		c := labelCheck(o)
		status[o.Status]++
		check[c]++
		rows = append(rows, row{o, c})
		fmt.Printf("%-10s %-24s %-18s %-60.60s %s\n", o.Status, c, o.Mechanism, o.Label, o.SiteID)
	}
	fmt.Printf("\nrecords=%d status=%v label_check=%v\n", len(outcomes), status, check)
	if *out != "" {
		j, _ := json.MarshalIndent(rows, "", "  ")
		if err := os.WriteFile(*out, j, 0o644); err != nil {
			fail(err)
		}
	}
}

// labelCheck compares the attributed site with the label the corpus test
// asserted. PR #801 labels are "file.go:Symbol" naming the generator; the
// synthetic fixture's are "Function|literal fragment" naming the site.
func labelCheck(o attr.Outcome) string {
	s := o.Site()
	if s == nil {
		return "n/a"
	}
	if fn, frag, ok := strings.Cut(o.Label, "|"); ok {
		if s.Function != fn {
			return "WRONG-function"
		}
		if s.ArgKind == "literal" && !strings.Contains(s.SQLArg, frag) {
			return "WRONG-call"
		}
		return "correct"
	}
	if o.Label == s.File+":"+s.Function {
		return "label=exec-site"
	}
	for _, bld := range s.Builders {
		if bld == o.Label {
			return "label=builder-of-site"
		}
	}
	return "label-differs"
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "attribute:", err)
	os.Exit(1)
}

var _ = sort.Strings

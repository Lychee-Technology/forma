// Command evidence is the EXPERIMENTAL static-to-dynamic join of the DSQL
// typed call-site spike (#740).
//
//	go run ./cmd/evidence -sites sites.json -outcomes attr.json -manifest manifest.txt
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"forma-dsql-spike/attr"
	"forma-dsql-spike/evidence"
	"forma-dsql-spike/sites"
)

func main() {
	sitesPath := flag.String("sites", "", "inventory JSON from sqlsites")
	outPath := flag.String("outcomes", "", "outcomes JSON from attribute")
	manPath := flag.String("manifest", "", "classification manifest")
	module := flag.String("module", "github.com/lychee-technology/forma", "module path")
	flag.Parse()

	var inv struct {
		sites.Result
		Mains []string `json:"main_packages"`
	}
	read(*sitesPath, &inv)
	var outs []attr.Outcome
	read(*outPath, &outs)
	f, err := os.Open(*manPath)
	if err != nil {
		fail(err)
	}
	m, errs := evidence.Parse(f)
	var entries []string
	for _, e := range sites.DefaultEntries {
		if e == "." {
			entries = append(entries, *module)
		} else {
			entries = append(entries, *module+e[1:])
		}
	}
	r := evidence.Join(&inv.Result, m, inv.Mains, entries, outs)
	for _, e := range errs {
		r.Problems = append(r.Problems, e.Error())
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(b))
	if len(r.Problems) > 0 {
		os.Exit(1)
	}
}

func read(path string, v any) {
	b, err := os.ReadFile(path)
	if err != nil {
		fail(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "evidence:", err)
	os.Exit(2)
}

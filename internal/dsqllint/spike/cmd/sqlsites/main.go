// Command sqlsites is the EXPERIMENTAL type-aware SQL call-site inventory of
// the DSQL typed call-site spike (#740). It is not wired into make or CI.
//
//	go run ./cmd/sqlsites -dir <forma checkout> [-mode verbatim|taint] [-out sites.json]
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"forma-dsql-spike/sites"
)

func main() {
	dir := flag.String("dir", "../../../..", "forma checkout")
	mode := flag.String("mode", "verbatim", "wrapper following: verbatim or taint")
	out := flag.String("out", "", "write the inventory JSON here")
	patterns := flag.String("patterns", "", "comma-separated packages to analyse instead of the shipped closure")
	tags := flag.String("tags", "", "build tags")
	flag.Parse()

	opt := sites.Options{Dir: *dir, Mode: sites.Mode(*mode)}
	if *patterns != "" {
		opt.Patterns = strings.Split(*patterns, ",")
	}
	if *tags != "" {
		opt.BuildFlags = []string{"-tags", *tags}
	}
	res, err := sites.Load(opt)
	if err != nil {
		fmt.Fprintln(os.Stderr, "sqlsites:", err)
		os.Exit(1)
	}
	mains, err := sites.MainPackages(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "sqlsites:", err)
		os.Exit(1)
	}
	if *out != "" {
		b, _ := json.MarshalIndent(struct {
			*sites.Result
			Mains []string `json:"main_packages"`
		}{res, mains}, "", "  ")
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "sqlsites:", err)
			os.Exit(1)
		}
	}
	byFamily, byVia, byKind := map[string]int{}, map[string]int{}, map[string]int{}
	for _, s := range res.Sites {
		byFamily[s.Family]++
		byVia[s.Via]++
		byKind[s.ArgKind]++
	}
	findings := map[string]int{}
	for _, f := range res.Findings {
		findings[f.Kind]++
	}
	fmt.Printf("mode=%s packages=%d sites=%d forwarders_folded=%d wrappers=%d interface_sinks=%d findings=%d\n",
		res.Mode, res.Packages, len(res.Sites), res.Forwarders, len(res.Wrappers), len(res.Interfaces), len(res.Findings))
	fmt.Printf("timings=%v peak_rss_kb=%d\n", res.Timings, res.PeakRSSKB)
	fmt.Printf("by_family=%s\nby_via=%s\nby_arg=%s\nfindings=%s\n", kv(byFamily), kv(byVia), kv(byKind), kv(findings))
	fmt.Printf("main_packages=%s\n", strings.Join(mains, " "))
}

func kv(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s:%d", k, m[k])
	}
	return strings.Join(parts, " ")
}

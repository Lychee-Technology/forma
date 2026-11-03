package sites

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestMutations applies each synthetic mutation to the disposable worktree
// named by SPIKE_MUT_DIR, re-runs the inventory in both modes, and records
// what changed against the unmutated baseline. It resets the worktree after
// every case. Results go to SPIKE_MUT_OUT.
func TestMutations(t *testing.T) {
	dir := os.Getenv("SPIKE_MUT_DIR")
	if dir == "" {
		t.Skip("SPIKE_MUT_DIR not set")
	}
	reset(t, dir)
	base := map[Mode]*Result{}
	baseMains, err := MainPackages(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []Mode{Verbatim, Taint} {
		r, err := Load(Options{Dir: dir, Mode: m})
		if err != nil {
			t.Fatal(err)
		}
		base[m] = r
	}
	var out []map[string]any
	for _, mu := range mutations {
		if only := os.Getenv("SPIKE_MUT_ONLY"); only != "" && !strings.HasPrefix(mu.Name, only) {
			continue
		}
		apply(t, dir, mu)
		row := map[string]any{"name": mu.Name, "group": mu.Group, "expect": mu.Expect, "describe": mu.Describe}
		mains, _ := MainPackages(dir)
		row["new_mains"] = diffStr(mains, baseMains)
		for _, m := range []Mode{Verbatim, Taint} {
			r, err := Load(Options{Dir: dir, Mode: m})
			if err != nil {
				row[string(m)+"_error"] = err.Error()
				continue
			}
			row[string(m)] = compare(base[m], r)
		}
		row["verdict"] = verdict(mu, row)
		out = append(out, row)
		t.Logf("%-46s %-16s %v", mu.Name, row["verdict"], row[string(Verbatim)])
		reset(t, dir)
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	if p := os.Getenv("SPIKE_MUT_OUT"); p != "" {
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func compare(base, got *Result) map[string]any {
	res := map[string]any{}
	for _, scheme := range []string{"pos", "ordinal", "expr"} {
		b, g := idSet(base, scheme), idSet(got, scheme)
		res[scheme+"_new"] = diffStr(keys(g), keys(b))
		res[scheme+"_gone"] = diffStr(keys(b), keys(g))
	}
	var bf, gf []string
	for _, f := range base.Findings {
		bf = append(bf, f.Kind+" "+f.Pos)
	}
	for _, f := range got.Findings {
		gf = append(gf, f.Kind+" "+f.Pos)
	}
	res["new_findings"] = diffStr(gf, bf)
	res["sites"] = len(got.Sites)
	return res
}

// verdict classifies a case: detected, rejected conservatively (a
// finding), correctly ignored, documented miss, or SILENT MISS.
func verdict(mu mutation, row map[string]any) string {
	if mu.Group == "identity" {
		return "identity"
	}
	v, ok := row[string(Verbatim)].(map[string]any)
	if !ok {
		return "load-error"
	}
	newSites := len(v["expr_new"].([]string))
	newFindings := len(v["new_findings"].([]string))
	switch {
	case mu.Expect == "mains":
		if len(row["new_mains"].([]string)) > 0 {
			return "detected"
		}
	case newFindings > 0:
		return "rejected-conservatively"
	case newSites > 0:
		return "detected"
	case mu.Expect == "no-change":
		return "correctly-ignored"
	case mu.Expect == "miss-documented" || mu.Expect == "runtime-only":
		return "miss-documented"
	}
	return "SILENT-MISS"
}

func idSet(r *Result, scheme string) map[string]bool {
	s := map[string]bool{}
	for _, site := range r.Sites {
		s[site.IDs[scheme]] = true
	}
	return s
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func diffStr(a, b []string) []string {
	in := map[string]bool{}
	for _, x := range b {
		in[x] = true
	}
	out := []string{}
	for _, x := range a {
		if !in[x] {
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}

func apply(t *testing.T, dir string, mu mutation) {
	t.Helper()
	for rel, text := range mu.Files {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range mu.Edits {
		replace(t, filepath.Join(dir, e.File), e.Old, e.New)
	}
	if mu.Name == "i5_move_file" {
		moveGetChangeLogStats(t, dir)
	}
}

func replace(t *testing.T, path, old, new string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(b), old) != 1 {
		t.Fatalf("%s: edit anchor occurs %d times: %q", path, strings.Count(string(b), old), old)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(b), old, new, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
}

func moveGetChangeLogStats(t *testing.T, dir string) {
	path := filepath.Join(dir, "internal/cdc/helpers.go")
	b, _ := os.ReadFile(path)
	s := string(b)
	start := strings.Index(s, "// GetChangeLogStats returns")
	end := strings.Index(s, "// SelectBatchRowIDs picks")
	fn := s[start:end]
	_ = os.WriteFile(path, []byte(s[:start]+s[end:]), 0o644)
	moved := "package cdc\n\nimport (\n\t\"context\"\n\t\"database/sql\"\n\t\"fmt\"\n)\n\n" + fn
	_ = os.WriteFile(filepath.Join(dir, "internal/cdc/helpers_stats.go"), []byte(moved), 0o644)
}

func reset(t *testing.T, dir string) {
	t.Helper()
	for _, args := range [][]string{{"checkout", "--", "."}, {"clean", "-fdq"}} {
		if out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
}

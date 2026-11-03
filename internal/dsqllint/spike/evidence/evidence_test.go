package evidence

import (
	"strings"
	"testing"

	"forma-dsql-spike/attr"
	"forma-dsql-spike/sites"
)

// SYNTHETIC inventory: two sites in one function, a PostgreSQL-only site, a
// DuckDB site and a COMMIT boundary.
func fixture() *sites.Result {
	return &sites.Result{Sites: []sites.Site{
		{ID: "p.F:pgx.Tx.Exec:aaaa", File: "p/f.go", Line: 10},
		{ID: "p.F:pgx.Tx.Exec:bbbb", File: "p/f.go", Line: 11},
		{ID: "p.Lock:pgx.Tx.QueryRow:cccc", File: "p/lock.go", Line: 5},
		{ID: "p.Duck:sql.Conn.ExecContext:dddd", File: "p/duck.go", Line: 7},
		{ID: "p.F:pgx.Tx.Commit:eeee", File: "p/f.go", Line: 12},
	}}
}

const goodManifest = `
dsql          p.F:pgx.Tx.Exec:aaaa         write path
dsql          p.F:pgx.Tx.Exec:bbbb         write path
postgres-only p.Lock:pgx.Tx.QueryRow:cccc  advisory lock, PostgreSQL engine only
duckdb        p.Duck:sql.Conn.ExecContext:dddd DuckDB staging
boundary      p.F:pgx.Tx.Commit:eeee       commit
unshipped-main m/cmd/sample sample data loader
`

func rec(site, status string) attr.Outcome {
	return attr.Outcome{SiteID: site, Status: status, SQL: "x", Test: "T"}
}

func run(t *testing.T, manifest string, inv *sites.Result, mains []string, outs ...attr.Outcome) Report {
	t.Helper()
	m, errs := Parse(strings.NewReader(manifest))
	r := Join(inv, m, mains, []string{"m/cmd/server"}, outs)
	for _, e := range errs {
		r.Problems = append(r.Problems, e.Error())
	}
	return r
}

func want(t *testing.T, r Report, substr string) {
	t.Helper()
	for _, p := range r.Problems {
		if strings.Contains(p, substr) {
			return
		}
	}
	t.Errorf("want a problem containing %q, got %q", substr, r.Problems)
}

var bothEvidenced = []attr.Outcome{rec("p.F:pgx.Tx.Exec:aaaa", "attributed"), rec("p.F:pgx.Tx.Exec:bbbb", "attributed")}

func TestCleanJoinPasses(t *testing.T) {
	r := run(t, goodManifest, fixture(), []string{"m/cmd/server", "m/cmd/sample"}, bothEvidenced...)
	if len(r.Problems) != 0 {
		t.Fatalf("clean join reported %q", r.Problems)
	}
	if r.DSQLSites != 2 || r.DSQLWithEvidence != 2 || r.SitesClassified != 5 {
		t.Fatalf("coverage %+v", r)
	}
}

// 1 and 8: a new or unclassified site fails.
func TestUnclassifiedSiteFails(t *testing.T) {
	inv := fixture()
	inv.Sites = append(inv.Sites, sites.Site{ID: "p.New:pgx.Tx.Exec:ffff", File: "p/new.go", Line: 3})
	want(t, run(t, goodManifest, inv, nil, bothEvidenced...), "unclassified site p.New:pgx.Tx.Exec:ffff")
}

// 2: a stale manifest entry fails.
func TestStaleEntryFails(t *testing.T) {
	inv := fixture()
	inv.Sites = inv.Sites[1:]
	want(t, run(t, goodManifest, inv, nil, bothEvidenced[1:]...), "stale manifest entry")
}

// 3: a dsql site without evidence fails, even when another site of the same
// function has evidence.
func TestDSQLSiteWithoutEvidenceFails(t *testing.T) {
	r := run(t, goodManifest, fixture(), nil, rec("p.F:pgx.Tx.Exec:aaaa", "attributed"))
	want(t, r, "dsql site p.F:pgx.Tx.Exec:bbbb has no corpus evidence")
}

// 4: a record with no attributable site fails.
func TestUnattributedRecordFails(t *testing.T) {
	want(t, run(t, goodManifest, fixture(), nil, append(bothEvidenced, rec("", "failed"))...), "attributes to no site")
}

// 5: a PostgreSQL-only (or DuckDB) site in the DSQL corpus fails.
func TestPostgresOnlyInCorpusFails(t *testing.T) {
	r := run(t, goodManifest, fixture(), nil, append(bothEvidenced, rec("p.Lock:pgx.Tx.QueryRow:cccc", "attributed"))...)
	want(t, r, "comes from postgres-only site")
}

// 6: ambiguous attribution fails.
func TestAmbiguousRecordFails(t *testing.T) {
	o := rec("", "ambiguous")
	o.Candidates = []string{"p.F:pgx.Tx.Exec:aaaa", "p.F:pgx.Tx.Exec:bbbb"}
	want(t, run(t, goodManifest, fixture(), nil, append(bothEvidenced, o)...), "attributes to several sites")
}

// 7: uncovered needs a tracking issue.
func TestUncoveredNeedsIssue(t *testing.T) {
	m := strings.Replace(goodManifest, "dsql          p.F:pgx.Tx.Exec:bbbb         write path", "uncovered p.F:pgx.Tx.Exec:bbbb not rendered yet", 1)
	want(t, run(t, m, fixture(), nil, bothEvidenced[:1]...), "needs a tracking issue")
	m = strings.Replace(goodManifest, "dsql          p.F:pgx.Tx.Exec:bbbb         write path", "uncovered p.F:pgx.Tx.Exec:bbbb not rendered yet, #802", 1)
	if r := run(t, m, fixture(), nil, bothEvidenced[:1]...); len(r.Problems) != 0 {
		t.Fatalf("uncovered with an issue still failed: %q", r.Problems)
	}
}

// A new main package must be shipped or listed unshipped.
func TestUnlistedMainFails(t *testing.T) {
	want(t, run(t, goodManifest, fixture(), []string{"m/cmd/new"}, bothEvidenced...), "neither a shipped entry point nor listed")
}

// An unsupported pattern fails closed.
func TestUnsupportedPatternFails(t *testing.T) {
	inv := fixture()
	inv.Findings = []sites.Finding{{Kind: "sink-as-value", Pos: "p/f.go:20:2", Detail: "f := tx.Exec"}}
	want(t, run(t, goodManifest, inv, nil, bothEvidenced...), "unsupported pattern sink-as-value")
}

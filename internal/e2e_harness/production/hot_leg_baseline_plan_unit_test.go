package production

import (
	"slices"
	"strings"
	"testing"
)

func TestParseBaselineSizes(t *testing.T) {
	ok := map[string][]int64{
		"1500":                       {1500},
		"2000, 2500":                 {2000, 2500},
		"10000,100000,1000000":       {10000, 100000, 1000000},
		"1500,2000,2500,3000,999999": {1500, 2000, 2500, 3000, 999999},
	}
	for raw, want := range ok {
		got, err := parseBaselineSizes(raw)
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("parseBaselineSizes(%q) = %v, %v; want %v", raw, got, err, want)
		}
	}
	bad := map[string]string{
		"1499":       "at least 1500",
		"x":          "at least 1500",
		"2000,2000":  "at least 500", // duplicate: the hot set alone overshoots
		"2000,2100":  "at least 500", // under the hot set's creates
		"2000,2499":  "at least 500",
		"4000,2000":  "at least 500",  // descending
		"2000,,3000": "at least 1500", // empty field
	}
	for raw, want := range bad {
		if got, err := parseBaselineSizes(raw); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("parseBaselineSizes(%q) = %v, %v; want an error naming %q", raw, got, err, want)
		}
	}
}

func TestHotLegCutoff(t *testing.T) {
	stmt := "WHERE schema_id = 21\n    AND (flushed_at = 0)\n ... AND (cl.flushed_at = 0 OR cl.flushed_at >= 1794422329844)"
	if got, err := hotLegCutoff(stmt); err != nil || got != 1794422329844 {
		t.Errorf("hotLegCutoff = %d, %v; want 1794422329844", got, err)
	}
	both := "(flushed_at = 0 OR flushed_at >= 7) AND (cl.flushed_at = 0 OR cl.flushed_at >= 7)"
	if got, err := hotLegCutoff(both); err != nil || got != 7 {
		t.Errorf("hotLegCutoff = %d, %v; want 7", got, err)
	}
	for _, bad := range []string{"(flushed_at = 0)", "(flushed_at = 0 OR flushed_at >= 7) AND (cl.flushed_at = 0 OR cl.flushed_at >= 8)"} {
		if _, err := hotLegCutoff(bad); err == nil {
			t.Errorf("hotLegCutoff(%q) accepted", bad)
		}
	}
}

func TestRetargetHotLeg(t *testing.T) {
	got, err := retargetHotLeg(profileStmt, "pg_abc", "hot")
	want := `SELECT * FROM pg_abc."hot"."change_log" JOIN pg_abc."hot"."entity_main" USING (x)`
	if err != nil || got != want {
		t.Errorf("retargetHotLeg = %q, %v; want %q", got, err, want)
	}
	if _, err := retargetHotLeg(profileStmt, "pg_other", "hot"); err == nil {
		t.Error("retargetHotLeg accepted a statement that reads nothing from the catalog")
	}
}

const profileFixture = `{
  "operator": [{"type": "RESULT_COLLECTOR", "extra_info": {}, "children": [
    {"type": "HASH_JOIN", "extra_info": {"Join Type": "INNER", "Estimated Cardinality": "3"}, "children": [
      {"type": "TABLE_SCAN", "intermediate_rows": 1000,
       "extra_info": {"Function": "READ_PARQUET", "Projections": ["id"]}},
      {"type": "TABLE_SCAN", "intermediate_rows": 9000,
       "extra_info": {"Table": "entity_main", "Estimated Cardinality": "0"}},
      {"type": "TABLE_SCAN", "intermediate_rows": 50,
       "extra_info": {"Table": "change_log"}}
    ]}
  ]}],
  "query": {"sql": "SELECT * FROM pg_abc.\"public\".\"change_log\" JOIN pg_abc.\"public\".\"entity_main\" USING (x)"}
}`

// profileStmt is the fixture's statement: it reads two tables of the
// PostgreSQL catalog pg_abc.
const profileStmt = `SELECT * FROM pg_abc."public"."change_log" JOIN pg_abc."public"."entity_main" USING (x)`

func TestSummarizeProfile(t *testing.T) {
	p, err := summarizeProfile([]byte(profileFixture), profileStmt, "pg_abc")
	if err != nil {
		t.Fatalf("summarizeProfile: %v", err)
	}
	if p.postgresRows != 9050 || p.parquetRows != 1000 {
		t.Errorf("rows postgres %d, parquet %d; want 9050, 1000", p.postgresRows, p.parquetRows)
	}
	wantPlan := `RESULT_COLLECTOR
  HASH_JOIN Join Type="INNER"
    TABLE_SCAN Function="READ_PARQUET" Projections=["id"]
    TABLE_SCAN Table="entity_main"
    TABLE_SCAN Table="change_log"
`
	if p.plan != wantPlan {
		t.Errorf("plan:\n%s\nwant:\n%s", p.plan, wantPlan)
	}
	// Row counts and estimates differ between the full and hot-only runs;
	// the plan must not.
	other := strings.NewReplacer(`"intermediate_rows": 9000`, `"intermediate_rows": 7`, `"Estimated Cardinality": "3"`, `"Estimated Cardinality": "1"`).Replace(profileFixture)
	if q, err := summarizeProfile([]byte(other), profileStmt, "pg_abc"); err != nil || q.plan != p.plan {
		t.Errorf("row counts or estimates changed the plan: %v\n%s", err, q.plan)
	}
	if _, err := summarizeProfile([]byte(profileFixture), "SELECT 1", "pg_abc"); err == nil {
		t.Error("a profile of another statement was accepted")
	}
	// A DuckDB table, and a bare name the statement does not read from the
	// PostgreSQL catalog, are neither source.
	for _, table := range []string{"memory.main.change_log", "eav_data"} {
		unknown := strings.Replace(profileFixture, `"Table": "change_log"`, `"Table": "`+table+`"`, 1)
		if _, err := summarizeProfile([]byte(unknown), profileStmt, "pg_abc"); err == nil || !strings.Contains(err.Error(), "unclassified") {
			t.Errorf("a scan of %s was not refused: %v", table, err)
		}
	}
}

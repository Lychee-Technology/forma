//go:build dsqlspike

package spikefix

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestSpikeDuckDBParseOracle checks whether DuckDB's parser (already a
// forma dependency) can serve as a statement-presence oracle for the DSQL
// corpus: it classifies each input as parsed, empty, or a parser error.
func TestSpikeDuckDBParseOracle(t *testing.T) {
	var in struct {
		Corpus []struct{ Origin, SQL string }
		Probes map[string]string
	}
	b, err := os.ReadFile(os.Getenv("SPIKE_PARSE_IN"))
	if err != nil {
		t.Skip("SPIKE_PARSE_IN not set")
	}
	if err := json.Unmarshal(b, &in); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	classify := func(q string) string {
		st, err := db.PrepareContext(context.Background(), q)
		if err == nil {
			_ = st.Close()
			return "parsed"
		}
		msg := err.Error()
		switch {
		case strings.Contains(msg, "Parser Error"):
			return "PARSER-ERROR"
		case strings.Contains(strings.ToLower(msg), "empty"):
			return "empty"
		case strings.Contains(msg, "multiple"):
			return "multiple-statements"
		default:
			return "parsed(bind-error)"
		}
	}
	for name, q := range in.Probes {
		t.Logf("probe %-18s %s", name, classify(q))
	}
	counts := map[string]int{}
	for _, s := range in.Corpus {
		c := classify(s.SQL)
		counts[c]++
		if c == "PARSER-ERROR" {
			t.Logf("corpus parser error: %s: %.90s", s.Origin, strings.Join(strings.Fields(s.SQL), " "))
		}
	}
	t.Logf("corpus statements=%d %v", len(in.Corpus), counts)
}

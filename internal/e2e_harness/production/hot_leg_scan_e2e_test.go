//go:build e2e

package production

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	forma "github.com/lychee-technology/forma"
)

// hotLegCopyPattern matches the statement DuckDB's postgres_scan sends for
// one table: a COPY of the projected columns. Group 1 is the table, group 2
// whatever follows it inside the COPY's SELECT.
var hotLegCopyPattern = regexp.MustCompile(`statement: COPY \(SELECT .+? FROM "public"\."(\w+)"\s*(.*?)\s*\) TO STDOUT`)

// scannerCTIDRange is the only filter the scanner adds on its own: the
// ctid range it splits a table scan into. The DuckDB 1.4.5 probe in #631
// saw it; 1.5.6 sends none on small tables.
var scannerCTIDRange = regexp.MustCompile(`^WHERE ctid BETWEEN '\(\d+,\d+\)'::tid AND '\(\d+,\d+\)'::tid$`)

// TestHotLegCopiesWholeTables pins what design.md §6.1 states (#631): the
// federated hot leg sends PostgreSQL no predicate. A query with a
// column-bound and an EAV filter reaches PostgreSQL as one unfiltered COPY
// per scan, change_log twice, entity_main and eav_data once each, and
// DuckDB applies the schema, flush and query filters to the copies.
//
// The test fails when that stops being true, for example after a DuckDB
// upgrade that starts pushing filters. design.md §6.1, requirements.md §7.1
// and the plan's hot-leg source then need revisiting with it.
//
// It reads PostgreSQL's statement log, so it needs the harness's own
// container; external infrastructure skips it.
func TestHotLegCopiesWholeTables(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	if cluster.Base.PGContainer == nil {
		t.Skip("needs the harness's PostgreSQL container to read its statement log")
	}
	env := NewEnv(t, cluster)
	simple := DefaultSchemaFixtures()[0]
	manager := env.EntityManager()

	// The flushed baseline gives the schema a manifest-listed object (#720).
	createSimpleRow(ctx, t, manager, simple, "baseline")
	mustFlush(ctx, t, env)
	hot := createSimpleRow(ctx, t, manager, simple, "hot")

	// Per database, so only sessions on this test's database log, and new
	// sessions only: DuckDB opens its own for the scans.
	env.ExecSQL(ctx, fmt.Sprintf("ALTER DATABASE %s SET log_statement = 'all'", pgx.Identifier{env.DBName}.Sanitize()))
	logOffset := len(readPostgresLog(ctx, t, cluster))

	result, err := manager.Query(ctx, &forma.QueryRequest{
		SchemaName: simple.Name, Page: 1, ItemsPerPage: 10,
		Condition: &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "name", Value: "equals:hot"}, // column-bound (text_01)
			&forma.KvCondition{Attr: "value", Value: "gte:0"},     // EAV-only
		}},
		Federated: &forma.FederatedQueryRequest{
			Enabled: true, PreferredTiers: []string{"hot", "warm", "cold"}, IncludeExecutionPlan: true,
		},
	})
	if err != nil {
		t.Fatalf("federated query: %v", err)
	}
	if result.ExecutionPlan == nil || !result.ExecutionPlan.Routing.UsedDuckDB {
		t.Fatalf("query did not route through DuckDB: %+v", result.ExecutionPlan)
	}
	if len(result.Data) != 1 || result.Data[0].RowID != hot {
		t.Fatalf("query returned %d rows, want only the hot row %s", len(result.Data), hot)
	}

	copies := awaitHotLegCopies(ctx, t, cluster, logOffset)
	for _, c := range copies {
		if c.rest != "" && !scannerCTIDRange.MatchString(c.rest) {
			t.Errorf("PostgreSQL received a filtered scan of %s (%q): the hot leg now pushes a predicate, "+
				"so design.md §6.1 and requirements.md §7.1 are stale", c.table, c.rest)
		}
	}
}

// hotLegCopy is one COPY statement: the table it scans and whatever follows
// the table name inside its SELECT.
type hotLegCopy struct{ table, rest string }

// awaitHotLegCopies polls the container log, which can lag the statements,
// until it holds the hot leg's four COPY statements after offset.
func awaitHotLegCopies(ctx context.Context, t *testing.T, cluster *Cluster, offset int) []hotLegCopy {
	t.Helper()
	want := map[string]int{"change_log": 2, "entity_main": 1, "eav_data": 1}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var copies []hotLegCopy
		got := map[string]int{}
		for _, m := range hotLegCopyPattern.FindAllStringSubmatch(readPostgresLog(ctx, t, cluster)[offset:], -1) {
			copies = append(copies, hotLegCopy{table: m[1], rest: m[2]})
			got[m[1]]++
		}
		if got["change_log"] >= want["change_log"] && got["entity_main"] >= want["entity_main"] && got["eav_data"] >= want["eav_data"] {
			return copies
		}
		if time.Now().After(deadline) {
			t.Fatalf("PostgreSQL log holds COPY scans %v, want at least %v", got, want)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func readPostgresLog(ctx context.Context, t *testing.T, cluster *Cluster) string {
	t.Helper()
	rc, err := cluster.Base.PGContainer.Logs(ctx)
	if err != nil {
		t.Fatalf("read postgres container log: %v", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read postgres container log: %v", err)
	}
	return strings.ReplaceAll(string(b), "\r", "")
}

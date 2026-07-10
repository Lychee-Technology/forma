//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
)

// TestQueryAcrossTiers verifies the engine assembly end to end: hot-only
// queries serve unflushed rows from Postgres, and after a flush the same
// rows come back through the DuckDB federated path reading real parquet.
func TestQueryAcrossTiers(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	simple := DefaultSchemaFixtures()[0] // e2e_simple

	var events []*Event
	for i := 0; i < 5; i++ {
		events = append(events, CreateEvent(simple, map[string]any{
			"name":  fmt.Sprintf("row-%d", i),
			"value": float64(i) + 0.25,
		}))
	}
	if err := env.ApplyEvents(ctx, events...); err != nil {
		t.Fatalf("apply events: %v", err)
	}
	want := rowIDSet(events)

	hot, err := env.Query(ctx, Query{Schema: simple, PreferHot: true, Limit: 10})
	if err != nil {
		t.Fatalf("hot query: %v", err)
	}
	assertRowIDs(t, "hot", hot, want)

	// Note: the DuckDB federated path requires at least one parquet file to
	// exist for the schema (read_parquet errors on an empty glob), matching
	// production where cdc-init bootstraps base files before federated
	// reads. Hot-only queries work before any flush.
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	postFlush, err := env.Query(ctx, Query{Schema: simple, Limit: 10})
	if err != nil {
		t.Fatalf("federated query after flush: %v", err)
	}
	assertRowIDs(t, "post-flush federated", postFlush, want)
	if !postFlush.Plan.Routing.UseDuckDB {
		t.Errorf("post-flush query did not route to duckdb: %+v", postFlush.Plan.Routing)
	}

	// With every row flushed the dirty set is empty, so records can only
	// come from the parquet glob — a glob mismatch would return zero rows.
	var unflushed int64
	if err := env.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM change_log WHERE flushed_at = 0").Scan(&unflushed); err != nil {
		t.Fatalf("count unflushed: %v", err)
	}
	if unflushed != 0 {
		t.Fatalf("expected empty dirty set after flush, got %d", unflushed)
	}

	filtered, err := env.Query(ctx, Query{
		Schema:  simple,
		Filters: []Filter{{Attr: "name", Value: "row-3"}},
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("filtered query: %v", err)
	}
	if len(filtered.Records) != 1 || filtered.Records[0].RowID != events[3].RowID {
		t.Errorf("filtered query returned %d records, want exactly row-3", len(filtered.Records))
	}
}

func rowIDSet(events []*Event) map[uuid.UUID]bool {
	want := make(map[uuid.UUID]bool, len(events))
	for _, ev := range events {
		want[ev.RowID] = true
	}
	return want
}

func assertRowIDs(t *testing.T, label string, result *QueryResult, want map[uuid.UUID]bool) {
	t.Helper()
	if len(result.Records) != len(want) {
		t.Fatalf("%s: got %d records, want %d", label, len(result.Records), len(want))
	}
	for _, rec := range result.Records {
		if !want[rec.RowID] {
			t.Errorf("%s: unexpected row id %s", label, rec.RowID)
		}
	}
	if result.Total != int64(len(want)) {
		t.Errorf("%s: total = %d, want %d", label, result.Total, len(want))
	}
}

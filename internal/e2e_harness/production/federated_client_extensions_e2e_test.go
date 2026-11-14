//go:build e2e

package production

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// TestFederatedClientNeitherLoadsPostgresScannerNorAttaches pins
// pgx-hot-leg-plan.md step 7: the federated engine reads the hot tier over
// pgx, in its pass's snapshot, so its DuckDB client never loads
// postgres_scanner and attaches no PostgreSQL catalog. It replaces
// TestHotLegCopiesWholeTables, which pinned the attach hot leg's unfiltered
// COPY scans.
//
// The client is the engine's own (Env.Duck), checked after it has served
// federated reads that need both legs: the hot form over flushed, updated
// and created rows, a filtered page the hot leg answers, an offset page by
// an attribute that interleaves the tiers, and the hot-excluded form.
func TestFederatedClientNeitherLoadsPostgresScannerNorAttaches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]

	flushed := buildOpenCreates(wide, 3)
	mustApplyEvents(ctx, t, env, "flushed rows", flushed...)
	mustFlush(ctx, t, env)
	hot := CreateEvent(wide, map[string]any{"title": "hot", "count": float64(100001)})
	mustApplyEvents(ctx, t, env, "hot rows", hot, UpdateEvent(wide, flushed[0].RowID, map[string]any{"title": "updated"}))

	for _, tc := range []struct {
		name       string
		q          Query
		rows, hots int
	}{
		{"hot form", Query{Schema: wide, Limit: 10, PreferredTiers: allTiers}, 4, 2},
		{"filtered by the hot leg", Query{Schema: wide, Filters: []Filter{{Attr: "title", Op: "equals", Value: "hot"}}, Limit: 10, PreferredTiers: allTiers}, 1, 1},
		{"offset page by count", Query{Schema: wide, Sorts: []Sort{{Attr: "count"}}, Limit: 2, Offset: 1, PreferredTiers: allTiers}, 2, 1},
		{"hot excluded", Query{Schema: wide, Limit: 10, PreferredTiers: parquetTiers}, 2, 0},
	} {
		res := mustQuery(ctx, t, env, tc.q)
		if !res.Plan.Routing.UseDuckDB {
			t.Fatalf("%s: did not route to the federated path: %+v", tc.name, res.Plan.Routing)
		}
		if !planHasEngine(res.Plan, "duckdb") {
			t.Fatalf("%s: DuckDB ran no statement: %+v", tc.name, res.Plan.Sources)
		}
		hotLeg, _ := planSource(res.Plan, hotLegPgxReason)
		if len(res.Records) != tc.rows || int(hotLeg.ActualRows) != tc.hots {
			t.Fatalf("%s: %d rows, %d from the hot leg; want %d and %d", tc.name, len(res.Records), hotLeg.ActualRows, tc.rows, tc.hots)
		}
	}

	var loaded sql.NullBool
	err := env.Duck.DB.QueryRowContext(ctx, "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'postgres_scanner'").Scan(&loaded)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("read duckdb_extensions(): %v", err)
	}
	if loaded.Valid && loaded.Bool {
		t.Errorf("the federated DuckDB client has postgres_scanner loaded")
	}

	rows, err := env.Duck.DB.QueryContext(ctx, "SELECT database_name, type FROM duckdb_databases()")
	if err != nil {
		t.Fatalf("read duckdb_databases(): %v", err)
	}
	defer func() { _ = rows.Close() }()
	databases := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatalf("scan duckdb_databases(): %v", err)
		}
		databases[name] = typ
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read duckdb_databases(): %v", err)
	}
	if databases["memory"] != "duckdb" {
		t.Fatalf("duckdb_databases() lists %v, without the client's own in-memory database", databases)
	}
	for name, typ := range databases {
		if typ == "postgres" {
			t.Errorf("the federated DuckDB client has attached the PostgreSQL catalog %s", name)
		}
	}
}

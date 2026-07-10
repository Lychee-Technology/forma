//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestBoundDateParityAcrossTiers proves the #194 contract end to end: a
// main-column-bound date/datetime attribute (unix_ms encoding) must read
// back the identical epoch-ms int64 from the hot tier (Postgres), the cold
// tier (base parquet from RunInit), and the warm tier (delta parquet from a
// flush). Three layers of parity, from physical to logical:
//  1. the parquet columns are physically BIGINT (typeof check);
//  2. the raw parquet values equal the hot-read values row by row;
//  3. the federated query returns the same int64s as the hot query.
//
// The hot baseline itself is anchored to the generated event attributes, so
// a bug that corrupts both sides identically cannot pass.
func TestBoundDateParityAcrossTiers(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 12})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	truth := boundDateTruth(t, creates)

	// Hot baseline: everything is unflushed, PreferHot serves from Postgres.
	hot, err := env.Query(ctx, Query{Schema: wide, PreferHot: true, Limit: 100})
	if err != nil {
		t.Fatalf("hot query: %v", err)
	}
	baseline := collectBoundDates(t, "hot", hot, len(creates))
	for rowID, want := range truth {
		if baseline[rowID] != want {
			t.Fatalf("hot read of %s = %+v, want %+v (event attrs)", rowID, baseline[rowID], want)
		}
	}

	// Cold tier: base parquet becomes the only source for creates[0:6].
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx,
		"DELETE FROM change_log WHERE schema_id = $1 AND row_id = ANY($2)",
		wide.ID, rowIDs(creates[0:6]))

	// Warm tier: flush creates[6:12] into a delta file.
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Hot tier: two more creates that stay unflushed.
	hotCreates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 2})
	if err := env.ApplyEvents(ctx, hotCreates...); err != nil {
		t.Fatalf("apply hot creates: %v", err)
	}
	for rowID, want := range boundDateTruth(t, hotCreates) {
		baseline[rowID] = want
	}

	// Layer 1+2: physical parquet parity for base and delta files.
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	m := manifests[wide.ID]
	if m == nil {
		t.Fatal("no manifest for e2e_wide")
	}
	checked := map[string]bool{}
	for _, f := range m.Files {
		assertParquetBoundDates(ctx, t, env, f.Path, f.Tier, baseline)
		checked[f.Tier] = true
	}
	if !checked["base"] || !checked["delta"] {
		t.Fatalf("expected both base and delta parquet files, got tiers %v", checked)
	}

	// Layer 3: the federated read returns the hot-identical int64s.
	fed, err := env.Query(ctx, Query{Schema: wide, Limit: 100})
	if err != nil {
		t.Fatalf("federated query: %v", err)
	}
	if !fed.Plan.Routing.UseDuckDB {
		t.Errorf("federated query did not route to duckdb: %+v", fed.Plan.Routing)
	}
	fedDates := collectBoundDates(t, "federated", fed, len(creates)+len(hotCreates))
	for rowID, want := range baseline {
		got, ok := fedDates[rowID]
		if !ok {
			t.Errorf("federated result missing row %s", rowID)
			continue
		}
		if got != want {
			t.Errorf("federated read of %s = %+v, want hot-identical %+v", rowID, got, want)
		}
	}
}

type boundDates struct {
	joined  int64 // epoch-ms, bigint_02
	touched int64 // epoch-ms, bigint_03
}

// boundDateTruth derives the expected epoch-ms values straight from the
// generated event attributes, independent of any storage path.
func boundDateTruth(t *testing.T, events []*Event) map[uuid.UUID]boundDates {
	t.Helper()
	truth := make(map[uuid.UUID]boundDates, len(events))
	for _, ev := range events {
		joined, err := time.ParseInLocation("2006-01-02", ev.Attrs["joined"].(string), time.UTC)
		if err != nil {
			t.Fatalf("parse joined attr of %s: %v", ev.RowID, err)
		}
		touched, err := time.Parse(time.RFC3339, ev.Attrs["touched"].(string))
		if err != nil {
			t.Fatalf("parse touched attr of %s: %v", ev.RowID, err)
		}
		truth[ev.RowID] = boundDates{joined: joined.UnixMilli(), touched: touched.UnixMilli()}
	}
	return truth
}

func collectBoundDates(t *testing.T, label string, result *QueryResult, wantRows int) map[uuid.UUID]boundDates {
	t.Helper()
	if len(result.Records) != wantRows {
		t.Fatalf("%s: got %d records, want %d", label, len(result.Records), wantRows)
	}
	got := make(map[uuid.UUID]boundDates, len(result.Records))
	for _, rec := range result.Records {
		joined, ok := rec.Int64Items["bigint_02"]
		if !ok {
			t.Fatalf("%s: row %s has no bigint_02 (joined) value", label, rec.RowID)
		}
		touched, ok := rec.Int64Items["bigint_03"]
		if !ok {
			t.Fatalf("%s: row %s has no bigint_03 (touched) value", label, rec.RowID)
		}
		got[rec.RowID] = boundDates{joined: joined, touched: touched}
	}
	return got
}

// assertParquetBoundDates reads one parquet file directly and checks that
// the bound date columns are physically BIGINT and hold the hot-identical
// epoch-ms values.
func assertParquetBoundDates(ctx context.Context, t *testing.T, env *Env, key, tier string, baseline map[uuid.UUID]boundDates) {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))

	var joinedType, touchedType string
	if err := env.Duck.DB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT typeof(joined), typeof(touched) FROM read_parquet('%s') LIMIT 1", path),
	).Scan(&joinedType, &touchedType); err != nil {
		t.Fatalf("%s parquet typeof: %v", tier, err)
	}
	if joinedType != "BIGINT" || touchedType != "BIGINT" {
		t.Fatalf("%s parquet types joined=%s touched=%s, want BIGINT/BIGINT", tier, joinedType, touchedType)
	}

	rows, err := env.Duck.DB.QueryContext(ctx,
		fmt.Sprintf("SELECT CAST(row_id AS VARCHAR), joined, touched FROM read_parquet('%s')", path))
	if err != nil {
		t.Fatalf("%s parquet scan: %v", tier, err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var rowIDStr string
		var joined, touched int64
		if err := rows.Scan(&rowIDStr, &joined, &touched); err != nil {
			t.Fatalf("%s parquet row scan: %v", tier, err)
		}
		rowID, err := uuid.Parse(rowIDStr)
		if err != nil {
			t.Fatalf("%s parquet row_id %q: %v", tier, rowIDStr, err)
		}
		want, ok := baseline[rowID]
		if !ok {
			t.Fatalf("%s parquet holds unknown row %s", tier, rowID)
		}
		if joined != want.joined || touched != want.touched {
			t.Errorf("%s parquet row %s joined=%d touched=%d, want hot-identical %d/%d",
				tier, rowID, joined, touched, want.joined, want.touched)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s parquet rows: %v", tier, err)
	}
	if n == 0 {
		t.Fatalf("%s parquet file %s is empty", tier, key)
	}
	t.Logf("%s parquet %s: %d rows byte-identical to hot baseline", tier, key, n)
}

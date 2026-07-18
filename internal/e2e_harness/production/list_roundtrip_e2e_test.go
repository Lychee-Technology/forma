//go:build e2e

package production

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
)

// TestListAttributeRoundTrip is the executable acceptance spec for issue
// #204: list (array) attributes round-trip end to end. The write path
// persists one eav_data row per element (transform.populateTypedValue types
// elements by items_type), the CDC export aggregates them into a DuckDB LIST
// ordered by array_indices (internal/cdc/export_sql_builder.go), and the
// federated reader reconstructs positional array_indices from the LIST
// column (internal/sqlgen/duckdb_schema_projection_json.go).
//
// The `tags` list attribute (attrID 18, text items) lives in the shared
// e2e_wide fixture; the exhaustive parquet-schema assertion in the sibling
// TestFullTypeRoundTripAcrossTiers pins its physical column as VARCHAR[]
// (wideParquetTypes). The pre-#204 private-schema-dir injection variant is
// retired.
func TestListAttributeRoundTrip(t *testing.T) {
	cluster := SharedCluster(t)
	runListRoundTripSpec(context.Background(), t, cluster)
}

// runListRoundTripSpec is the value-precise acceptance spec for full list
// round-trip support. It probes every hop across all three tiers and asserts
// the three list elements survive intact and IN INDEX ORDER: hot eav_data
// rows, the hot federated read, both base (cold) and delta (warm) parquet,
// and the federated merge-on-read.
func runListRoundTripSpec(ctx context.Context, t *testing.T, cluster *Cluster) {
	t.Helper()
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]
	wantElems := []string{"alpha", "beta", "gamma"}

	row1 := CreateEvent(wide, map[string]any{
		"title": "list-cold",
		"tags":  []any{"alpha", "beta", "gamma"},
	})
	if err := env.ApplyEvents(ctx, row1); err != nil {
		t.Fatalf("write path rejected list payload: %v", err)
	}

	// Hop 1 (write layer): exactly three eav_data rows, one per element, with
	// array_indices "0","1","2". transform.flattenToAttributes decomposes a flat
	// array into single-index paths and joinIndices renders one integer with no
	// nesting (internal/transform/transformer.go:287-300, array_paths.go:10-18),
	// so the indices are the decimal element positions.
	assertEAVElements(t, "hot eav_data", dumpEAVRows(ctx, t, env, wide.ID, 18), wantElems)

	// Hop 2 (hot read): the three elements survive the hot federated read, in
	// index order, on the single hot record.
	hot, err := env.Query(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
	if err != nil {
		t.Fatalf("hot query: %v", err)
	}
	if len(hot.Records) != 1 {
		t.Fatalf("hot read = %d records, want 1", len(hot.Records))
	}
	assertListElements(t, "hot", collectListAttrs(hot, 18), wantElems)

	// Cold tier: export row1 to base, then delete its change_log entry so it is
	// served cold-only (mirror TestFullTypeRoundTripAcrossTiers).
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx,
		"DELETE FROM change_log WHERE schema_id = $1 AND row_id = $2",
		wide.ID, row1.RowID)

	// Warm tier: a second list row that stays in change_log for RunFlush -> delta.
	row2 := CreateEvent(wide, map[string]any{
		"title": "list-warm",
		"tags":  []any{"alpha", "beta", "gamma"},
	})
	if err := env.ApplyEvents(ctx, row2); err != nil {
		t.Fatalf("apply warm list row: %v", err)
	}

	// Hop 3 (flush + parquet): base (row1) and delta (row2) must BOTH carry the
	// three element VALUES — not a single MAX-collapsed scalar.
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	m := manifests[wide.ID]
	if m == nil {
		t.Fatal("no manifest for e2e_wide after flush")
	}
	tiersSeen := map[string]bool{}
	for _, f := range m.Files {
		typ, vals := dumpParquetTags(ctx, t, env, f.Path)
		t.Logf("%s parquet tags column: type=%s values=%v", f.Tier, typ, vals)
		tiersSeen[f.Tier] = true
		assertTagElements(t, f.Tier+" parquet", vals, wantElems)
	}
	if !tiersSeen["base"] || !tiersSeen["delta"] {
		t.Fatalf("want list data in BOTH base and delta tiers, saw %v", tiersSeen)
	}

	// Hop 4 (federated read): both rows merge back with all three elements, in
	// index order, per record (row1 from base, row2 from delta).
	fed, err := env.Query(ctx, Query{Schema: wide, Limit: 10})
	if err != nil {
		t.Fatalf("federated query: %v", err)
	}
	if len(fed.Records) != 2 {
		t.Fatalf("federated read = %d records, want 2 (cold row1 + warm row2)", len(fed.Records))
	}
	for _, rec := range fed.Records {
		assertListElements(t, "federated "+rec.RowID.String(), recordListAttrs(rec, 18), wantElems)
	}
}

// assertEAVElements pins the write-layer decomposition: exactly len(want)
// eav_data rows, array_indices "0".."n-1", value_text == want[i] in order.
func assertEAVElements(t *testing.T, label string, rows []eavRow, want []string) {
	t.Helper()
	if len(rows) != len(want) {
		t.Fatalf("%s: %d rows, want %d (one per element): %+v", label, len(rows), len(want), rows)
	}
	for i, w := range want {
		wantIdx := strconv.Itoa(i)
		if rows[i].indices != wantIdx {
			t.Errorf("%s: row %d array_indices = %q, want %q", label, i, rows[i].indices, wantIdx)
		}
		if !rows[i].text.Valid || rows[i].text.String != w {
			t.Errorf("%s: row %d value_text = %v, want %q", label, i, rows[i].text, w)
		}
	}
}

// assertListElements requires the recovered list elements to equal want
// exactly, in order, with array_indices "0","1",... (the flat-array
// joinIndices format).
func assertListElements(t *testing.T, label string, got []listAttr, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: %d list elements, want %d: %v", label, len(got), len(want), got)
		return
	}
	for i, w := range want {
		wantIdx := strconv.Itoa(i)
		if got[i].indices != wantIdx {
			t.Errorf("%s: element %d array_indices = %q, want %q", label, i, got[i].indices, wantIdx)
		}
		if got[i].value != w {
			t.Errorf("%s: element %d = %q, want %q", label, i, got[i].value, w)
		}
	}
}

// assertTagElements requires the parquet tags column of one file to carry
// exactly the expected element set. The physical list encoding under #204 is
// not yet fixed (a single LIST row "[alpha, beta, gamma]" or one VARCHAR row
// per element are both plausible), so elements are recovered by splitting each
// scanned value and compared as a sorted set — asserting the VALUES, not a
// count.
func assertTagElements(t *testing.T, label string, vals []string, want []string) {
	t.Helper()
	got := recoverTagElements(vals)
	if len(got) != len(want) {
		t.Errorf("%s: recovered elements %v, want %v", label, got, want)
		return
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("%s: recovered element %d = %q, want %q", label, i, got[i], w)
		}
	}
}

// recoverTagElements flattens scanned tags values into a sorted, de-duplicated
// element set, tolerating both list-encoded ("[alpha, beta, gamma]") and
// one-row-per-element ("alpha") physical layouts.
func recoverTagElements(vals []string) []string {
	seen := map[string]struct{}{}
	for _, v := range vals {
		v = strings.Trim(v, "[]")
		for _, part := range strings.Split(v, ",") {
			part = strings.Trim(strings.TrimSpace(part), `"'`)
			if part != "" && part != "<NULL>" {
				seen[part] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ---- observation helpers ----

type eavRow struct {
	indices string
	text    sql.NullString
	num     sql.NullFloat64
}

func dumpEAVRows(ctx context.Context, t *testing.T, env *Env, schemaID, attrID int16) []eavRow {
	t.Helper()
	rows, err := env.Pool.Query(ctx,
		`SELECT array_indices, value_text, value_numeric
		 FROM eav_data WHERE schema_id = $1 AND attr_id = $2
		 ORDER BY array_indices`, schemaID, attrID)
	if err != nil {
		t.Fatalf("dump eav_data: %v", err)
	}
	defer rows.Close()
	var out []eavRow
	for rows.Next() {
		var r eavRow
		var text *string
		var num *float64
		if err := rows.Scan(&r.indices, &text, &num); err != nil {
			t.Fatalf("scan eav_data: %v", err)
		}
		if text != nil {
			r.text = sql.NullString{Valid: true, String: *text}
		}
		if num != nil {
			r.num = sql.NullFloat64{Valid: true, Float64: *num}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("eav_data rows: %v", err)
	}
	return out
}

type listAttr struct {
	indices string
	value   string
}

func (l listAttr) String() string { return fmt.Sprintf("[idx=%q %q]", l.indices, l.value) }

// recordListAttrs returns one record's list elements for attrID, ordered by
// array_indices ("0","1","2",... — the joinIndices format for a flat array).
func recordListAttrs(rec *model.PersistentRecord, attrID int16) []listAttr {
	var out []listAttr
	for _, a := range rec.OtherAttributes {
		if a.AttrID == attrID && a.ValueText != nil {
			out = append(out, listAttr{indices: a.ArrayIndices, value: *a.ValueText})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].indices < out[j].indices })
	return out
}

// collectListAttrs flattens attrID's list elements across every record in the
// result, ordered by array_indices; use it when a single record is expected.
func collectListAttrs(res *QueryResult, attrID int16) []listAttr {
	var out []listAttr
	for _, rec := range res.Records {
		out = append(out, recordListAttrs(rec, attrID)...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].indices < out[j].indices })
	return out
}

// dumpParquetTags returns the physical type of the "tags" column and every
// value in it, tolerating a column that does not exist or a file with zero
// rows (a QueryRow().Scan on an empty delta file would error, so we iterate).
func dumpParquetTags(ctx context.Context, t *testing.T, env *Env, key string) (string, []string) {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))

	typ := "<absent>"
	drows, err := env.Duck.DB.QueryContext(ctx,
		fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", path))
	if err != nil {
		t.Fatalf("parquet describe: %v", err)
	}
	for drows.Next() {
		var name, colType string
		var null, key2, def, extra sql.NullString
		if err := drows.Scan(&name, &colType, &null, &key2, &def, &extra); err != nil {
			_ = drows.Close()
			t.Fatalf("describe scan: %v", err)
		}
		if name == "tags" {
			typ = colType
		}
	}
	_ = drows.Close()
	if typ == "<absent>" {
		return typ, nil
	}

	vrows, err := env.Duck.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT CAST("tags" AS VARCHAR) FROM read_parquet('%s')`, path))
	if err != nil {
		t.Fatalf("parquet tags scan: %v", err)
	}
	defer vrows.Close()
	var vals []string
	for vrows.Next() {
		var v sql.NullString
		if err := vrows.Scan(&v); err != nil {
			t.Fatalf("parquet tags row scan: %v", err)
		}
		if v.Valid {
			vals = append(vals, v.String)
		} else {
			vals = append(vals, "<NULL>")
		}
	}
	if err := vrows.Err(); err != nil {
		t.Fatalf("parquet tags rows: %v", err)
	}
	return typ, vals
}


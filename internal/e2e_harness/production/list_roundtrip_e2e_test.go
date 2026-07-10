//go:build e2e

package production

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	forma "github.com/lychee-technology/forma"
)

// TestListAttributeRoundTrip probes issue #174 hypothesis 3: do list (array)
// attributes survive the write -> CDC export -> parquet -> federated path?
//
// Empirically established behavior (2026-07): they do not survive because they
// never enter the pipeline. The WRITE PATH rejects them at the transform
// boundary: transform.flattenToAttributes decomposes an array into indexed
// elements (the array machinery works), but each element resolves to the
// attribute's declared value type `list`, and populateTypedValue's type switch
// has no `list` case, so it hits default and returns
// "unsupported value type 'list'" (internal/transform/transformer.go:337-372).
// No eav_data row is persisted, so the downstream CDC collapse the static
// analysis predicted (export drops array_indices and MAX-collapses multi-row
// attributes — internal/cdc/export_sql_builder.go:79-88,135; reader hardcodes
// array_indices='' — internal/sqlgen/duckdb_schema_projection.go:328) is a
// latent SECOND blocker that is never reached today.
//
// This is variant B2 of the task plan: the shared e2e_wide fixture keeps `tags`
// OUT (an EAV attribute becomes an unconditional parquet column in the
// schema-driven CDC export, which would break the exhaustive column assertion
// in the sibling TestFullTypeRoundTripAcrossTiers). The `tags` list attribute
// is injected only into a PRIVATE temp copy of the fixture (writeListSchemaDir
// + WithSchemaDir), so no other test sees it. See task-6-report.md.
//
// This top-level test pins the CURRENT contract: list writes are rejected,
// cleanly, with a specific error, and nothing partially lands. The skipped
// "round-trip across tiers" subtest is the executable acceptance spec for the
// follow-up issue that lifts this limitation.
func TestListAttributeRoundTrip(t *testing.T) {
	cluster := SharedCluster(t)
	schemaDir := writeListSchemaDir(t)
	env := NewEnv(t, cluster, WithSchemaDir(schemaDir))
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide; here it carries attr 18 "tags" (list)

	ev := CreateEvent(wide, map[string]any{
		"title": "list-probe",
		"tags":  []any{"alpha", "beta", "gamma"},
	})
	err := env.ApplyEvents(ctx, ev)
	if err == nil {
		t.Fatalf("write path ACCEPTED a list payload — list support may have landed; " +
			"convert this probe to a real round-trip test (the skipped subtest below is the spec)")
	}
	t.Logf("OBSERVED write-path rejection: %v", err)

	// The rejection must be the specific unsupported-list-type error from
	// transform.populateTypedValue's default arm — not a generic failure — so a
	// future change that mishandles lists (silently drops, panics, or half
	// applies) is caught rather than passing as "still rejected".
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Errorf("rejection is not wrapped forma.ErrInvalidInput: %v", err)
	}
	if !strings.Contains(err.Error(), "unsupported value type 'list'") {
		t.Errorf("rejection = %q, want it to mention \"unsupported value type 'list'\"", err.Error())
	}
	if !strings.Contains(err.Error(), "attrID=18") {
		t.Errorf("rejection = %q, want it to name the offending attribute (attrID=18)", err.Error())
	}

	// The rejected create must not have partially landed: no eav_data row for
	// attr 18, and the hot read sees zero records for the schema.
	if rows := dumpEAVRows(ctx, t, env, wide.ID, 18); len(rows) != 0 {
		t.Errorf("eav_data has %d row(s) for rejected list attr 18, want 0: %+v", len(rows), rows)
	}
	hot, qerr := env.Query(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
	if qerr != nil {
		t.Fatalf("hot query after rejected create: %v", qerr)
	}
	if len(hot.Records) != 0 {
		t.Errorf("hot read returned %d record(s) after a rejected create, want 0", len(hot.Records))
	}

	t.Run("round-trip across tiers", func(t *testing.T) {
		t.Skipf("list round-trip blocked by #TODO-174-LIST: transform.populateTypedValue " +
			"rejects list attributes at write (internal/transform/transformer.go:372); even if " +
			"accepted, the CDC EAV export drops array_indices and MAX-collapses multi-element rows " +
			"(internal/cdc/export_sql_builder.go:79-88,135) and the federated reader hardcodes " +
			"array_indices='' (internal/sqlgen/duckdb_schema_projection.go:328). This subtest is the " +
			"executable acceptance spec for that issue; un-skip and it must pass end to end.")
		listRoundTripSpec(ctx, t, cluster)
	})
}

// listRoundTripSpec is the acceptance spec for full list round-trip support.
// It is only reached once #TODO-174-LIST is fixed and the skip above is
// removed. It probes every hop and asserts the three list elements survive
// intact: hot EAV rows, warm/cold parquet, and federated merge-on-read.
func listRoundTripSpec(ctx context.Context, t *testing.T, cluster *Cluster) {
	t.Helper()
	env := NewEnv(t, cluster, WithSchemaDir(writeListSchemaDir(t)))
	wide := DefaultSchemaFixtures()[1]

	ev := CreateEvent(wide, map[string]any{
		"title": "list-probe",
		"tags":  []any{"alpha", "beta", "gamma"},
	})
	if err := env.ApplyEvents(ctx, ev); err != nil {
		t.Fatalf("write path rejected list payload: %v", err)
	}

	// Hop 1 (write layer): three eav_data rows, one per element, with distinct
	// array_indices.
	rawRows := dumpEAVRows(ctx, t, env, wide.ID, 18)
	if len(rawRows) != 3 {
		t.Fatalf("eav_data rows for attr 18 = %d, want 3 (one per element): %+v", len(rawRows), rawRows)
	}

	// Hop 2 (hot read): three list attributes survive federated hot read.
	hot, err := env.Query(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
	if err != nil {
		t.Fatalf("hot query: %v", err)
	}
	if got := collectListAttrs(hot, 18); len(got) != 3 {
		t.Fatalf("hot list attrs = %v, want 3 elements", got)
	}

	// Hop 3 (flush + parquet): each tier's parquet preserves all three elements
	// (not a single MAX-collapsed scalar).
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
	for _, f := range m.Files {
		typ, vals := dumpParquetTags(ctx, t, env, f.Path)
		t.Logf("%s parquet tags column: type=%s values=%v", f.Tier, typ, vals)
		if len(vals) < 3 {
			t.Errorf("%s parquet collapsed list to %d value(s): %v", f.Tier, len(vals), vals)
		}
	}

	// Hop 4 (federated read): three elements survive the federated merge.
	fed, err := env.Query(ctx, Query{Schema: wide, Limit: 10})
	if err != nil {
		t.Fatalf("federated query: %v", err)
	}
	if got := collectListAttrs(fed, 18); len(got) != 3 {
		t.Fatalf("federated list attrs = %v, want the 3 hot-identical elements", got)
	}
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

func collectListAttrs(res *QueryResult, attrID int16) []listAttr {
	var out []listAttr
	for _, rec := range res.Records {
		for _, a := range rec.OtherAttributes {
			if a.AttrID == attrID && a.ValueText != nil {
				out = append(out, listAttr{indices: a.ArrayIndices, value: *a.ValueText})
			}
		}
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

// writeListSchemaDir materializes a private, throwaway schema directory: a
// verbatim copy of the bundled fixtures with the `tags` list attribute added
// to e2e_wide only. WithSchemaDir(this) keeps the list attribute out of the
// shared bundled fixture, so no sibling test sees an extra parquet column.
func writeListSchemaDir(t *testing.T) string {
	t.Helper()
	src := FixtureSchemasDir()
	dst := t.TempDir()

	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("read bundled schemas dir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o600); err != nil {
			t.Fatalf("write %s: %v", e.Name(), err)
		}
	}

	injectJSON(t, filepath.Join(dst, "e2e_wide.json"),
		`"token": { "type": "string", "format": "uuid" }`,
		`"token": { "type": "string", "format": "uuid" },
    "tags": { "type": "array", "items": { "type": "string" } }`)
	injectJSON(t, filepath.Join(dst, "e2e_wide_attributes.json"),
		`"token": { "attributeID": 17, "valueType": "uuid" }`,
		`"token": { "attributeID": 17, "valueType": "uuid" },
  "tags": { "attributeID": 18, "valueType": "list" }`)

	return dst
}

func injectJSON(t *testing.T, path, anchor, replacement string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	s := string(data)
	if !strings.Contains(s, anchor) {
		t.Fatalf("anchor %q not found in %s (fixture drift?)", anchor, path)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(s, anchor, replacement, 1)), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

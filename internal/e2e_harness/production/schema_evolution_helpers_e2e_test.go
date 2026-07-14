//go:build e2e

package production

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Schema-evolution fixtures (#189): every scenario evolves the e2e_simple
// fixture from a v1 generation to a v2 generation via writeSimpleSchemaDir +
// Env.EvolveSchema. The bundled e2e_simple carries name (text, bound to
// text_01) and value (numeric, EAV); scenario dirs replace it wholesale so
// the two generations can differ structurally (add/remove/retype/promote).

// writeSimpleSchemaDir materializes a throwaway schema directory: the bundled
// fixtures copied verbatim, except e2e_simple.json and
// e2e_simple_attributes.json are replaced with the given properties object
// and attributes document. Only e2e_simple mutates so sibling fixtures stay
// byte-identical (pattern from writeListSchemaDir).
func writeSimpleSchemaDir(t *testing.T, propsJSON, attrsJSON string) string {
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

	schemaDoc := fmt.Sprintf(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "E2E Simple",
  "description": "Schema-evolution generation of the e2e_simple fixture (#189)",
  "type": "object",
  "properties": %s
}
`, propsJSON)
	if err := os.WriteFile(filepath.Join(dst, "e2e_simple.json"), []byte(schemaDoc), 0o600); err != nil {
		t.Fatalf("write evolved e2e_simple.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dst, "e2e_simple_attributes.json"), []byte(attrsJSON), 0o600); err != nil {
		t.Fatalf("write evolved e2e_simple_attributes.json: %v", err)
	}
	return dst
}

// evolutionProfile seeds the evolving e2e_simple generations: name/value
// always, plus any extra per-ordinal attributes. Extra values encode the
// ordinal so filters select exact rows and sorts are deterministic.
func evolutionProfile(extra func(ordinal int) map[string]any) AttrProfile {
	return func(r *rand.Rand, ordinal int, partial bool) map[string]any {
		attrs := map[string]any{
			"name":  fmt.Sprintf("row-%05d-%04d", ordinal, r.Intn(10000)),
			"value": float64(ordinal) + float64(r.Intn(2))/2,
		}
		if extra == nil {
			return attrs
		}
		for k, v := range extra(ordinal) {
			attrs[k] = v
		}
		return attrs
	}
}

// seedGeneration applies `creates` create events under the Env's CURRENT
// schema generation and returns them.
func seedGeneration(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, creates int, profile AttrProfile) []*Event {
	t.Helper()
	events := env.GenerateScript(ScriptSpec{Schema: schema, Creates: creates, Profile: profile})
	if err := env.ApplyEvents(ctx, events...); err != nil {
		t.Fatalf("apply %d create events: %v", creates, err)
	}
	return events
}

// runInitBase exports the current hot rows as the base parquet generation and
// clears change_log so the based rows stop counting as dirty/hot (the
// seedAllTiers recipe). Returns the single base parquet key.
func runInitBase(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) string {
	t.Helper()
	init, err := env.RunInit(ctx, schema)
	if err != nil {
		t.Fatalf("run init (base export): %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", schema.ID)
	return soleParquetOf(t, "init", init.NewObjects)
}

// soleParquetOf returns the single .parquet key in keys, failing otherwise.
func soleParquetOf(t *testing.T, step string, keys []string) string {
	t.Helper()
	var parquet []string
	for _, k := range keys {
		if strings.HasSuffix(k, ".parquet") {
			parquet = append(parquet, k)
		}
	}
	if len(parquet) != 1 {
		t.Fatalf("%s created %d parquet objects %v, want exactly 1", step, len(parquet), parquet)
	}
	return parquet[0]
}

// describeParquetCols reads one parquet object's physical schema via DuckDB
// DESCRIBE and returns column name → DuckDB type (pattern from
// assertWideParquetSchema).
func describeParquetCols(ctx context.Context, t *testing.T, env *Env, key string) map[string]string {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
	rows, err := env.Duck.DB.QueryContext(ctx,
		fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", path))
	if err != nil {
		t.Fatalf("describe parquet %s: %v", key, err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var name, typ string
		var null, colKey, def, extra sql.NullString
		if err := rows.Scan(&name, &typ, &null, &colKey, &def, &extra); err != nil {
			t.Fatalf("describe scan for %s: %v", key, err)
		}
		got[name] = typ
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("describe rows for %s: %v", key, err)
	}
	return got
}

// requireParquetCols asserts every wanted column is present with the wanted
// DuckDB type. Generation-shape preconditions fail fast: without them a
// passing query proves nothing about cross-generation resolution.
func requireParquetCols(t *testing.T, tier string, cols, want map[string]string) {
	t.Helper()
	for col, typ := range want {
		if cols[col] != typ {
			t.Fatalf("%s parquet column %q = %q, want %q (generation shape precondition)", tier, col, cols[col], typ)
		}
	}
}

// forbidParquetCols asserts the named columns are physically absent.
func forbidParquetCols(t *testing.T, tier string, cols map[string]string, names ...string) {
	t.Helper()
	for _, name := range names {
		if typ, ok := cols[name]; ok {
			t.Fatalf("%s parquet unexpectedly carries column %q (type %s) — generation shape precondition", tier, name, typ)
		}
	}
}

// assertUsesDuckDB asserts the query plan routed through DuckDB, proving the
// parquet generations were actually read (not served hot-only).
func assertUsesDuckDB(t *testing.T, result *QueryResult) {
	t.Helper()
	if result == nil {
		return
	}
	if result.Plan == nil || !result.Plan.Routing.UseDuckDB {
		t.Fatalf("query did not route through DuckDB; parquet generations were never read: %+v", result.Plan)
	}
}

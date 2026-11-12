//go:build e2e

package production

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/model"
)

// collationRows is the e2e_wide rows the #816 index tests seed. Row i holds
// title and note t%05d, token the uuid 00000000-0000-4000-8000-%012x of i, and
// rank i % 1000, so a rank and a half-table title range select ten rows.
const collationRows = 20_000

// eavTextIndexDDL is init-db's eav_data text index (eavIndexDDL), which the
// harness does not build.
const eavTextIndexDDL = `CREATE INDEX eav_data_text_idx ON eav_data (schema_id, attr_id, value_text, row_id) WHERE value_text IS NOT NULL`

// collationProbe is one query, the rows it selects, and the index and key
// columns its plan must seek.
type collationProbe struct {
	name      string
	condition forma.Condition
	want      int
	index     string
	columns   []string
	eav       string // the eav_data attribute the probe reads, if any
}

// TestOneIndexOnCTextSeeksRangesAndEquality is #816's index criterion. Under a
// database whose default collation is not C, Forma's text columns are C, so one
// ordinary btree index on each, built without any COLLATE clause, serves
// equals and in and also the four ranges the Postgres route renders
// `<col> COLLATE "C" <op> $n` (#728). It covers a one-attribute declared index,
// a composite declared index with an equality prefix and a text range, and
// init-db's eav_data text index for text and for uuid attributes. On the
// composite index only the ranges are probed: an equality on title is as
// selective alone, so the planner rightly prefers the one-attribute index. Each plan
// runs under EXPLAIN (ANALYZE, BUFFERS) and must seek the index with the
// column in its Index Cond; the rows are checked too. No index on these
// columns names a collation of its own.
func TestOneIndexOnCTextSeeksRangesAndEquality(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	requireNonCDatabaseDefault(ctx, t, env)
	for _, col := range [][2]string{{"entity_main", "text_01"}, {"eav_data", "value_text"}} {
		if got := columnCollation(ctx, t, env, col[0], col[1]); got != `pg_catalog."C"` {
			t.Fatalf("%s.%s has collation %s, want the harness DDL's pg_catalog.\"C\"", col[0], col[1], got)
		}
	}
	seedCollationIndexes(ctx, t, env)

	recorder := &recordingPool{Pool: env.Pool}
	repo := internal.NewDBPersistentRecordRepository(recorder, env.Metadata)
	loaded := ""
	for _, p := range collationProbes(t, env) {
		loaded = loadEAVAttribute(ctx, t, env, loaded, p.eav)
		t.Run(p.name, func(t *testing.T) {
			plan, seeks := runCollationProbe(ctx, t, env, repo, recorder, p, true)
			t.Logf("%s: shared buffers hit %d, read %d; plan:\n%s", p.name, plan.SharedHit, plan.SharedRead, describePlan(plan, 0))
			if !seeks {
				t.Fatalf("the plan does not seek %s on %v; plan:\n%s", p.index, p.columns, describePlan(plan, 0))
			}
		})
	}
	if extra := keysUnderOtherCollation(ctx, t, env); len(extra) != 0 {
		t.Fatalf("indexes key a column under a collation other than the column's: %v", extra)
	}
}

// seedCollationIndexes declares by_title (title) and by_rank_title (rank,
// title) on e2e_wide, builds init-db's eav_data text index, seeds the rows and
// analyzes the tables.
func seedCollationIndexes(ctx context.Context, t *testing.T, env *Env) {
	t.Helper()
	dir := copyFixtureSchemaDir(t)
	editFixtureJSON(t, filepath.Join(dir, "e2e_wide.json"), func(doc map[string]any) {
		doc["x-forma-indexes"] = []any{
			map[string]any{"name": "by_title", "attributes": []any{"title"}},
			map[string]any{"name": "by_rank_title", "attributes": []any{"rank", "title"}},
		}
	})
	if err := env.EvolveSchema(ctx, dir); err != nil {
		t.Fatalf("evolve e2e_wide to declare by_title and by_rank_title: %v", err)
	}
	env.ExecSQL(ctx, eavTextIndexDDL)
	insertMainRows(ctx, t, env, SchemaIDWide, 1, collationRows)
	analyzeByteOrderTables(ctx, env)
}

// insertMainRows writes entity_main rows first..last of schemaID as
// collationRows describes, and returns the WAL the insert wrote.
func insertMainRows(ctx context.Context, t *testing.T, env *Env, schemaID int16, first, last int) int64 {
	t.Helper()
	return explainAnalyze(ctx, t, env.Pool, recordedQuery{sql: `
		INSERT INTO entity_main (ltbase_schema_id, ltbase_row_id, text_01, smallint_01, ltbase_created_at, ltbase_updated_at)
		SELECT $1::smallint, gen_random_uuid(), 't' || lpad(i::text, 5, '0'), (i % 1000)::smallint, 1, 1
		FROM generate_series($2::int, $3::int) AS i`, args: []any{schemaID, first, last}}).WALBytes
}

// insertEAVRows writes the eav_data value of attr (note or token) for the
// entity_main rows first..last of schemaID, and returns the WAL it wrote.
func insertEAVRows(ctx context.Context, t *testing.T, env *Env, schemaID int16, first, last int, attr string) int64 {
	t.Helper()
	_, cache, err := env.Registry.GetSchemaAttributeCacheByID(SchemaIDWide)
	if err != nil {
		t.Fatalf("read e2e_wide attributes: %v", err)
	}
	return explainAnalyze(ctx, t, env.Pool, recordedQuery{sql: `
		INSERT INTO eav_data (schema_id, row_id, attr_id, array_indices, value_text)
		SELECT ltbase_schema_id, ltbase_row_id, $4::smallint, '',
		       CASE WHEN $5 THEN '00000000-0000-4000-8000-' || lpad(to_hex(substr(text_01, 2)::int), 12, '0') ELSE text_01 END
		FROM entity_main WHERE ltbase_schema_id = $1 AND substr(text_01, 2)::int BETWEEN $2 AND $3`,
		args: []any{schemaID, first, last, cache[attr].AttributeID, attr == "token"}}).WALBytes
}

// loadEAVAttribute makes attr the only e2e_wide attribute in eav_data, unless
// it already is, and answers it. One attribute at a time keeps the column's
// statistics about that attribute: every uuid sorts below every t-prefixed
// note, so with both present the planner judges a token gt or a note lt to
// select half of value_text and scans, whatever the collation (#815).
func loadEAVAttribute(ctx context.Context, t *testing.T, env *Env, loaded, attr string) string {
	t.Helper()
	if attr == "" || attr == loaded {
		return loaded
	}
	env.ExecSQL(ctx, `DELETE FROM eav_data WHERE schema_id = $1`, SchemaIDWide)
	insertEAVRows(ctx, t, env, SchemaIDWide, 1, collationRows, attr)
	env.ExecSQL(ctx, `ANALYZE eav_data`)
	return attr
}

// collationProbes lists equals, in and the four ranges on each index.
func collationProbes(t *testing.T, env *Env) []collationProbe {
	t.Helper()
	title := declaredIndexFor(t, env, "e2e_wide.by_title").Name
	rankTitle := declaredIndexFor(t, env, "e2e_wide.by_rank_title").Name
	text := func(i int) string { return fmt.Sprintf("t%05d", i) }
	uuid := func(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012x", i) }
	kv := func(attr string, op forma.FilterType, v string) *forma.KvCondition {
		return &forma.KvCondition{Attr: attr, Value: string(op) + ":" + v}
	}
	var probes []collationProbe
	for _, s := range []struct {
		attr, index, column, eav string
		literal                  func(int) string
	}{
		{"title", title, "text_01", "", text},
		{"note", "eav_data_text_idx", "value_text", "note", text},
		{"token", "eav_data_text_idx", "value_text", "token", uuid},
	} {
		probe := func(op string, cond forma.Condition, want int) collationProbe {
			return collationProbe{s.attr + "/" + op, cond, want, s.index, []string{s.column}, s.eav}
		}
		lit := s.literal
		probes = append(probes,
			probe("equals", kv(s.attr, forma.FilterEquals, lit(42)), 1),
			probe("in", forma.In(s.attr, lit(42), lit(43)), 2),
			probe("gt", kv(s.attr, forma.FilterGreaterThan, lit(collationRows-5)), 5),
			probe("gte", kv(s.attr, forma.FilterGreaterEq, lit(collationRows-4)), 5),
			probe("lt", kv(s.attr, forma.FilterLessThan, lit(6)), 5),
			probe("lte", kv(s.attr, forma.FilterLessEq, lit(5)), 5))
	}
	// rank 7 holds rows 7, 1007, ..., 19007: ten of them on each side of t10000.
	composite := func(name string, title forma.Condition, want int) collationProbe {
		return collationProbe{"rank+title/" + name, and(eq("rank", "7"), title), want, rankTitle, []string{"smallint_01", "text_01"}, ""}
	}
	return append(probes,
		composite("gt", kv("title", forma.FilterGreaterThan, text(10000)), 10),
		composite("gte", kv("title", forma.FilterGreaterEq, text(10007)), 10),
		composite("lt", kv("title", forma.FilterLessThan, text(10000)), 10),
		composite("lte", kv("title", forma.FilterLessEq, text(9007)), 10))
}

// runCollationProbe runs p's query through the repository, checks its rows,
// and explains the SQL that answered: under ANALYZE when analyze is set. It
// reports whether a scan of p.index constrains every one of p.columns.
func runCollationProbe(ctx context.Context, t *testing.T, env *Env, repo *internal.DBPersistentRecordRepository,
	recorder *recordingPool, p collationProbe, analyze bool) (planNode, bool) {
	t.Helper()
	page, err := repo.QueryPersistentRecords(ctx, &model.PersistentRecordQuery{
		Tables: env.Tables, SchemaID: SchemaIDWide, Condition: p.condition, Limit: 1000,
	})
	if err != nil {
		t.Fatalf("query %s: %v", p.name, err)
	}
	assertShapeRows(t, page, SchemaIDWide, p.want, false)
	q := recorder.takeAnchorQuery(t)
	plan := explain(ctx, t, env.Pool, q)
	if analyze {
		plan = explainAnalyze(ctx, t, env.Pool, q)
	}
	scans := indexScansOf(plan, p.index)
	for _, col := range p.columns {
		if !anyIndexCondMentions(scans, col) {
			return plan, false
		}
	}
	return plan, len(scans) > 0
}

// requireNonCDatabaseDefault fails unless the database's default collation
// orders text otherwise than C, so a seek here cannot come from the default.
func requireNonCDatabaseDefault(ctx context.Context, t *testing.T, env *Env) {
	t.Helper()
	var collate string
	var agrees bool
	if err := env.Pool.QueryRow(ctx, `SELECT datcollate::text, ('a'::text < 'B'::text) = ('a'::text COLLATE "C" < 'B'::text)
		FROM pg_database WHERE datname = current_database()`).Scan(&collate, &agrees); err != nil {
		t.Fatalf("read the database collation: %v", err)
	}
	if agrees {
		t.Fatalf("the database default collation %s orders a and B as C does; this test needs one that does not", collate)
	}
}

// columnCollation answers the catalog's collation of table.column.
func columnCollation(ctx context.Context, t *testing.T, env *Env, table, column string) string {
	t.Helper()
	var got string
	if err := env.Pool.QueryRow(ctx, `
		SELECT quote_ident(n.nspname) || '.' || quote_ident(c.collname)
		FROM pg_attribute a JOIN pg_collation c ON c.oid = a.attcollation JOIN pg_namespace n ON n.oid = c.collnamespace
		WHERE a.attrelid = to_regclass($1) AND a.attname = $2`, table, column).Scan(&got); err != nil {
		t.Fatalf("read the collation of %s.%s: %v", table, column, err)
	}
	return got
}

// keysUnderOtherCollation lists the indexes of entity_main and eav_data that
// key a column under a collation other than the column's: the duplicate
// COLLATE "C" index a column outside the contract needs.
func keysUnderOtherCollation(ctx context.Context, t *testing.T, env *Env) []string {
	t.Helper()
	rows, err := env.Pool.Query(ctx, `
		SELECT DISTINCT i.indexrelid::regclass::text
		FROM pg_index i
		CROSS JOIN LATERAL unnest(i.indkey::int2[], i.indcollation::oid[]) AS k(attnum, coll)
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
		WHERE i.indrelid IN ('entity_main'::regclass, 'eav_data'::regclass) AND k.coll <> a.attcollation`)
	if err != nil {
		t.Fatalf("read index key collations: %v", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan index key collations: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read index key collations: %v", err)
	}
	return names
}

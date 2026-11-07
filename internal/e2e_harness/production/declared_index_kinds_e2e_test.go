//go:build e2e

package production

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/model"
)

const (
	kindRows     = 4_000 // e2e_wide rows holding a value in every indexed column
	kindNullRows = 3     // e2e_wide rows holding none
	kindRareBool = 5     // rows holding the rarer truth value of a bool kind
	hourMillis   = int64(time.Hour / time.Millisecond)
)

// indexableKind is one column-bound attribute kind a declaration admits, with
// the literal of the value seeded row i holds. Rows 1..kindRows hold strictly
// increasing values, so every operator below selects a known handful of rows.
type indexableKind struct {
	attr, column string
	literal      func(i int) string
	numeric      bool // the in operand lists JSON numbers, not strings
	time         bool // the attribute takes the $now operand
	// byteOrderRange marks a text kind: its ranges compare under the C
	// collation, which an index built under the column's collation cannot seek.
	byteOrderRange bool
	// operators replaces kindOperators for a kind with no ordered values.
	operators []kindOperator
}

// TestDeclaredIndexServesEveryIndexableKind is #647's sargability invariant
// over every binding schema load admits into a declaration, not only the
// outbox's text and unix_ms columns: for each column-bound value type and
// encoding, the hot-tier SQL Forma generates for equals, in, the four ranges,
// is_null and (for time kinds) lte:$now reads through a declared index led by
// the attribute's column, with the column in the Index Cond. A bool_smallint
// kind has two truth values and no order, so it takes equals, not_equals, in,
// not_in and is_null instead; two attributes, each rare on the opposite side,
// let both sides of the contract be selective. bool_text attributes are refused
// at load (TestIndexDeclarations_RefusedAtSchemaLoad), so they have no row here.
//
// The four ranges on a text kind are the exception since #728: they answer the
// same rows, and the test pins that they no longer seek the declared index.
// TestTextRangeSeeksOnlyAByteOrderIndex shows which key does seek them.
func TestDeclaredIndexServesEveryIndexableKind(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))

	sec := time.Now().Truncate(time.Second).UnixMilli()
	day := time.Now().UTC().Truncate(24 * time.Hour).UnixMilli()
	// Time values sit two steps apart around the seed instant: row 6 holds it,
	// so lte:$now selects rows 1-6 even if the test crosses a day boundary.
	at := func(base, step int64) func(int) string {
		return func(i int) string {
			return time.UnixMilli(base + int64(i-6)*2*step).UTC().Format(time.RFC3339)
		}
	}
	kinds := []indexableKind{
		{attr: "title", column: "text_01", literal: func(i int) string { return fmt.Sprintf("t%05d", i) }, byteOrderRange: true},
		{attr: "rank", column: "smallint_01", literal: strconv.Itoa, numeric: true},
		{attr: "count", column: "integer_01", literal: func(i int) string { return strconv.Itoa(i * 10) }, numeric: true},
		{attr: "amount", column: "bigint_01", literal: func(i int) string { return strconv.FormatInt(int64(i)*1_000_000_000, 10) }, numeric: true},
		{attr: "score", column: "double_01", literal: func(i int) string { return strconv.FormatFloat(float64(i)+0.25, 'f', -1, 64) }, numeric: true},
		{attr: "ref", column: "uuid_01", literal: func(i int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012x", i) }},
		{attr: "joined", column: "bigint_02", literal: at(day, dayMillis), time: true},   // date, unix_ms
		{attr: "touched", column: "bigint_03", literal: at(sec, hourMillis), time: true}, // datetime, unix_ms
		{attr: "stamp", column: "text_02", literal: at(sec, hourMillis), time: true},     // datetime, iso8601
		{attr: "flagged", column: "smallint_02", operators: boolKindOperators("flagged", true)},
		{attr: "cleared", column: "smallint_03", operators: boolKindOperators("cleared", false)},
	}
	if err := env.EvolveSchema(ctx, declareEveryKindDir(t, kinds)); err != nil {
		t.Fatalf("evolve e2e_wide to declare an index per kind: %v", err)
	}
	seedEveryKind(ctx, env, sec, day)

	wide := SchemaRef{ID: SchemaIDWide, Name: "e2e_wide"}
	recorder := &recordingPool{Pool: env.Pool}
	repo := internal.NewDBPersistentRecordRepository(recorder, env.Metadata)
	for _, k := range kinds {
		for _, op := range kindOperators(k) {
			t.Run(k.attr+"/"+op.name, func(t *testing.T) {
				page, err := repo.QueryPersistentRecords(ctx, &model.PersistentRecordQuery{
					Tables: env.Tables, SchemaID: wide.ID, Condition: op.condition, Limit: 1000,
				})
				if err != nil {
					t.Fatalf("query: %v", err)
				}
				assertShapeRows(t, page, wide.ID, op.want, false)

				plan := explain(ctx, t, env.Pool, recorder.takeAnchorQuery(t))
				seeks := false
				for _, ix := range plannedDeclaredIndexes(t, env) {
					seeks = seeks || (ix.Columns[1] == k.column && anyIndexCondMentions(indexScansOf(plan, ix.Name), k.column))
				}
				_, isRange := rangeSQLOps[op.name]
				if wantSeek := !(k.byteOrderRange && isRange); seeks != wantSeek {
					t.Fatalf("a declared index led by %s constrains it: %v, want %v; plan:\n%s", k.column, seeks, wantSeek, describePlan(plan, 0))
				}
			})
		}
	}
}

type kindOperator struct {
	name      string
	condition forma.Condition
	want      int
}

// kindOperators lists the operators #647 requires an index condition for,
// each with the number of seeded rows it selects.
func kindOperators(k indexableKind) []kindOperator {
	if k.operators != nil {
		return k.operators
	}
	kv := func(op forma.FilterType, i int) *forma.KvCondition {
		return &forma.KvCondition{Attr: k.attr, Value: string(op) + ":" + k.literal(i)}
	}
	elem := func(i int) any {
		if k.numeric {
			return json.Number(k.literal(i))
		}
		return k.literal(i)
	}
	ops := []kindOperator{
		{"equals", kv(forma.FilterEquals, 42), 1},
		{"in", forma.In(k.attr, elem(42), elem(43)), 2},
		{"gt", kv(forma.FilterGreaterThan, kindRows-5), 5},
		{"gte", kv(forma.FilterGreaterEq, kindRows-4), 5},
		{"lt", kv(forma.FilterLessThan, 6), 5},
		{"lte", kv(forma.FilterLessEq, 5), 5},
		{"is_null", forma.IsNull(k.attr), kindNullRows},
	}
	if k.time {
		ops = append(ops, kindOperator{"lte $now", forma.Now(k.attr, forma.FilterLessEq, 0), 6})
	}
	return ops
}

// boolKindOperators lists a bool_smallint kind's operators, spelled so each
// selects the rows holding rare, the truth value only kindRareBool rows hold: a
// plan has no reason to seek the side most rows are on.
func boolKindOperators(attr string, rare bool) []kindOperator {
	kv := func(op forma.FilterType, v bool) *forma.KvCondition {
		return &forma.KvCondition{Attr: attr, Value: string(op) + ":" + strconv.FormatBool(v)}
	}
	return []kindOperator{
		{"equals", kv(forma.FilterEquals, rare), kindRareBool},
		{"not_equals", kv(forma.FilterNotEquals, !rare), kindRareBool},
		{"in", forma.In(attr, rare), kindRareBool},
		{"not_in", forma.NotIn(attr, !rare), kindRareBool},
		{"is_null", forma.IsNull(attr), kindNullRows},
	}
}

// declareEveryKindDir writes a generation of the fixtures in which e2e_wide
// also binds stamp, a datetime, to text_02 under iso8601 and two bools to
// smallint columns under bool_smallint, and declares a one-attribute index on
// each kind.
func declareEveryKindDir(t *testing.T, kinds []indexableKind) string {
	t.Helper()
	dir := copyFixtureSchemaDir(t)
	var declarations []any
	for _, k := range kinds {
		declarations = append(declarations, map[string]any{"name": "by_" + k.attr, "attributes": []any{k.attr}})
	}
	editFixtureJSON(t, filepath.Join(dir, "e2e_wide.json"), func(doc map[string]any) {
		props := doc["properties"].(map[string]any)
		props["stamp"] = map[string]any{"type": "string", "format": "date-time"}
		props["flagged"] = map[string]any{"type": "boolean"}
		props["cleared"] = map[string]any{"type": "boolean"}
		doc["x-forma-indexes"] = declarations
	})
	editFixtureJSON(t, filepath.Join(dir, "e2e_wide_attributes.json"), func(doc map[string]any) {
		doc["stamp"] = map[string]any{"attributeID": 19, "valueType": "datetime",
			"column_binding": map[string]any{"col_name": "text_02", "encoding": "iso8601"}}
		doc["flagged"] = map[string]any{"attributeID": 20, "valueType": "bool",
			"column_binding": map[string]any{"col_name": "smallint_02", "encoding": "bool_smallint"}}
		doc["cleared"] = map[string]any{"attributeID": 21, "valueType": "bool",
			"column_binding": map[string]any{"col_name": "smallint_03", "encoding": "bool_smallint"}}
	})
	return dir
}

func editFixtureJSON(t *testing.T, path string, edit func(map[string]any)) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	edit(doc)
	if raw, err = json.Marshal(doc); err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// seedEveryKind writes the e2e_wide rows each kind's literal describes, plus
// kindNullRows rows with no indexed value, straight into the hot tier, and
// analyzes the table. stamp holds the iso8601 image of touched's instant. The
// rare side of each bool includes an image the write funnel does not produce (2,
// -1), which the rows it selects must count.
func seedEveryKind(ctx context.Context, env *Env, sec, day int64) {
	env.ExecSQL(ctx, `
		INSERT INTO entity_main (ltbase_schema_id, ltbase_row_id, text_01, smallint_01, integer_01, bigint_01, double_01,
		                         uuid_01, bigint_02, bigint_03, text_02, smallint_02, smallint_03, ltbase_created_at, ltbase_updated_at)
		SELECT $1::smallint, gen_random_uuid(), 't' || lpad(i::text, 5, '0'), i, i * 10, i::bigint * 1000000000, i + 0.25,
		       ('00000000-0000-0000-0000-' || lpad(to_hex(i), 12, '0'))::uuid,
		       $2::bigint + (i - 6)::bigint * 2 * $3::bigint,
		       $4::bigint + (i - 6)::bigint * 2 * $5::bigint,
		       to_char(to_timestamp(($4::bigint + (i - 6)::bigint * 2 * $5::bigint) / 1000) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		       CASE WHEN i <= 3 THEN 1 WHEN i <= $7::int THEN 2 ELSE 0 END,
		       CASE WHEN i <= 3 THEN 0 WHEN i <= $7::int THEN -1 ELSE 1 END,
		       $4::bigint, $4::bigint
		FROM generate_series(1, $6::int) AS i`,
		SchemaIDWide, day, dayMillis, sec, hourMillis, kindRows, kindRareBool)
	env.ExecSQL(ctx, `
		INSERT INTO entity_main (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at)
		SELECT $1::smallint, gen_random_uuid(), $2::bigint, $2::bigint FROM generate_series(1, $3::int)`,
		SchemaIDWide, sec, kindNullRows)
	env.ExecSQL(ctx, `ANALYZE entity_main`)
}

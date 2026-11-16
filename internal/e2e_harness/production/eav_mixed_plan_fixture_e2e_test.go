//go:build e2e

package production

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/model"
)

// The fixture of #815's mixed-attribute eav_data benchmark. Row i (1..rows) of
// e2e_wide has title t%06d and rank i % 1000. An eav_data load stores, for
// each attribute it names with a stride s, the value of every row whose i is a
// multiple of s: note holds the row's title, token the uuid
// 00000000-0000-4000-8000-%012x of i. Both formats sort as i does, bytewise,
// and every uuid sorts below every note, which is what puts a selective token
// gt or note lt at the far end of the shared value_text histogram.

// eavPlanRowsVar overrides the e2e_wide sizes the benchmark runs at, as a
// comma-separated list; the default keeps the CI run short.
const eavPlanRowsVar = "FORMA_EAV_PLAN_ROWS"

func eavPlanSizes(t *testing.T) []int {
	t.Helper()
	raw := os.Getenv(eavPlanRowsVar)
	if raw == "" {
		return []int{20_000}
	}
	var sizes []int
	for _, field := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || n < 1_000 || n > 999_999 {
			t.Fatalf("%s=%q: want sizes from 1000 to 999999 (titles hold six digits)", eavPlanRowsVar, raw)
		}
		sizes = append(sizes, n)
	}
	return sizes
}

// eavLoad is one eav_data population: the stride of each attribute it stores.
type eavLoad struct {
	name    string
	strides map[string]int
}

func noteValue(i int) string  { return fmt.Sprintf("t%06d", i) }
func tokenValue(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012x", i) }

func eavValue(attr string, i int) string {
	if attr == "token" {
		return tokenValue(i)
	}
	return noteValue(i)
}

// eavPlanBench holds one e2e_wide database and the repository that queries it.
type eavPlanBench struct {
	ctx      context.Context
	t        *testing.T
	env      *Env
	rows     int
	recorder *recordingPool
	repo     *internal.DBPersistentRecordRepository
	attrIDs  map[string]int16
	strides  map[string]int // what eav_data holds now
	report   []planRow
	setup    string   // the server, collations and index DDL, in Markdown
	plans    []string // the textPlanCases' SQL and plans, in Markdown
}

// newEAVPlanBench declares by_rank_title, builds init-db's eav_data text
// index, writes rows entity_main rows and vacuums and analyzes entity_main.
// eav_data stays empty and its autovacuum off, so its statistics change only
// when the benchmark analyzes it.
func newEAVPlanBench(ctx context.Context, t *testing.T, rows int) *eavPlanBench {
	t.Helper()
	env := NewEnv(t, SharedCluster(t))
	requireNonCDatabaseDefault(ctx, t, env)
	dir := copyFixtureSchemaDir(t)
	editFixtureJSON(t, filepath.Join(dir, "e2e_wide.json"), func(doc map[string]any) {
		doc["x-forma-indexes"] = []any{map[string]any{"name": "by_rank_title", "attributes": []any{"rank", "title"}}}
	})
	if err := env.EvolveSchema(ctx, dir); err != nil {
		t.Fatalf("evolve e2e_wide to declare by_rank_title: %v", err)
	}
	env.ExecSQL(ctx, eavTextIndexDDL)
	env.ExecSQL(ctx, `ALTER TABLE eav_data SET (autovacuum_enabled = false)`)
	env.ExecSQL(ctx, `
		INSERT INTO entity_main (ltbase_schema_id, ltbase_row_id, text_01, smallint_01, ltbase_created_at, ltbase_updated_at)
		SELECT $1::smallint, gen_random_uuid(), 't' || lpad(i::text, 6, '0'), (i % 1000)::smallint, 1, 1
		FROM generate_series(1, $2::int) AS i`, SchemaIDWide, rows)
	env.ExecSQL(ctx, `VACUUM (ANALYZE) entity_main`)

	_, cache, err := env.Registry.GetSchemaAttributeCacheByID(SchemaIDWide)
	if err != nil {
		t.Fatalf("read e2e_wide attributes: %v", err)
	}
	recorder := &recordingPool{Pool: env.Pool}
	return &eavPlanBench{
		ctx: ctx, t: t, env: env, rows: rows, recorder: recorder,
		repo:    internal.NewDBPersistentRecordRepository(recorder, env.Metadata),
		attrIDs: map[string]int16{"note": cache["note"].AttributeID, "token": cache["token"].AttributeID},
		strides: map[string]int{},
	}
}

// load empties eav_data and stores l. It does not analyze.
func (b *eavPlanBench) load(l eavLoad) {
	b.env.ExecSQL(b.ctx, `DELETE FROM eav_data`)
	b.strides = map[string]int{}
	b.add(l)
}

// add stores l beside what eav_data holds. It does not analyze.
func (b *eavPlanBench) add(l eavLoad) {
	for _, attr := range slices.Sorted(maps.Keys(l.strides)) {
		stride := l.strides[attr]
		b.env.ExecSQL(b.ctx, `
			INSERT INTO eav_data (schema_id, row_id, attr_id, array_indices, value_text)
			SELECT ltbase_schema_id, ltbase_row_id, $2::smallint, '',
			       CASE WHEN $4 THEN '00000000-0000-4000-8000-' || lpad(to_hex(n), 12, '0') ELSE text_01 END
			FROM (SELECT ltbase_schema_id, ltbase_row_id, text_01, substr(text_01, 2)::int AS n
			      FROM entity_main WHERE ltbase_schema_id = $1) m
			WHERE n % $3 = 0`, SchemaIDWide, b.attrIDs[attr], stride, attr == "token")
		b.strides[attr] = stride
	}
}

// analyze vacuums and analyzes eav_data, as autovacuum would after a load:
// the visibility map it sets is what lets an index-only scan skip the heap.
func (b *eavPlanBench) analyze() { b.env.ExecSQL(b.ctx, `VACUUM (ANALYZE) eav_data`) }

// requireNoStatistics fails unless eav_data has never been analyzed, which the
// "missing" statistics state depends on.
func (b *eavPlanBench) requireNoStatistics() {
	var n int
	if err := b.env.Pool.QueryRow(b.ctx, `SELECT count(*) FROM pg_stats WHERE tablename = 'eav_data'`).Scan(&n); err != nil {
		b.t.Fatalf("read eav_data statistics: %v", err)
	}
	if n != 0 {
		b.t.Fatalf("eav_data has statistics on %d columns, want none before the first ANALYZE", n)
	}
}

// eavProbe is one query of the matrix and the titles of the rows it must
// answer.
type eavProbe struct {
	name   string
	cond   forma.Condition
	want   map[string]bool
	index  string // the index whose Index Cond must name column when forced
	column string
	ranged bool // gt, gte, lt or lte
	set    bool // in
}

// eavProbes lists equals, in, the four ranges with five matching rows each,
// and a gt and an lt that select half of attr's rows, for what eav_data holds
// of attr now.
func (b *eavPlanBench) eavProbes(attr string) []eavProbe {
	var present []int
	for i := b.strides[attr]; i <= b.rows; i += b.strides[attr] {
		present = append(present, i)
	}
	n := len(present)
	at := func(k int) string { return eavValue(attr, present[k]) }
	probe := func(name string, op forma.FilterType, bound string) eavProbe {
		return eavProbe{
			name: attr + " " + name, cond: &forma.KvCondition{Attr: attr, Value: string(op) + ":" + bound},
			want: b.matching(attr, present, op, bound), index: "eav_data_text_idx", column: "value_text",
			ranged: op != forma.FilterEquals,
		}
	}
	in := probe("in", forma.FilterEquals, at(42))
	in.cond, in.ranged, in.set = forma.In(attr, at(42), at(43)), false, true
	in.want[noteValue(present[43])] = true
	return []eavProbe{
		probe("equals", forma.FilterEquals, at(42)), in,
		probe("gt 5", forma.FilterGreaterThan, at(n-6)),
		probe("gte 5", forma.FilterGreaterEq, at(n-5)),
		probe("lt 5", forma.FilterLessThan, at(5)),
		probe("lte 5", forma.FilterLessEq, at(4)),
		probe("gt half", forma.FilterGreaterThan, at(n/2-1)),
		probe("lt half", forma.FilterLessThan, at(n/2)),
	}
}

// matching answers the titles of the rows whose attr value compares to bound
// as op says, bytewise: Go compares strings by their bytes, as C does.
func (b *eavPlanBench) matching(attr string, present []int, op forma.FilterType, bound string) map[string]bool {
	want := map[string]bool{}
	for _, i := range present {
		c := strings.Compare(eavValue(attr, i), bound)
		ok := map[forma.FilterType]bool{
			forma.FilterEquals: c == 0, forma.FilterGreaterThan: c > 0, forma.FilterGreaterEq: c >= 0,
			forma.FilterLessThan: c < 0, forma.FilterLessEq: c <= 0,
		}[op]
		if ok {
			want[noteValue(i)] = true
		}
	}
	return want
}

// compositeProbes ranges title under rank 7 on by_rank_title: the
// column-bound control, which eav_data's statistics cannot move.
func (b *eavPlanBench) compositeProbes() []eavProbe {
	index := declaredIndexFor(b.t, b.env, "e2e_wide.by_rank_title").Name
	mid := noteValue(b.rows / 2)
	probe := func(name string, op forma.FilterType) eavProbe {
		want := map[string]bool{}
		for i := 7; i <= b.rows; i += 1000 {
			if c := strings.Compare(noteValue(i), mid); (op == forma.FilterGreaterThan && c > 0) || (op == forma.FilterLessThan && c < 0) {
				want[noteValue(i)] = true
			}
		}
		return eavProbe{
			name: "rank=7, title " + name, want: want, index: index, column: "text_01", ranged: true,
			cond: and(eq("rank", "7"), &forma.KvCondition{Attr: "title", Value: string(op) + ":" + mid}),
		}
	}
	return []eavProbe{probe("gt half", forma.FilterGreaterThan), probe("lt half", forma.FilterLessThan)}
}

// query runs p through the repository, checks every row it answers and the
// total, and returns the hot-tier query that answered.
func (b *eavPlanBench) query(p eavProbe) recordedQuery {
	b.t.Helper()
	page, err := b.repo.QueryPersistentRecords(b.ctx, &model.PersistentRecordQuery{
		Tables: b.env.Tables, SchemaID: SchemaIDWide, Condition: p.cond, Limit: 1000,
	})
	if err != nil {
		b.t.Fatalf("query %s: %v", p.name, err)
	}
	if page.TotalRecords != int64(len(p.want)) || len(page.Records) != min(len(p.want), 1000) {
		b.t.Fatalf("%s: %d records of %d, want %d", p.name, len(page.Records), page.TotalRecords, len(p.want))
	}
	for _, rec := range page.Records {
		if title := rec.TextItems["text_01"]; !p.want[title] {
			b.t.Fatalf("%s answered row %s, which does not match bytewise", p.name, title)
		}
	}
	return b.recorder.takeAnchorQuery(b.t)
}

// eligibilityProbe says whether p's plan must show the index eligible. That
// is a property of the operator and the collation, not of the bound, so the
// selective probes carry it: for half of an attribute, a bitmap over another
// index's (schema_id, attr_id) prefix rightly costs less even with sequential
// scans off. An in list whose attribute the statistics miss is likewise
// cheaper, by the planner's estimate, to filter on that prefix, so in counts
// under fresh statistics only.
func eligibilityProbe(p eavProbe, stats string) bool {
	return len(p.want) <= 5 && (stats == "fresh" || !p.set)
}

// forcedSeek reports whether, with sequential scans priced out, the plan of q
// reads p.index with p.column in an Index Cond: whether the index is eligible
// at all, apart from what the statistics make it cost.
func (b *eavPlanBench) forcedSeek(q recordedQuery, p eavProbe) bool {
	b.t.Helper()
	conn, err := b.env.Pool.Acquire(b.ctx)
	if err != nil {
		b.t.Fatalf("acquire a connection: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(b.ctx, `SET enable_seqscan = off`); err != nil {
		b.t.Fatalf("disable sequential scans: %v", err)
	}
	defer func() { _, _ = conn.Exec(b.ctx, `RESET enable_seqscan`) }()
	var raw []byte
	if err := conn.QueryRow(b.ctx, "EXPLAIN (FORMAT JSON) "+q.sql, q.args...).Scan(&raw); err != nil {
		b.t.Fatalf("explain %s without sequential scans: %v", p.name, err)
	}
	plan := decodeExplain(b.t, raw).Plan
	return anyIndexCondMentions(indexScansOf(plan, p.index), p.column)
}

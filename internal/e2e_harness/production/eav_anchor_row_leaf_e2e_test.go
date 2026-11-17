//go:build e2e

package production

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/model"
)

// TestEAVAnchorRowLeavesAnswerAsTheEntity proves #881's anchor rewrite on
// PostgreSQL: where a condition tests only EAV attributes, the anchor tests
// a leaf on its eav_data row t instead of reading every eav_data row of the
// schema as t and asking an EXISTS of each. The answer must not move. Each
// condition runs twice through the repository: as is, anchored on eav_data,
// and with an always-true entity_main leaf added, which anchors it on
// entity_main, where every EAV leaf stays an EXISTS over the entity's rows.
// Both must answer the same rows, in the same order, with the same total, on
// a full page and on a short offset page.
//
// The fixture holds what an equivalence could get wrong: negations
// (not_equals, not_in), which hold only where the attribute holds another
// value; list rows and the empty-list marker, several eav_data rows of one
// entity; entities with no eav_data row at all; and rows still unflushed in
// change_log, which the anchor's second branch reads.
func TestEAVAnchorRowLeavesAnswerAsTheEntity(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	const rows = 40
	seedAnchorRowLeafFixture(ctx, t, env, wide, rows)
	env.ExecSQL(ctx, `ANALYZE eav_data`)

	recorder := &recordingPool{Pool: env.Pool}
	repo := internal.NewDBPersistentRecordRepository(recorder, env.Metadata)
	for _, c := range anchorRowLeafConditions() {
		t.Run(c.name, func(t *testing.T) {
			for _, page := range [][2]int{{1000, 0}, {4, 2}} {
				eav, eavSQL := queryAnchored(ctx, t, repo, recorder, env, c.cond, page)
				if !strings.Contains(eavSQL, `SELECT DISTINCT t.row_id`) || !strings.Contains(firstAnchorBranch(eavSQL), "t.attr_id = $") {
					t.Fatalf("the anchor does not test a leaf on its eav_data row t:\n%s", eavSQL)
				}
				main, mainSQL := queryAnchored(ctx, t, repo, recorder, env, and(c.cond, isNotNull("ltbase_row_id")), page)
				if !strings.Contains(mainSQL, `SELECT m.ltbase_row_id AS row_id`) {
					t.Fatalf("the reference is not anchored on entity_main:\n%s", mainSQL)
				}
				if page[1] == 0 && (main.TotalRecords == 0 || main.TotalRecords == rows) {
					t.Fatalf("the reference answers %d of %d rows: the condition does not discriminate", main.TotalRecords, rows)
				}
				if eav.TotalRecords != main.TotalRecords || !slices.Equal(rowIDsOf(eav), rowIDsOf(main)) {
					t.Errorf("page %v: the eav_data anchor answers %d rows %v, the entity_main anchor %d rows %v",
						page, eav.TotalRecords, titlesOf(eav), main.TotalRecords, titlesOf(main))
				}
			}
		})
	}
}

type anchorRowLeafCondition struct {
	name string
	cond forma.Condition
}

func anchorRowLeafConditions() []anchorRowLeafCondition {
	kv := func(attr, op, value string) *forma.KvCondition {
		return &forma.KvCondition{Attr: attr, Value: op + ":" + value}
	}
	or := func(conditions ...forma.Condition) forma.Condition {
		return &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: conditions}
	}
	return []anchorRowLeafCondition{
		{"text range", kv("note", "gt", "n2")},
		{"uuid range", kv("token", "lt", anchorToken(21))},
		{"text not_equals", kv("note", "not_equals", "n2")},
		{"text not_in", kv("note", "not_in", `["n1","n3"]`)},
		{"text starts_with", kv("note", "starts_with", "n1")},
		{"integer range", kv("qty", "gte", "3")},
		{"bool equals", kv("active", "equals", "true")},
		{"text is_not_null", kv("note", "is_not_null", "true")},
		{"list is_not_null, empty list included", kv("tags", "is_not_null", "true")},
		{"and of two leaves", and(kv("note", "starts_with", "n"), kv("token", "gt", anchorToken(9)))},
		{"and with a negation", and(kv("note", "not_equals", "n2"), kv("qty", "lt", "5"))},
		{"and of a list presence and a negation", and(kv("tags", "is_not_null", "true"), kv("note", "not_in", `["n0"]`))},
		{"or of two attributes", or(kv("note", "equals", "n1"), kv("qty", "lt", "2"))},
		{"or of an and and a leaf", or(and(kv("note", "not_equals", "n1"), kv("qty", "gte", "2")), kv("token", "equals", anchorToken(27)))},
		{"and of an or and a leaf", and(or(kv("note", "equals", "n1"), kv("note", "equals", "n2")), kv("qty", "gte", "1"))},
	}
}

func isNotNull(attr string) *forma.KvCondition {
	return &forma.KvCondition{Attr: attr, Value: "is_not_null:true"}
}

func anchorToken(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012x", i) }

// seedAnchorRowLeafFixture writes rows e2e_wide entities titled t00..: row i
// holds note n<i mod 5> on even rows, token i on every third row, qty i mod 7
// unless i is a multiple of 5, active on odd rows, and tags [red, blue],
// [blue], the empty list or nothing as i mod 4 says. Every tenth row from 9
// holds its title alone, so it has no eav_data row. Every other row's
// change_log entries are then marked flushed, so half the rows reach the
// anchor's change_log branch.
func seedAnchorRowLeafFixture(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, rows int) {
	t.Helper()
	events := make([]*Event, 0, rows)
	for i := range rows {
		attrs := map[string]any{"title": fmt.Sprintf("t%02d", i)}
		if i%10 != 9 {
			addAnchorRowLeafAttrs(attrs, i)
		}
		events = append(events, CreateEvent(wide, attrs))
	}
	mustApplyEvents(ctx, t, env, "seed e2e_wide", events...)
	env.ExecSQL(ctx, `
		UPDATE change_log cl SET flushed_at = cl.changed_at + 1
		FROM entity_main m
		WHERE cl.schema_id = $1 AND m.ltbase_schema_id = $1 AND m.ltbase_row_id = cl.row_id
		  AND substr(m.text_01, 2)::int % 2 = 0`, wide.ID)
}

func addAnchorRowLeafAttrs(attrs map[string]any, i int) {
	if i%2 == 0 {
		attrs["note"] = fmt.Sprintf("n%d", i%5)
	} else {
		attrs["active"] = i%3 == 0
	}
	if i%3 == 0 {
		attrs["token"] = anchorToken(i)
	}
	if i%5 != 0 {
		attrs["qty"] = i % 7
	}
	switch i % 4 {
	case 0:
		attrs["tags"] = []any{"red", "blue"}
	case 1:
		attrs["tags"] = []any{"blue"}
	case 2:
		attrs["tags"] = []any{}
	}
}

// queryAnchored runs cond through the repository's hot-tier route and
// returns the page with the optimized query that answered it.
func queryAnchored(ctx context.Context, t *testing.T, repo *internal.DBPersistentRecordRepository, recorder *recordingPool,
	env *Env, cond forma.Condition, page [2]int) (*model.PersistentRecordPage, string) {
	t.Helper()
	result, err := repo.QueryPersistentRecords(ctx, &model.PersistentRecordQuery{
		Tables: env.Tables, SchemaID: SchemaIDWide, Condition: cond, Limit: page[0], Offset: page[1],
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	return result, recorder.takeAnchorQuery(t).sql
}

// firstAnchorBranch is the anchor's eav_data branch, ahead of its
// change_log branch.
func firstAnchorBranch(sql string) string {
	branch, _, _ := strings.Cut(sql, "UNION")
	return branch
}

func rowIDsOf(page *model.PersistentRecordPage) []string {
	ids := make([]string, 0, len(page.Records))
	for _, rec := range page.Records {
		ids = append(ids, rec.RowID.String())
	}
	return ids
}

func titlesOf(page *model.PersistentRecordPage) []string {
	titles := make([]string, 0, len(page.Records))
	for _, rec := range page.Records {
		titles = append(titles, rec.TextItems["text_01"])
	}
	return titles
}

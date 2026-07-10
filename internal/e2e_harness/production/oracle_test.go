package production

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/google/uuid"
	forma "github.com/lychee-technology/forma"
)

func wideCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"title":  {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
		"count":  {AttributeID: 3, ValueType: forma.ValueTypeInteger, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnInteger01}},
		"score":  {AttributeID: 5, ValueType: forma.ValueTypeNumeric, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnDouble01}},
		"note":   {AttributeID: 7, ValueType: forma.ValueTypeText},
		"active": {AttributeID: 8, ValueType: forma.ValueTypeBool},
		"born":   {AttributeID: 9, ValueType: forma.ValueTypeDate},
		"seen":   {AttributeID: 10, ValueType: forma.ValueTypeDateTime},
	}
}

func testSchema() SchemaRef { return SchemaRef{ID: 21, Name: "e2e_wide"} }

func mustState(t *testing.T, events []*Event) *ExpectedState {
	t.Helper()
	state, err := ExpectedStateFromEvents(events, testSchema(), wideCache())
	if err != nil {
		t.Fatalf("build expected state: %v", err)
	}
	return state
}

func mustRun(t *testing.T, state *ExpectedState, q Query) *ExpectedResult {
	t.Helper()
	result, err := state.Run(q)
	if err != nil {
		t.Fatalf("oracle run: %v", err)
	}
	return result
}

func newRowID(n byte) uuid.UUID {
	var b [16]byte
	b[0] = n
	b[6] = 0x70
	b[8] = 0x80
	id, _ := uuid.FromBytes(b[:])
	return id
}

func TestOracle_LWWFoldAndDeletes(t *testing.T) {
	rowA, rowB := newRowID(1), newRowID(2)
	events := []*Event{
		{Kind: EventCreate, Schema: testSchema(), RowID: rowA, Seq: 1, ChangedAt: 100,
			Attrs: map[string]any{"title": "a1", "count": float64(10)}},
		{Kind: EventCreate, Schema: testSchema(), RowID: rowB, Seq: 2, ChangedAt: 100,
			Attrs: map[string]any{"title": "b1", "count": float64(20)}},
		// Same millisecond as the create: Seq breaks the tie.
		{Kind: EventUpdate, Schema: testSchema(), RowID: rowA, Seq: 3, ChangedAt: 100,
			Attrs: map[string]any{"title": "a2"}},
		{Kind: EventDelete, Schema: testSchema(), RowID: rowB, Seq: 4, ChangedAt: 200},
	}
	state := mustState(t, events)

	result := mustRun(t, state, Query{Schema: testSchema()})
	if result.Total != 1 {
		t.Fatalf("total = %d, want 1 (deleted row must drop)", result.Total)
	}
	row := result.Rows[0]
	if row.RowID != rowA {
		t.Fatalf("visible row = %s, want %s", row.RowID, rowA)
	}
	if row.Attrs["title"] != "a2" {
		t.Errorf("title = %v, want update to win (a2)", row.Attrs["title"])
	}
	if row.Attrs["count"] != float64(10) {
		t.Errorf("count = %v, want 10 preserved through partial update", row.Attrs["count"])
	}
}

func TestOracle_TypedFilters(t *testing.T) {
	events := []*Event{
		{Kind: EventCreate, Schema: testSchema(), RowID: newRowID(1), Seq: 1, ChangedAt: 1, Attrs: map[string]any{
			"title": "alpha", "count": float64(10), "score": 1.5, "active": true,
			"born": "1990-05-01", "seen": "2026-07-10T01:02:03Z", "note": "hello world",
		}},
		{Kind: EventCreate, Schema: testSchema(), RowID: newRowID(2), Seq: 2, ChangedAt: 2, Attrs: map[string]any{
			"title": "beta", "count": float64(20), "score": 2.5, "active": false,
			"born": "2000-01-01", "seen": "2026-07-11T01:02:03Z", "note": "other",
		}},
		// Row without optional attributes: NULL semantics exclude it from
		// any comparison filter.
		{Kind: EventCreate, Schema: testSchema(), RowID: newRowID(3), Seq: 3, ChangedAt: 3, Attrs: map[string]any{
			"title": "gamma", "count": float64(30),
		}},
	}
	state := mustState(t, events)

	cases := []struct {
		name    string
		filters []Filter
		want    int64
	}{
		{"text equals", []Filter{{Attr: "title", Value: "alpha"}}, 1},
		{"text starts_with", []Filter{{Attr: "title", Op: "starts_with", Value: "a"}}, 1},
		{"text contains", []Filter{{Attr: "note", Op: "contains", Value: "world"}}, 1},
		{"numeric gt", []Filter{{Attr: "count", Op: "gt", Value: "15"}}, 2},
		{"numeric range and", []Filter{{Attr: "count", Op: "gte", Value: "10"}, {Attr: "count", Op: "lt", Value: "30"}}, 2},
		{"float equals", []Filter{{Attr: "score", Value: "2.5"}}, 1},
		{"bool true", []Filter{{Attr: "active", Value: "1"}}, 1},
		{"bool false", []Filter{{Attr: "active", Value: "0"}}, 1},
		{"date lt excludes null", []Filter{{Attr: "born", Op: "lt", Value: "1995-01-01"}}, 1},
		{"datetime gte", []Filter{{Attr: "seen", Op: "gte", Value: "2026-07-11T00:00:00Z"}}, 1},
		{"not_equals excludes null", []Filter{{Attr: "score", Op: "not_equals", Value: "1.5"}}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := mustRun(t, state, Query{Schema: testSchema(), Filters: tc.filters})
			if result.Total != tc.want {
				t.Fatalf("total = %d, want %d", result.Total, tc.want)
			}
		})
	}
}

func TestOracle_SortAndPaginate(t *testing.T) {
	var events []*Event
	counts := []float64{30, 10, 20, 40, 20} // duplicate 20 exercises the row_id tiebreak
	for i, c := range counts {
		events = append(events, &Event{
			Kind: EventCreate, Schema: testSchema(), RowID: newRowID(byte(i + 1)),
			Seq: i + 1, ChangedAt: int64(i + 1),
			Attrs: map[string]any{"count": c},
		})
	}
	state := mustState(t, events)

	result := mustRun(t, state, Query{
		Schema: testSchema(),
		Sorts:  []Sort{{Attr: "count"}},
		Limit:  2,
		Offset: 1,
	})
	if result.Total != 5 {
		t.Fatalf("total = %d, want 5", result.Total)
	}
	// Sorted by count ASC: 10(row2), 20(row3), 20(row5), 30(row1), 40(row4).
	// Rows 3 and 5 tie on count; row_id ASC puts row3 first. Page 2 of size
	// 2 starting at offset 1 = [row3, row5].
	want := []uuid.UUID{newRowID(3), newRowID(5)}
	if len(result.Rows) != 2 || result.Rows[0].RowID != want[0] || result.Rows[1].RowID != want[1] {
		got := make([]string, 0, len(result.Rows))
		for _, r := range result.Rows {
			got = append(got, r.RowID.String())
		}
		t.Fatalf("page = %v, want %v", got, want)
	}

	desc := mustRun(t, state, Query{Schema: testSchema(), Sorts: []Sort{{Attr: "count", Desc: true}}})
	if desc.Rows[0].Attrs["count"] != float64(40) {
		t.Fatalf("desc first count = %v, want 40", desc.Rows[0].Attrs["count"])
	}
}

func TestNormalizeValue_Conventions(t *testing.T) {
	cases := []struct {
		name  string
		value any
		vt    forma.ValueType
		want  any
	}{
		{"bool true is 1", true, forma.ValueTypeBool, float64(1)},
		{"bool false is 0", false, forma.ValueTypeBool, float64(0)},
		{"date string to millis", "1970-01-02", forma.ValueTypeDate, float64(86400000)},
		{"datetime string to millis", "1970-01-01T00:00:01Z", forma.ValueTypeDateTime, float64(1000)},
		{"int64 to float64", int64(7), forma.ValueTypeBigInt, float64(7)},
		{"uuid canonical", "0198C5F2-0000-7000-8000-000000000001", forma.ValueTypeUUID, "0198c5f2-0000-7000-8000-000000000001"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeValue(tc.value, tc.vt)
			if err != nil {
				t.Fatalf("normalizeValue: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %v (%T), want %v (%T)", got, got, tc.want, tc.want)
			}
		})
	}

	if _, err := normalizeValue(map[string]any{}, forma.ValueTypeText); err == nil {
		t.Error("expected error for non-string text value")
	}
}

func TestGenerateScript_Deterministic(t *testing.T) {
	spec := ScriptSpec{Schema: testSchema(), Creates: 8, Updates: 4, Deletes: 2}
	a := generateScript(rand.New(rand.NewSource(42)), spec)
	b := generateScript(rand.New(rand.NewSource(42)), spec)

	if len(a) != len(b) {
		t.Fatalf("script lengths differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Kind != b[i].Kind || !reflect.DeepEqual(a[i].Attrs, b[i].Attrs) {
			t.Fatalf("event %d differs between runs with the same seed", i)
		}
	}

	c := generateScript(rand.New(rand.NewSource(43)), spec)
	same := true
	for i := range a {
		if !reflect.DeepEqual(a[i].Attrs, c[i].Attrs) {
			same = false
			break
		}
	}
	if same {
		t.Error("different seeds produced identical scripts")
	}

	creates, deletes := 0, 0
	for _, ev := range a {
		switch ev.Kind {
		case EventCreate:
			creates++
		case EventDelete:
			deletes++
			if ev.Target == nil {
				t.Error("delete event has no target")
			}
		case EventUpdate:
			if ev.Target == nil {
				t.Error("update event has no target")
			}
		}
	}
	if creates != 8 || deletes != 2 {
		t.Fatalf("creates/deletes = %d/%d, want 8/2", creates, deletes)
	}
}

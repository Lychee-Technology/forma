package production

import (
	"testing"

	"github.com/google/uuid"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// The negative controls below prove computeDiff is not vacuous: starting
// from an engine result that matches the oracle, each injects one semantic
// discrepancy a broken tier merge could produce, and the diff must report
// it. Without them a passing AssertQueryMatches could mean the comparison
// never looked at the field.

// controlEvents: row 1 is created then updated (V2 must win), row 2 is
// created then deleted, row 3 is created once. Row 1's V2 carries a bound
// bigint past 2^53, where a float64 image is no longer exact.
func controlEvents() []*Event {
	s := testSchema()
	return []*Event{
		{Kind: EventCreate, Schema: s, RowID: newRowID(1), Seq: 1, ChangedAt: 10,
			Attrs: map[string]any{"title": "one-v1", "count": int64(1), "amount": int64(5), "note": "n1"}},
		{Kind: EventCreate, Schema: s, RowID: newRowID(2), Seq: 2, ChangedAt: 11,
			Attrs: map[string]any{"title": "two", "count": int64(2), "amount": int64(6)}},
		{Kind: EventCreate, Schema: s, RowID: newRowID(3), Seq: 3, ChangedAt: 12,
			Attrs: map[string]any{"title": "three", "count": int64(3), "amount": int64(7), "total": int64(1 << 53)}},
		{Kind: EventUpdate, Schema: s, RowID: newRowID(1), Seq: 4, ChangedAt: 20,
			Attrs: map[string]any{"title": "one-v2", "amount": int64(1<<53 + 1)}},
		{Kind: EventDelete, Schema: s, RowID: newRowID(2), Seq: 5, ChangedAt: 21},
	}
}

func controlRecord(id uuid.UUID, title string, count int32, amount int64, eav ...model.EAVRecord) *model.PersistentRecord {
	return &model.PersistentRecord{
		SchemaID:        testSchema().ID,
		RowID:           id,
		TextItems:       map[string]string{string(forma.MainColumnText01): title},
		Int32Items:      map[string]int32{string(forma.MainColumnInteger01): count},
		Int64Items:      map[string]int64{string(forma.MainColumnBigint01): amount},
		OtherAttributes: eav,
	}
}

func textEAV(attrID int16, v string) model.EAVRecord {
	return model.EAVRecord{AttrID: attrID, ValueText: &v}
}

func numericEAV(attrID int16, v float64) model.EAVRecord {
	return model.EAVRecord{AttrID: attrID, ValueNumeric: &v}
}

// controlActual is the correct engine answer for controlQuery: rows 1 and 3
// in descending count order, row 1 at V2, row 2 absent.
func controlActual() *QueryResult {
	return &QueryResult{Total: 2, Records: []*model.PersistentRecord{
		controlRecord(newRowID(3), "three", 3, 7, numericEAV(15, float64(1<<53))),
		controlRecord(newRowID(1), "one-v2", 1, 1<<53+1, textEAV(7, "n1")),
	}}
}

func controlQuery() Query {
	return Query{Schema: testSchema(), Sorts: []Sort{{Attr: "count", Desc: true}}, Limit: 10}
}

func controlDiff(t *testing.T, actual *QueryResult) *Diff {
	t.Helper()
	state := mustExactState(t, controlEvents())
	expected := mustRun(t, state, controlQuery())
	return computeDiff(controlQuery(), expected, actual, state.Cache)
}

func TestComputeDiff_CorrectEngineResultIsEmpty(t *testing.T) {
	if diff := controlDiff(t, controlActual()); !diff.IsEmpty() {
		t.Fatalf("positive control: diff of the correct result is not empty: %+v", diff)
	}
}

func TestComputeDiff_ReportsEachInjectedDiscrepancy(t *testing.T) {
	cases := []struct {
		name   string
		inject func(r *QueryResult)
		check  func(d *Diff) bool
	}{
		{"stale V1 value survives the merge", func(r *QueryResult) {
			r.Records[1].TextItems[string(forma.MainColumnText01)] = "one-v1"
		}, func(d *Diff) bool { return hasAttrMismatch(d, newRowID(1), "title") }},
		{"bound bigint rounded through float64", func(r *QueryResult) {
			r.Records[1].Int64Items[string(forma.MainColumnBigint01)] = 1 << 53
		}, func(d *Diff) bool { return hasAttrMismatch(d, newRowID(1), "amount") }},
		{"EAV attribute dropped", func(r *QueryResult) {
			r.Records[1].OtherAttributes = nil
		}, func(d *Diff) bool { return hasAttrMismatch(d, newRowID(1), "note") }},
		{"EAV numeric changed", func(r *QueryResult) {
			r.Records[0].OtherAttributes = []model.EAVRecord{numericEAV(15, float64(1<<53-2))}
		}, func(d *Diff) bool { return hasAttrMismatch(d, newRowID(3), "total") }},
		{"deleted row resurrected", func(r *QueryResult) {
			r.Records = append(r.Records, controlRecord(newRowID(2), "two", 2, 6))
			r.Total = 3
		}, func(d *Diff) bool { return len(d.Extra) == 1 && d.Extra[0] == newRowID(2).String() }},
		{"live row lost", func(r *QueryResult) {
			r.Records = r.Records[:1]
			r.Total = 1
		}, func(d *Diff) bool { return len(d.Missing) == 1 && d.Missing[0] == newRowID(1).String() }},
		{"total counts a duplicate version", func(r *QueryResult) {
			r.Total = 3
		}, func(d *Diff) bool { return d.ActualTotal == 3 && d.ExpectedTotal == 2 }},
		// Every field of the copy matches and the total is right, so only the
		// duplicate check can see it.
		{"a row returned twice with the right total", func(r *QueryResult) {
			r.Records = append(r.Records, controlRecord(newRowID(1), "one-v2", 1, 1<<53+1, textEAV(7, "n1")))
		}, func(d *Diff) bool { return len(d.Duplicate) == 1 && d.Duplicate[0] == newRowID(1).String() }},
		{"sort order inverted", func(r *QueryResult) {
			r.Records[0], r.Records[1] = r.Records[1], r.Records[0]
		}, func(d *Diff) bool { return d.Misordered }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual := controlActual()
			tc.inject(actual)
			diff := controlDiff(t, actual)
			if diff.IsEmpty() || !tc.check(diff) {
				t.Fatalf("diff did not report the injected discrepancy: %+v", diff)
			}
		})
	}
}

func hasAttrMismatch(d *Diff, id uuid.UUID, attr string) bool {
	for _, m := range d.Attrs {
		if m.RowID == id.String() && m.Attr == attr {
			return true
		}
	}
	return false
}

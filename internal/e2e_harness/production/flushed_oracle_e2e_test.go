//go:build e2e

package production

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"testing"

	"github.com/google/uuid"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"
)

// The flushed oracle for the pgx hot leg. Each request is answered while
// the fixture's rows sit in every tier, so the hot leg serves some of each
// page and the merge places them among the parquet rows; once every row is
// flushed, the same request is asked again, and DuckDB answers it from
// parquet alone. The two answers must hold the same rows in the same order,
// with the same values, and report the same total. The fixtures put rows in
// every tier and at every sort-key edge of pgx-hot-leg-plan.md step 2.
//
// Before step 7 these tests compared the attach and the pgx hot leg over one
// Env (pgx-hot-leg-plan.md step 6, the dual run). Its other fixtures have an
// oracle of their own now: the tie run's walks in
// TestPgxHotLegMixedTierTiebreakWalk (the flushed order), the deleted_ts
// walks, boundaries and offset form in TestDeletedTSKeysetWalk and
// TestDeletedTSKeysetBoundaries (row_id order and the Go predicate), and the
// flush race in TestFlushGraceExactAnchorCoversInFlightFlush and
// TestPgxHotLegRacedFlushWithWideningOff (the raced rows and values).

// flushedOracle records the requests a test answers on the Env's engine, to
// ask them again in settle.
type flushedOracle struct {
	env   *Env
	asked []askedQuery
}

// askedQuery is one recorded request and its answer.
type askedQuery struct {
	label string
	q     Query
	res   *QueryResult
}

func newFlushedOracle(env *Env) *flushedOracle {
	return &flushedOracle{env: env}
}

// answer runs q on the federated route, records it, and returns its page.
func (o *flushedOracle) answer(ctx context.Context, t *testing.T, label string, q Query) *QueryResult {
	t.Helper()
	res := o.query(ctx, t, label, q)
	o.asked = append(o.asked, askedQuery{label: label, q: q, res: res})
	return res
}

func (o *flushedOracle) query(ctx context.Context, t *testing.T, label string, q Query) *QueryResult {
	t.Helper()
	res, err := o.env.Query(ctx, q)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("%s: must answer on the federated route, got %+v", label, res.Plan.Routing)
	}
	return res
}

// settle flushes every row, waits past the flush's marks so that no pass
// reads them from the hot leg, and requires every recorded request to
// answer as it did. It returns how many recorded pages the hot leg served
// rows of, so a test can require that it reached the hot leg at all.
func (o *flushedOracle) settle(ctx context.Context, t *testing.T) int {
	t.Helper()
	mustFlush(ctx, t, o.env)
	waitWallClockTick(t)
	hotPages := 0
	for _, a := range o.asked {
		if src, ok := planSource(a.res.Plan, hotLegPgxReason); ok && src.ActualRows > 0 {
			hotPages++
		}
		flushed := o.query(ctx, t, a.label+", flushed", a.q)
		if src, ok := planSource(flushed.Plan, hotLegPgxReason); ok && src.RowEstimate > 0 {
			t.Fatalf("%s: the flushed answer still drew %d hot keys", a.label, src.RowEstimate)
		}
		requireSamePage(t, o.env, a.q.Schema, a.label, a.res, flushed)
	}
	return hotPages
}

// requireSamePage compares the mixed-tier and the flushed answers row by
// row.
func requireSamePage(t *testing.T, env *Env, schema SchemaRef, label string, mixed, flushed *QueryResult) {
	t.Helper()
	if !slices.Equal(pageRowIDs(mixed), pageRowIDs(flushed)) {
		t.Fatalf("%s: answers %v over the tiers, %v once flushed", label, pageRowIDs(mixed), pageRowIDs(flushed))
	}
	if mixed.Total != flushed.Total {
		t.Fatalf("%s: counts %d matches over the tiers, %d once flushed", label, mixed.Total, flushed.Total)
	}
	for i := range mixed.Records {
		a, f := recordImage(t, env, schema, mixed.Records[i]), recordImage(t, env, schema, flushed.Records[i])
		if !reflect.DeepEqual(a, f) {
			t.Fatalf("%s: row %s reads %v over the tiers and %v once flushed", label, mixed.Records[i].RowID, a, f)
		}
	}
}

// recordImage is what a caller reads of a record: its system timestamps and
// every attribute of its schema, as the oracle normalizes them.
func recordImage(t *testing.T, env *Env, schema SchemaRef, rec *model.PersistentRecord) map[string]any {
	t.Helper()
	cache, ok := env.Metadata.GetSchemaCacheByID(schema.ID)
	if !ok {
		t.Fatalf("no metadata for schema %s", schema.Name)
	}
	image := map[string]any{"created_at": rec.CreatedAt, "updated_at": rec.UpdatedAt}
	// A live row's deleted_at reads as nil from PostgreSQL and as 0 from
	// parquet (#274).
	if rec.DeletedAt != nil && *rec.DeletedAt != 0 {
		image["deleted_at"] = *rec.DeletedAt
	}
	for name, meta := range cache {
		if meta.ValueType == forma.ValueTypeList {
			if items := listItems(rec, meta.AttributeID); len(items) > 0 {
				image[name] = items
			}
			continue
		}
		v, err := actualAttrValue(rec, meta)
		if err != nil {
			t.Fatalf("read %s of row %s: %v", name, rec.RowID, err)
		}
		if v != nil {
			image[name] = v
		}
	}
	return image
}

// listItems renders a list attribute's elements, one per array index, as
// "<indices>=<value>", sorted.
func listItems(rec *model.PersistentRecord, attrID int16) []string {
	var items []string
	for _, a := range rec.OtherAttributes {
		if a.AttrID != attrID {
			continue
		}
		var v any
		switch {
		case a.ValueText != nil:
			v = *a.ValueText
		case a.ValueNumeric != nil:
			v = *a.ValueNumeric
		}
		items = append(items, fmt.Sprintf("%s=%v", a.ArrayIndices, v))
	}
	sort.Strings(items)
	return items
}

// offsetWalk pages through q, limit rows at a time, until a page comes back
// empty, recording every page, the empty one past the end included (#181).
func (o *flushedOracle) offsetWalk(ctx context.Context, t *testing.T, label string, q Query, limit int) []string {
	t.Helper()
	var walked []string
	for page := 0; page < 1000; page++ {
		q.Limit, q.Offset = limit, page*limit
		res := o.answer(ctx, t, fmt.Sprintf("%s, offset %d", label, q.Offset), q)
		if len(res.Records) == 0 {
			return walked
		}
		walked = append(walked, pageRowIDs(res)...)
	}
	t.Fatalf("%s: the walk did not end", label)
	return nil
}

// keysetWalk pages through q from start, limit rows at a time, recording
// every page, and returns the walked row IDs. next builds the cursor after a
// page's last row.
func (o *flushedOracle) keysetWalk(ctx context.Context, t *testing.T, label string, q Query, start *model.KeysetCursor, limit int,
	next func(*model.PersistentRecord) *model.KeysetCursor) []string {
	t.Helper()
	var walked []string
	cursor := start
	for page := 0; page < 1000; page++ {
		q.Keyset, q.Limit = cursor, limit
		res := o.answer(ctx, t, fmt.Sprintf("%s, page %d", label, page), q)
		if len(res.Records) == 0 {
			return walked
		}
		walked = append(walked, pageRowIDs(res)...)
		cursor = next(res.Records[len(res.Records)-1])
	}
	t.Fatalf("%s: the walk did not end", label)
	return nil
}

// TestPgxHotLegTiersMatchFlushed: rows in every tier (cold base parquet,
// warm delta parquet, hot), hot updates and deletes shadowing parquet rows,
// and a row created and deleted while hot. Each condition-DSL parity case,
// and an offset walk sorted on each attribute in both directions, must
// answer as the same request does once every row is flushed.
func TestPgxHotLegTiersMatchFlushed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]
	rows := seedParityTiers(ctx, t, env, wide)
	gone := CreateEvent(wide, parityAttrsAt(2, nil))
	mustApplyEvents(ctx, t, env, "a row created and deleted while hot", gone)
	// rows[0] is cold and rows[3] warm (seedParityTiers).
	mustApplyEvents(ctx, t, env, "delete a cold, a warm and a hot row",
		DeleteEvent(wide, rows[0].id), DeleteEvent(wide, rows[3].id), DeleteEvent(wide, gone.RowID))

	o := newFlushedOracle(env)
	all := o.offsetWalk(ctx, t, "default order", Query{Schema: wide, PreferredTiers: allTiers}, 2)
	if want := len(rows) - 2; len(all) != want {
		t.Fatalf("the default walk answers %d rows, want the %d live ones", len(all), want)
	}
	forEachParityCase(rows, func(f Filter, _ []uuid.UUID) {
		o.answer(ctx, t, fmt.Sprintf("%s %s:%s", f.Attr, f.Op, f.Value), Query{Schema: wide, Filters: []Filter{f}, Limit: 100, PreferredTiers: allTiers})
	})
	for _, a := range parityAttrs() {
		for _, desc := range []bool{false, true} {
			o.offsetWalk(ctx, t, fmt.Sprintf("sorted on %s desc=%v", a.attr, desc),
				Query{Schema: wide, Sorts: []Sort{{Attr: a.attr, Desc: desc}}, PreferredTiers: allTiers}, 2)
		}
	}
	if o.settle(ctx, t) == 0 {
		t.Fatal("no recorded page holds a hot row, so the hot leg was never compared")
	}
}

// TestPgxHotLegSortKeyEdgesMatchFlushed: the sort-key edges of step 2
// (lockstepEdgeRows) and generated rows, flushed, then the same edges again
// as hot rows and the generated rows updated, so every edge value ties
// across the tiers and hot versions shadow parquet ones. An offset walk
// sorted on each attribute, in both directions, and a keyset walk on each
// attribute but the column-bound uuid, must answer as the same request does
// once every row is flushed.
func TestPgxHotLegSortKeyEdgesMatchFlushed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]
	generated := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 6})
	mustApplyEvents(ctx, t, env, "flushed rows", append(generated, lockstepEdgeRows(wide)...)...)
	mustFlush(ctx, t, env)
	updates := make([]*Event, 0, len(generated)/2)
	for i := 0; i < len(generated); i += 2 {
		updates = append(updates, UpdateEvent(wide, generated[i].RowID, map[string]any{"title": fmt.Sprintf("updated-%02d", i), "count": float64(i)}))
	}
	mustApplyEvents(ctx, t, env, "hot rows", append(updates, lockstepEdgeRows(wide)...)...)

	cache, ok := env.Metadata.GetSchemaCacheByID(wide.ID)
	if !ok {
		t.Fatalf("no metadata cache for schema %d", wide.ID)
	}
	var attrs []string
	for name, meta := range cache {
		if meta.ValueType != forma.ValueTypeList {
			attrs = append(attrs, name)
		}
	}
	sort.Strings(attrs)
	o := newFlushedOracle(env)
	for _, attr := range attrs {
		for _, desc := range []bool{false, true} {
			q := Query{Schema: wide, Sorts: []Sort{{Attr: attr, Desc: desc}}, PreferredTiers: allTiers}
			label := fmt.Sprintf("sorted on %s desc=%v", attr, desc)
			walked := o.offsetWalk(ctx, t, label, q, 2)
			q.Limit = 100
			if whole := o.answer(ctx, t, label+", one page", q); !slices.Equal(pageRowIDs(whole), walked) {
				t.Fatalf("%s: the walk %v does not tile the whole page %v", label, walked, pageRowIDs(whole))
			}
			if meta := cache[attr]; meta.ValueType != forma.ValueTypeUUID || meta.ColumnBinding == nil {
				// A cursor on a column-bound uuid fails: its parquet column
				// is a DuckDB UUID (the export's TRY_CAST(col AS UUID)), and
				// DuckDB refuses to compare it with the cursor's
				// CAST(? AS VARCHAR) boundary (pgx-hot-leg-plan.md step 6,
				// Not fixed here).
				o.attributeKeysetWalk(ctx, t, wide, meta, desc)
			}
		}
	}
	if o.settle(ctx, t) == 0 {
		t.Fatal("no recorded page holds a hot row, so the hot leg was never compared")
	}
}

// attributeKeysetWalk walks the rows holding meta's attribute in its order,
// row_id ASC breaking ties: a first offset page, then keyset pages after each
// page's last row, two rows at a time. The walk must tile the one-page
// answer. A row without the attribute is left out: its NULL key is no
// boundary a cursor can carry.
func (o *flushedOracle) attributeKeysetWalk(ctx context.Context, t *testing.T, schema SchemaRef, meta forma.AttributeMetadata, desc bool) {
	t.Helper()
	dir := forma.SortOrderAsc
	if desc {
		dir = forma.SortOrderDesc
	}
	label := fmt.Sprintf("keyset on %s %s", meta.AttributeName, dir)
	q := Query{Schema: schema, Filters: []Filter{{Attr: meta.AttributeName, Op: "is_not_null", Value: "true"}},
		Sorts: []Sort{{Attr: meta.AttributeName, Desc: desc}}, PreferredTiers: allTiers, Limit: 100}
	want := pageRowIDs(o.answer(ctx, t, label+", one page", q))
	q.Limit = 2
	first := o.answer(ctx, t, label+", first page", q)
	walked := pageRowIDs(first)
	if len(first.Records) > 0 {
		cols := []model.KeysetColumn{{Attribute: meta.AttributeName, Direction: dir}, {Attribute: "row_id", Direction: forma.SortOrderAsc}}
		next := func(rec *model.PersistentRecord) *model.KeysetCursor {
			return &model.KeysetCursor{Columns: cols, Values: []any{keysetBoundary(t, o.env, meta, rec), rec.RowID.String()}, Mode: model.KeysetCursorModeAfter}
		}
		walked = append(walked, o.keysetWalk(ctx, t, label, q, next(first.Records[len(first.Records)-1]), 2, next)...)
	}
	if !slices.Equal(walked, want) {
		t.Fatalf("%s: walked %v, want the one-page order %v", label, walked, want)
	}
}

// keysetBoundary is rec's value of meta's attribute as a cursor boundary of
// the attribute's export type.
func keysetBoundary(t *testing.T, env *Env, meta forma.AttributeMetadata, rec *model.PersistentRecord) any {
	t.Helper()
	v, err := actualAttrValue(rec, meta)
	if err != nil || v == nil {
		t.Fatalf("row %s holds no %s to resume after (%v)", rec.RowID, meta.AttributeName, err)
	}
	typ, err := sortkey.AttributeType(meta)
	if err != nil {
		t.Fatalf("%s: %v", meta.AttributeName, err)
	}
	switch x := v.(type) {
	case int64:
		if typ == sortkey.Double {
			return float64(x)
		}
		return x
	case float64:
		switch typ {
		case sortkey.Boolean:
			return x != 0
		case sortkey.BigInt, sortkey.Integer, sortkey.SmallInt:
			return int64(x) // a date's epoch milliseconds
		}
		return x
	default:
		return x
	}
}

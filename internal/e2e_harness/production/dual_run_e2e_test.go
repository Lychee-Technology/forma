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
	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"
)

// The dual-run test of pgx-hot-leg-plan.md step 6: each request is answered
// under both hot-leg transports, over one Env's state, and the two answers
// must hold the same rows in the same order, with the same values, and
// report the same total. The fixtures put rows in every tier, at every
// sort-key edge of step 2, in runs that tie across the tiers on every key
// before row_id, in each deleted_ts encoding, and in the flush race
// (dual_run_walks_e2e_test.go). A publication after the snapshot is not a
// fixture: the attach transport has no snapshot to hold it to
// (TestPgxHotLegPublicationAfterTheSnapshot checks it instead).

// dualRun holds one engine per transport over the same Env.
type dualRun struct {
	env    *Env
	attach *fedengine.DBFederatedQueryEngine
	pgx    *fedengine.DBFederatedQueryEngine
}

func newDualRun(env *Env) *dualRun {
	return &dualRun{
		env:    env,
		attach: env.EngineWithTransport(forma.HotLegTransportAttach),
		pgx:    env.EngineWithTransport(forma.HotLegTransportPgx),
	}
}

// answer runs q under both transports, requires the same page from both on
// the federated route, and returns it.
func (d *dualRun) answer(ctx context.Context, t *testing.T, label string, q Query) *QueryResult {
	t.Helper()
	attach, err := d.env.QueryOn(ctx, d.attach, q)
	if err != nil {
		t.Fatalf("%s on attach: %v", label, err)
	}
	pgx, err := d.env.QueryOn(ctx, d.pgx, q)
	if err != nil {
		t.Fatalf("%s on pgx: %v", label, err)
	}
	if !attach.Plan.Routing.UseDuckDB || !pgx.Plan.Routing.UseDuckDB {
		t.Fatalf("%s: both transports must answer on the federated route, got attach %+v and pgx %+v", label, attach.Plan.Routing, pgx.Plan.Routing)
	}
	requireSamePage(t, d.env, q.Schema, label, attach, pgx)
	return pgx
}

// requireSamePage compares the attach and pgx answers row by row.
func requireSamePage(t *testing.T, env *Env, schema SchemaRef, label string, attach, pgx *QueryResult) {
	t.Helper()
	if !slices.Equal(pageRowIDs(attach), pageRowIDs(pgx)) {
		t.Fatalf("%s: attach answers %v, pgx %v", label, pageRowIDs(attach), pageRowIDs(pgx))
	}
	if attach.Total != pgx.Total {
		t.Fatalf("%s: attach counts %d matches, pgx %d", label, attach.Total, pgx.Total)
	}
	for i := range attach.Records {
		a, p := recordImage(t, env, schema, attach.Records[i]), recordImage(t, env, schema, pgx.Records[i])
		if !reflect.DeepEqual(a, p) {
			t.Fatalf("%s: row %s reads %v on attach and %v on pgx", label, attach.Records[i].RowID, a, p)
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
	if rec.DeletedAt != nil {
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
// empty, comparing every page, the empty one past the end included (#181).
func (d *dualRun) offsetWalk(ctx context.Context, t *testing.T, label string, q Query, limit int) []string {
	t.Helper()
	var walked []string
	for page := 0; page < 1000; page++ {
		q.Limit, q.Offset = limit, page*limit
		res := d.answer(ctx, t, fmt.Sprintf("%s, offset %d", label, q.Offset), q)
		if len(res.Records) == 0 {
			return walked
		}
		walked = append(walked, pageRowIDs(res)...)
	}
	t.Fatalf("%s: the walk did not end", label)
	return nil
}

// TestDualRunTiers: rows in every tier (cold base parquet, warm delta
// parquet, hot), hot updates and deletes shadowing parquet rows, and a row
// created and deleted while hot. Each condition-DSL parity case, and an
// offset walk sorted on each attribute in both directions, must answer the
// same under both transports.
func TestDualRunTiers(t *testing.T) {
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

	d := newDualRun(env)
	all := d.offsetWalk(ctx, t, "default order", Query{Schema: wide, PreferredTiers: allTiers}, 2)
	if want := len(rows) - 2; len(all) != want {
		t.Fatalf("the default walk answers %d rows, want the %d live ones", len(all), want)
	}
	forEachParityCase(rows, func(f Filter, _ []uuid.UUID) {
		d.answer(ctx, t, fmt.Sprintf("%s %s:%s", f.Attr, f.Op, f.Value), Query{Schema: wide, Filters: []Filter{f}, Limit: 100, PreferredTiers: allTiers})
	})
	for _, a := range parityAttrs() {
		for _, desc := range []bool{false, true} {
			d.offsetWalk(ctx, t, fmt.Sprintf("sorted on %s desc=%v", a.attr, desc),
				Query{Schema: wide, Sorts: []Sort{{Attr: a.attr, Desc: desc}}, PreferredTiers: allTiers}, 2)
		}
	}
}

// TestDualRunSortKeyEdges: the sort-key edges of step 2 (lockstepEdgeRows)
// and generated rows, flushed, then the same edges again as hot rows and the
// generated rows updated, so every edge value ties across the tiers and hot
// versions shadow parquet ones. An offset walk sorted on each attribute, in
// both directions, must answer the same under both transports.
func TestDualRunSortKeyEdges(t *testing.T) {
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
	d := newDualRun(env)
	for _, attr := range attrs {
		for _, desc := range []bool{false, true} {
			q := Query{Schema: wide, Sorts: []Sort{{Attr: attr, Desc: desc}}, PreferredTiers: allTiers}
			label := fmt.Sprintf("sorted on %s desc=%v", attr, desc)
			walked := d.offsetWalk(ctx, t, label, q, 2)
			q.Limit = 100
			if whole := d.answer(ctx, t, label+", one page", q); !slices.Equal(pageRowIDs(whole), walked) {
				t.Fatalf("%s: the walk %v does not tile the whole page %v", label, walked, pageRowIDs(whole))
			}
			if meta := cache[attr]; meta.ValueType != forma.ValueTypeUUID || meta.ColumnBinding == nil {
				// A cursor on a column-bound uuid fails on both transports:
				// its parquet column is a DuckDB UUID (the export's
				// TRY_CAST(col AS UUID)), and DuckDB refuses to compare it
				// with the cursor's CAST(? AS VARCHAR) boundary.
				d.attributeKeysetWalk(ctx, t, wide, meta, desc)
			}
		}
	}
}

// attributeKeysetWalk walks the rows holding meta's attribute in its order,
// row_id ASC breaking ties: a first offset page, then keyset pages after each
// page's last row, two rows at a time. The walk must tile the one-page
// answer. A row without the attribute is left out: its NULL key is no
// boundary a cursor can carry.
func (d *dualRun) attributeKeysetWalk(ctx context.Context, t *testing.T, schema SchemaRef, meta forma.AttributeMetadata, desc bool) {
	t.Helper()
	dir := forma.SortOrderAsc
	if desc {
		dir = forma.SortOrderDesc
	}
	label := fmt.Sprintf("keyset on %s %s", meta.AttributeName, dir)
	q := Query{Schema: schema, Filters: []Filter{{Attr: meta.AttributeName, Op: "is_not_null", Value: "true"}},
		Sorts: []Sort{{Attr: meta.AttributeName, Desc: desc}}, PreferredTiers: allTiers, Limit: 100}
	want := pageRowIDs(d.answer(ctx, t, label+", one page", q))
	q.Limit = 2
	first := d.answer(ctx, t, label+", first page", q)
	walked := pageRowIDs(first)
	if len(first.Records) > 0 {
		cols := []model.KeysetColumn{{Attribute: meta.AttributeName, Direction: dir}, {Attribute: "row_id", Direction: forma.SortOrderAsc}}
		next := func(rec *model.PersistentRecord) *model.KeysetCursor {
			return &model.KeysetCursor{Columns: cols, Values: []any{keysetBoundary(t, d.env, meta, rec), rec.RowID.String()}, Mode: model.KeysetCursorModeAfter}
		}
		walked = append(walked, d.keysetWalk(ctx, t, label, q, next(first.Records[len(first.Records)-1]), 2, next, nil)...)
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

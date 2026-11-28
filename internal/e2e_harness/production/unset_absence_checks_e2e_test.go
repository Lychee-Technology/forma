//go:build e2e

package production

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"

	forma "github.com/lychee-technology/forma"
)

// absenceRoute is one deployment a stage's queries run on: PostgreSQL alone,
// or the federated engine reading every tier through DuckDB.
type absenceRoute struct {
	name    string
	duckdb  bool
	manager func(*Env) forma.EntityManager
}

var absenceRoutes = []absenceRoute{
	{name: "postgres", manager: (*Env).PostgresEntityManager},
	{name: "duckdb", duckdb: true, manager: (*Env).DuckDBRoutedEntityManager},
}

// check runs every observation of one stage against the rows' expected
// state.
func (lc *absenceLifecycle) check(ctx context.Context, t *testing.T, stage string) {
	t.Helper()
	for _, r := range lc.rows {
		lc.checkGet(ctx, t, stage, r)
		lc.checkPreconditions(ctx, t, stage, r)
		lc.checkPublishedImage(ctx, t, stage, r)
	}
	for _, route := range absenceRoutes {
		label := stage + ", " + route.name
		got := lc.query(ctx, t, label, route, nil)
		lc.requireSelected(t, label, nil, got, func(*absenceRow) bool { return true })
		for _, r := range lc.rows {
			if attrs, ok := got[r.id]; ok {
				lc.compareAttrs(t, label+" Query", r, attrs)
			}
		}
		for _, a := range lc.subject.attrs {
			lc.checkSelects(ctx, t, label, route, forma.IsNull(a.name), func(r *absenceRow) bool { return !r.holds(a.name) })
			lc.checkSelects(ctx, t, label, route, forma.IsNotNull(a.name), func(r *absenceRow) bool { return r.holds(a.name) })
			for _, p := range a.probes() {
				lc.checkSelects(ctx, t, label, route, p, func(r *absenceRow) bool { return r.holdsValue(a.name, p) })
			}
		}
	}
}

func (r *absenceRow) holds(attr string) bool {
	_, ok := r.want[attr]
	return ok
}

// holdsValue reports whether the row holds the value an equals probe names.
func (r *absenceRow) holdsValue(attr string, probe *forma.KvCondition) bool {
	v, ok := r.want[attr]
	return ok && "equals:"+fmt.Sprint(v) == probe.Value
}

// probes are the equals predicates on the attribute's old and new values.
func (a absenceAttr) probes() []*forma.KvCondition {
	if !a.probe {
		return nil
	}
	return []*forma.KvCondition{
		{Attr: a.name, Value: fmt.Sprintf("equals:%v", a.old)},
		{Attr: a.name, Value: fmt.Sprintf("equals:%v", a.again)},
	}
}

// checkGet requires Get, which reads PostgreSQL, to return the row's state.
func (lc *absenceLifecycle) checkGet(ctx context.Context, t *testing.T, stage string, r *absenceRow) {
	t.Helper()
	rec, err := lc.env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: lc.schema.Name, RowID: &r.id})
	if err != nil {
		t.Fatalf("%s: Get the %s row: %v", stage, r.label, err)
	}
	lc.compareAttrs(t, stage+" Get", r, jsonNormalizedAttrs(t, rec.Attributes))
}

// compareAttrs requires got to hold exactly the row's expected value of each
// attribute under test, and no key for an absent one.
func (lc *absenceLifecycle) compareAttrs(t *testing.T, label string, r *absenceRow, got map[string]any) {
	t.Helper()
	for _, a := range lc.subject.attrs {
		want, present := r.want[a.name]
		have, has := got[a.name]
		switch {
		case !present && has:
			t.Errorf("%s: the %s row holds %s = %#v, want it absent", label, r.label, a.name, have)
		case present && !has:
			t.Errorf("%s: the %s row has no %s, want %#v", label, r.label, a.name, want)
		case present && !reflect.DeepEqual(have, want):
			t.Errorf("%s: the %s row holds %s = %#v, want %#v", label, r.label, a.name, have, want)
		}
	}
}

// query answers cond on route and returns each row's JSON-normalized
// attributes. The DuckDB route must have read through DuckDB, and the
// PostgreSQL route must not have entered the engine. A row returned twice
// fails the check, so a stale copy cannot hide behind the current one.
func (lc *absenceLifecycle) query(ctx context.Context, t *testing.T, label string, route absenceRoute, cond forma.Condition) map[uuid.UUID]map[string]any {
	t.Helper()
	res, err := route.manager(lc.env).Query(ctx, &forma.QueryRequest{
		SchemaName: lc.schema.Name, Page: 1, ItemsPerPage: 100, Condition: cond, IncludeExecutionPlan: true,
	})
	if err != nil {
		t.Fatalf("%s: Query %s: %v", label, describeCondition(cond), err)
	}
	switch {
	case route.duckdb && (res.ExecutionPlan == nil || !res.ExecutionPlan.Routing.UsedDuckDB):
		t.Fatalf("%s: Query %s did not read through DuckDB: %+v", label, describeCondition(cond), res.ExecutionPlan)
	case !route.duckdb && res.ExecutionPlan != nil:
		t.Fatalf("%s: Query %s entered the federated engine: %+v", label, describeCondition(cond), res.ExecutionPlan.Routing)
	}
	out := make(map[uuid.UUID]map[string]any, len(res.Data))
	for _, rec := range res.Data {
		if _, dup := out[rec.RowID]; dup {
			t.Errorf("%s: Query %s returned row %s more than once", label, describeCondition(cond), rec.RowID)
			continue
		}
		out[rec.RowID] = jsonNormalizedAttrs(t, rec.Attributes)
	}
	return out
}

// checkSelects requires cond to select exactly the rows selected reports.
func (lc *absenceLifecycle) checkSelects(ctx context.Context, t *testing.T, label string, route absenceRoute, cond *forma.KvCondition, selected func(*absenceRow) bool) {
	t.Helper()
	lc.requireSelected(t, label, cond, lc.query(ctx, t, label, route, cond), selected)
}

// requireSelected requires the row ids a query of cond returned to be
// exactly those of the rows selected reports: each of them, and no other.
func (lc *absenceLifecycle) requireSelected(t *testing.T, label string, cond forma.Condition, got map[uuid.UUID]map[string]any, selected func(*absenceRow) bool) {
	t.Helper()
	for _, r := range lc.rows {
		if _, ok := got[r.id]; ok != selected(r) {
			t.Errorf("%s: Query %s selects the %s row = %v, want %v", label, describeCondition(cond), r.label, ok, !ok)
		}
	}
	for id := range got {
		if !lc.hasRow(id) {
			t.Errorf("%s: Query %s returned row %s, which the lifecycle did not write", label, describeCondition(cond), id)
		}
	}
}

func (lc *absenceLifecycle) hasRow(id uuid.UUID) bool {
	for _, r := range lc.rows {
		if r.id == id {
			return true
		}
	}
	return false
}

func describeCondition(cond forma.Condition) string {
	kv, ok := cond.(*forma.KvCondition)
	if !ok || kv == nil {
		return "without a condition"
	}
	return kv.Attr + " " + kv.Value
}

// checkPreconditions requires a precondition on each attribute to observe
// the row's state through the row's writer: the one the state satisfies
// holds (AssertRow, which writes nothing), and the one it contradicts
// refuses an update with ErrPreconditionFailed. An equals precondition on a
// value the row does not hold refuses too.
func (lc *absenceLifecycle) checkPreconditions(ctx context.Context, t *testing.T, stage string, r *absenceRow) {
	t.Helper()
	for _, a := range lc.subject.attrs {
		holds, refused := forma.IsNotNull(a.name), forma.IsNull(a.name)
		if !r.holds(a.name) {
			holds, refused = refused, holds
		}
		conds := []*forma.KvCondition{refused}
		for _, p := range a.probes() {
			if r.holdsValue(a.name, p) {
				lc.requireAssertRow(ctx, t, stage, r, p)
				continue
			}
			conds = append(conds, p)
		}
		lc.requireAssertRow(ctx, t, stage, r, holds)
		for _, c := range conds {
			op := lc.updateOp(r)
			op.Updates["qty"] = float64(-1)
			op.Preconditions = c
			if err := lc.update(ctx, r, op); !errors.Is(err, forma.ErrPreconditionFailed) {
				t.Errorf("%s: update of the %s row under precondition %s = %v, want ErrPreconditionFailed",
					stage, r.label, describeCondition(c), err)
			}
		}
	}
}

func (lc *absenceLifecycle) requireAssertRow(ctx context.Context, t *testing.T, stage string, r *absenceRow, cond *forma.KvCondition) {
	t.Helper()
	err := lc.txr.InTx(ctx, func(ctx context.Context, tx forma.Tx) error {
		return tx.AssertRow(ctx, lc.schema.Name, r.id, cond)
	})
	if err != nil {
		t.Errorf("%s: AssertRow of the %s row under %s = %v, want it to hold", stage, r.label, describeCondition(cond), err)
	}
}

// checkPublishedImage requires the newest image of the row that the
// published generation lists, ranked as the compaction merge ranks versions,
// to hold what the row held at the last flush: NULL for an absent
// attribute, an empty LIST for [].
func (lc *absenceLifecycle) checkPublishedImage(ctx context.Context, t *testing.T, stage string, r *absenceRow) {
	t.Helper()
	if r.image == nil {
		return
	}
	m := loadSchemaManifest(ctx, t, lc.env, lc.schema)
	uris := make([]string, 0, len(m.Files))
	for _, f := range m.Files {
		uris = append(uris, "'"+lc.env.ParquetURI(f.Path)+"'")
	}
	for _, a := range lc.subject.attrs {
		query := fmt.Sprintf(`SELECT CAST(%q AS VARCHAR) FROM read_parquet([%s], union_by_name=true)
			WHERE row_id = '%s'::UUID ORDER BY changed_at DESC, deleted_at DESC NULLS LAST LIMIT 1`,
			a.name, strings.Join(uris, ", "), r.id)
		var got sql.NullString
		if err := lc.env.Duck.DB.QueryRowContext(ctx, query).Scan(&got); err != nil {
			t.Fatalf("%s: read the newest published image of the %s row (generation %d): %v", stage, r.label, m.Generation, err)
		}
		lc.compareImage(t, stage+", newest published image", r, a.name, got)
	}
}

// parquetImage renders v as DuckDB casts the exported column to VARCHAR.
func parquetImage(v any) string {
	list, ok := v.([]any)
	if !ok {
		return fmt.Sprint(v)
	}
	elems := make([]string, len(list))
	for i, e := range list {
		elems[i] = fmt.Sprint(e)
	}
	return "[" + strings.Join(elems, ", ") + "]"
}

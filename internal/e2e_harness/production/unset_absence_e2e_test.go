//go:build e2e

package production

import (
	"context"
	"testing"

	"github.com/google/uuid"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/factory"
	"github.com/lychee-technology/forma/internal/compaction"
)

// TestUnsetAbsenceSurvivesFlushAndCompaction is #941's end-to-end evidence
// for the invariant of #684's update-semantics decision: a newer entity image
// with an absent attribute must not inherit that attribute from an older
// image. Each subject runs the lifecycle on the production stack: writes
// through Update and Tx.Update, the CDC flush and its Parquet export, Protocol
// V2 publication, both compaction merges (promotion of the delta tier beside
// an older base, and a rewrite of every base and delta object into one) and
// the DuckDB federated read.
//
//  1. Rows are created with a value, flushed and promoted into base parquet.
//  2. Unset clears the value; PostgreSQL shows it absent at once, and the
//     federated read does too while the published image still holds the old
//     value, which proves the dirty set hides it.
//  3. The flush exports an image that represents absence (a NULL column), and
//     a promotion lists it beside the older base that still holds the value.
//  4. An update of another attribute writes a newer image, flushed and then
//     rewritten together with the old base into one row per row_id.
//  5. Setting the attribute again propagates through flush and compaction.
//
// Every stage checks Get, Query on the PostgreSQL and DuckDB routes, is_null
// and is_not_null, a predicate on the old value, preconditions on the
// attribute, and the newest image the published generation lists. The list
// subject keeps a row whose list was set to [] beside the cleared rows: it is
// present at every stage, an empty LIST in parquet, never NULL.
func TestUnsetAbsenceSurvivesFlushAndCompaction(t *testing.T) {
	t.Parallel()
	for _, s := range absenceSubjects() {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			runAbsenceLifecycle(t, s)
		})
	}
}

// absenceAttr is one e2e_wide attribute under test: the value its rows are
// created with and the one they are given again after the clear. probe says
// the attribute takes an equals predicate on its value.
type absenceAttr struct {
	name       string
	old, again any
	probe      bool
}

func (a absenceAttr) isList() bool {
	_, ok := a.old.([]any)
	return ok
}

// absenceSubject is one lifecycle: the attributes its rows clear together,
// and whether it keeps a row whose lists are set to [] instead.
type absenceSubject struct {
	name      string
	attrs     []absenceAttr
	emptyList bool
}

var (
	absenceBound = absenceAttr{name: "title", old: "old-title", again: "new-title", probe: true} // text_01
	absenceEAV   = absenceAttr{name: "note", old: "old-note", again: "new-note", probe: true}    // eav_data
	absenceList  = absenceAttr{name: "tags", old: []any{"alpha", "beta"}, again: []any{"gamma"}}
)

func absenceSubjects() []absenceSubject {
	return []absenceSubject{
		{name: "bound scalar", attrs: []absenceAttr{absenceBound}},
		{name: "eav scalar", attrs: []absenceAttr{absenceEAV}},
		{name: "list", attrs: []absenceAttr{absenceList}, emptyList: true},
	}
}

func runAbsenceLifecycle(t *testing.T, s absenceSubject) {
	ctx := context.Background()
	lc := newAbsenceLifecycle(ctx, t, NewEnv(t, SharedCluster(t)), s)

	lc.flush(ctx, t)
	lc.compact(ctx, t, absencePromote, compaction.PromotionApplied)
	lc.check(ctx, t, "created, promoted to base")

	lc.clear(ctx, t)
	lc.requireDirty(ctx, t, "cleared")
	lc.check(ctx, t, "cleared, hot")

	lc.flush(ctx, t)
	lc.check(ctx, t, "cleared, flushed to delta")
	lc.compact(ctx, t, absencePromote, compaction.PromotionApplied)
	lc.check(ctx, t, "cleared, promoted beside the old base")

	lc.touch(ctx, t, 2)
	lc.check(ctx, t, "touched, hot")
	lc.flush(ctx, t)
	lc.check(ctx, t, "touched, flushed to delta")
	lc.compact(ctx, t, CompactionOverrides{}, compaction.RewriteApplied)
	lc.check(ctx, t, "touched, rewritten with the old base")

	lc.setAgain(ctx, t)
	lc.check(ctx, t, "set again, hot")
	lc.flush(ctx, t)
	lc.check(ctx, t, "set again, flushed to delta")
	lc.compact(ctx, t, CompactionOverrides{}, compaction.RewriteApplied)
	lc.check(ctx, t, "set again, rewritten")
}

// absencePromote lowers the promotion threshold so the KB-scale delta tier
// is merged into a new base object, leaving the older base listed.
var absencePromote = CompactionOverrides{TargetBaseSizeBytes: 1}

// absenceRow is one entity of a lifecycle. want is what each attribute under
// test holds now (a missing key is absent) and image what the generation
// published by the last flush holds for it (nil before the first flush).
type absenceRow struct {
	label string
	id    uuid.UUID
	inTx  bool // writes go through Tx.Update instead of Update
	empty bool // the clear sets the lists to [] instead
	want  map[string]any
	image map[string]any
}

type absenceLifecycle struct {
	env     *Env
	schema  SchemaRef
	subject absenceSubject
	txr     forma.Transactor
	rows    []*absenceRow
}

// newAbsenceLifecycle creates the subject's rows with every attribute set:
// one written through Update, one through Tx.Update, and the [] row.
func newAbsenceLifecycle(ctx context.Context, t *testing.T, env *Env, s absenceSubject) *absenceLifecycle {
	t.Helper()
	txr, err := factory.NewTransactor(env.EntityManager())
	if err != nil {
		t.Fatalf("build the transactor: %v", err)
	}
	lc := &absenceLifecycle{env: env, schema: DefaultSchemaFixtures()[1], subject: s, txr: txr}
	lc.rows = []*absenceRow{{label: "update"}, {label: "tx.update", inTx: true}}
	if s.emptyList {
		lc.rows = append(lc.rows, &absenceRow{label: "emptied", empty: true})
	}
	for _, r := range lc.rows {
		data := map[string]any{"qty": float64(1)}
		r.want = map[string]any{}
		for _, a := range s.attrs {
			data[a.name], r.want[a.name] = a.old, a.old
		}
		rec, err := env.EntityManager().Create(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: lc.schema.Name},
			Type:             forma.OperationCreate,
			Data:             data,
		})
		if err != nil {
			t.Fatalf("create the %s row: %v", r.label, err)
		}
		r.id = rec.RowID
	}
	return lc
}

// clear makes every attribute under test absent with Unset, except on the
// [] row, whose lists are set to the empty list (its scalars are unset).
func (lc *absenceLifecycle) clear(ctx context.Context, t *testing.T) {
	t.Helper()
	for _, r := range lc.rows {
		op := lc.updateOp(r)
		for _, a := range lc.subject.attrs {
			if r.empty && a.isList() {
				op.Updates[a.name], r.want[a.name] = []any{}, []any{}
				continue
			}
			op.UpdateOps = append(op.UpdateOps, forma.Unset(a.name))
			delete(r.want, a.name)
		}
		if err := lc.update(ctx, r, op); err != nil {
			t.Fatalf("clear the %s row: %v", r.label, err)
		}
	}
}

// touch writes a newer image of every row through an attribute that is not
// under test, so a later merge has an image newer than the clear's to keep.
func (lc *absenceLifecycle) touch(ctx context.Context, t *testing.T, qty float64) {
	t.Helper()
	for _, r := range lc.rows {
		op := lc.updateOp(r)
		op.Updates["qty"] = qty
		if err := lc.update(ctx, r, op); err != nil {
			t.Fatalf("touch the %s row: %v", r.label, err)
		}
	}
}

// setAgain gives the cleared rows their attributes back with new values; the
// [] row keeps its state as the control.
func (lc *absenceLifecycle) setAgain(ctx context.Context, t *testing.T) {
	t.Helper()
	for _, r := range lc.rows {
		if r.empty {
			continue
		}
		op := lc.updateOp(r)
		for _, a := range lc.subject.attrs {
			op.Updates[a.name], r.want[a.name] = a.again, a.again
		}
		if err := lc.update(ctx, r, op); err != nil {
			t.Fatalf("set the %s row again: %v", r.label, err)
		}
	}
}

func (lc *absenceLifecycle) updateOp(r *absenceRow) *forma.EntityOperation {
	return &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: lc.schema.Name, RowID: r.id},
		Type:             forma.OperationUpdate,
		Updates:          map[string]any{},
	}
}

// update writes op through the row's writer: Update, or Tx.Update in a
// transaction of its own.
func (lc *absenceLifecycle) update(ctx context.Context, r *absenceRow, op *forma.EntityOperation) error {
	if !r.inTx {
		_, err := lc.env.EntityManager().Update(ctx, op)
		return err
	}
	return lc.txr.InTx(ctx, func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, op)
		return err
	})
}

// flush runs the production CDC flush, requires it to drain the dirty set,
// and records each row's state as the published image.
func (lc *absenceLifecycle) flush(ctx context.Context, t *testing.T) {
	t.Helper()
	mustFlush(ctx, t, lc.env)
	lc.published()
}

// published records each row's current state as its published image.
func (lc *absenceLifecycle) published() {
	for _, r := range lc.rows {
		r.image = make(map[string]any, len(r.want))
		for k, v := range r.want {
			r.image[k] = v
		}
	}
}

// compact runs one compaction pass and requires the outcome named, so no
// stage that claims a merge passes over a pass that merged nothing.
func (lc *absenceLifecycle) compact(ctx context.Context, t *testing.T, ov CompactionOverrides, want compaction.CompactionOutcome) {
	t.Helper()
	res, err := lc.env.RunCompactionWith(ctx, lc.schema, ov)
	if err != nil {
		t.Fatalf("compaction: %v", err)
	}
	if res.Outcome != want || res.NewBaseKey == "" {
		t.Fatalf("compaction outcome = %s (dirty ratio %.2f, new base %q), want %s with a new base",
			res.Outcome, res.DirtyRatio, res.NewBaseKey, want)
	}
}

// requireDirty requires every row to hold an unflushed change_log entry, so
// a stage that reads it hot reads it through the dirty set.
func (lc *absenceLifecycle) requireDirty(ctx context.Context, t *testing.T, stage string) {
	t.Helper()
	var dirty int
	if err := lc.env.Pool.QueryRow(ctx,
		`SELECT COUNT(DISTINCT row_id) FROM change_log WHERE schema_id = $1 AND flushed_at = 0`,
		lc.schema.ID).Scan(&dirty); err != nil {
		t.Fatalf("%s: count dirty rows: %v", stage, err)
	}
	if dirty != len(lc.rows) {
		t.Fatalf("%s: %d dirty rows, want all %d", stage, dirty, len(lc.rows))
	}
}

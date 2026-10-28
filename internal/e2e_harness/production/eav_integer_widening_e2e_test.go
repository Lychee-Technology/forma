//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

// eavWideningAttrs declares e2e_simple with two EAV-only integers, qty and
// ratio, at the given types.
func eavWideningAttrs(qty, ratio string) string {
	return fmt.Sprintf(`{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "qty": { "attributeID": 3, "valueType": "%s" },
  "ratio": { "attributeID": 4, "valueType": "%s" }
}
`, qty, ratio)
}

const eavWideningProps = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "qty": { "type": "number" },
    "ratio": { "type": "number" }
  }`

// eavWideningCase is one widening the rollout test drives through the old
// process: a value only attr's new type holds, stored on row, and the old
// type's refusal.
type eavWideningCase struct {
	row      *Event
	attr     string
	attrID   int16
	value    float64
	image    string
	declared forma.ValueType
}

// TestApplyAllowsEAVOnlyWideningAndAnOldProcessFailsClosed pins the D2 lift
// of #729 on PostgreSQL + DuckDB: smallint → integer and integer → numeric
// on EAV-only attributes apply, and the new declaration reads what a new
// writer stores on both routes. A process that started on the old
// declaration, modelled by an EntityManager built before the apply, refuses
// a row holding a value only the new type allows, on its OLTP and federated
// routes; an unrelated update through it fails and leaves the stored value,
// the row version and change_log as they were. Before #729 that process read
// 40000 as -25536 and 1001.5 as 1001 and wrote them back, which is why the
// widenings were refused. Each widening gets its own row and is repaired
// before the next, so each refusal is the only one the old process can meet.
func TestApplyAllowsEAVOnlyWideningAndAnOldProcessFailsClosed(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, eavWideningProps, eavWideningAttrs("smallint", "integer"))))
	simple := DefaultSchemaFixtures()[0]
	row := func(i int) *Event {
		return CreateEvent(simple, map[string]any{"name": fmt.Sprintf("row-%04d", i), "value": float64(i),
			"qty": float64(i), "ratio": float64(i * 10)})
	}
	qtyRow, ratioRow, narrow := row(1), row(2), row(3)
	mustApplyEvents(ctx, t, env, "v1 rows", qtyRow, ratioRow, narrow)
	mustFlush(ctx, t, env)
	old := env.EntityManager()

	if err := env.EvolveSchema(ctx, writeSimpleSchemaDir(t, eavWideningProps, eavWideningAttrs("integer", "numeric"))); err != nil {
		t.Fatalf("EAV-only smallint to integer and integer to numeric must apply: %v", err)
	}
	if n := activeGeneration(ctx, t, env, simple); n != 2 {
		t.Fatalf("active generation = %d, want 2", n)
	}
	for _, c := range []eavWideningCase{
		{qtyRow, "qty", 3, 40000, "40000", forma.ValueTypeSmallInt},
		{ratioRow, "ratio", 4, 1001.5, "1001.5", forma.ValueTypeInteger},
	} {
		rowID := c.row.RowID
		mustApplyEvents(ctx, t, env, c.attr+" "+c.image+", which only the new type holds",
			UpdateEvent(simple, rowID, map[string]any{c.attr: c.value}))
		for _, q := range []Query{{Schema: simple, Limit: 10}, {Schema: simple, Limit: 10, PreferHot: true}} {
			env.AssertQueryMatches(ctx, q)
		}
		if c.attr == "qty" {
			if oltp, federated := readQtyBothRoutes(ctx, t, env, simple, rowID); fmt.Sprint(oltp) != "40000" || fmt.Sprint(federated) != "40000" {
				t.Fatalf("the new declaration reads qty %v (OLTP) and %v (federated), want 40000", oltp, federated)
			}
		}
		assertOldProcessFailsClosed(ctx, t, env, old, simple, c)
		// Repair through the new declaration, so the next case's refusal is
		// the only one the old process's schema-wide federated read can meet.
		mustApplyEvents(ctx, t, env, "repair "+c.attr, UpdateEvent(simple, rowID, map[string]any{c.attr: float64(1)}))
	}

	// The refusal is row-scoped: a row the old types still hold reads.
	got, err := old.Get(ctx, &forma.QueryRequest{SchemaName: simple.Name, RowID: &narrow.RowID})
	if err != nil || fmt.Sprint(got.Attributes["qty"]) != "3" {
		t.Fatalf("the old process reads the unchanged row as %v, %v; want qty 3", got, err)
	}
}

// assertOldProcessFailsClosed: old, on the old declaration, refuses c's row
// on both routes with c's own refusal, and an unrelated update through it fails
// and leaves c's stored value, the row version and change_log as they were.
func assertOldProcessFailsClosed(ctx context.Context, t *testing.T, env *Env, old forma.EntityManager, schema SchemaRef, c eavWideningCase) {
	t.Helper()
	rowID := c.row.RowID
	_, err := old.Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
	want := fmt.Sprintf("stored %s image %s in value_numeric", c.declared, c.image)
	if err == nil || errors.Is(err, forma.ErrInvalidInput) || !strings.Contains(err.Error(), rowID.String()) ||
		!strings.Contains(err.Error(), want) {
		t.Fatalf("the old process's OLTP read of %s = %v, want the consistency error naming the row and %q", c.attr, err, want)
	}
	_, err = old.Query(ctx, &forma.QueryRequest{
		SchemaName: schema.Name, Page: 1, ItemsPerPage: 100,
		Federated: &forma.FederatedQueryRequest{Enabled: true, PreferredTiers: []string{"hot", "warm", "cold"}},
	})
	want = "does not fit declared type " + string(c.declared)
	if err == nil || !strings.Contains(err.Error(), rowID.String()) || !strings.Contains(err.Error(), want) {
		t.Fatalf("the old process's federated read of %s = %v, want the refusal naming the row and %q", c.attr, err, want)
	}

	before := readEAVIntegerState(ctx, t, env, schema.ID, rowID, c.attrID, "")
	_, err = old.Update(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: schema.Name, RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          map[string]any{"name": "unrelated"},
	})
	if err == nil || errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("an unrelated update through the old process over %s = %v, want the read refusal", c.attr, err)
	}
	if after := readEAVIntegerState(ctx, t, env, schema.ID, rowID, c.attrID, ""); after != before || after.value != c.image {
		t.Fatalf("the failed update changed the stored %s state from %+v to %+v", c.attr, before, after)
	}
}

//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"

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

// TestApplyAllowsEAVOnlyWideningAndAnOldProcessFailsClosed pins the D2 lift
// of #729 on PostgreSQL + DuckDB: smallint → integer and integer → numeric
// on EAV-only attributes apply, and the new declaration reads what a new
// writer stores on both routes. A process that started on the old
// declaration, modelled by an EntityManager built before the apply, refuses
// a row holding a value only the new type allows, on its OLTP and federated
// routes; an unrelated update through it fails and leaves the stored value,
// the row version and change_log as they were. Before #729 that process read
// 40000 as -25536 and 1001.5 as 1001 and wrote them back, which is why the
// widenings were refused.
func TestApplyAllowsEAVOnlyWideningAndAnOldProcessFailsClosed(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, eavWideningProps, eavWideningAttrs("smallint", "integer"))))
	simple := DefaultSchemaFixtures()[0]
	row := func(i int) *Event {
		return CreateEvent(simple, map[string]any{"name": fmt.Sprintf("row-%04d", i), "value": float64(i),
			"qty": float64(i), "ratio": float64(i * 10)})
	}
	wide, narrow := row(1), row(2)
	mustApplyEvents(ctx, t, env, "v1 rows", wide, narrow)
	mustFlush(ctx, t, env)
	old := env.EntityManager()

	if err := env.EvolveSchema(ctx, writeSimpleSchemaDir(t, eavWideningProps, eavWideningAttrs("integer", "numeric"))); err != nil {
		t.Fatalf("EAV-only smallint to integer and integer to numeric must apply: %v", err)
	}
	if n := activeGeneration(ctx, t, env, simple); n != 2 {
		t.Fatalf("active generation = %d, want 2", n)
	}
	mustApplyEvents(ctx, t, env, "values only the new types hold",
		UpdateEvent(simple, wide.RowID, map[string]any{"qty": float64(40000), "ratio": 1001.5}))
	for _, q := range []Query{{Schema: simple, Limit: 10}, {Schema: simple, Limit: 10, PreferHot: true}} {
		env.AssertQueryMatches(ctx, q)
	}
	if oltp, federated := readQtyBothRoutes(ctx, t, env, simple, wide.RowID); fmt.Sprint(oltp) != "40000" || fmt.Sprint(federated) != "40000" {
		t.Fatalf("the new declaration reads qty %v (OLTP) and %v (federated), want 40000", oltp, federated)
	}

	assertOldProcessRefusesRow(ctx, t, old, simple, wide.RowID)
	before := readEAVIntegerState(ctx, t, env, simple.ID, wide.RowID, 3, "")
	_, err := old.Update(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: simple.Name, RowID: wide.RowID},
		Type:             forma.OperationUpdate,
		Updates:          map[string]any{"name": "unrelated"},
	})
	if err == nil || errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("an unrelated update through the old process = %v, want the read refusal", err)
	}
	if after := readEAVIntegerState(ctx, t, env, simple.ID, wide.RowID, 3, ""); after != before || after.value != "40000" {
		t.Fatalf("the failed update changed the stored state from %+v to %+v", before, after)
	}

	// The refusal is row-scoped: a row the old types still hold reads.
	got, err := old.Get(ctx, &forma.QueryRequest{SchemaName: simple.Name, RowID: &narrow.RowID})
	if err != nil || fmt.Sprint(got.Attributes["qty"]) != "2" {
		t.Fatalf("the old process reads the unchanged row as %v, %v; want qty 2", got, err)
	}
}

// assertOldProcessRefusesRow: old, on smallint qty and integer ratio, refuses
// rowID on both routes with the consistency error naming the row and image.
func assertOldProcessRefusesRow(ctx context.Context, t *testing.T, old forma.EntityManager, schema SchemaRef, rowID uuid.UUID) {
	t.Helper()
	_, err := old.Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
	if err == nil || errors.Is(err, forma.ErrInvalidInput) || !strings.Contains(err.Error(), rowID.String()) ||
		!anyRefusal(err, "stored smallint image 40000 in value_numeric", "stored integer image 1001.5 in value_numeric") {
		t.Fatalf("the old process's OLTP read = %v, want the consistency error naming the row and image", err)
	}
	_, err = old.Query(ctx, &forma.QueryRequest{
		SchemaName: schema.Name, Page: 1, ItemsPerPage: 100,
		Federated: &forma.FederatedQueryRequest{Enabled: true, PreferredTiers: []string{"hot", "warm", "cold"}},
	})
	if err == nil || !strings.Contains(err.Error(), rowID.String()) ||
		!anyRefusal(err, "does not fit declared type smallint", "does not fit declared type integer") {
		t.Fatalf("the old process's federated read = %v, want the refusal", err)
	}
}

func anyRefusal(err error, wants ...string) bool {
	for _, want := range wants {
		if strings.Contains(err.Error(), want) {
			return true
		}
	}
	return false
}

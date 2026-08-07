//go:build e2e

package production

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	forma "github.com/lychee-technology/forma"
)

// Schema pair for #294: v1 carries an EAV attribute old_col (attrID 3, no
// column binding); v2 removes it. Re-adding v1 restores attrID 3 — attribute
// IDs are pinned explicitly so the re-add assertion is meaningful.
const remV1Props = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "old_col": { "type": "string" }
  }`

const remV1Attrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "old_col": { "attributeID": 3, "valueType": "text" }
}
`

const remV2Props = `{
    "name": { "type": "string" },
    "value": { "type": "number" }
  }`

const remV2Attrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" }
}
`

// remV2RetiredAttrs is the #342 blessed form of the same removal: instead of
// deleting old_col's entry, the generation keeps it as the attributeID ledger
// and marks it retired. Behavior must be identical to remV2Attrs
// (hand-deleted): invisible on read, row still updatable, value preserved.
const remV2RetiredAttrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "old_col": { "attributeID": 3, "valueType": "text", "retired": true }
}
`

// jsonNormalizedAttrs round-trips an attribute map through JSON so
// assertions compare plain strings/float64s rather than the transformer's
// internal pointer types.
func jsonNormalizedAttrs(t *testing.T, attrs map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(attrs)
	if err != nil {
		t.Fatalf("marshal attributes: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal attributes: %v", err)
	}
	return out
}

// getNormalizedAttrs reads one row through the Env's CURRENT EntityManager and
// returns its JSON-normalized attributes. The manager is resolved per call, not
// captured: EvolveSchema drops the memoized manager so a later generation binds
// fresh metadata — a captured manager would still answer under the old schema
// and make the re-add assertion vacuous.
func getNormalizedAttrs(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, rowID uuid.UUID, step string) map[string]any {
	t.Helper()
	record, err := env.EntityManager().Get(ctx, &forma.QueryRequest{
		SchemaName: schema.Name,
		RowID:      &rowID,
	})
	if err != nil {
		t.Fatalf("get row %s (%s): %v", rowID, step, err)
	}
	return jsonNormalizedAttrs(t, record.Attributes)
}

// TestUpdateAfterAttributeRemoval pins #294 tolerate-and-preserve: a row
// still carrying a dropped attribute's EAV data stays updatable, the stale
// EAV row is preserved untouched (never deleted), it is invisible while the
// schema does not address it, and it reappears when the attribute is
// re-added — the assertion that distinguishes preserve from drop.
func TestUpdateAfterAttributeRemoval(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	v1 := writeSimpleSchemaDir(t, remV1Props, remV1Attrs)
	v2 := writeSimpleSchemaDir(t, remV2Props, remV2Attrs)
	env := NewEnv(t, cluster, WithSchemaDir(v1))
	schema := DefaultSchemaFixtures()[0]

	create := CreateEvent(schema, map[string]any{
		"name":    "r1",
		"value":   float64(1),
		"old_col": "keep-me",
	})
	if err := env.ApplyEvents(ctx, create); err != nil {
		t.Fatalf("seed v1 row: %v", err)
	}

	if err := env.EvolveSchema(ctx, v2); err != nil {
		t.Fatalf("evolve schema to v2: %v", err)
	}

	// RED before the fix: this fails with
	// `unknown attribute id 3 for schema N (attribute not in metadata cache)`.
	if err := env.ApplyEvents(ctx, UpdateEvent(schema, create.RowID, map[string]any{
		"value": float64(2),
	})); err != nil {
		t.Fatalf("update row carrying dropped-attr EAV data under v2: %v", err)
	}

	// Preserve, not drop: the stale EAV row must still be physically present
	// after the update's replace cycle.
	var preserved string
	if err := env.Pool.QueryRow(ctx,
		`SELECT value_text FROM eav_data WHERE schema_id = $1 AND row_id = $2 AND attr_id = 3`,
		schema.ID, create.RowID).Scan(&preserved); err != nil {
		t.Fatalf("stale old_col EAV row missing after update (tolerate-and-preserve violated): %v", err)
	}
	if preserved != "keep-me" {
		t.Fatalf("stale old_col EAV value = %q, want %q", preserved, "keep-me")
	}

	// Under v2 the preserved value stays invisible on the read path.
	attrs := getNormalizedAttrs(ctx, t, env, schema, create.RowID, "under v2")
	if _, ok := attrs["old_col"]; ok {
		t.Fatalf("old_col visible under v2 schema: %v", attrs)
	}
	if attrs["value"] != float64(2) {
		t.Fatalf("value = %v, want 2", attrs["value"])
	}

	// Re-add old_col (evolve back to the v1 shape, same attributeID 3): the
	// preserved value must reappear.
	if err := env.EvolveSchema(ctx, v1); err != nil {
		t.Fatalf("re-add old_col (evolve back to v1 shape): %v", err)
	}
	attrs2 := getNormalizedAttrs(ctx, t, env, schema, create.RowID, "after re-add")
	if attrs2["old_col"] != "keep-me" {
		t.Fatalf("old_col after re-add = %v, want \"keep-me\" (preserve vs drop discriminator)", attrs2["old_col"])
	}
	if attrs2["value"] != float64(2) {
		t.Fatalf("value after re-add = %v, want 2", attrs2["value"])
	}

	// The row stays a normal citizen: another update under the restored
	// schema round-trips old_col like any live attribute.
	if err := env.ApplyEvents(ctx, UpdateEvent(schema, create.RowID, map[string]any{
		"value": float64(3),
	})); err != nil {
		t.Fatalf("update after re-add: %v", err)
	}
	attrs3 := getNormalizedAttrs(ctx, t, env, schema, create.RowID, "after second update")
	if attrs3["old_col"] != "keep-me" || attrs3["value"] != float64(3) {
		t.Fatalf("after second update got old_col=%v value=%v, want keep-me / 3", attrs3["old_col"], attrs3["value"])
	}
}

// TestRetiredMarkerRemovalWorkflow pins the #342 blessed removal workflow end
// to end, step for step against TestUpdateAfterAttributeRemoval above: a
// generation that RETIRES old_col rather than deleting its entry must behave
// identically to the hand-deleted generation — the preserved EAV row survives
// the update's replace cycle, stays invisible while retired, and reappears
// when the attribute is un-retired (re-added with the same id/name/type).
//
// The invisibility assertion is the load-bearing one: retired entries are
// stripped from the active caches (schemameta.activeAttributeCache), so a
// regression that let a retired entry reach consumers would surface old_col in
// the read path and fail here.
func TestRetiredMarkerRemovalWorkflow(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	v1 := writeSimpleSchemaDir(t, remV1Props, remV1Attrs)
	v2Retired := writeSimpleSchemaDir(t, remV2Props, remV2RetiredAttrs)
	env := NewEnv(t, cluster, WithSchemaDir(v1))
	schema := DefaultSchemaFixtures()[0]

	create := CreateEvent(schema, map[string]any{
		"name":    "r1",
		"value":   float64(1),
		"old_col": "keep-me",
	})
	if err := env.ApplyEvents(ctx, create); err != nil {
		t.Fatalf("seed v1 row: %v", err)
	}

	if err := env.EvolveSchema(ctx, v2Retired); err != nil {
		t.Fatalf("evolve schema to retired-marker v2: %v", err)
	}

	if err := env.ApplyEvents(ctx, UpdateEvent(schema, create.RowID, map[string]any{
		"value": float64(2),
	})); err != nil {
		t.Fatalf("update row carrying retired-attr EAV data under v2: %v", err)
	}

	// Preserve, not drop: retiring must leave the EAV row physically present.
	var preserved string
	if err := env.Pool.QueryRow(ctx,
		`SELECT value_text FROM eav_data WHERE schema_id = $1 AND row_id = $2 AND attr_id = 3`,
		schema.ID, create.RowID).Scan(&preserved); err != nil {
		t.Fatalf("retired old_col EAV row missing after update (tolerate-and-preserve violated): %v", err)
	}
	if preserved != "keep-me" {
		t.Fatalf("retired old_col EAV value = %q, want %q", preserved, "keep-me")
	}

	// Under the retired-marker generation the value stays invisible on read.
	attrs := getNormalizedAttrs(ctx, t, env, schema, create.RowID, "under retired-marker v2")
	if _, ok := attrs["old_col"]; ok {
		t.Fatalf("retired old_col visible under retired-marker v2 schema: %v", attrs)
	}
	if attrs["value"] != float64(2) {
		t.Fatalf("value = %v, want 2", attrs["value"])
	}

	// Un-retire old_col (evolve back to the v1 shape, same attributeID 3,
	// same name and valueType): the preserved value must reappear.
	if err := env.EvolveSchema(ctx, v1); err != nil {
		t.Fatalf("un-retire old_col (evolve back to v1 shape): %v", err)
	}
	attrs2 := getNormalizedAttrs(ctx, t, env, schema, create.RowID, "after un-retire")
	if attrs2["old_col"] != "keep-me" {
		t.Fatalf("old_col after un-retire = %v, want \"keep-me\" (preserve vs drop discriminator)", attrs2["old_col"])
	}
	if attrs2["value"] != float64(2) {
		t.Fatalf("value after un-retire = %v, want 2", attrs2["value"])
	}

	// The row stays a normal citizen: another update under the restored
	// schema round-trips old_col like any live attribute.
	if err := env.ApplyEvents(ctx, UpdateEvent(schema, create.RowID, map[string]any{
		"value": float64(3),
	})); err != nil {
		t.Fatalf("update after un-retire: %v", err)
	}
	attrs3 := getNormalizedAttrs(ctx, t, env, schema, create.RowID, "after second update")
	if attrs3["old_col"] != "keep-me" || attrs3["value"] != float64(3) {
		t.Fatalf("after second update got old_col=%v value=%v, want keep-me / 3", attrs3["old_col"], attrs3["value"])
	}
}

package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #696 without a validator: the shape rule is a storage invariant, so a
// manager that validates nothing refuses an object or array where a single
// value belongs exactly as one in report-only mode does. The cond649 schema
// adds a bound text scalar, a nested scalar and a member of an array of
// objects to ukaccount's cases; its registry serves the document's
// properties, which say what each attribute is declared as.

func condShapeProperties() map[string]*forma.PropertySchema {
	typed := func(typ string) *forma.PropertySchema { return &forma.PropertySchema{Type: typ} }
	return map[string]*forma.PropertySchema{
		"name": typed("string"), "label": typed("string"), "counter": typed("integer"),
		"stats": {Type: "object", Properties: map[string]*forma.PropertySchema{"note": typed("string"), "attempts": typed("integer")}},
		"items": {Type: "array", Items: &forma.PropertySchema{Type: "object", Properties: map[string]*forma.PropertySchema{
			"note": typed("string"), "qty": typed("integer"),
		}}},
		"tags": {Type: "array", Items: typed("string")},
	}
}

func condShapeUpdates() []shapeUpdate {
	refused := func(attr string, id int, vt, got string) string {
		return fmt.Sprintf("invalid value for attribute '%s' (attrID=%d): declared type %s takes a single value, got %s", attr, id, vt, got)
	}
	return []shapeUpdate{
		{"EAV {}", map[string]any{"name": map[string]any{}}, refused("name", 1, "text", "an empty object")},
		{"EAV []", map[string]any{"name": []any{}}, refused("name", 1, "text", "an empty array")},
		{"EAV [x]", map[string]any{"name": []any{"a"}}, refused("name", 1, "text", "an array of 1 element")},
		{"bound {}", map[string]any{"label": map[string]any{}}, refused("label", 23, "text", "an empty object")},
		{"bound []", map[string]any{"label": []any{}}, refused("label", 23, "text", "an empty array")},
		{"bound [x]", map[string]any{"label": []any{"a"}}, refused("label", 23, "text", "an array of 1 element")},
		{"integer {}", map[string]any{"counter": map[string]any{"x": 1}}, refused("counter", 2, "integer", "an object")},
		{"nested []", map[string]any{"stats": map[string]any{"note": []any{}}}, refused("stats.note", 12, "text", "an empty array")},
		{"dotted {}", map[string]any{"stats.note": map[string]any{}}, refused("stats.note", 12, "text", "an empty object")},
		{"array-of-objects member {}", map[string]any{"items": []any{map[string]any{"note": "ok"}, map[string]any{"note": map[string]any{}}}},
			refused("items.note", 31, "text", "an empty object")},
	}
}

// TestScalarShapeRefusedWithoutAValidator runs every update route the
// manager and a Tx have over one row per case: each refuses the value, the
// row's three tables are as they were, and the Tx commits the write after
// the refusals.
func TestScalarShapeRefusedWithoutAValidator(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache(), declaringProperties(condShapeProperties()))
	newRow := func() uuid.UUID {
		rowID := uuid.Must(uuid.NewV7())
		_, err := e.em.Create(e.ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName, RowID: rowID},
			Data: map[string]any{"name": "stored", "label": rowID.String(), "counter": 1, "stats": map[string]any{"note": "s"},
				"items": []any{map[string]any{"note": "i", "qty": 1}}}})
		require.NoError(t, err)
		return rowID
	}
	for _, tc := range condShapeUpdates() {
		rowID := newRow()
		before := e.snapshot(t, rowID)
		_, err := e.em.Update(e.ctx, condUpdate(rowID, nil, tc.updates))
		requireShapeRefused(t, err, tc.want, "Update %s", tc.name)
		_, err = e.em.BatchUpdate(e.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{textFitUpdateOp(rowID, tc.updates)}})
		requireShapeRefused(t, err, tc.want, "atomic BatchUpdate %s", tc.name)
		result, err := e.em.BatchUpdate(e.ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{textFitUpdateOp(rowID, tc.updates)}})
		require.NoError(t, err)
		require.Len(t, result.Failed, 1, "best-effort BatchUpdate %s", tc.name)
		assert.Contains(t, result.Failed[0].Error, tc.want)

		err = e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
			for route, write := range textImageTxRoutes() {
				requireShapeRefused(t, write(ctx, tx, rowID, tc.updates), tc.want, "%s %s", route, tc.name)
			}
			return nil
		})
		require.NoError(t, err, "%s: the refusals left the Tx usable", tc.name)
		assert.Equal(t, before, e.snapshot(t, rowID), "%s: the row as it was", tc.name)
	}
}

// TestScalarShapeLeavesValidContainerUpdatesAlone: objects, arrays of
// objects, lists and the empty-list marker are stored as before.
func TestScalarShapeLeavesValidContainerUpdatesAlone(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache(), declaringProperties(condShapeProperties()))
	rowID := e.create(t, map[string]any{"name": "a", "tags": []any{"x"}})
	_, err := e.em.Update(e.ctx, condUpdate(rowID, nil, map[string]any{
		"stats": map[string]any{"note": "y"}, "items": []any{map[string]any{"note": "i", "qty": 2}}, "tags": []any{"p", "q"},
	}))
	require.NoError(t, err)
	got := e.get(t, rowID)
	assert.Equal(t, "y", valueAtPath(got, "stats.note"))
	assert.Equal(t, []any{"p", "q"}, got["tags"])
	require.Len(t, got["items"], 1)

	_, err = e.em.Update(e.ctx, condUpdate(rowID, nil, map[string]any{"tags": []any{}, "stats": map[string]any{}}))
	require.NoError(t, err)
	got = e.get(t, rowID)
	assert.Equal(t, []any{}, got["tags"], "[] at a list is its empty-list marker")
	assert.Equal(t, "y", valueAtPath(got, "stats.note"), "{} at an object parent replaces nothing it does not name")
}

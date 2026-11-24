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

// #696 review: where the schema document does not settle an attribute's
// declaration, nothing shows the attribute holds an array, and its metadata
// says it holds one value, so the write refuses an array there as at a
// declared scalar. The cond649 schema is served three ways: a document that
// declares no property at all, one whose properties are behind a $ref to a
// file the registry does not serve, and one that declares them open ({}).
// Every update route refuses [] and ["a"] at bound and EAV text scalars,
// nested ones and members of an array of objects, and the row's three
// tables are as they were; a list, an array of objects and single values
// are written as before.

func unresolvedShapeDocuments() map[string]func(*stubSchemaRegistry) {
	serving := func(document string) func(*stubSchemaRegistry) {
		return func(registry *stubSchemaRegistry) { registry.document = document }
	}
	const lists = `"tags": {"type": "array", "items": {"type": "string"}}`
	return map[string]func(*stubSchemaRegistry){
		"no declarations": serving(`{"type": "object"}`),
		"unresolved $ref": serving(`{"type": "object", "properties": {` + lists + `,
			"name": {"$ref": "missing.json#/$defs/name"}, "label": {"$ref": "#/$defs/none"},
			"stats": {"$ref": "missing.json#/$defs/stats"},
			"items": {"type": "array", "items": {"$ref": "missing.json#/$defs/item"}}}}`),
		"open declarations": serving(`{"type": "object", "properties": {` + lists + `,
			"name": {}, "label": {"description": "any"},
			"stats": {"type": "object", "properties": {"note": {}}},
			"items": {"type": "array", "items": {"type": "object", "properties": {"note": {"anyOf": [{"type": "string"}, {}]}}}}}}`),
	}
}

func unresolvedShapeUpdates() []shapeUpdate {
	refused := func(attr string, id int, got string) string {
		return fmt.Sprintf("invalid value for attribute '%s' (attrID=%d): declared type text takes a single value "+
			"(the schema document declares no array here), got %s", attr, id, got)
	}
	return []shapeUpdate{
		{"EAV []", map[string]any{"name": []any{}}, refused("name", 1, "an empty array")},
		{"EAV [x]", map[string]any{"name": []any{"a"}}, refused("name", 1, "an array of 1 element")},
		{"bound []", map[string]any{"label": []any{}}, refused("label", 23, "an empty array")},
		{"bound [x]", map[string]any{"label": []any{"a"}}, refused("label", 23, "an array of 1 element")},
		{"nested [x]", map[string]any{"stats": map[string]any{"note": []any{"a"}}}, refused("stats.note", 12, "an array of 1 element")},
		{"dotted []", map[string]any{"stats.note": []any{}}, refused("stats.note", 12, "an empty array")},
		{"array-of-objects member [x]", map[string]any{"items": []any{map[string]any{"note": []any{"a"}}}},
			refused("items.note", 31, "an array of 1 element")},
	}
}

func TestScalarShapeRefusedWhereTheDocumentIsUnresolved(t *testing.T) {
	for document, serve := range unresolvedShapeDocuments() {
		t.Run(document, func(t *testing.T) {
			e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache(), serve)
			for _, tc := range unresolvedShapeUpdates() {
				rowID := uuid.Must(uuid.NewV7())
				_, err := e.em.Create(e.ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName, RowID: rowID},
					Data: map[string]any{"name": "stored", "label": rowID.String(), "stats": map[string]any{"note": "s"},
						"items": []any{map[string]any{"note": "i"}}}})
				require.NoError(t, err)
				before := e.snapshot(t, rowID)

				_, err = e.em.Update(e.ctx, condUpdate(rowID, nil, tc.updates))
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

			rowID := e.create(t, map[string]any{"name": "a", "label": "l", "tags": []any{"x"}})
			_, err := e.em.Update(e.ctx, condUpdate(rowID, nil, map[string]any{
				"name": "b", "label": "m", "stats": map[string]any{"note": "y"}, "tags": []any{"p", "q"},
				"items": []any{map[string]any{"note": "i"}, map[string]any{"note": "j"}},
			}))
			require.NoError(t, err, "single values, a list and an array of objects are written as before")
			got := e.get(t, rowID)
			assert.Equal(t, "b", got["name"])
			assert.Equal(t, "m", got["label"])
			assert.Equal(t, "y", valueAtPath(got, "stats.note"))
			assert.Equal(t, []any{"p", "q"}, got["tags"])
			assert.Equal(t, []any{map[string]any{"note": "i"}, map[string]any{"note": "j"}}, got["items"])
		})
	}
}

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

// #696 review round 3, on real PostgreSQL with no validator configured (the
// shape rule never consults validation, so report-only answers the same):
// pre-#204 primitive arrays declared through alternatives. name is an array
// of strings or null, stats.note open or an array of strings, and items an
// object or an array of objects whose note is an array of strings. Each is
// still written as the array it is, and an array at one of its elements is
// refused on every update route, the row's three tables as they were: the
// alternative admitting null or anything does not hide the array, and the
// array ancestry of items.note is the position the value takes, not the
// most an alternative admits.

const alternativeShapeDocument = `{"type": "object", "properties": {
	"name": {"anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "null"}]},
	"label": {"type": "string"},
	"stats": {"type": "object", "properties": {"note": {"oneOf": [{}, {"type": "array", "items": {"type": "string"}}]}}},
	"items": {"anyOf": [
		{"type": "object", "properties": {"note": {"type": "array", "items": {"type": "string"}}}},
		{"type": "array", "items": {"type": "object", "properties": {"note": {"type": "array", "items": {"type": "string"}}}}}]},
	"tags": {"type": "array", "items": {"type": "string"}}}}`

func alternativeShapeUpdates() []shapeUpdate {
	refused := func(attr string, id int, indices, got string) string {
		return fmt.Sprintf("invalid value for attribute '%s' (attrID=%d): declared type text takes a single value at array indices %q, got %s",
			attr, id, indices, got)
	}
	return []shapeUpdate{
		{"nullable alternative, inner array", map[string]any{"name": []any{[]any{"a"}}}, refused("name", 1, "0", "an array of 1 element")},
		{"open alternative, inner empty array", map[string]any{"stats": map[string]any{"note": []any{"a", []any{}}}},
			refused("stats.note", 12, "1", "an empty array")},
		{"object parent, inner array", map[string]any{"items": map[string]any{"note": []any{[]any{"1"}}}},
			refused("items.note", 31, "0", "an array of 1 element")},
		{"object parent, inner empty array", map[string]any{"items": map[string]any{"note": []any{[]any{}}}},
			refused("items.note", 31, "0", "an empty array")},
		{"array parent, inner array", map[string]any{"items": []any{map[string]any{"note": []any{"1", []any{"2"}}}}},
			refused("items.note", 31, "0,1", "an array of 1 element")},
	}
}

func TestScalarShapeAcrossNullableOpenAndParentAlternatives(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache(),
		func(registry *stubSchemaRegistry) { registry.document = alternativeShapeDocument })
	for _, tc := range alternativeShapeUpdates() {
		rowID := uuid.Must(uuid.NewV7())
		_, err := e.em.Create(e.ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName, RowID: rowID},
			Data: map[string]any{"name": []any{"stored"}, "label": rowID.String(), "stats": map[string]any{"note": []any{"s"}},
				"items": []any{map[string]any{"note": []any{"i"}}}}})
		require.NoError(t, err, "the declared arrays are created as before")
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

	rowID := e.create(t, map[string]any{"name": []any{"a"}, "label": "l"})
	_, err := e.em.Update(e.ctx, condUpdate(rowID, nil, map[string]any{
		"name": []any{"b", "c"}, "stats": map[string]any{"note": []any{"x"}},
		"items": []any{map[string]any{"note": []any{"i"}}, map[string]any{"note": []any{"j", "k"}}},
	}))
	require.NoError(t, err, "the declared arrays, in an array-of-objects parent too, are written as before")
	// The records are checked, not the read: a row without derived array
	// ancestry is read by inference, which places a nested pre-#204 array's
	// elements its own way (docs/error-handling.md, Known limits).
	assert.Equal(t, []string{"0=b", "1=c"}, e.textRecords(t, rowID, 1))
	assert.Equal(t, []string{"0=x"}, e.textRecords(t, rowID, 12))
	assert.Equal(t, []string{"0,0=i", "1,0=j", "1,1=k"}, e.textRecords(t, rowID, 31))

	rowID = e.create(t, map[string]any{"name": []any{"a"}, "label": "m"})
	_, err = e.em.Update(e.ctx, condUpdate(rowID, nil, map[string]any{"items": map[string]any{"note": []any{"y", "z"}}}))
	require.NoError(t, err, "the object-parent form is written as before")
	assert.Equal(t, []string{"0=y", "1=z"}, e.textRecords(t, rowID, 31))
}

// textRecords answers the row's eav_data records of attrID as
// "array_indices=value_text", in index order.
func (e *condPG) textRecords(t *testing.T, rowID uuid.UUID, attrID int16) []string {
	t.Helper()
	rows, err := e.pool.Query(e.ctx, fmt.Sprintf(
		"SELECT array_indices || '=' || COALESCE(value_text, '<null>') FROM %s WHERE schema_id = $1 AND row_id = $2 AND attr_id = $3 ORDER BY array_indices",
		sanitizeIdentifier(e.tables.EAVData)), condSchemaID, rowID, attrID)
	require.NoError(t, err)
	defer rows.Close()
	var records []string
	for rows.Next() {
		var record string
		require.NoError(t, rows.Scan(&record))
		records = append(records, record)
	}
	require.NoError(t, rows.Err())
	return records
}

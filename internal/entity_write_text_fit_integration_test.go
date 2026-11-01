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

// Written text PostgreSQL text cannot hold (#679): a NUL character or
// invalid UTF-8 in Data or Updates used to reach the INSERT, which failed
// with 22021. The caller got a redacted storage error instead of invalid
// input, and inside a Tx the failed statement aborted the transaction.

// textFitSchemaCache is the cond649 schema plus a text list and a text
// attribute inside an array of objects, so array elements are covered.
// Array elements live in EAV only: a list has no column form
// (schemameta.bindingRule).
func textFitSchemaCache() forma.SchemaAttributeCache {
	cache := condSchemaCache()
	cache["tags"] = forma.AttributeMetadata{AttributeID: 30, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeText}
	cache["items.note"] = forma.AttributeMetadata{AttributeID: 31, ValueType: forma.ValueTypeText}
	return cache
}

type unstorableText struct {
	name string
	doc  map[string]any
	want string // the published message
}

func unstorableTextCases() []unstorableText {
	const (
		nul     = "text cannot contain a NUL character (U+0000)"
		badUTF8 = "text must be valid UTF-8"
	)
	refused := func(attr string, id int, rule string) string {
		return fmt.Sprintf("invalid value for attribute '%s' (attrID=%d): %s", attr, id, rule)
	}
	return []unstorableText{
		{"EAV scalar, NUL", map[string]any{"name": "a\x00b"}, refused("name", 1, nul)},
		{"EAV scalar, invalid UTF-8", map[string]any{"name": "a\xffb"}, refused("name", 1, badUTF8)},
		{"nested EAV scalar", map[string]any{"stats": map[string]any{"note": "a\x00b"}}, refused("stats.note", 12, nul)},
		{"bound scalar, NUL", map[string]any{"label": "a\x00b"}, refused("label", 23, nul)},
		{"bound scalar, invalid UTF-8", map[string]any{"label": "a\xffb"}, refused("label", 23, badUTF8)},
		{"list element, NUL", map[string]any{"tags": []any{"ok", "a\x00b"}}, refused("tags", 30, nul)},
		{"list element, invalid UTF-8", map[string]any{"tags": []any{"a\xffb"}}, refused("tags", 30, badUTF8)},
		{"array-of-objects element", map[string]any{"items": []any{map[string]any{"note": "ok"}, map[string]any{"note": "a\x00b"}}}, refused("items.note", 31, nul)},
	}
}

// tableRows counts every row of the three tables, so a refused create is
// seen to write nothing.
func (e *condPG) tableRows(t *testing.T) [3]int64 {
	t.Helper()
	var n [3]int64
	for i, table := range []string{e.tables.EntityMain, e.tables.EAVData, e.tables.ChangeLog} {
		require.NoError(t, e.pool.QueryRow(e.ctx, "SELECT count(*) FROM "+sanitizeIdentifier(table)).Scan(&n[i]))
	}
	return n
}

func requireRefusedText(t *testing.T, err error, want string, msgAndArgs ...any) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrInvalidInput, msgAndArgs...)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published, err)
	assert.Equal(t, want, message, msgAndArgs...)
}

func textFitCreateOp(data map[string]any) forma.EntityOperation {
	return forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName}, Type: forma.OperationCreate, Data: data}
}

func textFitUpdateOp(rowID uuid.UUID, updates map[string]any) forma.EntityOperation {
	return forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName, RowID: rowID}, Type: forma.OperationUpdate, Updates: updates}
}

// TestUnstorableTextCreateIsRefusedAsInvalidInput: Create and BatchCreate,
// atomic and best-effort, refuse the value naming the attribute and write
// nothing.
func TestUnstorableTextCreateIsRefusedAsInvalidInput(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache())
	for _, tc := range unstorableTextCases() {
		before := e.tableRows(t)
		op := textFitCreateOp(tc.doc)
		_, err := e.em.Create(e.ctx, &op)
		requireRefusedText(t, err, tc.want, "Create %s", tc.name)

		_, err = e.em.BatchCreate(e.ctx, &forma.BatchOperation{Atomic: true,
			Operations: []forma.EntityOperation{textFitCreateOp(map[string]any{"name": "fine"}), textFitCreateOp(tc.doc)}})
		requireRefusedText(t, err, "operation[1]: "+tc.want, "atomic BatchCreate %s", tc.name)

		result, err := e.em.BatchCreate(e.ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{textFitCreateOp(tc.doc)}})
		require.NoError(t, err)
		require.Len(t, result.Failed, 1, "best-effort BatchCreate %s", tc.name)
		assert.Equal(t, tc.want, result.Failed[0].Error, "best-effort BatchCreate %s", tc.name)
		assert.Equal(t, before, e.tableRows(t), "%s: nothing written", tc.name)
	}
}

// TestUnstorableTextUpdateIsRefusedAsInvalidInput: Update with and without a
// precondition and BatchUpdate, atomic and best-effort, refuse the value
// naming the attribute and leave the row as it was.
func TestUnstorableTextUpdateIsRefusedAsInvalidInput(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache())
	other := e.create(t, map[string]any{"name": "other"})
	rowID := e.create(t, map[string]any{"name": "ab", "label": "ab", "tags": []any{"x"}})
	for _, tc := range unstorableTextCases() {
		before, otherBefore := e.snapshot(t, rowID), e.snapshot(t, other)
		_, err := e.em.Update(e.ctx, condUpdate(rowID, nil, tc.doc))
		requireRefusedText(t, err, tc.want, "Update %s", tc.name)
		_, err = e.em.Update(e.ctx, condUpdate(rowID, kv("label", "equals:ab"), tc.doc))
		requireRefusedText(t, err, tc.want, "conditional Update %s", tc.name)

		_, err = e.em.BatchUpdate(e.ctx, &forma.BatchOperation{Atomic: true,
			Operations: []forma.EntityOperation{textFitUpdateOp(other, map[string]any{"name": "changed"}), textFitUpdateOp(rowID, tc.doc)}})
		requireRefusedText(t, err, "operation[1]: "+tc.want, "atomic BatchUpdate %s", tc.name)

		result, err := e.em.BatchUpdate(e.ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{textFitUpdateOp(rowID, tc.doc)}})
		require.NoError(t, err)
		require.Len(t, result.Failed, 1, "best-effort BatchUpdate %s", tc.name)
		assert.Equal(t, tc.want, result.Failed[0].Error, "best-effort BatchUpdate %s", tc.name)

		assert.Equal(t, before, e.snapshot(t, rowID), "%s: the row as it was", tc.name)
		assert.Equal(t, otherBefore, e.snapshot(t, other), "%s: the atomic batch wrote nothing", tc.name)
	}
}

// TestUnstorableTextInATxLeavesTheTxUsable is the issue's reproduction: in a
// Tx, the refusal used to fail its statement, so the next operation was
// refused and InTx rolled back although fn returned nil. Every Tx write
// that takes a document now refuses the value before any statement: the
// write made before the refusals commits, and so does the one after.
func TestUnstorableTextInATxLeavesTheTxUsable(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache())
	earlier := e.create(t, map[string]any{"name": "before"})
	rowID := e.create(t, map[string]any{"name": "ab", "label": "ab"})
	cases := unstorableTextCases()

	err := e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		if _, err := tx.Update(ctx, condUpdate(earlier, nil, map[string]any{"name": "earlier"})); err != nil {
			return err
		}
		for _, tc := range cases {
			op := textFitCreateOp(tc.doc)
			_, err := tx.Create(ctx, &op)
			requireRefusedText(t, err, tc.want, "Tx.Create %s", tc.name)
			_, err = tx.Update(ctx, condUpdate(rowID, nil, tc.doc))
			requireRefusedText(t, err, tc.want, "Tx.Update %s", tc.name)
			_, err = tx.Update(ctx, condUpdate(rowID, kv("label", "equals:ab"), tc.doc))
			requireRefusedText(t, err, tc.want, "conditional Tx.Update %s", tc.name)
			_, err = tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: condSchemaName,
				Condition: kv("label", "equals:ab"), MaxAffected: 10, Updates: tc.doc})
			requireRefusedText(t, err, tc.want, "Tx.UpdateWhere %s", tc.name)
			_, err = tx.Upsert(ctx, &forma.UpsertRequest{
				Create:  forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName, RowID: rowID}, Data: map[string]any{"name": "ab"}},
				Target:  forma.ConflictTarget{RowID: true},
				Action:  forma.UpsertUpdate,
				Updates: tc.doc,
			})
			requireRefusedText(t, err, tc.want, "Tx.Upsert %s", tc.name)
		}
		_, err := tx.Update(ctx, condUpdate(rowID, kv("label", "equals:ab"), map[string]any{"name": "after"}))
		return err
	})
	require.NoError(t, err, "the refusals did not poison the Tx")
	assert.Equal(t, "earlier", e.get(t, earlier)["name"], "the write before the refusals committed")
	assert.Equal(t, "after", e.get(t, rowID)["name"], "the write after the refusals committed")
}

// TestStorableTextStillRoundTrips guards the refusal's edges: a string that
// is valid UTF-8 without NUL, multi-byte characters and the replacement
// character the HTTP decoder substitutes for invalid bytes included, is
// stored and read back unchanged on both storage kinds and in a list.
func TestStorableTextStillRoundTrips(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache())
	const text = "aé中\U0001F600�\u0001b"
	rowID := e.create(t, map[string]any{"name": text, "label": text, "tags": []any{text}})
	got := e.get(t, rowID)
	assert.Equal(t, text, got["name"])
	assert.Equal(t, text, got["label"])
	assert.Equal(t, []any{text}, got["tags"])
}

// TestMalformedListUpdateKeepsItsGuardOutcome: the intake moves only the
// text rule ahead of the row lock. An update the merged write refuses for
// another reason, here a scalar or a nested array sent for a text list, is
// still judged against the row first, so a false precondition answers
// ErrPreconditionFailed and a missing row ErrNotFound, as before #679.
func TestMalformedListUpdateKeepsItsGuardOutcome(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache())
	rowID := e.create(t, map[string]any{"name": "ab", "label": "ab", "tags": []any{"x"}})
	for name, updates := range map[string]map[string]any{
		"scalar":          {"tags": "ordinary"},
		"scalar with NUL": {"tags": "a\x00b"},
		"nested array":    {"tags": []any{[]any{"a"}}},
	} {
		_, err := e.em.Update(e.ctx, condUpdate(rowID, kv("label", "equals:other"), updates))
		require.ErrorIs(t, err, forma.ErrPreconditionFailed, "false precondition, %s", name)
		_, err = e.em.Update(e.ctx, condUpdate(uuid.New(), nil, updates))
		require.ErrorIs(t, err, forma.ErrNotFound, "missing row, %s", name)
		_, err = e.em.Update(e.ctx, condUpdate(rowID, nil, updates))
		require.ErrorIs(t, err, forma.ErrInvalidInput, "the merged write still refuses it, %s", name)
	}
}

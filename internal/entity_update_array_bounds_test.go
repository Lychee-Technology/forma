package internal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// A primitive array whose item types are given only through an anyOf of
// single types is still the array the document declares: under an
// element-typed (pre-#204) ledger entry, an update writes it as a create
// does, in either validation mode (#696 review R4-F1). note, a true scalar,
// still refuses an array, and so does mixed, whose items may be objects; the
// refusals leave the stored row as it was.
func TestUpdateWritesAPrimitiveArrayWhoseItemsAreTypedByACombinator(t *testing.T) {
	const document = `{"type":"object","properties":{"note":{"type":"string"},` +
		`"tags":{"type":"array","items":{"anyOf":[{"type":"string"},{"type":"integer"}]}},` +
		`"mixed":{"type":"array","items":{"anyOf":[{"type":"string"},{"type":"object"}]}}}}`
	const attributes = `{"note":{"attributeID":1,"valueType":"text"},"tags":{"attributeID":2,"valueType":"text"},` +
		`"mixed":{"attributeID":3,"valueType":"text"}}`
	for _, strict := range []bool{false, true} {
		m := newPositionsManager(t, "legacy", document, attributes, strict)
		_, cache, err := m.registry.GetSchemaAttributeCacheByName("legacy")
		require.NoError(t, err)
		require.True(t, cache["tags"].DeclaredArray)
		require.Equal(t, []string{}, cache["tags"].SettledArrayAncestors)
		require.False(t, cache["note"].DeclaredArray)
		require.False(t, cache["mixed"].DeclaredArray)

		row := m.create(t, map[string]any{"note": "n", "tags": []any{"a"}})
		require.Equal(t, []any{"a"}, row.Attributes["tags"])
		updated, err := m.update(row, map[string]any{"tags": []any{"b"}})
		require.NoError(t, err)
		require.Equal(t, []any{"b"}, updated.Attributes["tags"])

		before := m.stored(t, row)
		_, err = m.update(row, map[string]any{"note": []any{"x"}})
		assertShapeRefused(t, err, "note", "declared type text takes a single value, got an array")
		_, err = m.update(row, map[string]any{"mixed": []any{"x"}})
		assertShapeRefused(t, err, "mixed", "declared type text takes a single value, got an array")
		require.Same(t, before, m.stored(t, row), "a refused update must not store a row")
		require.Equal(t, []any{"b"}, m.read(t, row)["tags"])
		require.Equal(t, "n", m.read(t, row)["note"])
	}
}

// office's anyOf lets it be an object or one array of objects, so the
// document does not settle which ancestor of office.city is an array, but it
// bounds them: every reading holds office.city at most one array deep. A
// value sent inside an extra array is refused before anything is written, in
// either validation mode, the report-only refusal publishing the bound
// (#696 review R4-F2); an object and a single array of offices are still
// written.
func TestUpdateRefusesExtraArraysTheDocumentBoundsBesideAnAnyOf(t *testing.T) {
	const document = `{"type":"object","properties":{` +
		`"office":{"properties":{"city":{"type":"string"}},"anyOf":[{"type":"object"},` +
		`{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]}}}`
	const attributes = `{"office.city":{"attributeID":1,"valueType":"text"}}`
	for _, strict := range []bool{false, true} {
		m := newPositionsManager(t, "contact", document, attributes, strict)
		_, cache, err := m.registry.GetSchemaAttributeCacheByName("contact")
		require.NoError(t, err)
		meta := cache["office.city"]
		require.Nil(t, meta.SettledArrayAncestors, "the document does not settle office.city")
		require.True(t, meta.ArrayIndicesBounded)
		require.Equal(t, 1, meta.MaxArrayIndices)

		row := m.create(t, map[string]any{"office": []any{map[string]any{"city": "x"}}})
		updated, err := m.update(row, map[string]any{"office": []any{map[string]any{"city": "y"}}})
		require.NoError(t, err)
		require.Equal(t, map[string]any{"city": []any{"y"}}, updated.Attributes["office"])

		before := m.stored(t, row)
		_, err = m.update(row, map[string]any{"office": []any{[]any{map[string]any{"city": "b"}}}})
		if strict {
			require.ErrorIs(t, err, forma.ErrInvalidInput)
		} else {
			assertShapeRefused(t, err, "office.city", `a single text value at array indices "0,0" is nested in more arrays `+
				"than the schema document admits around it (at most 1 on any of its readings)")
		}
		require.Same(t, before, m.stored(t, row), "a refused update must not store a row")
		require.Equal(t, map[string]any{"city": []any{"y"}}, m.read(t, row)["office"])

		updated, err = m.update(row, map[string]any{"office": map[string]any{"city": "z"}})
		require.NoError(t, err)
		require.Equal(t, map[string]any{"city": "z"}, updated.Attributes["office"])
	}
}

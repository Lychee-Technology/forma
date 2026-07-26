package transform

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func dottedCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"contact.email": {AttributeID: 8, ValueType: forma.ValueTypeText},
		"name":          {AttributeID: 1, ValueType: forma.ValueTypeText},
	}
}

// TestNormalizeExpandsDottedKey pins the core rule: a literal dotted key that
// names a leaf attribute becomes its nested path, so schema validation sees a
// well-formed document and actually checks the value. Left flat, the key is an
// unknown property and its value is never examined at all (#314).
func TestNormalizeExpandsDottedKey(t *testing.T) {
	out := NormalizeDottedKeys(map[string]any{"contact.email": "x"}, dottedCache())
	require.Equal(t, map[string]any{"contact": map[string]any{"email": "x"}}, out)
}

// TestNormalizeLiteralWinsOverNested pins last-spelling-wins, matching
// encoding/json duplicate-key semantics and #312. On the update path the
// literal key is the caller's explicit value while the nested one was rebuilt
// from storage, so the literal must win.
func TestNormalizeLiteralWinsOverNested(t *testing.T) {
	in := map[string]any{
		"contact":       map[string]any{"email": "old"},
		"contact.email": "x",
	}
	out := NormalizeDottedKeys(in, dottedCache())
	require.Equal(t, map[string]any{"contact": map[string]any{"email": "x"}}, out)
}

// TestNormalizePreservesSiblings pins that expanding one leaf does not discard
// other properties of the same parent object.
func TestNormalizePreservesSiblings(t *testing.T) {
	cache := dottedCache()
	cache["contact.phone"] = forma.AttributeMetadata{AttributeID: 9, ValueType: forma.ValueTypeText}

	in := map[string]any{
		"contact":       map[string]any{"phone": "555", "email": "old"},
		"contact.email": "x",
	}
	out := NormalizeDottedKeys(in, cache)
	require.Equal(t, map[string]any{
		"contact": map[string]any{"phone": "555", "email": "x"},
	}, out)
}

// TestNormalizeLeavesUnknownDottedKeyAlone pins that only keys the metadata
// cache knows are expanded. An unknown dotted key stays put so the existing
// "attribute is not defined for schema" error still fires with its own message.
func TestNormalizeLeavesUnknownDottedKeyAlone(t *testing.T) {
	out := NormalizeDottedKeys(map[string]any{"nope.missing": 1}, dottedCache())
	require.Equal(t, map[string]any{"nope.missing": 1}, out)
}

// TestNormalizeIsDeterministic pins that the result does not depend on Go map
// iteration order. Run repeatedly because a map-order dependency is flaky by
// nature and would otherwise pass most runs.
func TestNormalizeIsDeterministic(t *testing.T) {
	in := map[string]any{
		"contact":       map[string]any{"email": "old"},
		"contact.email": "x",
	}
	for i := 0; i < 200; i++ {
		out := NormalizeDottedKeys(in, dottedCache())
		require.Equal(t, map[string]any{"contact": map[string]any{"email": "x"}}, out)
	}
}

// TestNormalizeDoesNotMutateInput pins that the caller's map is untouched. The
// update path reuses the merged map, and mutating it would corrupt the caller.
func TestNormalizeDoesNotMutateInput(t *testing.T) {
	in := map[string]any{"contact.email": "x"}
	NormalizeDottedKeys(in, dottedCache())
	require.Equal(t, map[string]any{"contact.email": "x"}, in)
}

// TestNormalizeExpandsNestedDottedKey pins that a dotted key is expanded even
// when it appears below the root, e.g. {"contact": {"email": ...}} spelled as
// a dotted key inside another object.
func TestNormalizeExpandsNestedDottedKey(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"a.b.c": {AttributeID: 4, ValueType: forma.ValueTypeText},
	}
	out := NormalizeDottedKeys(map[string]any{"a": map[string]any{"b.c": "v"}}, cache)
	require.Equal(t, map[string]any{
		"a": map[string]any{"b": map[string]any{"c": "v"}},
	}, out)
}

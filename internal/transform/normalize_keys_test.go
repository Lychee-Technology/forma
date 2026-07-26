package transform

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"
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

// TestNormalizeDoesNotMutateInput pins that the caller's map is untouched, all
// the way down. The update path reuses the merged map, and mutating it would
// corrupt the caller.
//
// The input must contain a nested map: a flat-only input cannot fail, because
// the only writes the implementation could make to the caller's data are into
// nested maps it did not copy. The aliasing assertion is what makes that
// provable — equality alone still passes when the nested map is shared and
// happens not to have been written to yet.
func TestNormalizeDoesNotMutateInput(t *testing.T) {
	nested := map[string]any{"phone": "555", "email": "old"}
	in := map[string]any{
		"contact":       nested,
		"contact.email": "x",
	}

	cache := dottedCache()
	cache["contact.phone"] = forma.AttributeMetadata{AttributeID: 9, ValueType: forma.ValueTypeText}
	out := NormalizeDottedKeys(in, cache)

	require.Equal(t, map[string]any{
		"contact":       map[string]any{"phone": "555", "email": "old"},
		"contact.email": "x",
	}, in, "caller's map must be unchanged, including its nested maps")
	// Aliasing assertion: maps are references, so writing through the output must
	// not be visible in the input. reflect gives the map headers directly; testify's
	// NotSame only accepts pointers.
	outContact, ok := out["contact"].(map[string]any)
	require.True(t, ok)
	require.NotEqual(t,
		reflect.ValueOf(nested).Pointer(),
		reflect.ValueOf(outContact).Pointer(),
		"output's nested map must not alias the input's")

	outContact["email"] = "mutated through the output"
	require.Equal(t, "old", nested["email"], "writes through the output must not reach the input")
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

// snapshotCache has a three-segment attribute, the shape that exposes the
// difference between expanding only leaves and expanding parent paths too.
// Three-segment attributes are ordinary here: contact.snapshot.code,
// contact.phones.kind, orders.items.price.
func snapshotCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"contact.snapshot.code": {AttributeName: "contact.snapshot.code", AttributeID: 20, ValueType: forma.ValueTypeText},
		"contact.email":         {AttributeName: "contact.email", AttributeID: 21, ValueType: forma.ValueTypeText},
	}
}

func newSnapshotStubRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: 100, schemaName: "test", cache: snapshotCache()}
}

// TestNormalizeThreeSegmentLiteralWinsThroughToAttributes pins last-spelling-wins
// for an attribute deeper than two segments, asserted through the real write
// path rather than on the normalizer's output alone.
//
// Expanding a literal moves it from its own sort position to that of its first
// segment — the earliest position among all spellings of the attribute. If an
// intermediate spelling ("contact.snapshot") is left flat, the flattener sorts
// it after the expanded "contact" subtree and its value wins the #312 dedupe,
// inverting precedence. Expanding every known-or-parent path keeps all spellings
// in one subtree, where the order this file applies them decides the winner.
func TestNormalizeThreeSegmentLiteralWinsThroughToAttributes(t *testing.T) {
	in := map[string]any{
		"contact.snapshot":      map[string]any{"code": "MIDDLE"},
		"contact.snapshot.code": "LITERAL",
	}

	registry := newSnapshotStubRegistry()
	records := toEAV(t, registry, uuid.Must(uuid.NewV7()), NormalizeDottedKeys(in, snapshotCache()))

	requireNoDuplicatePK(t, records)
	require.Len(t, records, 1)
	require.NotNil(t, records[0].ValueText)
	require.Equal(t, "LITERAL", *records[0].ValueText)
}

// TestNormalizeExpandsDottedKeyNamingParentPath pins that a dotted key naming an
// interior path is expanded too. Left flat, JSON Schema sees "contact.snapshot"
// as an unknown property and never inspects the object under it, so a wrongly
// typed "code" is invisible to validation — the exact bypass #314 closes.
func TestNormalizeExpandsDottedKeyNamingParentPath(t *testing.T) {
	out := NormalizeDottedKeys(map[string]any{
		"contact.snapshot": map[string]any{"code": 99999},
	}, snapshotCache())

	require.Equal(t, map[string]any{
		"contact": map[string]any{"snapshot": map[string]any{"code": 99999}},
	}, out)
}

// TestNormalizeExpandsInsideArrayElements pins that array elements are walked.
// flattenToAttributes recurses into []any, so a dotted key inside an element
// reaches storage as tags.a.b; skipping arrays would leave the bypass open for
// exactly the array-of-object attributes it matters for (orders.items.price).
func TestNormalizeExpandsInsideArrayElements(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"tags.a.b": {AttributeName: "tags.a.b", AttributeID: 30, ValueType: forma.ValueTypeNumeric},
	}

	out := NormalizeDottedKeys(map[string]any{"tags": []any{map[string]any{"a.b": 1}}}, cache)

	require.Equal(t, map[string]any{
		"tags": []any{map[string]any{"a": map[string]any{"b": 1}}},
	}, out)
}

// TestNormalizeKeepsScalarParentRejectable pins that a type error stays a type
// error. {"contact":"SCALAR"} is rejected today because "contact" is not a
// defined attribute; expanding the sibling literal over it would overwrite the
// scalar, turn the 400 into a 200, and silently drop the caller's value. When an
// intermediate segment already holds a non-map, the literal is left unexpanded
// so the existing rejection still fires.
func TestNormalizeKeepsScalarParentRejectable(t *testing.T) {
	in := map[string]any{
		"contact":       "SCALAR",
		"contact.email": "x",
	}

	out := NormalizeDottedKeys(in, snapshotCache())
	require.Equal(t, map[string]any{"contact": "SCALAR", "contact.email": "x"}, out)

	transformer := NewTransformer(newSnapshotStubRegistry())
	_, err := transformer.ToAttributes(context.Background(), 100, uuid.Must(uuid.NewV7()), out)
	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.True(t, errors.Is(err, forma.ErrInvalidInput))
}

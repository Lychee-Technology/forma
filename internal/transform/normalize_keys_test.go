package transform

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/stretchr/testify/require"
)

// A nil ArrayPaths below means "this schema declares no arrays", which is the
// accurate set for a fixture that contains none. Fixtures that do contain an
// array pass one explicitly, so the array rules are live wherever they could
// change the outcome; the derived-from-schema cases live in
// normalize_keys_arrays_test.go.

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
	out := NormalizeDottedKeys(map[string]any{"contact.email": "x"}, dottedCache(), nil)
	require.Equal(t, map[string]any{"contact": map[string]any{"email": "x"}}, out)
}

// TestNormalizeExpandsDottedKeyNamingParentPath pins that a dotted key naming an
// interior path is expanded too. Left flat, JSON Schema sees "contact.snapshot"
// as an unknown property and never inspects the object under it, so a wrongly
// typed "code" is invisible to validation — the same bypass, one level up.
func TestNormalizeExpandsDottedKeyNamingParentPath(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"contact.snapshot.code": {AttributeID: 20, ValueType: forma.ValueTypeText},
	}

	out := NormalizeDottedKeys(map[string]any{
		"contact.snapshot": map[string]any{"code": 99999},
	}, cache, nil)

	require.Equal(t, map[string]any{
		"contact": map[string]any{"snapshot": map[string]any{"code": 99999}},
	}, out)
}

// TestNormalizeExpandsNestedDottedKey pins that a dotted key is expanded even
// when it appears below the root.
func TestNormalizeExpandsNestedDottedKey(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"a.b.c": {AttributeID: 4, ValueType: forma.ValueTypeText},
	}
	out := NormalizeDottedKeys(map[string]any{"a": map[string]any{"b.c": "v"}}, cache, nil)
	require.Equal(t, map[string]any{
		"a": map[string]any{"b": map[string]any{"c": "v"}},
	}, out)
}

// TestNormalizeExpandsInsideArrayElements pins that array elements are walked.
// flattenToAttributes recurses into []any, so a dotted key inside an element
// names a real attribute; skipping arrays would leave the bypass open for
// exactly the array-of-object attributes it matters for (orders.items.price).
func TestNormalizeExpandsInsideArrayElements(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"tags.a.b": {AttributeID: 30, ValueType: forma.ValueTypeNumeric},
	}
	// "tags" is declared an array, which is the whole point: being inside one of
	// its elements must not disable expansion. Passing nil here would make the
	// test blind to that, since nil means "nothing is an array".
	arrays := schemavalidate.ArrayPaths{"tags": {}}

	out := NormalizeDottedKeys(map[string]any{"tags": []any{map[string]any{"a.b": 1}}}, cache, arrays)

	require.Equal(t, map[string]any{
		"tags": []any{map[string]any{"a": map[string]any{"b": 1}}},
	}, out)
}

// TestNormalizeLiteralWinsOverNested pins last-spelling-wins, matching
// encoding/json duplicate-key semantics and the writer's own rule. On the update
// path the literal key is the caller's explicit value while the nested one was
// rebuilt from storage, so the validator must see the literal.
func TestNormalizeLiteralWinsOverNested(t *testing.T) {
	in := map[string]any{
		"contact":       map[string]any{"email": "old"},
		"contact.email": "x",
	}
	out := NormalizeDottedKeys(in, dottedCache(), nil)
	require.Equal(t, map[string]any{"contact": map[string]any{"email": "x"}}, out)
}

// TestNormalizeThreeSegmentLiteralWins pins precedence for an attribute deeper
// than two segments. Expanding moves a literal to the sort position of its first
// segment, so an intermediate spelling must be expanded as well; otherwise the
// two land in different places and the validator sees the wrong value.
func TestNormalizeThreeSegmentLiteralWins(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"contact.snapshot.code": {AttributeID: 20, ValueType: forma.ValueTypeText},
	}

	out := NormalizeDottedKeys(map[string]any{
		"contact.snapshot":      map[string]any{"code": "MIDDLE"},
		"contact.snapshot.code": "LITERAL",
	}, cache, nil)

	require.Equal(t, map[string]any{
		"contact": map[string]any{"snapshot": map[string]any{"code": "LITERAL"}},
	}, out)
}

// TestNormalizePreservesSiblings pins that expanding one leaf does not discard
// other properties of the same parent object — they would be written but never
// validated.
func TestNormalizePreservesSiblings(t *testing.T) {
	cache := dottedCache()
	cache["contact.phone"] = forma.AttributeMetadata{AttributeID: 9, ValueType: forma.ValueTypeText}

	out := NormalizeDottedKeys(map[string]any{
		"contact":       map[string]any{"phone": "555", "email": "old"},
		"contact.email": "x",
	}, cache, nil)

	require.Equal(t, map[string]any{
		"contact": map[string]any{"phone": "555", "email": "x"},
	}, out)
}

// TestNormalizeMergesSiblingsAtDepth pins that the merge is recursive, not one
// level deep. Two spellings can meet above the depth where they diverge: here
// both hold an object at "deep", and only under it do they name different
// attributes. The writer stores both, so a shallow merge would hand one of them
// to storage without the validator ever seeing it.
//
// Four segments are required to show it: with three, the two spellings meet at
// the object whose keys are the attributes themselves, so assigning and merging
// are indistinguishable (verified with a shallow-merge mutant).
func TestNormalizeMergesSiblingsAtDepth(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"contact.snapshot.deep.a": {AttributeID: 50, ValueType: forma.ValueTypeText},
		"contact.snapshot.deep.b": {AttributeID: 51, ValueType: forma.ValueTypeText},
	}

	out := NormalizeDottedKeys(map[string]any{
		"contact":          map[string]any{"snapshot": map[string]any{"deep": map[string]any{"a": "A"}}},
		"contact.snapshot": map[string]any{"deep": map[string]any{"b": "B"}},
	}, cache, nil)

	require.Equal(t, map[string]any{
		"contact": map[string]any{
			"snapshot": map[string]any{"deep": map[string]any{"a": "A", "b": "B"}},
		},
	}, out)
}

// TestNormalizeLeavesUnknownDottedKeyAlone pins that only names the metadata
// cache knows are expanded. An unknown dotted key stays put so the writer's
// "attribute is not defined for schema" error still fires with its own message.
func TestNormalizeLeavesUnknownDottedKeyAlone(t *testing.T) {
	out := NormalizeDottedKeys(map[string]any{"nope.missing": 1}, dottedCache(), nil)
	require.Equal(t, map[string]any{"nope.missing": 1}, out)
}

// TestNormalizeIsDeterministic pins that the result does not depend on Go map
// iteration order. Last-spelling-wins is a consequence of sorting each map's
// keys, not of map order, so a change that drops the sort would silently break
// precedence. Run repeatedly because such a dependency is flaky by nature.
func TestNormalizeIsDeterministic(t *testing.T) {
	in := map[string]any{
		"contact":       map[string]any{"email": "old"},
		"contact.email": "x",
	}
	for i := 0; i < 200; i++ {
		out := NormalizeDottedKeys(in, dottedCache(), nil)
		require.Equal(t, map[string]any{"contact": map[string]any{"email": "x"}}, out)
	}
}

// TestNormalizeNormalizingIsPure states the guarantee this package can actually
// make: calling NormalizeDottedKeys on a payload leaves that payload producing
// exactly the records it produced before, at the level of stored records rather
// than map equality.
//
// It does not — and cannot — detect the writer being handed the normalized
// document instead. Both sides here are computed from the caller's own map and
// the return value is discarded, so nothing in this package observes what a
// service chooses to pass along. That miswiring is a service-layer concern and
// belongs to Task 5's tests.
//
// The fixture uses the shapes normalization rewrites most aggressively — both
// spellings of one attribute, a shrunk list, an array on an expansion path —
// because purity is only interesting where the rewriting is real.
func TestNormalizeNormalizingIsPure(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"contact.email":      {AttributeName: "contact.email", AttributeID: 10, ValueType: forma.ValueTypeText},
		"contact.tags":       {AttributeName: "contact.tags", AttributeID: 11, ValueType: forma.ValueTypeList},
		"contact.snapshot.a": {AttributeName: "contact.snapshot.a", AttributeID: 12, ValueType: forma.ValueTypeText},
		"contact.snapshot.b": {AttributeName: "contact.snapshot.b", AttributeID: 13, ValueType: forma.ValueTypeText},
	}
	registry := &stubSchemaRegistry{schemaID: 100, schemaName: "test", cache: cache}
	rowID := uuid.Must(uuid.NewV7())

	payload := func() map[string]any {
		return map[string]any{
			"contact": map[string]any{
				"email":    "old",
				"tags":     []any{"x", "y"},
				"snapshot": []any{map[string]any{"a": "FROM-ARRAY", "b": "SIBLING"}},
			},
			"contact.email":      "new",
			"contact.tags":       []any{"z"},
			"contact.snapshot.a": "LITERAL",
		}
	}

	// The fixture's own arrays, so purity is asserted with the array rules live
	// rather than switched off.
	arrays := schemavalidate.ArrayPaths{"contact.tags": {}, "contact.snapshot": {}}

	before := toEAV(t, registry, rowID, payload())

	shared := payload()
	NormalizeDottedKeys(shared, cache, arrays)
	after := toEAV(t, registry, rowID, shared)

	require.ElementsMatch(t, before, after,
		"normalization must not change any record the writer produces")
}

// TestNormalizeDoesNotMutateInput pins that the caller's map is untouched, all
// the way down — it is the same map the writer is handed, so a stray write here
// would corrupt what gets stored.
//
// A flat-only input cannot fail this test at all, because the only writes the
// implementation could make are into containers it did not copy. So the fixture
// is three levels deep and includes an array element, and each depth is asserted
// separately: an aliasing mutant confined to array elements passes a depth-1
// assertion. Equality alone is not enough either — a shared map that has not been
// written to yet still compares equal — so the pointer assertions are what make
// the copying provable.
func TestNormalizeDoesNotMutateInput(t *testing.T) {
	element := map[string]any{"kind": "mobile"}
	snapshot := map[string]any{"code": "old"}
	nested := map[string]any{
		"phone":    "555",
		"snapshot": snapshot,
		"phones":   []any{element},
	}
	in := map[string]any{
		"contact":               nested,
		"contact.snapshot.code": "x",
	}

	cache := forma.SchemaAttributeCache{
		"contact.phone":         {AttributeID: 1, ValueType: forma.ValueTypeText},
		"contact.snapshot.code": {AttributeID: 2, ValueType: forma.ValueTypeText},
		"contact.phones.kind":   {AttributeID: 3, ValueType: forma.ValueTypeText},
	}
	// contact.phones is an array, but it is not on contact.snapshot.code's path:
	// an unrelated array must not suppress expansion.
	out := NormalizeDottedKeys(in, cache, schemavalidate.ArrayPaths{"contact.phones": {}})

	require.Equal(t, map[string]any{
		"contact": map[string]any{
			"phone":    "555",
			"snapshot": map[string]any{"code": "old"},
			"phones":   []any{map[string]any{"kind": "mobile"}},
		},
		"contact.snapshot.code": "x",
	}, in, "caller's map must be unchanged, all the way down")

	outContact := requireChildMap(t, out, "contact")
	outSnapshot := requireChildMap(t, outContact, "snapshot")
	outPhones, ok := outContact["phones"].([]any)
	require.True(t, ok)
	require.Len(t, outPhones, 1)
	outElement, ok := outPhones[0].(map[string]any)
	require.True(t, ok)

	requireNotAliased(t, "contact", nested, outContact)
	requireNotAliased(t, "contact.snapshot", snapshot, outSnapshot)
	requireNotAliased(t, "contact.phones[0]", element, outElement)

	outSnapshot["code"] = "mutated through the output"
	outElement["kind"] = "mutated through the output"
	require.Equal(t, "old", snapshot["code"], "writes through the output must not reach the input")
	require.Equal(t, "mobile", element["kind"], "writes through the output must not reach input array elements")
}

func requireChildMap(t *testing.T, parent map[string]any, key string) map[string]any {
	t.Helper()

	child, ok := parent[key].(map[string]any)
	require.Truef(t, ok, "expected %q to be an object", key)
	return child
}

// requireNotAliased compares map headers directly; testify's NotSame only
// accepts pointers, and maps are references rather than pointer values.
func requireNotAliased(t *testing.T, label string, input, output map[string]any) {
	t.Helper()

	require.NotEqualf(t,
		reflect.ValueOf(input).Pointer(),
		reflect.ValueOf(output).Pointer(),
		"output map at %s must not alias the input's", label)
}

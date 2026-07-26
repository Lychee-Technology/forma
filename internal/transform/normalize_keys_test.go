package transform

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
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
// A flat-only input cannot fail this test at all, because the only writes the
// implementation could make to the caller's data are into containers it did not
// copy. So the fixture is three levels deep and includes an array element: every
// container the implementation walks is a place it could hand back the caller's
// own map. Each depth is asserted separately — an aliasing mutant confined to
// array elements passes a depth-1 assertion.
//
// Equality alone is also not enough: a shared map that simply has not been
// written to yet still compares equal. The pointer assertions are what make the
// copying provable.
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
	out := NormalizeDottedKeys(in, cache)

	require.Equal(t, map[string]any{
		"contact": map[string]any{
			"phone":    "555",
			"snapshot": map[string]any{"code": "old"},
			"phones":   []any{map[string]any{"kind": "mobile"}},
		},
		"contact.snapshot.code": "x",
	}, in, "caller's map must be unchanged, all the way down")

	// Aliasing assertions: maps are references, so writing through the output must
	// not be visible in the input. reflect gives the map headers directly; testify's
	// NotSame only accepts pointers.
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

func requireNotAliased(t *testing.T, label string, input, output map[string]any) {
	t.Helper()

	require.NotEqualf(t,
		reflect.ValueOf(input).Pointer(),
		reflect.ValueOf(output).Pointer(),
		"output map at %s must not alias the input's", label)
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

func stubRegistryFor(cache forma.SchemaAttributeCache) forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: 100, schemaName: "test", cache: cache}
}

func newSnapshotStubRegistry() forma.SchemaRegistry {
	return stubRegistryFor(snapshotCache())
}

// requireNormalizationParity asserts that normalizing before the write path
// produces exactly the records the write path produces without it.
//
// The un-normalized run is the behaviour shipping today — flattenToAttributes
// handles both spellings and #312's dedupe resolves the collisions — so it is
// the baseline any reshaping must reproduce. Asserting the normalizer's output
// shape alone cannot catch a reshaping that quietly drops an attribute, because
// the shape it produces is the shape the assertion was written from.
func requireNormalizationParity(
	t *testing.T,
	cache forma.SchemaAttributeCache,
	data map[string]any,
) []model.EAVRecord {
	t.Helper()

	registry := stubRegistryFor(cache)
	rowID := uuid.Must(uuid.NewV7())

	baseline := toEAV(t, registry, rowID, data)
	normalized := toEAV(t, registry, rowID, NormalizeDottedKeys(data, cache))

	require.ElementsMatch(t, baseline, normalized,
		"normalizing must not change which records reach storage")
	return normalized
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

// itemsCache has two sub-attributes under one array-valued interior path, the
// shape where an array must merge rather than replace: array indices are part
// of the EAV primary key, so contact.items.a at indices 0 and 1 and
// contact.items.b at index 0 are three distinct rows.
func itemsCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"contact.items.a": {AttributeName: "contact.items.a", AttributeID: 40, ValueType: forma.ValueTypeText},
		"contact.items.b": {AttributeName: "contact.items.b", AttributeID: 41, ValueType: forma.ValueTypeText},
	}
}

// TestNormalizeArrayValuedInteriorPathKeepsSiblingAttributes pins that two
// arrays merge element-wise instead of one replacing the other.
//
// The literal names only contact.items.b, so replacing would wipe
// contact.items.a — a 200 with the caller's other attribute gone. This is the
// merge-update shape: storage rebuilds contact.items nested and the caller
// PATCHes a literal naming one sub-attribute. Merging by index reproduces the
// flattener exactly, because an index is part of the row identity.
func TestNormalizeArrayValuedInteriorPathKeepsSiblingAttributes(t *testing.T) {
	records := requireNormalizationParity(t, itemsCache(), map[string]any{
		"contact":       map[string]any{"items": []any{map[string]any{"a": "A0"}, map[string]any{"a": "A1"}}},
		"contact.items": []any{map[string]any{"b": "B0"}},
	})

	requireNoDuplicatePK(t, records)
	require.Len(t, records, 3)
	require.Equal(t, "A0", *findRecord(t, records, 40, "0").ValueText)
	require.Equal(t, "A1", *findRecord(t, records, 40, "1").ValueText)
	require.Equal(t, "B0", *findRecord(t, records, 41, "0").ValueText)
}

// TestNormalizeDeepRecursiveMergeKeepsBothAttributes pins that merging is
// recursive, not one level deep. Two spellings can diverge below the depth where
// they meet: here both hold an object at "deep", and only under it do they name
// different attributes — both of which the flattener writes. A shallow merge
// assigns the incoming "deep" wholesale and loses contact.snapshot.deep.a.
//
// Four segments are required to show it: with three, the two spellings meet at
// the object whose keys are the attributes themselves, so assigning and merging
// are indistinguishable. Verified — a shallow-merge mutant leaves the
// three-segment version green.
func TestNormalizeDeepRecursiveMergeKeepsBothAttributes(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"contact.snapshot.deep.a": {AttributeName: "contact.snapshot.deep.a", AttributeID: 50, ValueType: forma.ValueTypeText},
		"contact.snapshot.deep.b": {AttributeName: "contact.snapshot.deep.b", AttributeID: 51, ValueType: forma.ValueTypeText},
	}

	records := requireNormalizationParity(t, cache, map[string]any{
		"contact":          map[string]any{"snapshot": map[string]any{"deep": map[string]any{"a": "A"}}},
		"contact.snapshot": map[string]any{"deep": map[string]any{"b": "B"}},
	})

	require.Len(t, records, 2)
	require.Equal(t, "A", *findRecord(t, records, 50, "").ValueText)
	require.Equal(t, "B", *findRecord(t, records, 51, "").ValueText)
}

// TestNormalizeScalarLeafOnInteriorPathStaysRejectable is the scalar-parent rule
// one level deeper: the non-map sits at the last segment of the expansion rather
// than an intermediate one. "contact.snapshot" is an interior path — the schema
// defines contact.snapshot.code, not contact.snapshot — so a scalar there is a
// type error the flattener rejects. Expanding an object over it would turn that
// 400 into a 200 with "SCALAR" silently dropped.
func TestNormalizeScalarLeafOnInteriorPathStaysRejectable(t *testing.T) {
	in := map[string]any{
		"contact":          map[string]any{"snapshot": "SCALAR"},
		"contact.snapshot": map[string]any{"code": "X"},
	}

	out := NormalizeDottedKeys(in, snapshotCache())
	require.Equal(t, map[string]any{
		"contact":          map[string]any{"snapshot": "SCALAR"},
		"contact.snapshot": map[string]any{"code": "X"},
	}, out)

	transformer := NewTransformer(newSnapshotStubRegistry())
	_, err := transformer.ToAttributes(context.Background(), 100, uuid.Must(uuid.NewV7()), out)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestNormalizeKeepsScalarUnderContainerLiteralRejectable pins the other half of
// the scalar guard: the non-map is at the last segment and the expansion would
// put a container over it. There is no payload this combination accepts today —
// a scalar under a list attribute is rejected as "requires an array value", and
// a map over a scalar attribute recurses to an undefined name — so expanding
// would convert a 400 into a 200 that drops the scalar. The cache is not
// consulted: whether the name is a defined attribute does not change that.
func TestNormalizeKeepsScalarUnderContainerLiteralRejectable(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"contact.email": {AttributeName: "contact.email", AttributeID: 60, ValueType: forma.ValueTypeList},
	}

	in := map[string]any{
		"contact":       map[string]any{"email": "old"},
		"contact.email": []any{"a", "b"},
	}

	out := NormalizeDottedKeys(in, cache)
	require.Equal(t, in, out, "the literal must stay flat so the scalar is still flattened and rejected")

	transformer := NewTransformer(stubRegistryFor(cache))
	_, err := transformer.ToAttributes(context.Background(), 100, uuid.Must(uuid.NewV7()), out)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestNormalizeExpandsThroughArrayOnPath pins that an array sitting on the path
// does not block expansion when replacing it loses nothing. Left flat, the
// literal is an unknown property that the flattener still accepts — so unlike
// the scalar cases there is no 400 backstop, and the value reaches storage
// without JSON Schema ever inspecting it (#314).
func TestNormalizeExpandsThroughArrayOnPath(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"contact.snapshot.a": {AttributeName: "contact.snapshot.a", AttributeID: 70, ValueType: forma.ValueTypeText},
	}

	records := requireNormalizationParity(t, cache, map[string]any{
		"contact":            map[string]any{"snapshot": []any{map[string]any{"a": "FROM-ARRAY"}}},
		"contact.snapshot.a": "LITERAL",
	})

	require.Len(t, records, 1)
	require.Equal(t, "LITERAL", *findRecord(t, records, 70, "").ValueText)

	out := NormalizeDottedKeys(map[string]any{
		"contact":            map[string]any{"snapshot": []any{map[string]any{"a": "FROM-ARRAY"}}},
		"contact.snapshot.a": "LITERAL",
	}, cache)
	require.Equal(t, map[string]any{
		"contact": map[string]any{"snapshot": map[string]any{"a": "LITERAL"}},
	}, out)
}

// TestNormalizeKeepsArrayOnPathWhenExpansionWouldLoseData pins the deliberate
// limit of the rule above. Here the array element also carries
// contact.snapshot.b, which the flattener writes and which replacing the array
// would destroy; JSON cannot hold both an array and an object at one key, so the
// two spellings are irreconcilable by reshaping. Keeping the caller's data wins
// over closing the bypass, and the flattener's own dedupe still resolves the
// payload exactly as it does today.
func TestNormalizeKeepsArrayOnPathWhenExpansionWouldLoseData(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"contact.snapshot.a": {AttributeName: "contact.snapshot.a", AttributeID: 70, ValueType: forma.ValueTypeText},
		"contact.snapshot.b": {AttributeName: "contact.snapshot.b", AttributeID: 71, ValueType: forma.ValueTypeText},
	}

	records := requireNormalizationParity(t, cache, map[string]any{
		"contact":            map[string]any{"snapshot": []any{map[string]any{"a": "FROM-ARRAY", "b": "SIBLING"}}},
		"contact.snapshot.a": "LITERAL",
	})

	require.Len(t, records, 2)
	require.Equal(t, "LITERAL", *findRecord(t, records, 70, "").ValueText)
	require.Equal(t, "SIBLING", *findRecord(t, records, 71, "0").ValueText)
}

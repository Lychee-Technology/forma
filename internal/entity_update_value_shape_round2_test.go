package internal

import (
	"context"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regressions of #696 review round 2 over the in-memory repository: a
// pointer across a nested $id is followed, so a referenced scalar takes no
// array and a referenced array keeps its own (R2-F1); two attribute paths
// that print alike are judged apart (R2-F2); and an object-or-array union
// keeps its array updates (R2-F3), which read back as written (R3-F2).

// nestedIDDocument reaches note, a scalar, and codes, a primitive array
// under an element-typed (pre-#204) ledger entry, through pointers that
// cross inner's $id into a oneOf.
const nestedIDDocument = `{"type":"object","$defs":{"inner":{"$id":"inner.json","properties":{
	"x":{"oneOf":[{"type":"string"},{"type":"integer"}]},
	"y":{"oneOf":[{"type":"array","items":{"type":"string"}},{"type":"integer"}]}}}},"properties":{
	"id":{"type":"string"},
	"note":{"$ref":"#/$defs/inner/properties/x/oneOf/0"},
	"codes":{"$ref":"#/$defs/inner/properties/y/oneOf/0"}}}`

const nestedIDAttributes = `{"id":{"attributeID":1,"valueType":"text"},
	"note":{"attributeID":2,"valueType":"text"},"codes":{"attributeID":3,"valueType":"text"}}`

// TestUpdateJudgesValuesReferencedAcrossANestedID pins R2-F1: in both
// modes, on every surface, an array at the referenced scalar note is
// refused and the stored records stay as they were, an extra array around
// the referenced array codes too, while codes takes a new array and note a
// new single value.
func TestUpdateJudgesValuesReferencedAcrossANestedID(t *testing.T) {
	for _, mode := range []string{"strict", "report-only"} {
		for _, surface := range updateSurfaces {
			t.Run(mode+"/"+surface.name, func(t *testing.T) {
				env := newShapeEnvFor(t, mode, "nestedid", nestedIDDocument, nestedIDAttributes)
				rowID := env.create(t, map[string]any{"note": "n", "codes": []any{"c1"}})
				before := env.stored(rowID)
				for _, tc := range []struct {
					updates map[string]any
					attr    string
				}{
					{map[string]any{"note": []any{"x"}}, "note"},
					{map[string]any{"note": []any{}}, "note"},
					{map[string]any{"codes": []any{[]any{"c"}}}, "codes"},
				} {
					_, err := surface.update(context.Background(), env.em, env.update(rowID, tc.updates))
					assertShapeRefused(t, err, tc.attr)
					assert.Equal(t, before, env.stored(rowID), "%v", tc.updates)
				}

				_, err := surface.update(context.Background(), env.em, env.update(rowID, map[string]any{"codes": []any{"c2", "c3"}, "note": "m"}))
				require.NoError(t, err)
				got := env.get(t, rowID)
				assertEqualValue(t, "codes", []any{"c2", "c3"}, got["codes"])
				assertEqualValue(t, "note", "m", got["note"])
			})
		}
	}
}

// spacedDocuments declares two paths whose segments print alike when
// joined by spaces: "a" / "b c" and "a b" / "c", both ways round: the first
// path a primitive array under an element-typed ledger entry and the second
// a scalar, and the reverse.
func spacedDocuments() map[string]string {
	array := `{"type":"array","items":{"type":"string"}}`
	scalar := `{"type":"string"}`
	of := func(first, second string) string {
		return `{"type":"object","properties":{"id":{"type":"string"},
			"a":{"type":"object","properties":{"b c":` + first + `}},
			"a b":{"type":"object","properties":{"c":` + second + `}}}}`
	}
	return map[string]string{"first an array": of(array, scalar), "second an array": of(scalar, array)}
}

// spacedRegistry serves the file registry's schema with the attributes
// "a.b c" and "a b.c" beside its own. The file registry refuses the pair,
// since both fold to one parquet column, so this is a registry that does
// not: the rule must still judge them apart.
type spacedRegistry struct{ forma.SchemaRegistry }

func (r spacedRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	id, cache, err := r.SchemaRegistry.GetSchemaAttributeCacheByName(name)
	return id, withSpacedAttributes(cache), err
}

func (r spacedRegistry) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	name, cache, err := r.SchemaRegistry.GetSchemaAttributeCacheByID(id)
	return name, withSpacedAttributes(cache), err
}

func withSpacedAttributes(cache forma.SchemaAttributeCache) forma.SchemaAttributeCache {
	if cache == nil {
		return nil
	}
	out := make(forma.SchemaAttributeCache, len(cache)+2)
	for name, meta := range cache {
		out[name] = meta
	}
	for id, name := range map[int16]string{2: "a.b c", 3: "a b.c"} {
		out[name] = forma.AttributeMetadata{AttributeName: name, AttributeID: id, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}}
	}
	return out
}

// TestUpdateJudgesPathsThatPrintAlikeApart pins R2-F2: an update with
// arrays at both paths is refused at the scalar one, whichever it is, in
// both modes and on every surface, leaving the stored records unchanged,
// while the declared array alone is written.
func TestUpdateJudgesPathsThatPrintAlikeApart(t *testing.T) {
	wrap := func(r forma.SchemaRegistry) forma.SchemaRegistry { return spacedRegistry{r} }
	for order, document := range spacedDocuments() {
		arrayAt, scalarAt := []string{"a", "b c"}, "a b.c"
		if order == "second an array" {
			arrayAt, scalarAt = []string{"a b", "c"}, "a.b c"
		}
		for _, mode := range []string{"strict", "report-only"} {
			for _, surface := range updateSurfaces {
				t.Run(order+"/"+mode+"/"+surface.name, func(t *testing.T) {
					env := newShapeEnvWith(t, mode, "spaced", document, `{"id":{"attributeID":1,"valueType":"text"}}`, wrap)
					rowID := env.create(t, map[string]any{arrayAt[0]: map[string]any{arrayAt[1]: []any{"w"}}})
					before := env.stored(rowID)
					require.Len(t, before, 2, "precondition: id and the declared array's element are stored")
					both := map[string]any{
						"a":   map[string]any{"b c": []any{"x"}},
						"a b": map[string]any{"c": []any{"y"}},
					}
					_, err := surface.update(context.Background(), env.em, env.update(rowID, both))
					assertShapeRefused(t, err, scalarAt)
					assert.Equal(t, before, env.stored(rowID))

					declared := map[string]any{arrayAt[0]: map[string]any{arrayAt[1]: []any{"x", "z"}}}
					_, err = surface.update(context.Background(), env.em, env.update(rowID, declared))
					require.NoError(t, err)
					assertEqualValue(t, "the declared array", []any{"x", "z"}, valueAtKeys(env.get(t, rowID), arrayAt))
				})
			}
		}
	}
}

// valueAtKeys answers the value at keys, one map level per key.
func valueAtKeys(doc map[string]any, keys []string) any {
	var at any = doc
	for _, key := range keys {
		m, ok := at.(map[string]any)
		if !ok {
			return nil
		}
		at = m[key]
	}
	return at
}

// unionDocument declares office by properties beside an anyOf admitting an
// object or one array of objects. The registry derives it an array of
// objects (#696 review R3-F2), so office.city has office for its array
// ancestor, and a record of either form reads back in the form it was
// written in.
const unionDocument = `{"type":"object","properties":{"id":{"type":"string"},"note":{"type":"string"},
	"office":{"properties":{"city":{"type":"string"}},"anyOf":[{"type":"object"},
		{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]}}}`

const unionAttributes = `{"id":{"attributeID":1,"valueType":"text"},"note":{"attributeID":2,"valueType":"text"},
	"office.city":{"attributeID":3,"valueType":"text"}}`

// TestUpdateKeepsTheArrayOfAnObjectOrArrayUnionWithDerivedAncestry pins
// R2-F3 and R3-F2: an update in either mode and without a validator, on
// every surface, writes the array of objects the schema admits, as a create
// does, and reads it back as that array, not as an array at the scalar
// member (R3-F2: Get answered {"city": ["o2"]}); the object form is written
// and read back as an object. An array at the scalar member, an extra array
// around the union and an array at the scalar note stay refused with the
// row unchanged.
func TestUpdateKeepsTheArrayOfAnObjectOrArrayUnionWithDerivedAncestry(t *testing.T) {
	for _, mode := range []string{"strict", "report-only", "no validator"} {
		for _, surface := range updateSurfaces {
			t.Run(mode+"/"+surface.name, func(t *testing.T) {
				env := newShapeEnvFor(t, mode, "union", unionDocument, unionAttributes)
				require.Equal(t, []string{"office"}, env.cache["office.city"].ArrayAncestors, "precondition: office derived an array of objects")
				rowID := env.create(t, map[string]any{"note": "n", "office": []any{map[string]any{"city": "o1"}}})
				assertEqualValue(t, "office as created", []any{map[string]any{"city": "o1"}}, env.get(t, rowID)["office"])

				for _, office := range []any{
					[]any{map[string]any{"city": "o2"}},
					[]any{map[string]any{"city": "o4"}, map[string]any{"city": "o5"}},
				} {
					got, err := surface.update(context.Background(), env.em, env.update(rowID, map[string]any{"office": office}))
					require.NoError(t, err, "the union admits an array of objects")
					assertEqualValue(t, "office as answered", office, got["office"])
					assertEqualValue(t, "office as read back", office, env.get(t, rowID)["office"])
				}
				assert.Equal(t, []string{"3[0]=o4", "3[1]=o5"}, recordsOf(env.stored(rowID), "3["), "stored as a create stores it")

				got, err := surface.update(context.Background(), env.em, env.update(rowID, map[string]any{"office": map[string]any{"city": "o3"}}))
				require.NoError(t, err, "the union admits an object")
				assertEqualValue(t, "office as answered", map[string]any{"city": "o3"}, got["office"])
				assertEqualValue(t, "office as read back", map[string]any{"city": "o3"}, env.get(t, rowID)["office"])
				assert.Equal(t, []string{"3[]=o3"}, recordsOf(env.stored(rowID), "3["), "the object form stores no index")

				before := env.stored(rowID)
				for _, tc := range []struct {
					updates map[string]any
					attr    string
				}{
					{map[string]any{"office": []any{map[string]any{"city": []any{"x"}}}}, "office.city"},
					{map[string]any{"office": map[string]any{"city": []any{"x"}}}, "office.city"},
					{map[string]any{"office": []any{[]any{map[string]any{"city": "x"}}}}, "office.city"},
					{map[string]any{"note": []any{"x"}}, "note"},
				} {
					_, err := surface.update(context.Background(), env.em, env.update(rowID, tc.updates))
					assertShapeRefused(t, err, tc.attr)
					assert.Equal(t, before, env.stored(rowID), "%v", tc.updates)
				}
				assertEqualValue(t, "office after the refusals", map[string]any{"city": "o3"}, env.get(t, rowID)["office"])
			})
		}
	}
}

// recordsOf answers the records among stored whose spelling starts with
// prefix.
func recordsOf(stored []string, prefix string) []string {
	var out []string
	for _, record := range stored {
		if strings.HasPrefix(record, prefix) {
			out = append(out, record)
		}
	}
	return out
}

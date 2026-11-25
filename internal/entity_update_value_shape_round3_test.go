package internal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regressions of #696 review round 3 over the in-memory repository: a
// definition named like a subschema the skeleton moves is not overwritten
// by it (R3-F1), and an array placement the read would not place back is
// refused with a skeleton as without one (R3-F2).

// collidingDefsDocument defines a string that note references and an array
// of strings that codes references, under the names of the keys the
// skeleton moves arrays' items and tags' oneOf to. Before R3-F1, the moved
// subschemas replaced both: note admitted an array, and codes refused its
// own.
const collidingDefsDocument = `{"type":"object","$defs":{
	"forma-shape/properties/arrays/items":{"type":"string"},
	"forma-shape/properties/tags/oneOf/0":{"type":"array","items":{"type":"string"}}},
	"properties":{"id":{"type":"string"},
	"note":{"$ref":"#/$defs/forma-shape~1properties~1arrays~1items"},
	"codes":{"$ref":"#/$defs/forma-shape~1properties~1tags~1oneOf~10"},
	"arrays":{"type":"array","items":{"type":"array","items":{"type":"string"}}},
	"tags":{"oneOf":[{"type":"string"},{"type":"integer"}]}}}`

const collidingDefsAttributes = `{"id":{"attributeID":1,"valueType":"text"},
	"note":{"attributeID":2,"valueType":"text"},"codes":{"attributeID":3,"valueType":"text"},
	"tags":{"attributeID":4,"valueType":"text"}}`

// TestUpdateJudgesDefinitionsNamedLikeMovedSubschemas pins R3-F1: in both
// modes, on every surface, an array at note, which references a string
// definition, is refused and the stored records stay as they were, while
// codes, which references an array definition, takes a new array that
// reads back as written.
func TestUpdateJudgesDefinitionsNamedLikeMovedSubschemas(t *testing.T) {
	for _, mode := range []string{"strict", "report-only"} {
		for _, surface := range updateSurfaces {
			t.Run(mode+"/"+surface.name, func(t *testing.T) {
				env := newShapeEnvFor(t, mode, "colliding", collidingDefsDocument, collidingDefsAttributes)
				rowID := env.create(t, map[string]any{"note": "n", "codes": []any{"c1"}, "tags": "t"})
				before := env.stored(rowID)
				for _, updates := range []map[string]any{
					{"note": []any{"bad"}},
					{"note": []any{}},
					{"note": []any{"bad"}, "codes": []any{"c2"}},
					{"tags": []any{"bad"}},
				} {
					_, err := surface.update(context.Background(), env.em, env.update(rowID, updates))
					attr := "note"
					if _, ok := updates["tags"]; ok {
						attr = "tags"
					}
					assertShapeRefused(t, err, attr)
					assert.Equal(t, before, env.stored(rowID), "%v", updates)
				}

				_, err := surface.update(context.Background(), env.em, env.update(rowID, map[string]any{"codes": []any{"c2", "c3"}, "note": "m"}))
				require.NoError(t, err, "codes references an array of strings")
				got := env.get(t, rowID)
				assertEqualValue(t, "codes", []any{"c2", "c3"}, got["codes"])
				assertEqualValue(t, "note", "m", got["note"])
			})
		}
	}
}

// nestedUnionDocument nests one object-or-array union, inner, inside
// another, outer. The registry derives both arrays of objects, so a record
// of outer.inner.city with one index reads it as outer's: the read places
// indices outermost first.
const nestedUnionDocument = `{"type":"object","properties":{"id":{"type":"string"},
	"outer":{"properties":{"inner":{"$ref":"#/$defs/inner"}},"anyOf":[{"type":"object"},
		{"type":"array","items":{"type":"object","properties":{"inner":{"$ref":"#/$defs/inner"}}}}]}},
	"$defs":{"inner":{"properties":{"city":{"type":"string"}},"anyOf":[{"type":"object"},
		{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]}}}`

const nestedUnionAttributes = `{"id":{"attributeID":1,"valueType":"text"},"outer.inner.city":{"attributeID":2,"valueType":"text"}}`

// TestUpdateRefusesAnArrayPlacementTheReadDoesNotPlaceBack pins R3-F2's
// rule where the schema and the read part: an array at inner alone, which
// the schema admits, would be stored with one index that the read gives to
// outer, so it is refused in every mode, with the row unchanged. Each form
// the read places back is written and read back as written: arrays at both
// unions, at outer alone, and at neither.
func TestUpdateRefusesAnArrayPlacementTheReadDoesNotPlaceBack(t *testing.T) {
	city := func(v string) map[string]any { return map[string]any{"city": v} }
	for _, mode := range []string{"strict", "report-only", "no validator"} {
		for _, surface := range updateSurfaces {
			t.Run(mode+"/"+surface.name, func(t *testing.T) {
				env := newShapeEnvFor(t, mode, "nestedunion", nestedUnionDocument, nestedUnionAttributes)
				require.Equal(t, []string{"outer", "outer.inner"}, env.cache["outer.inner.city"].ArrayAncestors, "precondition")
				rowID := env.create(t, map[string]any{"outer": map[string]any{"inner": city("c0")}})

				for _, outer := range []any{
					[]any{map[string]any{"inner": []any{city("c1"), city("c2")}}},
					[]any{map[string]any{"inner": city("c3")}},
					map[string]any{"inner": city("c4")},
				} {
					_, err := surface.update(context.Background(), env.em, env.update(rowID, map[string]any{"outer": outer}))
					require.NoError(t, err, "%v", outer)
					assertEqualValue(t, "outer as read back", outer, env.get(t, rowID)["outer"])
				}

				before := env.stored(rowID)
				_, err := surface.update(context.Background(), env.em, env.update(rowID,
					map[string]any{"outer": map[string]any{"inner": []any{city("x")}}}))
				assertShapeRefused(t, err, "outer.inner.city")
				assert.Equal(t, before, env.stored(rowID))
				assertEqualValue(t, "outer after the refusal", map[string]any{"inner": city("c4")}, env.get(t, rowID)["outer"])
			})
		}
	}
}

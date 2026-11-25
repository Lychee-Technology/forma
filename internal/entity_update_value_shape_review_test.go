package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regressions of #696 review round 1 over the in-memory repository: the
// skeleton keeps what excludes a container (R1-F1) and every array the
// schema declares through then/else and a $ref into a rewritten keyword
// (R1-F2); the dotted empty-list marker keeps its placement (R1-F3); the
// array-placement rule holds without a validator (R1-F4); and a Go
// container is refused where a single value belongs, while a Go scalar
// representation is not (R1-F5).

const reviewShapeSchema = "reviewshapes"

// reviewShapeDocument declares, besides id:
//   - note, text whose second branch admits only false (R1-F1), and status,
//     an enum;
//   - aliases, a primitive array under an element-typed ledger entry, with
//     then and else but no if (R1-F2), and picked, one reached through a
//     $ref into pick's oneOf (R1-F2);
//   - items, an array of objects with a scalar member name and a list member
//     tags (R1-F3, R1-F4);
//   - ref, a UUID.
//
// The registry derives every attribute's array ancestry.
const reviewShapeDocument = `{"type":"object","properties":{
	"id":{"type":"string"},
	"note":{"anyOf":[{"type":"string"},{"enum":[false]}]},
	"status":{"type":"string","enum":["a","b"]},
	"aliases":{"type":"array","items":{"type":"string"},"then":{"type":"string"},"else":{"type":"integer"}},
	"pick":{"oneOf":[{"type":"array","items":{"type":"string"}},{"type":"integer"}]},
	"picked":{"$ref":"#/properties/pick/oneOf/0"},
	"items":{"type":"array","items":{"type":"object","properties":{
		"name":{"type":"string"},"tags":{"type":"array","items":{"type":"string"}}}}},
	"ref":{"type":"string","format":"uuid"}}}`

const reviewShapeAttributes = `{
	"id":{"attributeID":1,"valueType":"text"},"note":{"attributeID":2,"valueType":"text"},
	"status":{"attributeID":3,"valueType":"text"},"aliases":{"attributeID":4,"valueType":"text"},
	"pick":{"attributeID":5,"valueType":"text"},"picked":{"attributeID":6,"valueType":"text"},
	"items.name":{"attributeID":7,"valueType":"text"},
	"items.tags":{"attributeID":8,"valueType":"list","items_type":"text"},
	"ref":{"attributeID":9,"valueType":"uuid"}}`

func newReviewShapeEnv(t *testing.T, mode string) valueShapeEnv {
	t.Helper()
	env := newShapeEnvFor(t, mode, reviewShapeSchema, reviewShapeDocument, reviewShapeAttributes)
	require.Equal(t, []string{"items"}, env.cache["items.tags"].ArrayAncestors, "precondition: array ancestry is derived")
	return env
}

// TestUpdateRefusesAnArrayWhereEveryBranchExcludesOne pins R1-F1: dropping
// enum must not leave the second branch of note admitting an array. In both
// modes, on every surface, ["new"] is refused and the row is unchanged,
// while report-only mode still admits a single value the schema's enum
// refuses, which storage represents.
func TestUpdateRefusesAnArrayWhereEveryBranchExcludesOne(t *testing.T) {
	for _, mode := range []string{"strict", "report-only"} {
		env := newReviewShapeEnv(t, mode)
		rowID := env.create(t, map[string]any{"note": "n", "status": "a"})
		before := env.stored(rowID)
		for _, surface := range updateSurfaces {
			for _, updates := range []map[string]any{{"note": []any{"new"}}, {"note": []any{}}, {"note": []any{false}}} {
				_, err := surface.update(context.Background(), env.em, env.update(rowID, updates))
				assertShapeRefused(t, err, "note")
				assert.Equal(t, before, env.stored(rowID), "%s/%s/%v", mode, surface.name, updates)
			}
		}

		_, err := env.em.Update(context.Background(), env.update(rowID, map[string]any{"status": "zzz", "note": false}))
		if mode == "strict" {
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			continue
		}
		require.NoError(t, err, "report-only admits a representable violation of enum")
		got := env.get(t, rowID)
		assert.Equal(t, "zzz", got["status"])
		assert.Equal(t, "false", got["note"])
	}
}

// TestUpdateWritesArraysDeclaredThroughIgnoredOrReferencedKeywords pins
// R1-F2: a primitive array under an element-typed ledger entry whose schema
// carries then and else without if, and one declared through a $ref into
// another property's oneOf, stay updatable in strict and report-only mode,
// while an array nested inside either is still refused.
func TestUpdateWritesArraysDeclaredThroughIgnoredOrReferencedKeywords(t *testing.T) {
	for _, mode := range []string{"strict", "report-only"} {
		for _, surface := range updateSurfaces {
			t.Run(mode+"/"+surface.name, func(t *testing.T) {
				env := newReviewShapeEnv(t, mode)
				rowID := env.create(t, map[string]any{"aliases": []any{"a"}, "picked": []any{"p"}, "pick": []any{"q"}})
				updates := map[string]any{"aliases": []any{"b", "c"}, "picked": []any{"r"}, "pick": []any{"s", "t"}}
				_, err := surface.update(context.Background(), env.em, env.update(rowID, updates))
				require.NoError(t, err)
				got := env.get(t, rowID)
				for _, attr := range []string{"aliases", "picked", "pick"} {
					assertEqualValue(t, attr, updates[attr], got[attr])
				}

				before := env.stored(rowID)
				for _, attr := range []string{"aliases", "picked"} {
					_, err := surface.update(context.Background(), env.em, env.update(rowID, map[string]any{attr: []any{[]any{"x"}}}))
					assertShapeRefused(t, err, attr)
					assert.Equal(t, before, env.stored(rowID))
				}
			})
		}
	}
}

// TestUpdateKeepsTheDottedEmptyListMarker pins R1-F3: beside an array of
// objects, a literal "items.tags": [] stores the list's empty-list marker
// without indices, a record apart from the array's elements, and the row
// reads it back under the literal key beside the array, with a validator in
// either mode and without one. Arrays sent for the scalar member, and for
// the list through the literal key, are still refused.
func TestUpdateKeepsTheDottedEmptyListMarker(t *testing.T) {
	for _, mode := range []string{"strict", "report-only", "no validator"} {
		for _, surface := range updateSurfaces {
			t.Run(mode+"/"+surface.name, func(t *testing.T) {
				env := newReviewShapeEnv(t, mode)
				rowID := env.create(t, map[string]any{"items": []any{map[string]any{"name": "a"}}})
				_, err := surface.update(context.Background(), env.em, env.update(rowID, map[string]any{"items.tags": []any{}}))
				require.NoError(t, err)
				assert.Equal(t, []string{"1[]=" + idOf(t, env, rowID), "7[0]=a", "8[]=<none>"}, env.stored(rowID),
					"the marker is a record of its own, without indices")
				got := env.get(t, rowID)
				assertEqualValue(t, "items", []any{map[string]any{"name": "a"}}, got["items"])
				assertEqualValue(t, "items.tags", []any{}, got["items.tags"])

				before := env.stored(rowID)
				for _, tc := range []struct {
					updates map[string]any
					attr    string
				}{
					{map[string]any{"items": []any{map[string]any{"name": []any{"x"}}}}, "items.name"},
					{map[string]any{"items.name": []any{"x"}}, "items.name"},
					{map[string]any{"items.tags": []any{"x"}}, "items.tags"},
				} {
					_, err := surface.update(context.Background(), env.em, env.update(rowID, tc.updates))
					require.Error(t, err, "%v", tc.updates)
					assertShapeRefused(t, err, tc.attr)
					assert.Equal(t, before, env.stored(rowID))
				}
			})
		}
	}
}

// idOf answers the row's stored id.
func idOf(t *testing.T, env valueShapeEnv, rowID uuid.UUID) string {
	t.Helper()
	id, ok := env.get(t, rowID)["id"].(string)
	require.True(t, ok)
	return id
}

// TestUpdateWithoutAValidatorJudgesArraysByArrayAncestry pins R1-F4: with no
// validator, the registry's array ancestry says items is one array of
// objects, so an extra array around it, an [] inside the list member's
// value and an array around a member of items sent as an object are refused
// and the row unchanged, while the array of objects and the list's
// empty-list marker inside one element are written.
func TestUpdateWithoutAValidatorJudgesArraysByArrayAncestry(t *testing.T) {
	for _, surface := range updateSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			env := newReviewShapeEnv(t, "no validator")
			rowID := env.create(t, map[string]any{"items": []any{map[string]any{"name": "a"}}})
			before := env.stored(rowID)
			for _, tc := range []struct {
				updates map[string]any
				want    string
			}{
				{map[string]any{"items": []any{[]any{map[string]any{"name": "new"}}}},
					`invalid value for attribute 'items.name' (attrID=7): declared type text takes a single value, got a value at array indices "0,0", inside arrays its stored record would not read back from`},
				{map[string]any{"items": []any{[]any{map[string]any{"tags": []any{}}}}},
					`invalid value for attribute 'items.tags' (attrID=8): declared type list takes an array of single text values, got an empty array at array indices "0,0", inside arrays its stored record would not read back from`},
				{map[string]any{"items": []any{map[string]any{"tags": []any{[]any{}}}}},
					`invalid value for attribute 'items.tags' (attrID=8): empty list at array indices "0,0" is nested`},
				{map[string]any{"items": map[string]any{"name": []any{"x"}}},
					`invalid value for attribute 'items.name' (attrID=7): declared type text takes a single value, got a value at array indices "0", inside arrays its stored record would not read back from`},
			} {
				_, err := surface.update(context.Background(), env.em, env.update(rowID, tc.updates))
				assert.Contains(t, refusalMessage(t, err), tc.want, "%v", tc.updates)
				assert.Equal(t, before, env.stored(rowID))
			}

			updates := map[string]any{"items": []any{map[string]any{"name": "new"}, map[string]any{"name": "n2", "tags": []any{}}}}
			_, err := surface.update(context.Background(), env.em, env.update(rowID, updates))
			require.NoError(t, err)
			assertEqualValue(t, "items", updates["items"], env.get(t, rowID)["items"])
		})
	}
}

// refusalMessage answers the published message err refuses with, as one
// operation's failure or the call's own error.
func refusalMessage(t *testing.T, err error) string {
	t.Helper()
	var reported bestEffortFailure
	if errors.As(err, &reported) {
		return reported.failure.Error
	}
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok, "expected a published message, got %v", err)
	return message
}

// TestUpdateRefusesAGoContainerAtASingleValue pins R1-F5: in report-only
// mode and without a validator, a Go slice or map sent for a text attribute
// is refused before the update writes, naming the attribute, its type and
// the container, instead of being stored as its formatted image. A Go value
// that represents a single value is converted as before: a []byte as text,
// a uuid.UUID for a UUID or as its text.
func TestUpdateRefusesAGoContainerAtASingleValue(t *testing.T) {
	for _, mode := range []string{"report-only", "no validator"} {
		env := newReviewShapeEnv(t, mode)
		rowID := env.create(t, map[string]any{"note": "n"})
		before := env.stored(rowID)
		for _, surface := range updateSurfaces {
			for kind, value := range map[string]any{"slice ([]string)": []string{"new"}, "map (map[string]string)": map[string]string{"a": "new"}, "array ([2]int)": [2]int{1, 2}} {
				_, err := surface.update(context.Background(), env.em, env.update(rowID, map[string]any{"note": value}))
				assertShapeRefused(t, err, "note")
				assert.Contains(t, refusalMessage(t, err), "declared type text takes a single value, got a Go "+kind, "%s/%s", mode, surface.name)
				assert.Equal(t, before, env.stored(rowID))
			}
		}

		id := uuid.New()
		_, err := env.em.Update(context.Background(), env.update(rowID, map[string]any{"note": []byte("bytes"), "ref": id, "status": id}))
		require.NoError(t, err, mode)
		got := env.get(t, rowID)
		assert.Equal(t, "bytes", got["note"], "a []byte is text")
		assert.Equal(t, id.String(), got["status"], "a uuid.UUID is its text")
		assert.Equal(t, id, got["ref"])
	}
}

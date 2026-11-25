package internal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/require"
)

// The shape of an update's values is a storage invariant (#696), enforced in
// every validation mode, before any statement. These tests run the update
// surfaces over the in-memory repository against one schema that gathers
// the constructs earlier attempts at #696 got wrong in one direction or the
// other; the PostgreSQL tests (value_shape_integration_test.go) cover the
// transactional surfaces and the stored row.

const valueShapeSchema = "shapes"

// valueShapeDocument declares, besides id and the scalar note:
//   - tags, a list;
//   - phones, codes and labels, primitive arrays under element-typed
//     (pre-#204) ledger entries: through a nullable union, through items
//     typed by a combinator, and through an anyOf beside an object;
//   - profile, a typeless parent of a nullable primitive array (aliases)
//     and a scalar (memo);
//   - items, an array of objects;
//   - office, declared by properties beside an anyOf admitting an object or
//     one array of objects;
//   - ext, an allOf the registry's lenient parser does not resolve, which
//     leaves every attribute's array ancestry underived.
const valueShapeDocument = `{"type":"object","properties":{
	"id":{"type":"string"},
	"note":{"type":"string"},
	"tags":{"type":"array","items":{"type":"string"}},
	"phones":{"type":["array","null"],"items":{"type":"string"}},
	"codes":{"type":"array","items":{"anyOf":[{"type":"string"},{"type":"integer"}]}},
	"labels":{"anyOf":[{"type":"array","items":{"type":"string"}},{"type":"object"}]},
	"profile":{"properties":{"aliases":{"type":["array","null"],"items":{"type":"string"}},"memo":{"type":"string"}}},
	"items":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"}}}},
	"office":{"properties":{"city":{"type":"string"}},"anyOf":[{"type":"object"},
		{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]},
	"ext":{"allOf":[{"$ref":"#/$defs/ext"}]}},
	"$defs":{"ext":{"type":"object","properties":{"y":{"type":"string"}}}}}`

const valueShapeAttributes = `{
	"id":{"attributeID":1,"valueType":"text"},"note":{"attributeID":2,"valueType":"text"},
	"tags":{"attributeID":3,"valueType":"list","items_type":"text"},
	"phones":{"attributeID":4,"valueType":"text"},"codes":{"attributeID":5,"valueType":"text"},
	"labels":{"attributeID":6,"valueType":"text"},
	"profile.aliases":{"attributeID":7,"valueType":"text"},"profile.memo":{"attributeID":8,"valueType":"text"},
	"items.name":{"attributeID":9,"valueType":"text"},"office.city":{"attributeID":10,"valueType":"text"},
	"ext.y":{"attributeID":11,"valueType":"text"}}`

// valueShapeEnv is a manager over one schema, the shapes schema unless
// built by newShapeEnvFor.
type valueShapeEnv struct {
	em       forma.EntityManager
	repo     *mockPersistentRecordRepository
	schemaID int16
	schema   string
	cache    forma.SchemaAttributeCache
}

// newValueShapeEnv builds the manager. mode is "strict", "report-only" or
// "no validator".
func newValueShapeEnv(t *testing.T, mode string) valueShapeEnv {
	t.Helper()
	env := newShapeEnvFor(t, mode, valueShapeSchema, valueShapeDocument, valueShapeAttributes)
	require.Nil(t, env.cache["items.name"].ArrayAncestors, "precondition: the unresolved ext leaves array ancestry underived")
	return env
}

// newShapeEnvFor builds a manager over the schema name, declared by document
// and attributes. mode is "strict", "report-only" or "no validator".
func newShapeEnvFor(t *testing.T, mode, name, document, attributes string) valueShapeEnv {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".json"), []byte(document), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+"_attributes.json"), []byte(attributes), 0o600))
	registry, err := newFileSchemaRegistryFromDir(dir)
	require.NoError(t, err)
	schemaID, cache, err := registry.GetSchemaAttributeCacheByName(name)
	require.NoError(t, err)

	var validator *schemavalidate.Validator
	if mode != "no validator" {
		validator, err = schemavalidate.New(registry, dir)
		require.NoError(t, err)
		require.NotNil(t, validator.Shape(schemaID))
		require.False(t, validator.Shape(schemaID).Partial(), "the schema keeps a full skeleton")
	}
	config := createTestConfig()
	config.Entity.SchemaDirectory = dir
	config.Entity.ValidateUpdatesStrict = mode == "strict"
	repo := newMockPersistentRecordRepository()
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), repo, nil, registry, config, validator)
	return valueShapeEnv{em: em, repo: repo, schemaID: schemaID, schema: name, cache: cache}
}

func (e valueShapeEnv) create(t *testing.T, data map[string]any) uuid.UUID {
	t.Helper()
	data["id"] = uuid.NewString()
	created, err := e.em.Create(context.Background(), &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: e.schema},
		Type:             forma.OperationCreate,
		Data:             data,
	})
	require.NoError(t, err, "create %v", data)
	return created.RowID
}

func valueShapeUpdate(rowID uuid.UUID, updates map[string]any) *forma.EntityOperation {
	return shapeUpdateOf(valueShapeSchema, rowID, updates)
}

func shapeUpdateOf(schema string, rowID uuid.UUID, updates map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: schema, RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          updates,
	}
}

// update answers an update of the env's schema.
func (e valueShapeEnv) update(rowID uuid.UUID, updates map[string]any) *forma.EntityOperation {
	return shapeUpdateOf(e.schema, rowID, updates)
}

// stored answers the row's EAV records as storedTextRecords spells them.
func (e valueShapeEnv) stored(rowID uuid.UUID) []string {
	return storedTextRecords(e.repo.records[e.schemaID][rowID])
}

func (e valueShapeEnv) get(t *testing.T, rowID uuid.UUID) map[string]any {
	t.Helper()
	record, err := e.em.Get(context.Background(), &forma.QueryRequest{SchemaName: e.schema, RowID: &rowID})
	require.NoError(t, err)
	return record.Attributes
}

// valueShapeSeed is a row holding a value at every attribute.
func valueShapeSeed() map[string]any {
	return map[string]any{
		"note": "n", "tags": []any{"t1", "t2"}, "phones": []any{"p1"}, "codes": []any{"c1", 2}, "labels": []any{"l1"},
		"profile": map[string]any{"aliases": []any{"a1"}, "memo": "m"},
		"items":   []any{map[string]any{"name": "i1"}}, "office": []any{map[string]any{"city": "o1"}},
		"ext": map[string]any{"y": "e"},
	}
}

// TestUpdateWritesEveryArrayTheSchemaDeclares pins the preservation half of
// #696: wherever the schema declares an array (a list, a primitive array
// under an element-typed ledger entry in each spelling that defeated an
// earlier attempt, an array of objects, the office union), strict and
// report-only updates write it as create does, on every surface.
func TestUpdateWritesEveryArrayTheSchemaDeclares(t *testing.T) {
	for _, mode := range []string{"strict", "report-only"} {
		for _, surface := range updateSurfaces {
			t.Run(mode+"/"+surface.name, func(t *testing.T) {
				env := newValueShapeEnv(t, mode)
				rowID := env.create(t, valueShapeSeed())
				updates := map[string]any{
					"tags": []any{"t3"}, "phones": []any{"p2", "p3"}, "codes": []any{"c2", "c3"}, "labels": []any{"l2"},
					"profile": map[string]any{"aliases": []any{"a2"}}, "items": []any{map[string]any{"name": "i2"}, map[string]any{"name": "i3"}},
					"office": []any{map[string]any{"city": "o2"}},
				}
				_, err := surface.update(context.Background(), env.em, valueShapeUpdate(rowID, updates))
				require.NoError(t, err)
				got := env.get(t, rowID)
				for _, path := range []string{"tags", "phones", "codes", "labels", "profile.aliases", "items", "office"} {
					assertEqualValue(t, path, valueAtPath(updates, path), valueAtPath(got, path))
				}

				_, err = surface.update(context.Background(), env.em, valueShapeUpdate(rowID, map[string]any{"office": map[string]any{"city": "o3"}}))
				require.NoError(t, err, "the office union admits an object too")
				assertEqualValue(t, "office", map[string]any{"city": "o3"}, env.get(t, rowID)["office"])
			})
		}
	}
}

// valueShapeRefusals are updates whose values sit where the schema declares
// no such container, each with the attribute its refusal names: an object or
// array at a scalar, empty or not, directly, nested, dotted and inside an
// array of objects; an object at an element-typed array that the schema's
// anyOf would admit but the store cannot hold; a value nested in a declared
// primitive array; extra arrays around an array of objects, with array
// ancestry underived, and around the office union; an object inside a
// list's nested array.
var valueShapeRefusals = []struct {
	name    string
	updates map[string]any
	attr    string
}{
	{"empty object", map[string]any{"note": map[string]any{}}, "note"},
	{"object", map[string]any{"note": map[string]any{"x": "y"}}, "note"},
	{"empty array", map[string]any{"note": []any{}}, "note"},
	{"singleton array", map[string]any{"note": []any{"a"}}, "note"},
	{"typed nil slice", map[string]any{"note": []any(nil)}, "note"},
	{"nested", map[string]any{"profile": map[string]any{"memo": []any{"x"}}}, "profile.memo"},
	{"literal dotted key", map[string]any{"profile.memo": map[string]any{}}, "profile.memo"},
	{"dotted key across the array of objects", map[string]any{"items.name": []any{"x"}}, "items.name"},
	{"member of an array of objects", map[string]any{"items": []any{map[string]any{"name": []any{"x"}}}}, "items.name"},
	{"object at an element-typed array", map[string]any{"labels": map[string]any{}}, "labels"},
	{"nested in a declared array", map[string]any{"phones": []any{[]any{"p"}}}, "phones"},
	{"extra array around an array of objects", map[string]any{"items": []any{[]any{map[string]any{"name": "x"}}}}, "items.name"},
	{"extra array around the office union", map[string]any{"office": []any{[]any{map[string]any{"city": "x"}}}}, "office.city"},
	{"object nested in a list", map[string]any{"tags": []any{[]any{map[string]any{}}}}, "tags"},
}

// TestUpdateRefusesAValueWhoseShapeTheSchemaDoesNotDeclare pins the refusal
// half: in strict and report-only modes, on every surface, each update is
// refused as caller input naming the attribute, and the stored row is left
// as it was.
func TestUpdateRefusesAValueWhoseShapeTheSchemaDoesNotDeclare(t *testing.T) {
	for _, mode := range []string{"strict", "report-only"} {
		env := newValueShapeEnv(t, mode)
		rowID := env.create(t, valueShapeSeed())
		before := storedTextRecords(env.repo.records[env.schemaID][rowID])
		for _, surface := range updateSurfaces {
			for _, tc := range valueShapeRefusals {
				t.Run(mode+"/"+surface.name+"/"+tc.name, func(t *testing.T) {
					_, err := surface.update(context.Background(), env.em, valueShapeUpdate(rowID, tc.updates))
					assertShapeRefused(t, err, tc.attr)
					assertEqualValue(t, "stored records", before, storedTextRecords(env.repo.records[env.schemaID][rowID]))
				})
			}
		}
	}
}

// TestUpdateWithoutAValidatorRefusesEveryArrayAtASingleValue pins the
// fallback: without a validator nothing declares an array at an attribute
// that is not a list, so a primitive array under an element-typed ledger
// entry is refused too, while a list and an array of objects are written.
func TestUpdateWithoutAValidatorRefusesEveryArrayAtASingleValue(t *testing.T) {
	env := newValueShapeEnv(t, "no validator")
	rowID := env.create(t, map[string]any{"note": "n", "tags": []any{"t1"}, "items": []any{map[string]any{"name": "i1"}}})
	before := storedTextRecords(env.repo.records[env.schemaID][rowID])
	for _, updates := range []map[string]any{
		{"note": []any{"a"}}, {"note": []any{}}, {"note": map[string]any{}}, {"phones": []any{"p"}},
		{"items": []any{map[string]any{"name": []any{"x"}}}},
	} {
		_, err := env.em.Update(context.Background(), valueShapeUpdate(rowID, updates))
		require.Error(t, err, "%v", updates)
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		assertEqualValue(t, "stored records", before, storedTextRecords(env.repo.records[env.schemaID][rowID]))
	}
	_, err := env.em.Update(context.Background(), valueShapeUpdate(rowID, map[string]any{
		"tags": []any{"t2"}, "items": []any{map[string]any{"name": "i2"}},
	}))
	require.NoError(t, err)
}

// TestStrictUpdateKeepsANilListApartFromAnEmptyOne pins that a nil []any
// reaches strict validation as JSON null, which the list's schema refuses,
// while [] is still the empty-list marker that clears the list.
func TestStrictUpdateKeepsANilListApartFromAnEmptyOne(t *testing.T) {
	env := newValueShapeEnv(t, "strict")
	rowID := env.create(t, valueShapeSeed())
	before := storedTextRecords(env.repo.records[env.schemaID][rowID])
	_, err := env.em.Update(context.Background(), valueShapeUpdate(rowID, map[string]any{"tags": []any(nil)}))
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	assertEqualValue(t, "stored records", before, storedTextRecords(env.repo.records[env.schemaID][rowID]))

	_, err = env.em.Update(context.Background(), valueShapeUpdate(rowID, map[string]any{"tags": []any{}}))
	require.NoError(t, err)
	assertEqualValue(t, "tags", []any{}, env.get(t, rowID)["tags"])
}

// assertShapeRefused asserts err refuses attr's value as caller input, as
// one operation's failure or the call's own error.
func assertShapeRefused(t *testing.T, err error, attr string) {
	t.Helper()
	message := ""
	var reported bestEffortFailure
	switch {
	case errors.As(err, &reported):
		message = reported.failure.Error
	case errors.Is(err, forma.ErrInvalidInput):
		published, ok := forma.ResolvePublicMessage(err)
		require.True(t, ok, "expected a published message, got %v", err)
		message = published
	default:
		t.Fatalf("expected the update to be refused as invalid input, got %v", err)
	}
	for _, part := range []string{"invalid value for attribute '" + attr + "'", "cannot be stored where a single value belongs"} {
		if !strings.Contains(message, part) {
			t.Fatalf("expected the refusal to name %q, got %q", part, message)
		}
	}
}

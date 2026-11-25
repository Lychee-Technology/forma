package internal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
)

// positionsManager is an entity manager over a file registry read from the
// given schema files, its validator enforcing updates when strict.
type positionsManager struct {
	em       forma.EntityManager
	registry forma.SchemaRegistry
	repo     *mockPersistentRecordRepository
	schema   string
}

func newPositionsManager(t *testing.T, schema, document, attributes string, strict bool) positionsManager {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, schema+".json"), []byte(document), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, schema+"_attributes.json"), []byte(attributes), 0o600))
	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)
	validator, err := schemavalidate.New(SnapshotSchemaDocuments(registry), dir)
	require.NoError(t, err)
	config := createTestConfig()
	config.Entity.SchemaDirectory = dir
	config.Entity.ValidateUpdatesStrict = strict
	repo := newMockPersistentRecordRepository()
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), repo, nil, registry, config, validator)
	return positionsManager{em: em, registry: registry, repo: repo, schema: schema}
}

func (m positionsManager) create(t *testing.T, data map[string]any) *forma.DataRecord {
	t.Helper()
	created, err := m.em.Create(context.Background(), &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: m.schema},
		Type:             forma.OperationCreate,
		Data:             data,
	})
	require.NoError(t, err)
	return created
}

func (m positionsManager) update(row *forma.DataRecord, updates map[string]any) (*forma.DataRecord, error) {
	return m.em.Update(context.Background(), &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: m.schema, RowID: row.RowID},
		Type:             forma.OperationUpdate,
		Updates:          updates,
	})
}

// stored is the row the repository holds.
func (m positionsManager) stored(t *testing.T, row *forma.DataRecord) any {
	t.Helper()
	schemaID, _, err := m.registry.GetSchemaAttributeCacheByName(m.schema)
	require.NoError(t, err)
	stored := m.repo.records[schemaID][row.RowID]
	require.NotNil(t, stored)
	return stored
}

func (m positionsManager) read(t *testing.T, row *forma.DataRecord) map[string]any {
	t.Helper()
	got, err := m.em.Get(context.Background(), &forma.QueryRequest{SchemaName: m.schema, RowID: &row.RowID})
	require.NoError(t, err)
	return got.Attributes
}

// An attribute whose ancestor is declared only through an anyOf, which the
// parser does not read, leaves the whole schema without array ancestry
// (withArrayAncestors), yet items.name's own path is
// settled: one array of objects. A single value sent inside an extra array
// around it is refused in every validation mode, as where ancestry is
// declared (#696 review R2-F1), and the stored row is left as it was; the
// array of objects itself is still written.
func TestUpdateRefusesExtraArraysWhereAnUnrelatedPathIsUnresolved(t *testing.T) {
	const document = `{"type":"object","properties":{` +
		`"items":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"}}}},` +
		`"meta":{"anyOf":[{"type":"object","properties":{"note":{"type":"string"}}}]}}}`
	const attributes = `{"items.name":{"attributeID":1,"valueType":"text"},"meta.note":{"attributeID":2,"valueType":"text"}}`
	for _, strict := range []bool{false, true} {
		m := newPositionsManager(t, "orders", document, attributes, strict)
		_, cache, err := m.registry.GetSchemaAttributeCacheByName("orders")
		require.NoError(t, err)
		require.Nil(t, cache["items.name"].ArrayAncestors, "the unrelated anyOf leaves ancestry underived")
		require.Equal(t, []string{"items"}, cache["items.name"].SettledArrayAncestors)

		row := m.create(t, map[string]any{"items": []any{map[string]any{"name": "a"}}})
		before := m.stored(t, row)
		_, err = m.update(row, map[string]any{"items": []any{[]any{map[string]any{"name": "x"}}}})
		if strict {
			require.ErrorIs(t, err, forma.ErrInvalidInput)
		} else {
			assertShapeRefused(t, err, "items.name", `a single text value at array indices "0,0" is nested in more arrays `+
				"than the attribute's arrays of objects (items)")
		}
		require.Same(t, before, m.stored(t, row), "a refused update must not store a row")
		require.Equal(t, []any{map[string]any{"name": "a"}}, m.read(t, row)["items"])

		updated, err := m.update(row, map[string]any{"items": []any{map[string]any{"name": "b"}}})
		require.NoError(t, err)
		require.Equal(t, []any{map[string]any{"name": "b"}}, updated.Attributes["items"])
	}
}

// office declares city by "properties" alone beside an anyOf that admits an
// array of objects, so the registry's lenient reading infers no array above
// office.city though the document lets office be such an array. That is no
// proof an index there is wrong: an array of offices is written by create
// and update alike, in strict mode too (#696 review R2-F2), and reads back
// as the create's does. (Under that lenient ancestry the read takes the
// index for one of city's own elements; that is the ancestry's reading,
// unchanged by #696.) areas, an array of objects by its type, still refuses
// a value inside an extra array.
func TestUpdateWritesAnArrayOfObjectsTheDocumentLeavesOpen(t *testing.T) {
	const document = `{"type":"object","properties":{` +
		`"office":{"properties":{"city":{"type":"string"}},"anyOf":[{"type":"object"},` +
		`{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]},` +
		`"areas":{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}}}`
	const attributes = `{"office.city":{"attributeID":1,"valueType":"text"},"areas.city":{"attributeID":2,"valueType":"text"}}`
	for _, strict := range []bool{false, true} {
		m := newPositionsManager(t, "contact", document, attributes, strict)
		_, cache, err := m.registry.GetSchemaAttributeCacheByName("contact")
		require.NoError(t, err)
		require.Equal(t, []string{}, cache["office.city"].ArrayAncestors)
		require.Nil(t, cache["office.city"].SettledArrayAncestors, "the document does not settle office.city")

		row := m.create(t, map[string]any{"office": []any{map[string]any{"city": "x"}}, "areas": []any{map[string]any{"city": "a"}}})
		updated, err := m.update(row, map[string]any{"office": []any{map[string]any{"city": "y"}}})
		require.NoError(t, err)
		require.Equal(t, map[string]any{"city": []any{"y"}}, updated.Attributes["office"])
		require.Equal(t, map[string]any{"city": []any{"x"}}, row.Attributes["office"], "the create's reading")

		before := m.stored(t, row)
		_, err = m.update(row, map[string]any{"areas": []any{[]any{map[string]any{"city": "b"}}}})
		if strict {
			require.ErrorIs(t, err, forma.ErrInvalidInput)
		} else {
			assertShapeRefused(t, err, "areas.city", `a single text value at array indices "0,0" is nested in more arrays `+
				"than the attribute's arrays of objects (areas)")
		}
		require.Same(t, before, m.stored(t, row), "a refused update must not store a row")
		require.Equal(t, []any{map[string]any{"city": "a"}}, m.read(t, row)["areas"])
	}
}

// A primitive array declared through a nullable type union, which the
// parser drops, is still an array the document positively declares: under an
// element-typed (pre-#204) ledger entry, a strict update writes it as a
// strict create does (#696 review R2-F3). note, a true scalar, still refuses
// an array.
func TestStrictUpdateWritesANullablePrimitiveArrayUnderAnElementTypedLedger(t *testing.T) {
	m := newPositionsManager(t, "legacy",
		`{"type":"object","properties":{"note":{"type":"string"},`+
			`"tags":{"type":["array","null"],"items":{"type":"string"}}}}`,
		`{"note":{"attributeID":1,"valueType":"text"},"tags":{"attributeID":2,"valueType":"text"}}`, true)
	_, cache, err := m.registry.GetSchemaAttributeCacheByName("legacy")
	require.NoError(t, err)
	require.True(t, cache["tags"].DeclaredArray)
	require.False(t, cache["note"].DeclaredArray)

	row := m.create(t, map[string]any{"note": "n", "tags": []any{"a"}})
	require.Equal(t, []any{"a"}, row.Attributes["tags"])
	updated, err := m.update(row, map[string]any{"tags": []any{"b"}})
	require.NoError(t, err)
	require.Equal(t, []any{"b"}, updated.Attributes["tags"])

	before := m.stored(t, row)
	_, err = m.update(row, map[string]any{"note": []any{"x"}})
	assertShapeRefused(t, err, "note", "declared type text takes a single value, got an array")
	require.Same(t, before, m.stored(t, row), "a refused update must not store a row")
	require.Equal(t, []any{"b"}, m.read(t, row)["tags"])
}

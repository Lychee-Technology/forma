package internal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Unit tests of the unique-key pieces that need no database (#651).

func TestRefuseDuplicateCreateRowIDs(t *testing.T) {
	rowID := uuid.New()
	op := func(schema string, id uuid.UUID) forma.EntityOperation {
		return forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: schema, RowID: id}, Type: forma.OperationCreate}
	}
	err := refuseDuplicateCreateRowIDs([]forma.EntityOperation{op("a", rowID), op("a", uuid.Nil), op("a", uuid.Nil), op("a", rowID)})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	published, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok)
	assert.Equal(t, "operation[3]: duplicate row id "+rowID.String()+" for schema a in batch create (already used by operation[0])", published)

	// Nil asks Forma to assign, and the same ID in two schemas is two rows.
	require.NoError(t, refuseDuplicateCreateRowIDs([]forma.EntityOperation{op("a", uuid.Nil), op("a", uuid.Nil), op("a", rowID), op("b", rowID)}))
}

// The claim diff answers fresh slices: an aliased diff once rewrote the
// desired entries while it filtered them, and released a key the update left
// alone.
func TestEntriesMissingFromLeavesItsArgumentsAlone(t *testing.T) {
	desired := []uniqueKeyEntry{{keyID: 1, value: "a"}, {keyID: 2, value: "new"}}
	held := []uniqueKeyEntry{{keyID: 1, value: "a"}, {keyID: 2, value: "old"}}
	wantDesired, wantHeld := append([]uniqueKeyEntry(nil), desired...), append([]uniqueKeyEntry(nil), held...)

	assert.Equal(t, []uniqueKeyEntry{{keyID: 2, value: "new"}}, entriesMissingFrom(desired, held))
	assert.Equal(t, []uniqueKeyEntry{{keyID: 2, value: "old"}}, entriesMissingFrom(held, desired))
	assert.Equal(t, wantDesired, desired)
	assert.Equal(t, wantHeld, held)
}

func ukTestSpecs() []uniqueKeySpec {
	text := forma.AttributeMetadata{AttributeID: 1, ValueType: forma.ValueTypeText}
	bound := forma.AttributeMetadata{AttributeID: 2, ValueType: forma.ValueTypeText,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}}
	return []uniqueKeySpec{
		{id: 1, name: "pair", attrs: []uniqueKeyAttribute{{name: "tenant", meta: bound}, {name: "email", meta: text}}},
		{id: 2, name: "email", attrs: []uniqueKeyAttribute{{name: "email", meta: text}}},
	}
}

func ukTestRecord(tenant *string, email *string) *model.PersistentRecord {
	record := &model.PersistentRecord{SchemaID: 1, RowID: uuid.New(), TextItems: map[string]string{}}
	if tenant != nil {
		record.TextItems["text_01"] = *tenant
	}
	if email != nil {
		record.OtherAttributes = []model.EAVRecord{{SchemaID: 1, RowID: record.RowID, AttrID: 1, ValueText: email}}
	}
	return record
}

func TestUniqueKeyEntriesOfSkipsKeysWithAnAbsentComponent(t *testing.T) {
	tenant, email := "t", "e@x"
	entries, err := uniqueKeyEntriesOf(ukTestSpecs(), ukTestRecord(&tenant, &email))
	require.NoError(t, err)
	assert.Equal(t, []uniqueKeyEntry{{keyID: 1, keyName: "pair", value: "1:t3:e@x"}, {keyID: 2, keyName: "email", value: "3:e@x"}}, entries)

	entries, err = uniqueKeyEntriesOf(ukTestSpecs(), ukTestRecord(nil, &email))
	require.NoError(t, err)
	assert.Equal(t, []uniqueKeyEntry{{keyID: 2, keyName: "email", value: "3:e@x"}}, entries, "no tenant: only the single-attribute key applies")

	deleted := ukTestRecord(&tenant, &email)
	stamp := int64(1)
	deleted.DeletedAt = &stamp
	entries, err = uniqueKeyEntriesOf(ukTestSpecs(), deleted)
	require.NoError(t, err)
	assert.Empty(t, entries, "a row that is not live holds no entry")
}

// A key value from array elements has no single value, so the write fails
// rather than leave the row unkeyed.
func TestUniqueKeyEntriesOfRefusesArrayElements(t *testing.T) {
	email := "e@x"
	record := ukTestRecord(nil, nil)
	record.OtherAttributes = []model.EAVRecord{{SchemaID: 1, RowID: record.RowID, AttrID: 1, ArrayIndices: "0", ValueText: &email}}
	_, err := uniqueKeyEntriesOf(ukTestSpecs(), record)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "compute unique key email")
	assert.Contains(t, err.Error(), "holds array elements")
}

func TestUniqueKeyDuplicatesErrorListsTheRows(t *testing.T) {
	a, b, c := uuid.MustParse("00000000-0000-7000-8000-00000000000a"), uuid.MustParse("00000000-0000-7000-8000-00000000000b"),
		uuid.MustParse("00000000-0000-7000-8000-00000000000c")
	err := duplicatesOf("account", "handle", map[string][]uuid.UUID{"2:h1": {b, a}, "2:h2": {c}, "2:h3": {c, a}})
	require.Error(t, err)
	assert.Equal(t, "schema account: 2 values of unique key handle are shared by more than one live row, so the key is left unbuilt; "+
		"resolve these rows (each group shares one value) and build again: ["+a.String()+" "+b.String()+"] ["+a.String()+" "+c.String()+"]", err.Error())
	assert.NoError(t, duplicatesOf("account", "handle", map[string][]uuid.UUID{"2:h1": {a}}))
}

// ukTestCatalog resolves schema account's keys pair [tenant, email] and
// email [email], with tenant stored as tenant says and email in EAV.
func ukTestCatalog(t *testing.T, tenant forma.AttributeMetadata) *UniqueKeyCatalog {
	t.Helper()
	cache := forma.SchemaAttributeCache{"tenant": tenant, "email": {AttributeID: 1, ValueType: forma.ValueTypeText}}
	specs, err := resolveUniqueKeys("account", []forma.UniqueKeyDeclaration{
		{ID: 1, Name: "pair", Attributes: []string{"tenant", "email"}},
		{ID: 2, Name: "email", Attributes: []string{"email"}},
	}, cache)
	require.NoError(t, err)
	return &UniqueKeyCatalog{bySchema: map[int16][]uniqueKeySpec{1: specs}, names: map[int16]string{1: "account"}}
}

func ukBoundTenant(column forma.MainColumn, encoding forma.MainColumnEncoding) forma.AttributeMetadata {
	return forma.AttributeMetadata{AttributeID: 2, ValueType: forma.ValueTypeText,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: column, Encoding: encoding}}
}

// A registry that declares a key needs a repository that enforces it.
func TestCheckUniqueKeysRefusesARepositoryThatCannotEnforce(t *testing.T) {
	em := &entityManager{uniqueKeys: ukTestCatalog(t, ukBoundTenant(forma.MainColumnText01, ""))}
	err := em.checkUniqueKeys(&mockPersistentRecordRepository{}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema account declares unique keys, which only the PostgreSQL repository enforces")

	em.uniqueKeys = &UniqueKeyCatalog{}
	require.NoError(t, em.checkUniqueKeys(&mockPersistentRecordRepository{}, nil))
}

// A repository enforces the keys it was built with. A manager is accepted
// only when its registry declares the same keys over the same storage, and
// the check never changes what the repository enforces.
func TestCheckUniqueKeysNeedsTheRepositoryToEnforceTheRegistrysKeys(t *testing.T) {
	enforced := ukTestCatalog(t, ukBoundTenant(forma.MainColumnText01, ""))
	repo := NewDBPersistentRecordRepository(nil, nil, WithUniqueKeyCatalog(enforced))
	check := func(declared *UniqueKeyCatalog) error {
		t.Helper()
		err := (&entityManager{uniqueKeys: declared}).checkUniqueKeys(repo, nil)
		assert.Same(t, enforced, repo.uniqueKeys, "a check leaves the repository's keys in place")
		return err
	}

	require.NoError(t, check(enforced))
	require.NoError(t, check(ukTestCatalog(t, ukBoundTenant(forma.MainColumnText01, ""))), "a separately resolved equal catalog")
	require.NoError(t, check(ukTestCatalog(t, ukBoundTenant(forma.MainColumnText01, forma.MainColumnEncodingDefault))),
		"an unnamed encoding reads as the default")

	for name, tc := range map[string]struct {
		declared *UniqueKeyCatalog
		want     string
	}{
		"another column": {ukTestCatalog(t, ukBoundTenant(forma.MainColumnText02, "")),
			"schema account: unique key pair reads attribute tenant from column text_01 (encoding default), where the registry stores it in column text_02 (encoding default)"},
		"EAV": {ukTestCatalog(t, forma.AttributeMetadata{AttributeID: 2, ValueType: forma.ValueTypeText}),
			"schema account: unique key pair reads attribute tenant from column text_01 (encoding default), where the registry stores it in EAV attribute id 2"},
		"another type": {ukTestCatalog(t, forma.AttributeMetadata{AttributeID: 2, ValueType: forma.ValueTypeUUID,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}}),
			"schema account: the repository enforces unique key pair (id 1), where the registry declares unique key pair (id 1) over other attributes or types"},
		"no keys": {&UniqueKeyCatalog{},
			"schema account: the repository enforces unique key pair, which the registry does not declare"},
	} {
		err := check(tc.declared)
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), tc.want, name)
	}

	err := (&entityManager{uniqueKeys: enforced}).checkUniqueKeys(NewDBPersistentRecordRepository(nil, nil), nil)
	require.Error(t, err, "a repository built without keys cannot serve a registry that declares one")
	assert.Contains(t, err.Error(), "schema account: the repository does not enforce unique key pair")
	require.NoError(t, (&entityManager{uniqueKeys: &UniqueKeyCatalog{}}).checkUniqueKeys(NewDBPersistentRecordRepository(nil, nil), nil))
}

// A key over a relation root, or anything beneath one, is refused: writes
// strip that subtree, so the key would reserve nothing while reads answer the
// parent's values on every child (audit of #682).
func TestNewUniqueKeyCatalogRefusesAKeyUnderARelationRoot(t *testing.T) {
	for _, attrs := range [][]string{{"contactSnapshot.email"}, {"leadId", "contactSnapshot.email"}} {
		_, err := ukVisitCatalog(t, attrs...)
		require.Error(t, err, "a key over %v reads a value no write stores", attrs)
		assert.Contains(t, err.Error(), "schema visit: unique key k: attribute contactSnapshot.email lies under relation root contactSnapshot")
	}
	catalog, err := ukVisitCatalog(t, "leadId")
	require.NoError(t, err, "the relation's foreign key is stored, so a key over it is enforced")
	assert.Len(t, catalog.keyedSchemas(), 1)
}

// ukVisitCatalog resolves the shipped schemas with visit declaring one key, k,
// over attrs. visit.contactSnapshot is the shipped relation root.
func ukVisitCatalog(t *testing.T, attrs ...string) (*UniqueKeyCatalog, error) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.CopyFS(dir, os.DirFS(shippedSchemaDir)))
	path := filepath.Join(dir, "visit.json")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))
	doc["x-forma-unique-keys"] = []any{map[string]any{"id": 1, "name": "k", "attributes": attrs}}
	raw, err = json.Marshal(doc)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err, "the registry checks a key against the attributes alone")
	return NewUniqueKeyCatalog(registry)
}

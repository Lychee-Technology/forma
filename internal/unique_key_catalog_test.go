package internal

import (
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
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

// A registry that declares a key needs a repository that enforces it.
func TestInstallUniqueKeysRefusesARepositoryThatCannotEnforce(t *testing.T) {
	em := &entityManager{uniqueKeys: &UniqueKeyCatalog{
		bySchema: map[int16][]uniqueKeySpec{1: ukTestSpecs()}, names: map[int16]string{1: "account"},
	}}
	err := em.installUniqueKeys(&mockPersistentRecordRepository{}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema account declares unique keys, which only the PostgreSQL repository enforces")

	em.uniqueKeys = &UniqueKeyCatalog{}
	require.NoError(t, em.installUniqueKeys(&mockPersistentRecordRepository{}, nil))
}

// The keys live on the repository, so a second manager over it must enforce
// the same ones.
func TestInstallUniqueKeysRefusesARepositoryEnforcingOtherKeys(t *testing.T) {
	keyed := &UniqueKeyCatalog{bySchema: map[int16][]uniqueKeySpec{1: ukTestSpecs()}, names: map[int16]string{1: "account"}}
	same := &UniqueKeyCatalog{bySchema: map[int16][]uniqueKeySpec{1: ukTestSpecs()}, names: map[int16]string{1: "account"}}
	repo := NewDBPersistentRecordRepository(nil, nil)
	require.NoError(t, (&entityManager{uniqueKeys: keyed}).installUniqueKeys(repo, nil))
	require.NoError(t, (&entityManager{uniqueKeys: same}).installUniqueKeys(repo, nil), "the same keys share a repository")
	err := (&entityManager{uniqueKeys: &UniqueKeyCatalog{}}).installUniqueKeys(repo, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already enforces another registry's unique keys")
	assert.Same(t, same, repo.uniqueKeys, "a refused install leaves the repository's keys alone")
}

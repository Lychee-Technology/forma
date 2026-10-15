package internal

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/uniquekey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of unique keys on the EntityManager surfaces (#651
// part B): every write keeps the key table equal to the entries recomputed
// from the live rows, and a conflict writes nothing.

// Key IDs of ukaccount (testdata/unique_keys/ukaccount.json).
const (
	ukKeyTenantEmail int16 = 1
	ukKeyHandle      int16 = 2
	ukKeySeq         int16 = 3
)

func (e *txEnv) mustCreateAccount(t *testing.T, tenant, email, handle string) uuid.UUID {
	t.Helper()
	record, err := e.manager.Create(e.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData(tenant, email, handle)))
	require.NoError(t, err)
	return record.RowID
}

func (e *txEnv) mainRowCount(t *testing.T, schema string) int {
	t.Helper()
	var n int
	require.NoError(t, e.postgresPool.QueryRow(e.ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE ltbase_schema_id = $1",
		sanitizeIdentifier(e.tables.EntityMain)), e.schemaID(t, schema)).Scan(&n))
	return n
}

// requireKeyConflict asserts err is the ErrUniqueConflict of key, published
// without the value.
func requireKeyConflict(t *testing.T, err error, schema, key, value string) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrUniqueConflict)
	assert.Contains(t, err.Error(), fmt.Sprintf("schema %s already has a row with this value of unique key %s", schema, key))
	published, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok, "a key conflict publishes its message")
	assert.Equal(t, fmt.Sprintf("schema %s already has a row with this value of unique key %s", schema, key), published)
	assert.NotContains(t, published, value, "the published message must not carry the key value")
}

func TestUniqueKeyCreateConflictWritesNothing(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	first := env.mustCreateAccount(t, "t1", "same@x", "h1")

	_, err := env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("t1", "same@x", "h2")))
	requireKeyConflict(t, err, ukAccount, "tenant_email", "same@x")
	assert.Equal(t, 1, env.mainRowCount(t, ukAccount), "the refused create must not write its row")

	// The key is the pair: the same email under another tenant is free.
	env.mustCreateAccount(t, "t2", "same@x", "h3")

	// The composite value is the length-prefixed canonical components, the
	// column-bound tenant first.
	want, err := uniquekey.Encode([]uniquekey.Component{{Type: forma.ValueTypeText, Value: "t1"}, {Type: forma.ValueTypeText, Value: "same@x"}})
	require.NoError(t, err)
	assert.Contains(t, env.keyEntriesOfRow(t, first), keyEntry{schemaID: ukSchemaIDs[ukAccount], keyID: ukKeyTenantEmail, value: want, rowID: first})

	// Integers compare on their canonical value.
	data := ukAccountData("", "", "")
	data["seq"] = 7
	_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, data))
	require.NoError(t, err)
	data["seq"] = 7.0
	_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, data))
	requireKeyConflict(t, err, ukAccount, "seq", "7")
	env.assertKeyTableMatchesRows(t)
}

// A row with any key attribute absent is not subject to that key and holds
// no entry for it.
func TestUniqueKeyAbsentComponentsNeverConflict(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	a := env.mustCreateAccount(t, "t1", "", "")
	b := env.mustCreateAccount(t, "t1", "", "")
	c := env.mustCreateAccount(t, "", "lonely@x", "")
	for _, rowID := range []uuid.UUID{a, b, c} {
		assert.Empty(t, env.keyEntriesOfRow(t, rowID))
	}
	env.assertKeyTableMatchesRows(t)
}

func TestUniqueKeyUpdateMovesTheKey(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	a := env.mustCreateAccount(t, "t1", "a@x", "h1")

	_, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, a, map[string]any{"handle": "h2"}))
	require.NoError(t, err)
	env.assertKeyTableMatchesRows(t)

	// The released value is free at once, the taken one is not.
	env.mustCreateAccount(t, "t2", "b@x", "h1")
	_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("t3", "c@x", "h2")))
	requireKeyConflict(t, err, ukAccount, "handle", "h2")

	// An update that leaves the key's attributes alone keeps its entries.
	before := env.keyEntriesOfRow(t, a)
	_, err = env.manager.Update(env.ctx, txUpdateOp(ukAccount, a, map[string]any{"note": "touched"}))
	require.NoError(t, err)
	assert.Equal(t, before, env.keyEntriesOfRow(t, a))
	env.assertKeyTableMatchesRows(t)
}

func TestUniqueKeyUpdateOntoATakenValueWritesNothing(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	env.mustCreateAccount(t, "t1", "a@x", "h1")
	b := env.mustCreateAccount(t, "t1", "b@x", "h2")
	before, entries := env.snapshot(t, ukAccount, b), env.keyEntries(t)

	_, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, b, map[string]any{"handle": "h1", "note": "moved"}))
	requireKeyConflict(t, err, ukAccount, "handle", "h1")
	_, err = env.manager.Update(env.ctx, txUpdateOp(ukAccount, b, map[string]any{"email": "a@x"}))
	requireKeyConflict(t, err, ukAccount, "tenant_email", "a@x")

	assert.Equal(t, before, env.snapshot(t, ukAccount, b), "the refused update must not write")
	assert.Equal(t, entries, env.keyEntries(t))
}

func TestUniqueKeyDeleteReleasesForReuse(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	a := env.mustCreateAccount(t, "t1", "a@x", "h1")
	require.NoError(t, env.manager.Delete(env.ctx, txDeleteOp(ukAccount, a)))
	assert.Empty(t, env.keyEntries(t))

	b := env.mustCreateAccount(t, "t1", "a@x", "h1")
	assert.Len(t, env.keyEntriesOfRow(t, b), 2)
	env.assertKeyTableMatchesRows(t)
}

func TestUniqueKeyBatchCreate(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	env.mustCreateAccount(t, "t1", "a@x", "taken")
	create := func(handle string) forma.EntityOperation {
		return *ukCreateOp(ukAccount, uuid.Nil, ukAccountData("", "", handle))
	}

	_, err := env.manager.BatchCreate(env.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{create("x1"), create("taken")}})
	requireKeyConflict(t, err, ukAccount, "handle", "taken")
	_, err = env.manager.BatchCreate(env.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{create("twice"), create("twice")}})
	requireKeyConflict(t, err, ukAccount, "handle", "twice")
	assert.Equal(t, 1, env.mainRowCount(t, ukAccount), "a refused atomic batch writes nothing")

	result, err := env.manager.BatchCreate(env.ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{create("x2"), create("taken"), create("x2")}})
	require.NoError(t, err)
	require.Len(t, result.Successful, 1)
	require.Len(t, result.Failed, 2)
	for _, failed := range result.Failed {
		assert.Equal(t, "UNIQUE_CONFLICT", failed.Code)
	}
	assert.Equal(t, 2, env.mainRowCount(t, ukAccount))
	env.assertKeyTableMatchesRows(t)
}

func TestUniqueKeyBatchUpdateAndDelete(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	a := env.mustCreateAccount(t, "t1", "a@x", "h1")
	b := env.mustCreateAccount(t, "t1", "b@x", "h2")
	update := func(rowID uuid.UUID, handle string) forma.EntityOperation {
		return *txUpdateOp(ukAccount, rowID, map[string]any{"handle": handle})
	}

	_, err := env.manager.BatchUpdate(env.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{update(a, "h3"), update(b, "h4")}})
	require.NoError(t, err)
	env.assertKeyTableMatchesRows(t)

	// A swap would hold at commit, but each move is checked against the
	// entries as stored, so an atomic batch refuses it and writes nothing.
	entries := env.keyEntries(t)
	_, err = env.manager.BatchUpdate(env.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{update(a, "h4"), update(b, "h3")}})
	requireKeyConflict(t, err, ukAccount, "handle", "h4")
	assert.Equal(t, entries, env.keyEntries(t))

	result, err := env.manager.BatchUpdate(env.ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{update(a, "h5"), update(b, "h5")}})
	require.NoError(t, err)
	require.Len(t, result.Failed, 1)
	assert.Equal(t, "UNIQUE_CONFLICT", result.Failed[0].Code)
	env.assertKeyTableMatchesRows(t)

	_, err = env.manager.BatchDelete(env.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{*txDeleteOp(ukAccount, a), *txDeleteOp(ukAccount, b)}})
	require.NoError(t, err)
	assert.Empty(t, env.keyEntries(t))
	env.mustCreateAccount(t, "t1", "a@x", "h5")
	env.assertKeyTableMatchesRows(t)
}

// A schema without keys sends no key statement on any write.
func TestUniqueKeyUnkeyedSchemaWritesNoEntries(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	rowID := env.mustCreate(t, ukPlain, map[string]any{"title": "a"})
	_, err := env.manager.Update(env.ctx, txUpdateOp(ukPlain, rowID, map[string]any{"title": "b"}))
	require.NoError(t, err)
	_, err = env.manager.BatchCreate(env.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{
		*ukCreateOp(ukPlain, uuid.Nil, map[string]any{"title": "c"}),
	}})
	require.NoError(t, err)
	require.NoError(t, env.manager.Delete(env.ctx, txDeleteOp(ukPlain, rowID)))
	assert.Empty(t, env.keyEntries(t))
}

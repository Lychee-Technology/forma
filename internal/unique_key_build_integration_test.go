package internal

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/uniquekey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of the offline unique-key build and the startup
// refusal (#651 rule 11). Rows that predate a key are written through a
// manager that enforces none, as a server before the declaration would.

// withBuildBatchSize lowers the build's batch size for one test, so a
// handful of rows crosses every page boundary of the scan, the delete and
// the insert.
func withBuildBatchSize(t *testing.T, n int) {
	t.Helper()
	previous := uniqueKeyBuildBatchSize
	uniqueKeyBuildBatchSize = n
	t.Cleanup(func() { uniqueKeyBuildBatchSize = previous })
}

// preKeyManager writes as a server whose schemas declare no key.
func (e *txEnv) preKeyManager(t *testing.T) forma.EntityManager {
	t.Helper()
	return newUniqueKeyManager(t, e.integrationEnv, WithUniqueKeys(&UniqueKeyCatalog{}))
}

func preKeyCreate(t *testing.T, env *txEnv, em forma.EntityManager, schema string, data map[string]any) uuid.UUID {
	t.Helper()
	record, err := em.Create(env.ctx, ukCreateOp(schema, uuid.Nil, data))
	require.NoError(t, err)
	return record.RowID
}

// stateRows reads the state table as (schema, key) -> attributes hash.
func (e *txEnv) stateRows(t *testing.T) map[[2]int16]string {
	t.Helper()
	built, err := readUniqueKeyState(e.ctx, e.postgresPool, e.tables.EntityMain)
	require.NoError(t, err)
	return built
}

func TestBuildUniqueKeysPopulatesTheKeyTableAndState(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	withBuildBatchSize(t, 2)
	pre := env.preKeyManager(t)
	for i := 0; i < 5; i++ {
		data := ukAccountData("t", fmt.Sprintf("u%d@x", i), fmt.Sprintf("h%d", i))
		data["seq"] = i
		preKeyCreate(t, env, pre, ukAccount, data)
	}
	preKeyCreate(t, env, pre, ukAccount, ukAccountData("t", "", "")) // no component of any key complete
	deleted := preKeyCreate(t, env, pre, ukAccount, ukAccountData("t", "gone@x", "gone"))
	require.NoError(t, pre.Delete(env.ctx, txDeleteOp(ukAccount, deleted)))
	preKeyCreate(t, env, pre, ukFlowDef, map[string]any{"name": "flow", "version": 1})

	// A stale entry, as an interrupted earlier build leaves, is replaced.
	_, err := env.postgresPool.Exec(env.ctx, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, 'stale', $3)",
		sanitizeIdentifier(uniquekey.KeyTable(env.tables.EntityMain))), ukSchemaIDs[ukAccount], ukKeyHandle, uuid.New())
	require.NoError(t, err)

	catalog := env.ukCatalog(t)
	report, err := catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables, UniqueKeyBuildRequest{Schema: ukAccount})
	require.NoError(t, err)
	assert.Equal(t, []UniqueKeyBuildResult{
		{Schema: ukAccount, Key: "tenant_email", Entries: 5},
		{Schema: ukAccount, Key: "handle", Entries: 5},
		{Schema: ukAccount, Key: "seq", Entries: 5},
	}, report.Keys)
	_, err = catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables, UniqueKeyBuildRequest{Schema: ukFlowDef, Key: "name_version"})
	require.NoError(t, err)

	state := env.stateRows(t)
	for _, spec := range catalog.keysOf(ukSchemaIDs[ukAccount]) {
		assert.Equal(t, spec.hash, state[[2]int16{ukSchemaIDs[ukAccount], spec.id}], "key %s is built for its attributes", spec.name)
	}
	_, pending, err := catalog.BuildUniqueKeysOfEmptySchemas(env.ctx, env.postgresPool, env.tables)
	require.NoError(t, err)
	assert.Empty(t, pending)
	require.NoError(t, catalog.CheckUniqueKeysBuilt(env.ctx, env.postgresPool, env.tables.EntityMain))
	env.assertKeyTableMatchesRows(t)

	// A second run finds every key built and writes nothing.
	entries := env.keyEntries(t)
	report, err = catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables, UniqueKeyBuildRequest{Schema: ukAccount})
	require.NoError(t, err)
	for _, k := range report.Keys {
		assert.True(t, k.AlreadyBuilt, k.Key)
	}
	assert.Equal(t, entries, env.keyEntries(t))

	// The built keys are enforced from here on.
	_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("", "", "h3")))
	requireKeyConflict(t, err, ukAccount, "handle", "h3")
}

func TestBuildUniqueKeysRefusesDuplicatesAndWritesNothing(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	pre := env.preKeyManager(t)
	h1 := preKeyCreate(t, env, pre, ukAccount, ukAccountData("t", "a@x", "dup"))
	h2 := preKeyCreate(t, env, pre, ukAccount, ukAccountData("t", "b@x", "dup"))
	e1 := preKeyCreate(t, env, pre, ukAccount, ukAccountData("t", "same@x", "u1"))
	e2 := preKeyCreate(t, env, pre, ukAccount, ukAccountData("t", "same@x", "u2"))

	catalog := env.ukCatalog(t)
	_, err := catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables, UniqueKeyBuildRequest{Schema: ukAccount})
	require.Error(t, err)
	var duplicates *UniqueKeyDuplicatesError
	require.ErrorAs(t, err, &duplicates)
	for _, want := range []string{"unique key tenant_email", "unique key handle", "schema " + ukAccount} {
		assert.Contains(t, err.Error(), want)
	}
	for _, rowID := range []uuid.UUID{h1, h2, e1, e2} {
		assert.Contains(t, err.Error(), rowID.String())
	}
	assert.NotContains(t, err.Error(), "same@x", "the listing names rows, not values")
	assert.Empty(t, env.keyEntries(t), "a refused build writes nothing, not even the keys without duplicates")
	assert.Empty(t, env.stateRows(t))

	err = catalog.CheckUniqueKeysBuilt(env.ctx, env.postgresPool, env.tables.EntityMain)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unique key handle of schema ukaccount is not built: run forma-tools build-unique-keys --schema ukaccount --key handle")

	// A key without duplicates can be built on its own.
	report, err := catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables, UniqueKeyBuildRequest{Schema: ukAccount, Key: "seq"})
	require.NoError(t, err)
	assert.Equal(t, []UniqueKeyBuildResult{{Schema: ukAccount, Key: "seq"}}, report.Keys)
}

func TestCheckUniqueKeysBuiltRefusesUnbuiltChangedAndUndeclaredKeys(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	catalog := env.ukCatalog(t)
	check := func() error { return catalog.CheckUniqueKeysBuilt(env.ctx, env.postgresPool, env.tables.EntityMain) }

	err := check()
	require.Error(t, err)
	for _, want := range []string{
		"unique key tenant_email of schema ukaccount is not built: run forma-tools build-unique-keys --schema ukaccount --key tenant_email",
		"unique key name_version of schema ukflowdef is not built",
		"unique key business_key of schema ukinstance is not built",
	} {
		assert.Contains(t, err.Error(), want)
	}
	assert.NotContains(t, err.Error(), ukPlain)

	// init-db's step builds the keys of empty schemas and names the rest.
	preKeyCreate(t, env, env.preKeyManager(t), ukTaskLink, map[string]any{"parentTaskHash": "p"})
	built, pending, err := catalog.BuildUniqueKeysOfEmptySchemas(env.ctx, env.postgresPool, env.tables)
	require.NoError(t, err)
	assert.Len(t, built, 12, "ukaccount's three keys, ukflowdef's, ukinstance's, uktyped's six and ukcreated's")
	assert.Equal(t, []string{"unique key parent_task_hash of schema uktasklink is not built: run forma-tools build-unique-keys --schema uktasklink --key parent_task_hash"}, pending)
	_, err = catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables, UniqueKeyBuildRequest{Schema: ukTaskLink})
	require.NoError(t, err)
	require.NoError(t, check())

	// A key whose attributes changed under its id is refused, by the check
	// and by the build alike.
	stateTable := sanitizeIdentifier(uniquekey.StateTable(env.tables.EntityMain))
	_, err = env.postgresPool.Exec(env.ctx, fmt.Sprintf("UPDATE %s SET attributes_hash = 'other' WHERE ltbase_schema_id = $1 AND key_id = $2",
		stateTable), ukSchemaIDs[ukAccount], ukKeyHandle)
	require.NoError(t, err)
	for _, err := range []error{check(), func() error {
		_, err := catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables, UniqueKeyBuildRequest{Schema: ukAccount, Key: "handle"})
		return err
	}()} {
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unique key handle of schema ukaccount (id 2) was built for another attribute list")
		assert.Contains(t, err.Error(), "declare the new attribute list under a new id")
	}
	_, err = env.postgresPool.Exec(env.ctx, fmt.Sprintf("DELETE FROM %s WHERE ltbase_schema_id = $1 AND key_id = $2", stateTable),
		ukSchemaIDs[ukAccount], ukKeyHandle)
	require.NoError(t, err)
	_, err = catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables, UniqueKeyBuildRequest{Schema: ukAccount, Key: "handle"})
	require.NoError(t, err)

	// A built key the schema no longer declares is refused until a build of
	// the schema drops it; one of a schema the registry does not hold is not
	// this registry's to judge.
	keyTable := sanitizeIdentifier(uniquekey.KeyTable(env.tables.EntityMain))
	for _, schemaID := range []int16{ukSchemaIDs[ukPlain], 999} {
		_, err = env.postgresPool.Exec(env.ctx, fmt.Sprintf("INSERT INTO %s VALUES ($1, 9, 'h', 0)", stateTable), schemaID)
		require.NoError(t, err)
		_, err = env.postgresPool.Exec(env.ctx, fmt.Sprintf("INSERT INTO %s VALUES ($1, 9, 'v', $2)", keyTable), schemaID, uuid.New())
		require.NoError(t, err)
	}
	err = check()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema ukplain no longer declares a unique key that is still built (key id 9): run forma-tools build-unique-keys --schema ukplain")
	assert.NotContains(t, err.Error(), "999")
	report, err := catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables, UniqueKeyBuildRequest{Schema: ukPlain})
	require.NoError(t, err)
	assert.Equal(t, []int16{9}, report.DroppedKeyIDs)
	require.NoError(t, check())
	for _, k := range env.keyEntries(t) {
		assert.NotEqual(t, ukSchemaIDs[ukPlain], k.schemaID, "the dropped key's entries are gone")
	}
}

func TestBuildUniqueKeysRefusesUnknownSchemaAndKey(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	catalog := env.ukCatalog(t)
	_, err := catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables, UniqueKeyBuildRequest{Schema: "nosuch"})
	require.ErrorContains(t, err, "schema nosuch is not registered")
	_, err = catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables, UniqueKeyBuildRequest{Schema: ukAccount, Key: "nosuch"})
	require.ErrorContains(t, err, "schema ukaccount declares no unique key nosuch")
	require.False(t, errors.Is(err, forma.ErrInvalidInput), "an operator tool error, not a request error")
}

func TestBuildUniqueKeysRebuildRestoresEntriesAWriterMissed(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	catalog := env.ukCatalog(t)
	build := func(rebuild bool) (UniqueKeyBuildReport, error) {
		return catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables,
			UniqueKeyBuildRequest{Schema: ukAccount, Key: "handle", Rebuild: rebuild})
	}
	_, err := build(false)
	require.NoError(t, err)
	_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("", "", "kept")))
	require.NoError(t, err)
	// A writer that does not maintain the key, such as an older binary still
	// running after the key was built, leaves its row without an entry.
	preKeyCreate(t, env, env.preKeyManager(t), ukAccount, ukAccountData("", "", "missed"))

	report, err := build(false)
	require.NoError(t, err)
	assert.Equal(t, []UniqueKeyBuildResult{{Schema: ukAccount, Key: "handle", AlreadyBuilt: true}}, report.Keys)
	assert.Len(t, env.keyEntries(t), 1, "a build of a built key writes nothing")

	report, err = build(true)
	require.NoError(t, err)
	assert.Equal(t, []UniqueKeyBuildResult{{Schema: ukAccount, Key: "handle", Entries: 2, Rebuilt: true}}, report.Keys)
	assert.Len(t, env.keyEntries(t), 2)
	_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("", "", "missed")))
	requireKeyConflict(t, err, ukAccount, "handle", "missed")

	// A rebuild never redefines a key: one built for other attributes is
	// still refused.
	_, err = env.postgresPool.Exec(env.ctx, fmt.Sprintf("UPDATE %s SET attributes_hash = 'other' WHERE ltbase_schema_id = $1 AND key_id = $2",
		sanitizeIdentifier(uniquekey.StateTable(env.tables.EntityMain))), ukSchemaIDs[ukAccount], ukKeyHandle)
	require.NoError(t, err)
	_, err = build(true)
	require.ErrorContains(t, err, "was built for another attribute list")
}

// A rebuild exists because a built key's entries are suspect, so one that
// finds duplicates must not leave the key built over them: the startup check
// would pass and a writer could take the missed value once more. Every key
// the run was building is left unbuilt until a build succeeds.
func TestBuildUniqueKeysFailedRebuildLeavesTheKeysUnbuilt(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	catalog := env.ukCatalog(t)
	check := func() error { return catalog.CheckUniqueKeysBuilt(env.ctx, env.postgresPool, env.tables.EntityMain) }
	build := func(rebuild bool) error {
		_, err := catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables, UniqueKeyBuildRequest{Schema: ukAccount, Rebuild: rebuild})
		return err
	}
	_, pending, err := catalog.BuildUniqueKeysOfEmptySchemas(env.ctx, env.postgresPool, env.tables)
	require.NoError(t, err)
	require.Empty(t, pending)
	_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("t", "a@x", "dup")))
	require.NoError(t, err)
	// A writer that does not maintain the keys duplicates a value after the
	// build, which the startup check cannot see.
	missed := preKeyCreate(t, env, env.preKeyManager(t), ukAccount, ukAccountData("t", "b@x", "dup"))
	require.NoError(t, check())

	err = build(true)
	var duplicates *UniqueKeyDuplicatesError
	require.ErrorAs(t, err, &duplicates)
	assert.Equal(t, "handle", duplicates.Key)
	state := env.stateRows(t)
	for _, spec := range catalog.keysOf(ukSchemaIDs[ukAccount]) {
		assert.NotContains(t, state, [2]int16{ukSchemaIDs[ukAccount], spec.id},
			"key %s is unbuilt: tenant_email and seq have no duplicates, but the rebuild was building them too", spec.name)
	}
	err = check()
	require.Error(t, err, "the server refuses to start after a failed rebuild")
	assert.Contains(t, err.Error(), "unique key handle of schema ukaccount is not built: run forma-tools build-unique-keys --schema ukaccount --key handle")
	assert.Contains(t, err.Error(), "unique key tenant_email of schema ukaccount is not built")

	// Once the group is resolved, a plain build enforces the keys again,
	// over the rows of both writers.
	require.NoError(t, env.manager.Delete(env.ctx, txDeleteOp(ukAccount, missed)))
	require.NoError(t, build(false))
	require.NoError(t, check())
	env.assertKeyTableMatchesRows(t)
	_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("t", "c@x", "dup")))
	requireKeyConflict(t, err, ukAccount, "handle", "dup")
}

package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of which unique keys a repository enforces (#651):
// the catalog a repository holds is fixed once it serves writes, two managers
// share it only when they read every key attribute from the same storage, and
// a key built while its attributes were stored elsewhere is not trusted.

// ukEnvWithAttributes is env's database seen through a copy of the
// unique-key schema files whose ukaccount attributes edit has changed, as a
// server restarted with an edited attributes file sees it. Its manager has a
// repository of its own.
func ukEnvWithAttributes(t *testing.T, env *txEnv, edit func(attrs map[string]map[string]any)) *txEnv {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.CopyFS(dir, os.DirFS(ukSchemaDir)))
	path := filepath.Join(dir, ukAccount+"_attributes.json")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var attrs map[string]map[string]any
	require.NoError(t, json.Unmarshal(raw, &attrs))
	edit(attrs)
	raw, err = json.Marshal(attrs)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	applySchemaLedger(t, env.ctx, env.postgresPool, env.schemaTable, dir)

	registry, err := schemameta.NewFileSchemaRegistry(env.postgresPool, env.schemaTable, dir)
	require.NoError(t, err)
	metadata, err := schemameta.NewMetadataLoader(env.postgresPool, env.schemaTable, dir).LoadMetadata(env.ctx)
	require.NoError(t, err)
	other := &integrationEnv{
		ctx: env.ctx, registry: registry, metadata: metadata,
		tables: env.tables, schemaTable: env.schemaTable, postgresPool: env.postgresPool,
	}
	other.manager = newUniqueKeyManager(t, other)
	return &txEnv{integrationEnv: other}
}

// bindTenantToText02 moves ukaccount's tenant from text_01 to text_02.
func bindTenantToText02(attrs map[string]map[string]any) {
	attrs["tenant"]["column_binding"] = map[string]any{"col_name": "text_02"}
}

// A manager whose registry reads a key attribute from another column than
// the repository's keys do is refused, and the repository goes on enforcing
// the keys it had. Accepting it used to replace the repository's keys, after
// which the first manager wrote tenant to text_01 while its key read text_02,
// found the tuple absent, and committed a duplicate (review of #682).
func TestUniqueKeySharedRepositoryRefusesAnotherStorage(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	manager := env.manager.(*entityManager)
	repo := manager.repository.(*DBPersistentRecordRepository)
	enforced := repo.uniqueKeys
	data := ukAccountData("tenant", "dup@x", "")
	_, err := manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, data))
	require.NoError(t, err)
	_, err = manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, data))
	requireKeyConflict(t, err, ukAccount, "tenant_email", "dup@x")

	other := ukEnvWithAttributes(t, env, bindTenantToText02)
	_, err = NewEntityManager(transform.NewPersistentRecordTransformer(other.registry), repo, nil,
		other.registry, manager.config, nil)
	require.Error(t, err, "a registry that stores tenant in text_02 cannot share a repository whose keys read text_01")
	assert.Contains(t, err.Error(), "tenant_email")
	assert.Contains(t, err.Error(), "tenant")
	assert.Same(t, enforced, repo.uniqueKeys, "a refused manager leaves the repository's keys alone")

	_, err = manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, data))
	requireKeyConflict(t, err, ukAccount, "tenant_email", "dup@x")
	assert.Equal(t, 1, env.mainRowCount(t, ukAccount), "the first manager must still enforce its keys")
	env.assertKeyTableMatchesRows(t)

	// A manager over the same registry enforces the same keys, so it may
	// share the repository; it still does not replace them.
	_, err = NewEntityManager(transform.NewPersistentRecordTransformer(env.registry), repo, nil,
		env.registry, manager.config, nil)
	require.NoError(t, err)
	assert.Same(t, enforced, repo.uniqueKeys, "an equivalent manager leaves the repository's keys in place")
}

// Building managers over a repository that is serving writes reads its keys
// and writes nothing to it: run under -race, this fails if a construction
// assigns the catalog the write cores read. Keys stay enforced throughout.
func TestUniqueKeySharedRepositoryConstructionDuringWrites(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	manager := env.manager.(*entityManager)
	repo := manager.repository.(*DBPersistentRecordRepository)
	enforced := repo.uniqueKeys
	equivalent := env.ukCatalog(t)

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := range 200 {
			var opts []EntityManagerOption
			if i%2 == 0 {
				opts = append(opts, WithUniqueKeys(equivalent))
			}
			if _, err := NewEntityManager(transform.NewPersistentRecordTransformer(env.registry), repo, nil,
				env.registry, manager.config, nil, opts...); err != nil {
				t.Errorf("construction %d: %v", i, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := range 20 {
			data := ukAccountData("t", fmt.Sprintf("w%d@x", i), fmt.Sprintf("h%d", i))
			if _, err := manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, data)); err != nil {
				t.Errorf("create %d: %v", i, err)
				return
			}
			if _, err := manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, data)); !assert.ErrorIs(t, err, forma.ErrUniqueConflict) {
				return
			}
		}
	}()
	close(start)
	wg.Wait()

	assert.Same(t, enforced, repo.uniqueKeys, "constructions must not replace the keys a repository enforces")
	assert.Equal(t, 20, env.mainRowCount(t, ukAccount))
	env.assertKeyTableMatchesRows(t)
}

// A key built while its attributes were stored elsewhere is not built for
// the storage the write path now reads. Here the attributes file moves tenant
// from text_01 to text_02 and binds handle, an EAV attribute, to text_01,
// with no data migration: a row written before reads tenant absent and handle
// "acme" (text_01 still holds its old tenant). The key's declaration is
// unchanged, so the old startup check passed, and a create of handle "acme" took
// a value the row holds as read, committing a duplicate. The check now
// refuses until a build re-derives the entries from the rows as stored.
func TestUniqueKeyStartupRefusesAKeyBuiltForAnotherStorage(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	_, _, err := env.ukCatalog(t).BuildUniqueKeysOfEmptySchemas(env.ctx, env.postgresPool, env.tables)
	require.NoError(t, err)
	env.mustCreateAccount(t, "acme", "x@y", "")

	moved := ukEnvWithAttributes(t, env, func(attrs map[string]map[string]any) {
		bindTenantToText02(attrs)
		attrs["handle"]["column_binding"] = map[string]any{"col_name": "text_01"}
	})
	catalog := moved.ukCatalog(t)
	err = catalog.CheckUniqueKeysBuilt(env.ctx, env.postgresPool, env.tables.EntityMain)
	require.Error(t, err, "keys built while tenant was in text_01 and handle in EAV must not pass as built")
	assert.Contains(t, err.Error(), "unique key tenant_email of schema ukaccount")
	assert.Contains(t, err.Error(), "unique key handle of schema ukaccount")
	assert.Contains(t, err.Error(), "forma-tools build-unique-keys --schema ukaccount --key handle")
	assert.NotContains(t, err.Error(), "unique key seq of schema ukaccount", "seq is stored where it was")

	// A plain build re-derives the moved keys from the rows as now stored.
	report, err := catalog.BuildUniqueKeys(env.ctx, env.postgresPool, env.tables, UniqueKeyBuildRequest{Schema: ukAccount})
	require.NoError(t, err)
	rebuilt := map[string]bool{}
	for _, k := range report.Keys {
		rebuilt[k.Key] = k.Rebuilt
	}
	assert.Equal(t, map[string]bool{"tenant_email": true, "handle": true, "seq": false}, rebuilt)
	require.NoError(t, catalog.CheckUniqueKeysBuilt(env.ctx, env.postgresPool, env.tables.EntityMain))
	moved.assertKeyTableMatchesRows(t)

	_, err = moved.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("", "", "acme")))
	requireKeyConflict(t, err, ukAccount, "handle", "acme")
	_, err = moved.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("acme", "x@y", "")))
	require.NoError(t, err, "no live row holds tenant acme as stored now, so the tuple is free")
	moved.assertKeyTableMatchesRows(t)
}

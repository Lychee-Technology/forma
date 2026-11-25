package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL regressions of #696 review round 4: a unique key over a
// member of an object-or-array union loads, as it did before the registry
// derived the union's array form (R4-F2), keys rows that write the object
// form, and refuses the array form with a published 4xx that leaves the
// row, its key entry and its change_log as they were.

// keyedUnionDocument is unionDocument with a unique key over office.city.
const keyedUnionDocument = `{"type":"object",
	"x-forma-unique-keys":[{"id":1,"name":"office_city","attributes":["office.city"]}],
	"properties":{"id":{"type":"string"},"note":{"type":"string"},
	"office":{"properties":{"city":{"type":"string"}},"anyOf":[{"type":"object"},
		{"type":"array","items":{"type":"object","properties":{"city":{"type":"string"}}}}]}}}`

// setupKeyedUnionEnv loads keyedUnionDocument as schema keyedunion and
// answers a manager over it per mode, on a repository that enforces its
// keys as the factory's does.
func setupKeyedUnionEnv(t *testing.T) (*txEnv, func(strict bool) round3Env) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "keyedunion.json"), []byte(keyedUnionDocument), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "keyedunion_attributes.json"), []byte(unionAttributes), 0o600))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	schemaTable := createSchemaRegistryTableWith(t, ctx, pool, map[string]int16{"keyedunion": 302})
	seedSchemaLedger(t, ctx, pool, schemaTable, dir)
	registry, err := schemameta.NewFileSchemaRegistry(pool, schemaTable, dir)
	require.NoError(t, err, "a key over the union's member loads")
	metadata, err := schemameta.NewMetadataLoader(pool, schemaTable, dir).LoadMetadata(ctx)
	require.NoError(t, err)
	catalog, err := NewUniqueKeyCatalog(registry)
	require.NoError(t, err, "the write path resolves the key")
	env := &txEnv{integrationEnv: &integrationEnv{ctx: ctx, registry: registry, metadata: metadata, tables: tables,
		schemaTable: schemaTable, postgresPool: pool}}
	return env, func(strict bool) round3Env {
		validator, err := schemavalidate.New(SnapshotSchemaDocuments(registry), dir)
		require.NoError(t, err)
		config := &forma.Config{
			Database: forma.DatabaseConfig{TableNames: forma.TableNames{
				SchemaRegistry: schemaTable, EntityMain: tables.EntityMain, EAVData: tables.EAVData, ChangeLog: tables.ChangeLog,
			}},
			Query:       forma.QueryConfig{DefaultPageSize: 20, MaxPageSize: 100},
			Entity:      forma.EntityConfig{SchemaDirectory: dir, ValidateUpdatesStrict: strict},
			Transaction: forma.TransactionConfig{DefaultTimeout: 30 * time.Second},
		}
		repo := NewDBPersistentRecordRepository(pool, metadata, WithUniqueKeyCatalog(catalog))
		em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), repo, nil, registry, config, validator,
			WithUniqueKeys(catalog))
		txr, err := NewTransactor(em)
		require.NoError(t, err)
		return round3Env{txEnv: env, em: em, txr: txr}
	}
}

// requireKeyInArrayRefusal asserts err is the published refusal of a key
// attribute written inside an array.
func requireKeyInArrayRefusal(t *testing.T, err error, msgAndArgs ...any) {
	t.Helper()
	want := "invalid value for attribute 'office.city' (attrID=3): unique key office_city takes one value of it per row, got a value at array indices"
	if failure, ok := err.(bestEffortFailure); ok {
		assert.Equal(t, "UPDATE_FAILED", failure.failure.Code, msgAndArgs...)
		assert.Contains(t, failure.failure.Error, want, msgAndArgs...)
		return
	}
	require.ErrorIs(t, err, forma.ErrInvalidInput, msgAndArgs...)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published, err)
	assert.Contains(t, message, want, msgAndArgs...)
	assert.Contains(t, message, "a unique key's attribute cannot be stored inside an array", msgAndArgs...)
}

// TestUniqueKeyOverAnObjectOrArrayUnionOnPostgreSQL pins R4-F2: in both
// modes, on every update surface but Claim (which refuses any write to a
// key component first), an update writing office as an object moves the
// row's key entry, and one writing it as an array of objects is refused
// with ErrInvalidInput, leaving entity_main, eav_data, change_log and the
// key table as they were, and a Tx that saw the refusal commits. A create
// and an upsert targeting the key are refused the same way, creating
// nothing, and an object-form create still conflicts on the key.
func TestUniqueKeyOverAnObjectOrArrayUnionOnPostgreSQL(t *testing.T) {
	env, managerFor := setupKeyedUnionEnv(t)
	seq := 0
	city := func() string { seq++; return fmt.Sprintf("city-%d", seq) }
	for _, strict := range []bool{false, true} {
		e := managerFor(strict)
		for _, surface := range round3Surfaces {
			if surface.name == "Tx.Claim" {
				continue
			}
			label := fmt.Sprintf("strict=%v %s", strict, surface.name)
			rowID, id := e.createRound3Row(t, "keyedunion", map[string]any{"note": "n", "office": map[string]any{"city": city()}})

			moved := city()
			require.NoError(t, surface.update(t, e, "keyedunion", rowID, &id, map[string]any{"office": map[string]any{"city": moved}}), label)
			assert.Equal(t, map[string]any{"city": moved}, e.read(t, "keyedunion", rowID)["office"], label)
			_, err := e.em.Create(e.ctx, txCreateOp("keyedunion", map[string]any{"id": uuid.NewString(), "office": map[string]any{"city": moved}}))
			require.ErrorIs(t, err, forma.ErrUniqueConflict, "%s: the moved entry keys the row", label)

			before, keys := e.snapshot(t, "keyedunion", rowID), env.keyEntries(t)
			for _, office := range []any{
				[]any{map[string]any{"city": city()}},
				[]any{map[string]any{"city": city()}, map[string]any{"city": city()}},
			} {
				err := surface.update(t, e, "keyedunion", rowID, &id, map[string]any{"office": office})
				requireKeyInArrayRefusal(t, err, "%s %v", label, office)
				assert.Equal(t, before, e.snapshot(t, "keyedunion", rowID), "%s %v: the row as it was", label, office)
				assert.Equal(t, keys, env.keyEntries(t), "%s %v: the key entries as they were", label, office)
			}
		}

		label := fmt.Sprintf("strict=%v", strict)
		keys := env.keyEntries(t)
		_, err := e.em.Create(e.ctx, txCreateOp("keyedunion", map[string]any{"id": uuid.NewString(), "office": []any{map[string]any{"city": city()}}}))
		requireKeyInArrayRefusal(t, err, "%s Create", label)
		err = e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
			create := forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: "keyedunion"}, Type: forma.OperationCreate,
				Data: map[string]any{"id": uuid.NewString(), "office": []any{map[string]any{"city": city()}}}}
			_, err := tx.Upsert(ctx, &forma.UpsertRequest{Create: create, Target: forma.ConflictTarget{Key: "office_city"},
				Action: forma.UpsertUpdate, Updates: map[string]any{"note": "m"}})
			return err
		})
		requireKeyInArrayRefusal(t, err, "%s Tx.Upsert on the key", label)
		assert.Equal(t, keys, env.keyEntries(t), "%s: the refused creates took no entry", label)
	}
}

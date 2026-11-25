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

// Real-PostgreSQL regressions of #696 review round 3, over a schema
// directory of their own: a definition named like a subschema the skeleton
// moves keeps its own verdict (R3-F1), and an object-or-array union's array
// form reads back as written (R3-F2), on the manager's and the Tx's update
// surfaces in both validation modes, with entity_main, eav_data and
// change_log compared directly before and after each refusal.

// round3Schemas are the directory's schemas and their IDs.
var round3Schemas = map[string]int16{"colliding": 300, "union": 301}

// round3Env is a manager and a transactor over round3Schemas in one mode.
type round3Env struct {
	*txEnv
	em  forma.EntityManager
	txr forma.Transactor
}

// setupRound3Env builds the tables and the registry once; newRound3Env
// builds a manager over them per mode.
func setupRound3Env(t *testing.T) (*txEnv, string) {
	t.Helper()
	dir := t.TempDir()
	for name, files := range map[string][2]string{
		"colliding": {collidingDefsDocument, collidingDefsAttributes},
		"union":     {unionDocument, unionAttributes},
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name+".json"), []byte(files[0]), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name+"_attributes.json"), []byte(files[1]), 0o600))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	schemaTable := createSchemaRegistryTableWith(t, ctx, pool, round3Schemas)
	seedSchemaLedger(t, ctx, pool, schemaTable, dir)
	registry, err := schemameta.NewFileSchemaRegistry(pool, schemaTable, dir)
	require.NoError(t, err)
	metadata, err := schemameta.NewMetadataLoader(pool, schemaTable, dir).LoadMetadata(ctx)
	require.NoError(t, err)
	env := &integrationEnv{ctx: ctx, registry: registry, metadata: metadata, tables: tables, schemaTable: schemaTable, postgresPool: pool}
	return &txEnv{integrationEnv: env}, dir
}

func newRound3Env(t *testing.T, env *txEnv, dir string, strict bool) round3Env {
	t.Helper()
	validator, err := schemavalidate.New(SnapshotSchemaDocuments(env.registry), dir)
	require.NoError(t, err)
	config := &forma.Config{
		Database: forma.DatabaseConfig{TableNames: forma.TableNames{
			SchemaRegistry: env.schemaTable, EntityMain: env.tables.EntityMain, EAVData: env.tables.EAVData, ChangeLog: env.tables.ChangeLog,
		}},
		Query:       forma.QueryConfig{DefaultPageSize: 20, MaxPageSize: 100},
		Entity:      forma.EntityConfig{SchemaDirectory: dir, ValidateUpdatesStrict: strict},
		Transaction: forma.TransactionConfig{DefaultTimeout: 30 * time.Second},
	}
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(env.registry),
		NewDBPersistentRecordRepository(env.postgresPool, env.metadata), nil, env.registry, config, validator)
	txr, err := NewTransactor(em)
	require.NoError(t, err)
	return round3Env{txEnv: env, em: em, txr: txr}
}

// round3Surface runs one update of a row of schema whose id is id.
type round3Surface struct {
	name   string
	update func(t *testing.T, e round3Env, schema string, rowID uuid.UUID, id *string, updates map[string]any) error
}

// inTx runs write in a Tx that reads the row after it and commits, and
// answers write's error.
func (e round3Env) inTx(t *testing.T, schema string, rowID uuid.UUID, write func(ctx context.Context, tx forma.Tx) error) error {
	t.Helper()
	var refusal error
	require.NoError(t, e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		refusal = write(ctx, tx)
		_, err := tx.Get(ctx, &forma.QueryRequest{SchemaName: schema, RowID: &rowID})
		require.NoError(t, err, "the Tx stays usable")
		return nil
	}), "the Tx commits")
	return refusal
}

var round3Surfaces = []round3Surface{
	{"Update", func(t *testing.T, e round3Env, schema string, rowID uuid.UUID, _ *string, updates map[string]any) error {
		_, err := e.em.Update(e.ctx, txUpdateOp(schema, rowID, updates))
		return err
	}},
	{"atomic BatchUpdate", func(t *testing.T, e round3Env, schema string, rowID uuid.UUID, _ *string, updates map[string]any) error {
		_, err := e.em.BatchUpdate(e.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{*txUpdateOp(schema, rowID, updates)}})
		return err
	}},
	{"best-effort BatchUpdate", func(t *testing.T, e round3Env, schema string, rowID uuid.UUID, _ *string, updates map[string]any) error {
		result, err := e.em.BatchUpdate(e.ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{*txUpdateOp(schema, rowID, updates)}})
		require.NoError(t, err)
		if len(result.Failed) > 0 {
			return bestEffortFailure{failure: result.Failed[0]}
		}
		return nil
	}},
	{"Tx.Update", func(t *testing.T, e round3Env, schema string, rowID uuid.UUID, _ *string, updates map[string]any) error {
		return e.inTx(t, schema, rowID, func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.Update(ctx, txUpdateOp(schema, rowID, updates))
			return err
		})
	}},
	{"Tx.UpdateWhere", func(t *testing.T, e round3Env, schema string, rowID uuid.UUID, id *string, updates map[string]any) error {
		return e.inTx(t, schema, rowID, func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: schema,
				Condition: &forma.KvCondition{Attr: "id", Value: "equals:" + *id}, Updates: updates, MaxAffected: 1})
			return err
		})
	}},
	{"Tx.Claim", func(t *testing.T, e round3Env, schema string, rowID uuid.UUID, id *string, updates map[string]any) error {
		// A claim must make the row stop matching its condition, so it
		// moves the row's id too.
		moved := uuid.NewString()
		claimed := map[string]any{"id": moved}
		for k, v := range updates {
			claimed[k] = v
		}
		err := e.inTx(t, schema, rowID, func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: schema,
				Condition: &forma.KvCondition{Attr: "id", Value: "equals:" + *id}, Limit: 1, Updates: claimed})
			return err
		})
		if err == nil {
			*id = moved
		}
		return err
	}},
	{"Tx.Upsert on the row ID", func(t *testing.T, e round3Env, schema string, rowID uuid.UUID, _ *string, updates map[string]any) error {
		return e.inTx(t, schema, rowID, func(ctx context.Context, tx forma.Tx) error {
			create := forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: schema, RowID: rowID},
				Type: forma.OperationCreate, Data: map[string]any{"id": uuid.NewString()}}
			_, err := tx.Upsert(ctx, &forma.UpsertRequest{Create: create, Target: forma.ConflictTarget{RowID: true},
				Action: forma.UpsertUpdate, Updates: updates})
			return err
		})
	}},
}

// createRound3Row creates a row of schema holding data and a fresh id, and
// answers its row ID and id.
func (e round3Env) createRound3Row(t *testing.T, schema string, data map[string]any) (uuid.UUID, string) {
	t.Helper()
	id := uuid.NewString()
	data["id"] = id
	created, err := e.em.Create(e.ctx, txCreateOp(schema, data))
	require.NoError(t, err)
	return created.RowID, id
}

func (e round3Env) read(t *testing.T, schema string, rowID uuid.UUID) map[string]any {
	t.Helper()
	record, err := e.em.Get(e.ctx, &forma.QueryRequest{SchemaName: schema, RowID: &rowID})
	require.NoError(t, err)
	return record.Attributes
}

// TestRound3ShapeRegressionsOnPostgreSQL pins R3-F1 and R3-F2 on
// PostgreSQL. R3-F1: an array at note, which references a string
// definition named like a moved subschema, is refused, leaving the row
// exactly as it was, and codes, which references an array definition named
// so, takes a new array. R3-F2: the union's array form, written by an
// update, reads back as an array of objects, its object form as an object,
// and an array at its scalar member is refused with the row unchanged.
func TestRound3ShapeRegressionsOnPostgreSQL(t *testing.T) {
	env, dir := setupRound3Env(t)
	for _, strict := range []bool{false, true} {
		e := newRound3Env(t, env, dir, strict)
		for _, surface := range round3Surfaces {
			label := fmt.Sprintf("strict=%v %s", strict, surface.name)

			rowID, id := e.createRound3Row(t, "colliding", map[string]any{"note": "n", "codes": []any{"c1"}, "tags": "t"})
			before := e.snapshot(t, "colliding", rowID)
			for _, updates := range []map[string]any{{"note": []any{"bad"}}, {"note": []any{}}, {"note": []any{"bad"}, "codes": []any{"c2"}}} {
				err := surface.update(t, e, "colliding", rowID, &id, updates)
				requireShapeRefusal(t, err, "note", label)
				assert.Equal(t, before, e.snapshot(t, "colliding", rowID), "%s %v: the row as it was", label, updates)
			}
			require.NoError(t, surface.update(t, e, "colliding", rowID, &id, map[string]any{"codes": []any{"c2", "c3"}}), label)
			assert.Equal(t, []any{"c2", "c3"}, e.read(t, "colliding", rowID)["codes"], label)

			rowID, id = e.createRound3Row(t, "union", map[string]any{"note": "n", "office": map[string]any{"city": "o0"}})
			for _, office := range []any{
				[]any{map[string]any{"city": "o1"}, map[string]any{"city": "o2"}},
				map[string]any{"city": "o3"},
				[]any{map[string]any{"city": "o4"}},
			} {
				require.NoError(t, surface.update(t, e, "union", rowID, &id, map[string]any{"office": office}), "%s %v", label, office)
				assert.Equal(t, office, e.read(t, "union", rowID)["office"], "%s %v: read back as written", label, office)
			}
			before = e.snapshot(t, "union", rowID)
			err := surface.update(t, e, "union", rowID, &id, map[string]any{"office": []any{map[string]any{"city": []any{"x"}}}})
			requireShapeRefusal(t, err, "office.city", label)
			assert.Equal(t, before, e.snapshot(t, "union", rowID), "%s: the row as it was", label)
		}
	}
}

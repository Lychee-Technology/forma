package internal

import (
	"context"
	"fmt"
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

// #696 under strict update validation (Entity.ValidateUpdatesStrict), on
// real PostgreSQL: the shape rule is a storage invariant, so strict mode
// refuses the same containers with the same published refusal, before the
// validator is consulted. shapestrict declares required scalars, bound
// (tenant) and in EAV (email), beside optional ones, bound (label) and in
// EAV (note, a nullable string).

const (
	shapeStrictDir    = "testdata/scalar_shape_strict"
	shapeStrictSchema = "shapestrict"
)

// setupShapeStrictEnv builds a manager over shapestrict's file registry
// that validates creates and updates strictly.
func setupShapeStrictEnv(t *testing.T) *txEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	schemaTable := createSchemaRegistryTableWith(t, ctx, pool, map[string]int16{shapeStrictSchema: 140})
	seedSchemaLedger(t, ctx, pool, schemaTable, shapeStrictDir)
	registry, err := schemameta.NewFileSchemaRegistry(pool, schemaTable, shapeStrictDir)
	require.NoError(t, err)
	metadata, err := schemameta.NewMetadataLoader(pool, schemaTable, shapeStrictDir).LoadMetadata(ctx)
	require.NoError(t, err)
	validator, err := schemavalidate.New(SnapshotSchemaDocuments(registry), shapeStrictDir)
	require.NoError(t, err)
	config := &forma.Config{
		Database: forma.DatabaseConfig{TableNames: forma.TableNames{
			SchemaRegistry: schemaTable, EntityMain: tables.EntityMain, EAVData: tables.EAVData, ChangeLog: tables.ChangeLog,
		}},
		Query:       forma.QueryConfig{DefaultPageSize: 20, MaxPageSize: 100},
		Entity:      forma.EntityConfig{SchemaDirectory: shapeStrictDir, ValidateUpdatesStrict: true},
		Transaction: forma.TransactionConfig{DefaultTimeout: 30 * time.Second},
	}
	env := &integrationEnv{ctx: ctx, registry: registry, metadata: metadata, tables: tables, schemaTable: schemaTable, postgresPool: pool}
	env.manager = mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry),
		NewDBPersistentRecordRepository(pool, metadata), nil, registry, config, validator)
	txr, err := NewTransactor(env.manager)
	require.NoError(t, err)
	return &txEnv{integrationEnv: env, txr: txr.(*transactor)}
}

// shapeStrictUpdates are the issue's shapes at each of shapestrict's
// scalars: required and optional, bound and in EAV.
func shapeStrictUpdates() []shapeUpdate {
	var cases []shapeUpdate
	for _, attr := range []struct {
		name string
		id   int
	}{{"tenant", 1}, {"email", 2}, {"label", 3}, {"note", 4}} {
		for _, shape := range []struct {
			name  string
			value any
			got   string
		}{
			{"{}", map[string]any{}, "an empty object"},
			{"[]", []any{}, "an empty array"},
			{"[x]", []any{"x"}, "an array of 1 element"},
			{"[x, y]", []any{"x", "y"}, "an array of 2 elements"},
			{"{x: y}", map[string]any{"x": "y"}, "an object"},
		} {
			cases = append(cases, shapeUpdate{attr.name + " " + shape.name, map[string]any{attr.name: shape.value},
				ukShapeRefusal(attr.name, attr.id, shape.got)})
		}
	}
	return cases
}

// shapeStrictTxRoutes are the Tx routes that write an existing row, found
// by its label where a route takes a condition.
func shapeStrictTxRoutes() map[string]func(context.Context, forma.Tx, uuid.UUID, string, map[string]any) error {
	where := func(label string) forma.Condition { return &forma.KvCondition{Attr: "label", Value: "equals:" + label} }
	return map[string]func(context.Context, forma.Tx, uuid.UUID, string, map[string]any) error{
		"Tx.Update": func(ctx context.Context, tx forma.Tx, rowID uuid.UUID, _ string, updates map[string]any) error {
			_, err := tx.Update(ctx, txUpdateOp(shapeStrictSchema, rowID, updates))
			return err
		},
		"Tx.UpdateWhere": func(ctx context.Context, tx forma.Tx, _ uuid.UUID, label string, updates map[string]any) error {
			_, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: shapeStrictSchema, Condition: where(label), MaxAffected: 1, Updates: updates})
			return err
		},
		"Tx.Upsert by row ID": func(ctx context.Context, tx forma.Tx, rowID uuid.UUID, _ string, updates map[string]any) error {
			result, err := tx.Upsert(ctx, &forma.UpsertRequest{
				Create: *ukCreateOp(shapeStrictSchema, rowID, map[string]any{"tenant": "other", "email": "other@x"}),
				Target: forma.ConflictTarget{RowID: true}, Action: forma.UpsertUpdate, Updates: updates,
			})
			if err == nil && result.Outcome != forma.UpsertUpdated {
				err = fmt.Errorf("upsert outcome %v", result.Outcome)
			}
			return err
		},
		"Tx.Claim": func(ctx context.Context, tx forma.Tx, _ uuid.UUID, label string, updates map[string]any) error {
			_, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: shapeStrictSchema, Condition: where(label), Limit: 1, Updates: updates})
			return err
		},
	}
}

// TestScalarShapeRefusedUnderStrictValidation is the issue's acceptance in
// strict mode: every route refuses each shape as invalid input naming the
// attribute, a required one included, and the row's three tables are as
// they were, read by direct SQL after each refusal and after a transaction
// that only saw the refusal committed. A transaction that goes on after the
// refusal commits its next write.
func TestScalarShapeRefusedUnderStrictValidation(t *testing.T) {
	env := setupShapeStrictEnv(t)
	i := 0
	newRow := func() (uuid.UUID, string) {
		i++
		label := fmt.Sprintf("l-%d", i)
		record, err := env.manager.Create(env.ctx, ukCreateOp(shapeStrictSchema, uuid.Nil,
			map[string]any{"tenant": fmt.Sprintf("T-%d", i), "email": fmt.Sprintf("e-%d@x", i), "label": label, "note": "n"}))
		require.NoError(t, err)
		return record.RowID, label
	}
	other, _ := newRow()

	for _, tc := range shapeStrictUpdates() {
		rowID, label := newRow()
		before := env.snapshot(t, shapeStrictSchema, rowID)

		_, err := env.manager.Update(env.ctx, txUpdateOp(shapeStrictSchema, rowID, tc.updates))
		requireShapeRefused(t, err, tc.want, "Update %s", tc.name)
		assert.Equal(t, before, env.snapshot(t, shapeStrictSchema, rowID), "Update %s: the row as it was", tc.name)

		_, err = env.manager.BatchUpdate(env.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{
			*txUpdateOp(shapeStrictSchema, other, map[string]any{"note": "changed"}), *txUpdateOp(shapeStrictSchema, rowID, tc.updates)}})
		requireShapeRefused(t, err, "operation[1]: "+tc.want, "atomic BatchUpdate %s", tc.name)
		assert.Equal(t, before, env.snapshot(t, shapeStrictSchema, rowID), "atomic BatchUpdate %s: the row as it was", tc.name)

		result, err := env.manager.BatchUpdate(env.ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{*txUpdateOp(shapeStrictSchema, rowID, tc.updates)}})
		require.NoError(t, err)
		require.Len(t, result.Failed, 1, "best-effort BatchUpdate %s", tc.name)
		assert.Contains(t, result.Failed[0].Error, tc.want, "best-effort BatchUpdate %s", tc.name)
		assert.Equal(t, before, env.snapshot(t, shapeStrictSchema, rowID), "best-effort BatchUpdate %s: the row as it was", tc.name)

		for route, write := range shapeStrictTxRoutes() {
			require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
				requireShapeRefused(t, write(ctx, tx, rowID, label, tc.updates), tc.want, "%s %s", route, tc.name)
				return nil
			}), "%s %s", route, tc.name)
			assert.Equal(t, before, env.snapshot(t, shapeStrictSchema, rowID), "%s %s: the committed row as it was", route, tc.name)
		}
	}

	rowID, label := newRow()
	before := env.snapshot(t, shapeStrictSchema, rowID)
	for route, write := range shapeStrictTxRoutes() {
		require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
			requireShapeRefused(t, write(ctx, tx, rowID, label, map[string]any{"email": []any{"x"}}), ukShapeRefusal("email", 2, "an array of 1 element"), route)
			_, err := tx.Update(ctx, txUpdateOp(shapeStrictSchema, other, map[string]any{"note": route}))
			return err
		}), "%s: the refusal left the Tx usable", route)
		assert.Equal(t, route, env.attrs(t, shapeStrictSchema, other)["note"], "%s: the write after the refusal committed", route)
		assert.Equal(t, before, env.snapshot(t, shapeStrictSchema, rowID), "%s: the refused row as it was", route)
	}
}

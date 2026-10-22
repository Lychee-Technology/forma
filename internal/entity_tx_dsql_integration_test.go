package internal

import (
	"context"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

const dsqlTxSchemaName = "dsql_tx"

// newDSQLTxEnv answers a manager and transactor on the aurora-dsql engine
// over the engine's test target, with one all-column attribute, status.
func newDSQLTxEnv(t *testing.T) (*condPG, dsqlTarget) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	d := openDSQLTarget(t, ctx)
	tables := d.createDSQLTables(t, ctx)
	cache := forma.SchemaAttributeCache{
		"status": {AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
	}
	metadata := schemameta.NewMetadataCache()
	require.NoError(t, metadata.RegisterSchema(dsqlTxSchemaName, 1, cache))
	registry := &stubSchemaRegistry{schemaID: 1, schemaName: dsqlTxSchemaName, cache: cache}

	config := createTestConfig()
	config.Database.Engine = forma.EngineAuroraDSQL
	config.Database.TableNames = forma.TableNames{EntityMain: tables.EntityMain, EAVData: tables.EAVData, ChangeLog: tables.ChangeLog}
	config.Transaction.DefaultTimeout = 30 * time.Second
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), newDSQLModeRepo(t, d.pool, metadata), nil, registry, config, nil)
	txr, err := NewTransactor(em)
	require.NoError(t, err)
	return &condPG{ctx: ctx, pool: d.pool, tables: tables, em: em, txr: txr.(*transactor)}, d
}

// On the aurora-dsql engine a Tx writes a row's guard once however many of
// its operations lock the row, so WithTxMaxPhysicalRows charges one guard row
// per row (docs/claims.md, docs/transactions.md). Each case locks its row
// twice: a claim and a predicate update lock their candidates and then write
// each through Update, and an assertion precedes an update. An all-column
// update modifies three rows: the guard, entity_main and change_log.
func TestDSQLEngineTxChargesEachRowGuardOnce(t *testing.T) {
	pending := &forma.KvCondition{Attr: "status", Value: "equals:pending"}
	cases := map[string]func(ctx context.Context, tx forma.Tx, rowID uuid.UUID) error{
		"claim": func(ctx context.Context, tx forma.Tx, _ uuid.UUID) error {
			_, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: dsqlTxSchemaName, Limit: 1,
				Condition: pending, Updates: map[string]any{"status": "done"}})
			return err
		},
		"update where": func(ctx context.Context, tx forma.Tx, _ uuid.UUID) error {
			_, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: dsqlTxSchemaName,
				Condition: pending, Updates: map[string]any{"status": "done"}})
			return err
		},
		"assert then update": func(ctx context.Context, tx forma.Tx, rowID uuid.UUID) error {
			if err := tx.AssertRow(ctx, dsqlTxSchemaName, rowID, nil); err != nil {
				return err
			}
			_, err := tx.Update(ctx, &forma.EntityOperation{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: dsqlTxSchemaName, RowID: rowID},
				Updates:          map[string]any{"status": "done"}})
			return err
		},
	}
	for name, write := range cases {
		t.Run(name, func(t *testing.T) {
			env, d := newDSQLTxEnv(t)
			created, err := env.em.Create(env.ctx, &forma.EntityOperation{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: dsqlTxSchemaName},
				Data:             map[string]any{"status": "pending"}})
			require.NoError(t, err)

			require.NoError(t, env.txr.InTx(env.ctx, func(ctx context.Context, tx forma.Tx) error {
				if err := write(ctx, tx, created.RowID); err != nil {
					return err
				}
				require.EqualValues(t, 3, tx.(*txSession).meter.rows, "guard, entity_main and change_log, each once")
				return nil
			}, forma.WithTxMaxPhysicalRows(3)))

			require.EqualValues(t, 2, d.guardGeneration(env.ctx, t, env.tables, created.RowID),
				"the create and the Tx each wrote the guard once")
			stored, err := env.em.Get(env.ctx, &forma.QueryRequest{SchemaName: dsqlTxSchemaName, RowID: &created.RowID})
			require.NoError(t, err)
			require.Equal(t, "done", stored.Attributes["status"])
		})
	}
}

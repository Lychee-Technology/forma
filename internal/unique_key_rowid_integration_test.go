package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of caller-supplied row IDs (#651 part A): a non-nil
// RowID on Create, BatchCreate and Tx.Create is the new row's ID, a live
// duplicate is ErrUniqueConflict that writes nothing, and a deleted ID can be
// recreated above its tombstone.

func (e *txEnv) mainRowExists(t *testing.T, schema string, rowID uuid.UUID) bool {
	t.Helper()
	var exists bool
	require.NoError(t, e.postgresPool.QueryRow(e.ctx, fmt.Sprintf(
		"SELECT EXISTS (SELECT 1 FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2)",
		sanitizeIdentifier(e.tables.EntityMain)), e.schemaID(t, schema), rowID).Scan(&exists))
	return exists
}

func TestCreateHonoursCallerRowIDOnEverySurface(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	plain := func(title string) map[string]any { return map[string]any{"title": title} }

	viaCreate := uuid.New()
	record, err := env.manager.Create(env.ctx, ukCreateOp(ukPlain, viaCreate, plain("create")))
	require.NoError(t, err)
	assert.Equal(t, viaCreate, record.RowID)

	viaTx := uuid.New()
	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		record, err := tx.Create(ctx, ukCreateOp(ukPlain, viaTx, plain("tx")))
		if err == nil && record.RowID != viaTx {
			return fmt.Errorf("Tx.Create answered row %s, want %s", record.RowID, viaTx)
		}
		return err
	}))

	for _, atomic := range []bool{true, false} {
		ids := []uuid.UUID{uuid.New(), uuid.New()}
		result, err := env.manager.BatchCreate(env.ctx, &forma.BatchOperation{Atomic: atomic, Operations: []forma.EntityOperation{
			*ukCreateOp(ukPlain, ids[0], plain("batch")), *ukCreateOp(ukPlain, ids[1], plain("batch")),
		}})
		require.NoError(t, err)
		require.Empty(t, result.Failed)
		require.Len(t, result.Successful, 2)
		for i, id := range ids {
			assert.Equal(t, id, result.Successful[i].RowID, "atomic=%v", atomic)
			assert.True(t, env.mainRowExists(t, ukPlain, id), "atomic=%v", atomic)
		}
	}

	for _, id := range []uuid.UUID{viaCreate, viaTx} {
		assert.True(t, env.mainRowExists(t, ukPlain, id))
	}
}

func TestCreateWithNilRowIDStillAssignsAVersion7ID(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	record, err := env.manager.Create(env.ctx, ukCreateOp(ukPlain, uuid.Nil, map[string]any{"title": "assigned"}))
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, record.RowID)
	assert.Equal(t, uuid.Version(7), record.RowID.Version())
	assert.True(t, env.mainRowExists(t, ukPlain, record.RowID))
}

// The duplicate is refused before any write on every surface, on a schema
// with keys and one without: direct SQL shows the live row unchanged.
func TestCreateWithLiveRowIDIsUniqueConflictAndWritesNothing(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	cases := map[string]func() map[string]any{
		ukPlain:   func() map[string]any { return map[string]any{"title": "dup"} },
		ukAccount: func() map[string]any { return ukAccountData("t", "dup@x", "dup-"+uuid.NewString()) },
	}
	for schema, data := range cases {
		t.Run(schema, func(t *testing.T) {
			rowID := uuid.New()
			_, err := env.manager.Create(env.ctx, ukCreateOp(schema, rowID, data()))
			require.NoError(t, err)
			before := env.snapshot(t, schema, rowID)
			entriesBefore := env.keyEntries(t)
			assertRowIDConflict := func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, forma.ErrUniqueConflict)
				assert.Contains(t, err.Error(), fmt.Sprintf("schema %s already has a row with ID %s", schema, rowID))
			}

			_, err = env.manager.Create(env.ctx, ukCreateOp(schema, rowID, data()))
			assertRowIDConflict(t, err)

			other := uuid.New()
			result, err := env.manager.BatchCreate(env.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{
				*ukCreateOp(ukPlain, other, map[string]any{"title": "rolled back"}), *ukCreateOp(schema, rowID, data()),
			}})
			assertRowIDConflict(t, err)
			assert.Nil(t, result)
			assert.False(t, env.mainRowExists(t, ukPlain, other), "an atomic batch rolls back entirely")

			result, err = env.manager.BatchCreate(env.ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{
				*ukCreateOp(ukPlain, other, map[string]any{"title": "kept"}), *ukCreateOp(schema, rowID, data()),
			}})
			require.NoError(t, err)
			require.Len(t, result.Failed, 1)
			assert.Equal(t, "UNIQUE_CONFLICT", result.Failed[0].Code)
			assert.True(t, env.mainRowExists(t, ukPlain, other), "a best-effort batch keeps its other operations")

			// Inside a Tx the conflict is an expected outcome: the Tx goes on
			// and commits its other writes.
			survivor := uuid.New()
			require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
				_, err := tx.Create(ctx, ukCreateOp(schema, rowID, data()))
				if !errors.Is(err, forma.ErrUniqueConflict) {
					return fmt.Errorf("Tx.Create of a live row ID answered %v, want ErrUniqueConflict", err)
				}
				_, err = tx.Create(ctx, ukCreateOp(ukPlain, survivor, map[string]any{"title": "after the conflict"}))
				return err
			}))
			assert.True(t, env.mainRowExists(t, ukPlain, survivor))

			assert.Equal(t, before, env.snapshot(t, schema, rowID), "the live row must be untouched")
			assert.Equal(t, entriesBefore, env.keyEntries(t), "no key entry may change")
			env.assertKeyTableMatchesRows(t)
		})
	}
}

// #274 still holds for a caller's row ID: the recreate's version stamps
// above the tombstone the delete retained, and its keys are taken again.
func TestRecreateOfDeletedRowIDStampsAboveTheTombstone(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	rowID := uuid.New()
	data := ukAccountData("t", "again@x", "again")
	_, err := env.manager.Create(env.ctx, ukCreateOp(ukAccount, rowID, data))
	require.NoError(t, err)
	require.NoError(t, env.manager.Delete(env.ctx, txDeleteOp(ukAccount, rowID)))
	assert.Empty(t, env.keyEntriesOfRow(t, rowID), "a delete releases the row's keys")

	var tombstone int64
	require.NoError(t, env.postgresPool.QueryRow(env.ctx, fmt.Sprintf(
		"SELECT max(changed_at) FROM %s WHERE schema_id = $1 AND row_id = $2 AND deleted_at IS NOT NULL",
		sanitizeIdentifier(env.tables.ChangeLog)), env.schemaID(t, ukAccount), rowID).Scan(&tombstone))

	_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, rowID, data))
	require.NoError(t, err)
	updatedAt, _ := env.hotVersion(t, env.postgresPool, ukAccount, rowID)
	assert.Greater(t, updatedAt, tombstone)
	assert.Len(t, env.keyEntriesOfRow(t, rowID), 2, "the recreate takes its tenant_email and handle keys again; seq is absent")
	env.assertKeyTableMatchesRows(t)
}

func TestBatchCreateRefusesARowIDTwiceBeforeAnyWrite(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	rowID := uuid.New()
	for _, atomic := range []bool{true, false} {
		_, err := env.manager.BatchCreate(env.ctx, &forma.BatchOperation{Atomic: atomic, Operations: []forma.EntityOperation{
			*ukCreateOp(ukPlain, uuid.New(), map[string]any{"title": "first"}),
			*ukCreateOp(ukPlain, rowID, map[string]any{"title": "a"}),
			*ukCreateOp(ukPlain, rowID, map[string]any{"title": "b"}),
		}})
		require.ErrorIs(t, err, forma.ErrInvalidInput, "atomic=%v", atomic)
		assert.Contains(t, err.Error(), rowID.String())
		assert.Zero(t, env.schemaRowCount(t, ukPlain), "atomic=%v: the batch must write nothing", atomic)
	}
}

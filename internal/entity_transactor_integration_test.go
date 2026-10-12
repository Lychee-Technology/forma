package internal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL proofs of InTx (#648). Every post-state is read with
// direct SQL over all three tables (entity_main, EAV, change_log).

// The #593 transaction list.

func TestTxCreateAndCreateAcrossTwoSchemasCommitTogether(t *testing.T) {
	env := setupTxEnv(t)
	var a, b *forma.DataRecord
	err := env.inTx(func(ctx context.Context, tx forma.Tx) (err error) {
		if a, err = tx.Create(ctx, txCreateOp(txSchemaA, txActivity("first"))); err != nil {
			return err
		}
		b, err = tx.Create(ctx, txCreateOp(txSchemaB, txVisit("scheduled")))
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, "first", env.attrs(t, txSchemaA, a.RowID)["summary"])
	assert.Equal(t, "scheduled", env.attrs(t, txSchemaB, b.RowID)["status"])
	for _, row := range []struct {
		schema string
		id     uuid.UUID
	}{{txSchemaA, a.RowID}, {txSchemaB, b.RowID}} {
		updatedAt, hot := env.hotVersion(t, env.postgresPool, row.schema, row.id)
		assert.Equal(t, []int64{updatedAt}, hot, "%s has its change_log entry", row.schema)
	}
}

func TestTxUpdateAndCreateCommitTogether(t *testing.T) {
	env := setupTxEnv(t)
	a := env.mustCreate(t, txSchemaA, txActivity("before"))
	var b *forma.DataRecord
	err := env.inTx(func(ctx context.Context, tx forma.Tx) (err error) {
		if _, err = tx.Update(ctx, txUpdateOp(txSchemaA, a, map[string]any{"summary": "after"})); err != nil {
			return err
		}
		b, err = tx.Create(ctx, txCreateOp(txSchemaB, txVisit("scheduled")))
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, "after", env.attrs(t, txSchemaA, a)["summary"])
	assert.Equal(t, "scheduled", env.attrs(t, txSchemaB, b.RowID)["status"])
}

func TestTxUpdateDeleteAndCreateCommitTogether(t *testing.T) {
	env := setupTxEnv(t)
	a := env.mustCreate(t, txSchemaA, txActivity("before"))
	d := env.mustCreate(t, txSchemaB, txVisit("scheduled"))
	var c *forma.DataRecord
	err := env.inTx(func(ctx context.Context, tx forma.Tx) (err error) {
		if _, err = tx.Update(ctx, txUpdateOp(txSchemaA, a, map[string]any{"summary": "after"})); err != nil {
			return err
		}
		if err = tx.Delete(ctx, txDeleteOp(txSchemaB, d)); err != nil {
			return err
		}
		c, err = tx.Create(ctx, txCreateOp(txSchemaC, txCommunication("email")))
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, "after", env.attrs(t, txSchemaA, a)["summary"])
	assert.Equal(t, "email", env.attrs(t, txSchemaC, c.RowID)["type"])
	gone := env.snapshot(t, txSchemaB, d)
	assert.Empty(t, gone.main)
	assert.Equal(t, "[]", gone.eav)
	assert.Contains(t, gone.changeLog, `"deleted_at"`, "the delete's tombstone committed")
	assert.NotContains(t, gone.changeLog, `"deleted_at":null`)
}

func TestTxValidationFailureRollsBackEveryEarlierOperation(t *testing.T) {
	env := setupTxEnv(t)
	a := env.mustCreate(t, txSchemaA, txActivity("before"))
	before := env.snapshot(t, txSchemaA, a)
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		if _, err := tx.Create(ctx, txCreateOp(txSchemaB, txVisit("scheduled"))); err != nil {
			return err
		}
		if _, err := tx.Update(ctx, txUpdateOp(txSchemaA, a, map[string]any{"summary": "after"})); err != nil {
			return err
		}
		_, err := tx.Create(ctx, txCreateOp(txSchemaC, txCommunication("fax")))
		return err
	})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	assert.Equal(t, before, env.snapshot(t, txSchemaA, a))
	assert.Zero(t, env.schemaRowCount(t, txSchemaB))
	assert.Zero(t, env.schemaRowCount(t, txSchemaC))
}

func TestTxStorageFailureRollsBackEveryEarlierOperation(t *testing.T) {
	env := setupTxEnv(t)
	a := env.mustCreate(t, txSchemaA, txActivity("before"))
	before := env.snapshot(t, txSchemaA, a)
	var injected error
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		if _, err := tx.Update(ctx, txUpdateOp(txSchemaA, a, map[string]any{"summary": "after"})); err != nil {
			return err
		}
		injected = env.armFaultAfterMainInsert()
		_, err := tx.Create(ctx, txCreateOp(txSchemaB, txVisit("scheduled")))
		return err
	})
	require.ErrorIs(t, err, injected)
	assert.Equal(t, before, env.snapshot(t, txSchemaA, a))
	assert.Zero(t, env.schemaRowCount(t, txSchemaB))
}

func TestTxChangeLogRowsRollBackWithTheTransaction(t *testing.T) {
	env := setupTxEnv(t)
	a := env.mustCreate(t, txSchemaA, txActivity("before"))
	before := env.snapshot(t, txSchemaA, a)
	var b *forma.DataRecord
	stop := errors.New("caller abandons the step")
	err := env.inTx(func(ctx context.Context, tx forma.Tx) (err error) {
		if _, err = tx.Update(ctx, txUpdateOp(txSchemaA, a, map[string]any{"summary": "after"})); err != nil {
			return err
		}
		if b, err = tx.Create(ctx, txCreateOp(txSchemaB, txVisit("scheduled"))); err != nil {
			return err
		}
		// Inside the transaction both change_log writes exist.
		raw := tx.(*txSession).tx.Tx
		_, hotA := env.hotVersion(t, raw, txSchemaA, a)
		_, hotB := env.hotVersion(t, raw, txSchemaB, b.RowID)
		require.Len(t, hotA, 1)
		require.Len(t, hotB, 1)
		return stop
	})
	require.ErrorIs(t, err, stop)
	assert.Equal(t, before.changeLog, env.snapshot(t, txSchemaA, a).changeLog, "A's change_log is as it was")
	assert.Equal(t, "[]", env.snapshot(t, txSchemaB, b.RowID).changeLog, "B left no change_log row")
}

// Special scenario 1 of #593: update A, create B and C in three schemas, and
// the C step fails. After InTx, A is exactly as before and B and C left
// nothing in any table, whichever way C failed.
func TestTxRollbackScenarioOne(t *testing.T) {
	variants := []struct {
		name string
		// stepC runs C's step and answers what fn returns.
		stepC func(env *txEnv, ctx context.Context, tx forma.Tx) error
		check func(t *testing.T, err error)
	}{
		{
			name: "fn returns an error after creating C",
			stepC: func(env *txEnv, ctx context.Context, tx forma.Tx) error {
				if _, err := tx.Create(ctx, txCreateOp(txSchemaC, txCommunication("email"))); err != nil {
					return err
				}
				return errScenarioStep
			},
			check: func(t *testing.T, err error) { require.ErrorIs(t, err, errScenarioStep) },
		},
		{
			name: "C fails with a storage error after its main-table insert",
			stepC: func(env *txEnv, ctx context.Context, tx forma.Tx) error {
				injected := env.armFaultAfterMainInsert()
				_, err := tx.Create(ctx, txCreateOp(txSchemaC, txCommunication("email")))
				if !errors.Is(err, injected) {
					return errors.New("the injected fault did not fire")
				}
				return err
			},
			check: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "injected storage fault after the entity_main insert")
			},
		},
		{
			name: "C's Create refuses invalid input before writing",
			stepC: func(env *txEnv, ctx context.Context, tx forma.Tx) error {
				_, err := tx.Create(ctx, txCreateOp(txSchemaC, txCommunication("fax")))
				return err
			},
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, forma.ErrInvalidInput)
				require.NotErrorIs(t, err, forma.ErrTxAborted, "a pre-write refusal does not poison")
			},
		},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			env := setupTxEnv(t)
			a := env.mustCreate(t, txSchemaA, txActivity("before"))
			before := env.snapshot(t, txSchemaA, a)

			err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
				if _, err := tx.Update(ctx, txUpdateOp(txSchemaA, a, map[string]any{"summary": "after"})); err != nil {
					return err
				}
				if _, err := tx.Create(ctx, txCreateOp(txSchemaB, txVisit("scheduled"))); err != nil {
					return err
				}
				return v.stepC(env, ctx, tx)
			})
			v.check(t, err)

			assert.Equal(t, before, env.snapshot(t, txSchemaA, a),
				"A's attributes, ltbase_updated_at, EAV rows and change_log rows are unchanged")
			assert.Zero(t, env.schemaRowCount(t, txSchemaB), "B does not exist")
			assert.Zero(t, env.schemaRowCount(t, txSchemaC), "C does not exist")
		})
	}
}

var errScenarioStep = errors.New("the C step failed")

func TestTxRepeatedWritesToOneRow(t *testing.T) {
	env := setupTxEnv(t)
	var rowID uuid.UUID
	var versions []int64
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		raw := tx.(*txSession).tx.Tx
		stamp := func() {
			updatedAt, hot := env.hotVersion(t, raw, txSchemaA, rowID)
			require.Equal(t, []int64{updatedAt}, hot, "one hot change_log entry, stamped like the row")
			versions = append(versions, updatedAt)
		}
		created, err := tx.Create(ctx, txCreateOp(txSchemaA, txActivity("created")))
		require.NoError(t, err)
		rowID = created.RowID
		stamp()

		first, err := tx.Update(ctx, txUpdateOp(txSchemaA, rowID, map[string]any{"summary": "first update"}))
		require.NoError(t, err)
		assert.Equal(t, "call", first.Attributes["type"], "the update merged onto the uncommitted create")
		stamp()

		second, err := tx.Update(ctx, txUpdateOp(txSchemaA, rowID, map[string]any{"direction": "inbound"}))
		require.NoError(t, err)
		assert.Equal(t, "first update", second.Attributes["summary"], "the second update merged onto the first")
		assert.Equal(t, "inbound", second.Attributes["direction"])
		stamp()
		return nil
	})
	require.NoError(t, err)

	require.Len(t, versions, 3)
	assert.Less(t, versions[0], versions[1])
	assert.Less(t, versions[1], versions[2])
	updatedAt, hot := env.hotVersion(t, env.postgresPool, txSchemaA, rowID)
	assert.Equal(t, versions[2], updatedAt, "the stored version is the last write's stamp")
	assert.Equal(t, []int64{versions[2]}, hot)
}

func TestTxPoisonedByAStorageErrorNeverCommits(t *testing.T) {
	env := setupTxEnv(t)
	var opErr error
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Create(ctx, txCreateOp(txSchemaA, txActivity("earlier")))
		require.NoError(t, err)
		injected := env.armFaultAfterMainInsert()
		_, opErr = tx.Create(ctx, txCreateOp(txSchemaB, txVisit("scheduled")))
		require.ErrorIs(t, opErr, injected)
		_, later := tx.Create(ctx, txCreateOp(txSchemaC, txCommunication("email")))
		require.ErrorIs(t, later, forma.ErrTxAborted)
		return nil // fn ignores the failure
	})
	require.ErrorIs(t, err, forma.ErrTxAborted)
	require.ErrorIs(t, err, opErr)
	for _, schema := range []string{txSchemaA, txSchemaB, txSchemaC} {
		assert.Zero(t, env.schemaRowCount(t, schema), schema)
	}
}

// The row limit is checked after each statement (#648 rule 19 as amended in
// #673's review): the statement that crosses it has run inside the
// transaction, no statement runs after it, and nothing the Tx wrote commits,
// the crossing statement's rows included.
func TestTxRowLimitBreachStopsAtTheCrossingStatementAndCommitsNothing(t *testing.T) {
	env := setupTxEnv(t)
	withEAV := func() map[string]any {
		data := txActivity("before")
		data["leadId"], data["nextFollowUpAt"] = "lead-eav", "2026-10-04T10:00:00Z"
		return data
	}
	d, a := env.mustCreate(t, txSchemaA, withEAV()), env.mustCreate(t, txSchemaA, withEAV())
	beforeD, beforeA := env.snapshot(t, txSchemaA, d), env.snapshot(t, txSchemaA, a)
	eavD, eavA := jsonArrayLen(t, beforeD.eav), jsonArrayLen(t, beforeA.eav)
	require.Positive(t, eavA)
	// A Delete modifies its main row, its EAV rows and its change_log row.
	// Deleting d fits; deleting a then fits its main row, and its EAV DELETE
	// crosses the limit.
	limit := (1 + eavD + 1) + eavA

	var opErr error
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		require.NoError(t, tx.Delete(ctx, txDeleteOp(txSchemaA, d)))
		opErr = tx.Delete(ctx, txDeleteOp(txSchemaA, a))
		require.ErrorIs(t, opErr, forma.ErrInvalidInput)
		session := tx.(*txSession)
		assert.Equal(t, int64(limit+1), session.meter.rows, "the crossing statement's rows were counted")
		inside := env.snapshotOn(t, session.tx.Tx, txSchemaA, a)
		assert.Empty(t, inside.main, "the main-row DELETE before the crossing statement ran")
		assert.Equal(t, "[]", inside.eav, "the crossing EAV DELETE ran")
		assert.Equal(t, beforeA.changeLog, inside.changeLog, "the change_log upsert after it never did")

		sent := session.meter.statements
		require.ErrorIs(t, tx.Delete(ctx, txDeleteOp(txSchemaA, uuid.New())), forma.ErrTxAborted)
		assert.Equal(t, sent, session.meter.statements, "nothing runs after the crossing statement")
		return nil // fn ignores the breach
	}, forma.WithTxMaxPhysicalRows(limit))
	require.ErrorIs(t, err, forma.ErrTxAborted)
	require.ErrorIs(t, err, opErr)
	assert.Equal(t, beforeD, env.snapshot(t, txSchemaA, d), "the delete that fit did not commit")
	assert.Equal(t, beforeA, env.snapshot(t, txSchemaA, a), "neither did the crossing statement")
}

func jsonArrayLen(t *testing.T, raw string) int {
	t.Helper()
	var items []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(raw), &items))
	return len(items)
}

func TestTxUpdateValidationFailureAfterTheReadPoisons(t *testing.T) {
	env := setupTxEnv(t)
	a := env.mustCreate(t, txSchemaA, txActivity("before"))
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, txUpdateOp(txSchemaA, a, map[string]any{"type": "fax"}))
		require.ErrorIs(t, err, forma.ErrInvalidInput, "strict update validation runs on the merged document")
		require.ErrorIs(t, tx.Delete(ctx, txDeleteOp(txSchemaA, a)), forma.ErrTxAborted,
			"the merge ran after the row was locked and read, so the refusal poisons")
		return nil
	})
	require.ErrorIs(t, err, forma.ErrTxAborted)
	assert.Equal(t, "before", env.attrs(t, txSchemaA, a)["summary"])
}

func TestTxNotFoundLeavesTheTxUsable(t *testing.T) {
	env := setupTxEnv(t)
	var created *forma.DataRecord
	err := env.inTx(func(ctx context.Context, tx forma.Tx) (err error) {
		_, err = tx.Update(ctx, txUpdateOp(txSchemaA, uuid.New(), map[string]any{"summary": "x"}))
		require.ErrorIs(t, err, forma.ErrNotFound)
		require.ErrorIs(t, tx.Delete(ctx, txDeleteOp(txSchemaA, uuid.New())), forma.ErrNotFound)
		created, err = tx.Create(ctx, txCreateOp(txSchemaA, txActivity("after a miss")))
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, "after a miss", env.attrs(t, txSchemaA, created.RowID)["summary"])
}

// One write path: the same Create, Update and Delete leave the same rows in
// all three tables whether they run through the EntityManager or a Tx. Only
// row IDs and time stamps may differ.
func TestTxWritesStoreWhatTheEntityManagerStores(t *testing.T) {
	env := setupTxEnv(t)
	payload := func() map[string]any {
		data := txActivity("created")
		data["id"] = "activity-one-write-path"
		data["leadId"] = "lead-eav" // an EAV attribute, so the EAV table is compared too
		return data
	}
	update := map[string]any{"summary": "updated", "leadId": "lead-eav-2"}

	type writer struct {
		create func(op *forma.EntityOperation) uuid.UUID
		update func(op *forma.EntityOperation)
		delete func(op *forma.EntityOperation)
	}
	direct := writer{
		create: func(op *forma.EntityOperation) uuid.UUID {
			record, err := env.manager.Create(env.ctx, op)
			require.NoError(t, err)
			return record.RowID
		},
		update: func(op *forma.EntityOperation) { _, err := env.manager.Update(env.ctx, op); require.NoError(t, err) },
		delete: func(op *forma.EntityOperation) { require.NoError(t, env.manager.Delete(env.ctx, op)) },
	}
	inTx := func(fn func(ctx context.Context, tx forma.Tx) error) { require.NoError(t, env.inTx(fn)) }
	transactional := writer{
		create: func(op *forma.EntityOperation) (rowID uuid.UUID) {
			inTx(func(ctx context.Context, tx forma.Tx) error {
				record, err := tx.Create(ctx, op)
				if err == nil {
					rowID = record.RowID
				}
				return err
			})
			return rowID
		},
		update: func(op *forma.EntityOperation) {
			inTx(func(ctx context.Context, tx forma.Tx) error { _, err := tx.Update(ctx, op); return err })
		},
		delete: func(op *forma.EntityOperation) {
			inTx(func(ctx context.Context, tx forma.Tx) error { return tx.Delete(ctx, op) })
		},
	}

	steps := func(w writer) []rowSnapshot {
		rowID := w.create(txCreateOp(txSchemaA, payload()))
		afterCreate := env.snapshot(t, txSchemaA, rowID)
		w.update(txUpdateOp(txSchemaA, rowID, update))
		afterUpdate := env.snapshot(t, txSchemaA, rowID)
		w.delete(txDeleteOp(txSchemaA, rowID))
		return []rowSnapshot{afterCreate, afterUpdate, env.snapshot(t, txSchemaA, rowID)}
	}
	want, got := steps(direct), steps(transactional)
	for i, step := range []string{"create", "update", "delete"} {
		assert.Equal(t, withoutIdentity(t, want[i]), withoutIdentity(t, got[i]), "after %s", step)
	}
}

// withoutIdentity masks the columns that legitimately differ between two
// writes of the same data: row IDs, and time stamps (kept only as set or
// unset).
func withoutIdentity(t *testing.T, s rowSnapshot) rowSnapshot {
	t.Helper()
	masked := map[string]bool{
		"ltbase_row_id": true, "row_id": true, "ltbase_created_at": true, "ltbase_updated_at": true,
		"ltbase_deleted_at": true, "changed_at": true, "deleted_at": true,
	}
	var mask func(v any) any
	mask = func(v any) any {
		switch v := v.(type) {
		case map[string]any:
			for k, field := range v {
				if masked[k] && field != nil {
					v[k] = "<set>"
					continue
				}
				v[k] = mask(field)
			}
		case []any:
			for i := range v {
				v[i] = mask(v[i])
			}
		}
		return v
	}
	normalize := func(raw string) string {
		if raw == "" {
			return raw
		}
		var v any
		require.NoError(t, json.Unmarshal([]byte(raw), &v))
		out, err := json.Marshal(mask(v))
		require.NoError(t, err)
		return string(out)
	}
	return rowSnapshot{main: normalize(s.main), eav: normalize(s.eav), changeLog: normalize(s.changeLog)}
}

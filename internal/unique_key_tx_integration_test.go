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

// Real-PostgreSQL tests of unique keys on the Tx surfaces (#651 part B, and
// the LTFlow mappings PR 1 can express): a conflict before a write leaves the
// Tx usable, and the key table matches the rows after every commit.

func TestUniqueKeyTxMethods(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	a := env.mustCreateAccount(t, "t1", "a@x", "h1")
	b := env.mustCreateAccount(t, "t1", "b@x", "h2")

	var created uuid.UUID
	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Create(ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("t1", "a@x", "h9")))
		if !errors.Is(err, forma.ErrUniqueConflict) {
			return fmt.Errorf("Tx.Create onto a taken key answered %v", err)
		}
		_, err = tx.Update(ctx, txUpdateOp(ukAccount, b, map[string]any{"handle": "h1"}))
		if !errors.Is(err, forma.ErrUniqueConflict) {
			return fmt.Errorf("Tx.Update onto a taken key answered %v", err)
		}
		// The conflicts did not poison the Tx: the moves below commit.
		if _, err := tx.Update(ctx, txUpdateOp(ukAccount, a, map[string]any{"handle": "h3"})); err != nil {
			return err
		}
		if err := tx.Delete(ctx, txDeleteOp(ukAccount, b)); err != nil {
			return err
		}
		// The delete released b's values inside the Tx, and the move freed h1.
		record, err := tx.Create(ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("t1", "b@x", "h1")))
		if err != nil {
			return err
		}
		created = record.RowID
		return nil
	}))
	assert.Len(t, env.keyEntriesOfRow(t, created), 2)
	assert.False(t, env.mainRowExists(t, ukAccount, b))
	env.assertKeyTableMatchesRows(t)
}

// A rolled-back Tx leaves no key entry (invariant 3).
func TestUniqueKeyRolledBackTxLeavesNoEntry(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	rollback := errors.New("roll back")
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		if _, err := tx.Create(ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("t1", "a@x", "h1"))); err != nil {
			return err
		}
		return rollback
	})
	require.ErrorIs(t, err, rollback)
	assert.Empty(t, env.keyEntries(t))
	env.mustCreateAccount(t, "t1", "a@x", "h1")
	env.assertKeyTableMatchesRows(t)
}

// The Tx row limit counts unique-key entries with the entity's own rows, and
// a refused write's entries although they never commit (transactions.md,
// Limits).
func TestUniqueKeyTxRowLimitCountsEntries(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	env.mustCreateAccount(t, "t1", "a@x", "h1")
	var refused, created int64
	var rowID uuid.UUID
	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		meter := tx.(*txSession).meter
		_, err := tx.Create(ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("t1", "b@x", "h1")))
		require.ErrorIs(t, err, forma.ErrUniqueConflict)
		refused = meter.rows
		record, err := tx.Create(ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("t1", "b@x", "h2")))
		require.NoError(t, err)
		created, rowID = meter.rows-refused, record.RowID
		return nil
	}))
	assert.Equal(t, int64(2), refused, "tenant_email's entry was inserted, then deleted when handle conflicted")
	eav := jsonArrayLen(t, env.snapshot(t, ukAccount, rowID).eav)
	assert.Equal(t, int64(1+eav+1+2), created, "main row, EAV rows, change_log row and two key entries")
}

func ukHandleWhere(note string) *forma.MutateWhereRequest {
	return &forma.MutateWhereRequest{SchemaName: ukAccount, Condition: &forma.KvCondition{Attr: "note", Value: "equals:" + note}}
}

func TestUniqueKeyUpdateWhereAndDeleteWhere(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	moved := env.mustCreateAccount(t, "t1", "a@x", "h1")
	_, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, moved, map[string]any{"note": "move"}))
	require.NoError(t, err)

	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		req := ukHandleWhere("move")
		req.Updates = map[string]any{"handle": "h2"}
		_, err := tx.UpdateWhere(ctx, req)
		return err
	}))
	env.assertKeyTableMatchesRows(t)
	env.mustCreateAccount(t, "t2", "b@x", "h1")

	// Two rows set to one value: the first row takes it, the second
	// conflicts after a write, which poisons the Tx, and nothing commits.
	for _, handle := range []string{"p1", "p2"} {
		rowID := env.mustCreateAccount(t, "t3", handle+"@x", handle)
		_, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, rowID, map[string]any{"note": "pair"}))
		require.NoError(t, err)
	}
	entries := env.keyEntries(t)
	err = env.inTx(func(ctx context.Context, tx forma.Tx) error {
		req := ukHandleWhere("pair")
		req.Updates = map[string]any{"handle": "same"}
		_, err := tx.UpdateWhere(ctx, req)
		if !errors.Is(err, forma.ErrUniqueConflict) {
			return fmt.Errorf("UpdateWhere answered %v, want ErrUniqueConflict", err)
		}
		if _, err := tx.Get(ctx, &forma.QueryRequest{SchemaName: ukAccount, RowID: &moved}); !errors.Is(err, forma.ErrTxAborted) {
			return fmt.Errorf("a read after the conflict answered %v, want ErrTxAborted", err)
		}
		return nil
	})
	require.ErrorIs(t, err, forma.ErrTxAborted, "the Tx was poisoned by a conflict after its first row")
	require.ErrorIs(t, err, forma.ErrUniqueConflict, "the poisoned Tx names its cause")
	assert.Equal(t, entries, env.keyEntries(t))

	// A conflict at the first row writes nothing and leaves the Tx usable.
	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		req := ukHandleWhere("move")
		req.Updates = map[string]any{"handle": "p1"}
		if _, err := tx.UpdateWhere(ctx, req); !errors.Is(err, forma.ErrUniqueConflict) {
			return fmt.Errorf("UpdateWhere onto a taken key answered %v", err)
		}
		_, err := tx.DeleteWhere(ctx, ukHandleWhere("pair"))
		return err
	}))
	env.mustCreateAccount(t, "t9", "z@x", "p1")
	env.assertKeyTableMatchesRows(t)
}

// LTFlow publishes a definition once per (name, version): a second publish
// of the same pair is refused, a new version is not.
func TestUniqueKeyLTFlowDefinitionNameVersion(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	def := func(version int, body string) *forma.EntityOperation {
		return ukCreateOp(ukFlowDef, uuid.Nil, map[string]any{"name": "onboarding", "version": version, "body": body})
	}
	_, err := env.manager.Create(env.ctx, def(1, "v1"))
	require.NoError(t, err)
	_, err = env.manager.Create(env.ctx, def(1, "v1 again"))
	requireKeyConflict(t, err, ukFlowDef, "name_version", "onboarding")
	_, err = env.manager.Create(env.ctx, def(2, "v2"))
	require.NoError(t, err)
	assert.Equal(t, 2, env.mainRowCount(t, ukFlowDef))
	env.assertKeyTableMatchesRows(t)
}

// LTFlow links a child task to its parent once per parent task hash.
func TestUniqueKeyLTFlowTaskLinkParentHash(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	link := func(child string) *forma.EntityOperation {
		return ukCreateOp(ukTaskLink, uuid.Nil, map[string]any{"parentTaskHash": "sha256:abc", "childTaskId": child})
	}
	_, err := env.manager.Create(env.ctx, link("c1"))
	require.NoError(t, err)
	_, err = env.manager.Create(env.ctx, link("c2"))
	requireKeyConflict(t, err, ukTaskLink, "parent_task_hash", "sha256:abc")
	assert.Equal(t, 1, env.mainRowCount(t, ukTaskLink))
}

// LTFlow starts an instance inside InTx; a duplicate business key surfaces
// as ErrUniqueConflict from InTx itself, and the Tx's other writes roll back.
func TestUniqueKeyLTFlowInstanceCreateInTx(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	start := func(ctx context.Context, tx forma.Tx) error {
		if _, err := tx.Create(ctx, ukCreateOp(ukPlain, uuid.Nil, map[string]any{"title": "audit"})); err != nil {
			return err
		}
		_, err := tx.Create(ctx, ukCreateOp(ukInstance, uuid.Nil, map[string]any{
			"definitionId": "def-1", "businessKey": "order-42", "status": "running",
		}))
		return err
	}
	require.NoError(t, env.inTx(start))
	err := env.inTx(start)
	requireKeyConflict(t, err, ukInstance, "business_key", "order-42")
	assert.Equal(t, 1, env.mainRowCount(t, ukInstance))
	assert.Equal(t, 1, env.mainRowCount(t, ukPlain), "the losing Tx's audit row rolls back with it")
	env.assertKeyTableMatchesRows(t)
}

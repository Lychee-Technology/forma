package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tx reads and row assertions (#650) on real PostgreSQL, over the cond649
// schema (condSchemaCache). Post-state is read with direct SQL or through the
// pool, never through the Tx under test.

func (e *condPG) inTx(fn func(ctx context.Context, tx forma.Tx) error) error {
	return e.txr.InTx(e.ctx, fn)
}

func condGet(rowID uuid.UUID) *forma.QueryRequest {
	return &forma.QueryRequest{SchemaName: condSchemaName, RowID: &rowID}
}

func condQuery(cond forma.Condition) *forma.QueryRequest {
	return &forma.QueryRequest{SchemaName: condSchemaName, Condition: cond, Page: 1, ItemsPerPage: 100}
}

func rowIDs(records []*forma.DataRecord) []uuid.UUID {
	ids := make([]uuid.UUID, len(records))
	for i, r := range records {
		ids[i] = r.RowID
	}
	return ids
}

// xmin answers the transaction id that wrote the row's current entity_main
// version, or "" when the row does not exist.
func (e *condPG) xmin(t *testing.T, rowID uuid.UUID) string {
	t.Helper()
	var xmin string
	err := e.pool.QueryRow(e.ctx, fmt.Sprintf("SELECT xmin::text FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		sanitizeIdentifier(e.tables.EntityMain)), condSchemaID, rowID).Scan(&xmin)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	require.NoError(t, err)
	return xmin
}

// sessionPID answers the backend the Tx runs on.
func sessionPID(tx forma.Tx) uint32 {
	return tx.(*txSession).tx.Conn().PgConn().PID()
}

func TestTxReadsSeeTheTxsOwnWrites(t *testing.T) {
	env := newCondPG(t)
	var x, y uuid.UUID
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		created, err := tx.Create(ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName},
			Data: map[string]any{"name": "original", "version": int64(1)}})
		require.NoError(t, err)
		x = created.RowID
		kept, err := tx.Create(ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName},
			Data: map[string]any{"name": "kept", "version": int64(1)}})
		require.NoError(t, err)
		y = kept.RowID

		got, err := tx.Get(ctx, condGet(x))
		require.NoError(t, err, "a row created earlier in the Tx is readable")
		assert.Equal(t, "original", got.Attributes["name"])

		_, err = tx.Update(ctx, condUpdate(x, nil, map[string]any{"name": "renamed"}, forma.Increment("version", 1)))
		require.NoError(t, err)
		result, err := tx.Query(ctx, condQuery(kv("name", "equals:renamed")))
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{x}, rowIDs(result.Data), "Query matches the updated attribute")
		assert.Equal(t, int64(2), result.Data[0].Attributes["version"])
		stale, err := tx.Query(ctx, condQuery(kv("name", "equals:original")))
		require.NoError(t, err)
		assert.Empty(t, stale.Data, "the pre-update value no longer matches")

		_, err = env.em.Get(env.ctx, condGet(y))
		require.ErrorIs(t, err, forma.ErrNotFound, "another connection sees no uncommitted row")

		require.NoError(t, tx.Delete(ctx, condDelete(x, nil)))
		_, err = tx.Get(ctx, condGet(x))
		require.ErrorIs(t, err, forma.ErrNotFound, "a row deleted earlier in the Tx is gone")
		_, err = tx.Get(ctx, condGet(x), forma.ForUpdate())
		require.ErrorIs(t, err, forma.ErrNotFound)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, "kept", env.get(t, y)["name"], "after commit the kept row is visible")
	_, err = env.em.Get(env.ctx, condGet(x))
	require.ErrorIs(t, err, forma.ErrNotFound)
}

func TestTxGetAndAssertRowAnswerNotFoundForAMissingRow(t *testing.T) {
	env := newCondPG(t)
	missing := uuid.New()
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		require.ErrorIs(t, tx.AssertRow(ctx, condSchemaName, missing, nil), forma.ErrNotFound)
		require.ErrorIs(t, tx.AssertRow(ctx, condSchemaName, missing, versionEquals(1)), forma.ErrNotFound,
			"a missing row is ErrNotFound whatever the precondition")
		_, err := tx.Get(ctx, condGet(missing), forma.ForUpdate())
		require.ErrorIs(t, err, forma.ErrNotFound)
		_, err = tx.Get(ctx, condGet(missing))
		require.ErrorIs(t, err, forma.ErrNotFound)
		return nil
	})
	require.NoError(t, err, "the not-found outcomes leave the Tx able to commit")
}

// AssertRow and Get(ForUpdate) lock the row and write nothing, whatever the
// outcome: entity_main (its current version included), EAV and change_log
// are the same after the Tx commits.
func TestAssertRowWritesNothing(t *testing.T) {
	env := newCondPG(t)
	row := env.create(t, map[string]any{"name": "fenced", "version": int64(5), "counter": 3})
	before, xmin := env.snapshot(t, row), env.xmin(t, row)

	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		require.NoError(t, tx.AssertRow(ctx, condSchemaName, row, and(versionEquals(5), kv("counter", "equals:3"))))
		require.ErrorIs(t, tx.AssertRow(ctx, condSchemaName, row, versionEquals(6)), forma.ErrPreconditionFailed)
		require.ErrorIs(t, tx.AssertRow(ctx, condSchemaName, row, kv("counter", "gt:3")), forma.ErrPreconditionFailed)
		require.NoError(t, tx.AssertRow(ctx, condSchemaName, row, nil))
		got, err := tx.Get(ctx, condGet(row), forma.ForUpdate())
		require.NoError(t, err)
		assert.Equal(t, int64(5), got.Attributes["version"])
		return nil
	})
	require.NoError(t, err, "a failed assertion the caller swallows leaves the Tx able to commit")
	assert.Equal(t, before, env.snapshot(t, row), "entity_main, EAV and change_log are unchanged")
	assert.Equal(t, xmin, env.xmin(t, row), "no new entity_main row version was written")
}

// A Tx that only asserted and read, then rolled back, holds no lock
// afterwards and changed nothing.
func TestTxThatAssertedAndReadLeavesNoTraceOnRollback(t *testing.T) {
	env := newCondPG(t)
	row := env.create(t, map[string]any{"name": "fenced", "version": int64(5)})
	before := env.snapshot(t, row)

	var pid uint32
	bail := errors.New("bail out")
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		pid = sessionPID(tx)
		require.NoError(t, tx.AssertRow(ctx, condSchemaName, row, versionEquals(5)))
		_, err := tx.Get(ctx, condGet(row), forma.ForUpdate())
		require.NoError(t, err)
		_, err = tx.Query(ctx, condQuery(versionEquals(5)))
		require.NoError(t, err)
		return bail
	})
	require.ErrorIs(t, err, bail)

	var held int
	require.NoError(t, env.pool.QueryRow(env.ctx, `SELECT count(*) FROM pg_locks
		WHERE pid = $1 AND locktype IN ('advisory', 'tuple', 'transactionid')`, int32(pid)).Scan(&held))
	assert.Zero(t, held, "the rolled-back backend holds no advisory, tuple or transaction lock")

	probe, err := env.pool.Begin(env.ctx)
	require.NoError(t, err)
	defer func() { _ = probe.Rollback(env.ctx) }()
	var free bool
	require.NoError(t, probe.QueryRow(env.ctx, "SELECT pg_try_advisory_xact_lock($1)", rowVersionLockKey(condSchemaID, row)).Scan(&free))
	assert.True(t, free, "the row's advisory lock is free")
	_, err = probe.Exec(env.ctx, fmt.Sprintf("SELECT 1 FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2 FOR UPDATE NOWAIT",
		sanitizeIdentifier(env.tables.EntityMain)), condSchemaID, row)
	require.NoError(t, err, "the entity_main row lock is free")
	assert.Equal(t, before, env.snapshot(t, row), "nothing changed")
}

// The premise of the DSQL fence (#633): every Forma write modifies the row's
// entity_main row, an update of EAV-only attributes included, so a write
// always conflicts with a FOR UPDATE on that row.
func TestEveryWriteModifiesTheEntityMainRow(t *testing.T) {
	env := newCondPG(t)
	var row uuid.UUID
	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		created, err := tx.Create(ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName},
			Data: map[string]any{"name": "a", "counter": 1}})
		row = created.RowID
		return err
	}))
	created := env.xmin(t, row)
	require.NotEmpty(t, created, "Create writes the entity_main row")

	// name and counter are both EAV attributes: no main column changes value.
	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, condUpdate(row, nil, map[string]any{"name": "b"}, forma.Increment("counter", 1)))
		return err
	}))
	updatedInTx := env.xmin(t, row)
	assert.NotEqual(t, created, updatedInTx, "a Tx update of EAV-only attributes rewrites the entity_main row")

	_, err := env.em.Update(env.ctx, condUpdate(row, nil, map[string]any{"name": "c"}))
	require.NoError(t, err)
	assert.NotEqual(t, updatedInTx, env.xmin(t, row), "so does an EntityManager update")

	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		return tx.Delete(ctx, condDelete(row, nil))
	}))
	assert.Empty(t, env.xmin(t, row), "Delete removes the entity_main row")
}

// Every $now in one Tx names the transaction's instant, however far the
// wall clock has moved between two reads.
func TestNowIsOneInstantAcrossTxReads(t *testing.T) {
	env := newCondPG(t)
	row := env.create(t, map[string]any{"name": "clock"})
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, condUpdate(row, nil, nil, forma.ServerTime("due", 0), forma.ServerTime("due_main", 0)))
		require.NoError(t, err)
		atNow := and(kv("due", "equals:$now"), kv("due_main", "equals:$now"))
		first, err := tx.Query(ctx, condQuery(atNow))
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{row}, rowIDs(first.Data))

		awaitClockPastNow(t, ctx, tx)
		second, err := tx.Query(ctx, condQuery(atNow))
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{row}, rowIDs(second.Data), "$now resolved to the same instant after the clock moved")
		return nil
	})
	require.NoError(t, err)
}

// A page past the last match is recounted with a second statement (#181).
// That statement runs on the transaction too: the total counts rows only
// the Tx can see.
func TestTxQueryRecountsPastTheLastPageOnTheTransaction(t *testing.T) {
	env := newCondPG(t)
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		for range 3 {
			_, err := tx.Create(ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName},
				Data: map[string]any{"name": "uncommitted"}})
			require.NoError(t, err)
		}
		result, err := tx.Query(ctx, &forma.QueryRequest{SchemaName: condSchemaName, Condition: kv("name", "equals:uncommitted"),
			Page: 9, ItemsPerPage: 5})
		require.NoError(t, err)
		assert.Empty(t, result.Data)
		assert.Equal(t, 3, result.TotalRecords, "the recount saw the Tx's uncommitted rows")
		return errors.New("roll back")
	})
	require.Error(t, err)
}

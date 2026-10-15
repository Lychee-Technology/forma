package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/uniquekey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The rounds of an upsert on a unique key (#651), each pinned by another
// transaction's commit at the afterTargetLookup hook rather than left to a
// race: the holder deleted or moved away before its lock, a concurrent
// creator that commits or rolls back, and a value that never settles.

// onTargetLookup installs fn as the env's afterTargetLookup hook until the
// test ends, and answers how many lookups ran.
func onTargetLookup(t *testing.T, env *txEnv, fn func(lookup int, holder uuid.UUID, found bool)) *int {
	t.Helper()
	lookups := new(int)
	env.txr.testHooks = &txTestHooks{afterTargetLookup: func(holder uuid.UUID, found bool) {
		*lookups++
		fn(*lookups, holder, found)
	}}
	t.Cleanup(func() { env.txr.testHooks = nil })
	return lookups
}

// A holder that is gone before its lock is granted, deleted or moved to
// another value, is not the conflict: the next round finds the target free
// and creates.
func TestUpsertHolderGoneBeforeItsLockCreates(t *testing.T) {
	gone := map[string]func(env *txEnv, holder uuid.UUID) error{
		"deleted": func(env *txEnv, holder uuid.UUID) error {
			return env.manager.Delete(env.ctx, txDeleteOp(ukIdem, holder))
		},
		"moved": func(env *txEnv, holder uuid.UUID) error {
			_, err := env.manager.Update(env.ctx, txUpdateOp(ukIdem, holder, map[string]any{"idempotency_key": "moved"}))
			return err
		},
	}
	for name, makeGone := range gone {
		t.Run(name, func(t *testing.T) {
			env := setupUniqueKeyEnv(t)
			holder := env.mustCreate(t, ukIdem, map[string]any{"instance_id": "i1", "idempotency_key": "k1", "status": "running"})
			lookups := onTargetLookup(t, env, func(lookup int, found uuid.UUID, ok bool) {
				if lookup == 1 {
					require.True(t, ok)
					require.Equal(t, holder, found)
					require.NoError(t, makeGone(env, holder))
				}
			})
			res, err := env.upsertInTx(t, onIdempotency(idemCreate("i1", "k1", "new", "h"), forma.UpsertDoNothing))
			require.NoError(t, err)
			assert.Equal(t, forma.UpsertCreated, res.Outcome)
			assert.NotEqual(t, holder, res.Record.RowID)
			assert.Equal(t, 2, *lookups, "a second round found the target free")
			env.requireStored(t, res.Record)
			env.assertKeyTableMatchesRows(t)
		})
	}
}

// A creator that takes the target between this upsert's lookup and its
// create makes the create wait on the key table. If it commits, the next
// round finds and answers its row; if it rolls back, the create proceeds.
func TestUpsertSettlesAgainstAConcurrentCreator_PostgresOnly(t *testing.T) {
	for _, commits := range []bool{true, false} {
		t.Run(fmt.Sprintf("creator commits %v", commits), func(t *testing.T) {
			env := setupUniqueKeyEnv(t)
			keyTable := uniquekey.KeyTable(env.tables.EntityMain)
			rollback := errors.New("the concurrent creator rolls back")
			var rival uuid.UUID
			rivalDone := make(chan error, 1)
			lookups := onTargetLookup(t, env, func(lookup int, _ uuid.UUID, found bool) {
				if lookup != 1 {
					return
				}
				require.False(t, found)
				created, release := make(chan struct{}), make(chan struct{})
				go func() {
					rivalDone <- env.inTx(func(ctx context.Context, tx forma.Tx) error {
						record, err := tx.Create(ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: ukIdem},
							Data: map[string]any{"instance_id": "i1", "idempotency_key": "k1", "status": "rival"}})
						if err == nil {
							rival = record.RowID
						}
						close(created)
						<-release
						if !commits {
							return rollback
						}
						return err
					})
				}()
				<-created
				go func() {
					defer close(release)
					assert.Eventually(t, func() bool { return keyTableWaiter(env, keyTable) }, 10*time.Second, 10*time.Millisecond,
						"the upsert's create never waited on the rival's entry")
				}()
			})

			res, err := env.upsertInTx(t, onIdempotency(idemCreate("i1", "k1", "mine", "h"), forma.UpsertDoNothing))
			require.NoError(t, err)
			if commits {
				require.NoError(t, <-rivalDone)
				assert.Equal(t, forma.UpsertExisting, res.Outcome)
				assert.Equal(t, rival, res.Record.RowID)
				assert.Equal(t, "rival", res.Record.Attributes["status"])
				assert.Equal(t, 2, *lookups)
			} else {
				require.ErrorIs(t, <-rivalDone, rollback)
				assert.Equal(t, forma.UpsertCreated, res.Outcome)
				assert.Equal(t, "mine", res.Record.Attributes["status"])
				assert.Equal(t, 1, *lookups)
			}
			env.requireStored(t, res.Record)
			assert.Equal(t, 1, env.mainRowCount(t, ukIdem))
			env.assertKeyTableMatchesRows(t)
		})
	}
}

// keyTableWaiter reports whether a session waits on another transaction's
// lock in a statement on keyTable (see waitForKeyTableWaiter).
func keyTableWaiter(env *txEnv, keyTable string) bool {
	var waiting bool
	err := env.postgresPool.QueryRow(env.ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
		WHERE wait_event_type = 'Lock' AND wait_event = 'transactionid' AND position($1 in query) > 0)`, keyTable).Scan(&waiting)
	return err == nil && waiting
}

// An upsert whose target moves to a new holder at every round runs out of
// rounds. That is ErrRetryableConflict, and the upsert wrote nothing.
func TestUpsertThatNeverSettlesIsARetryableConflict(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	env.mustCreate(t, ukIdem, map[string]any{"instance_id": "i1", "idempotency_key": "k1", "status": "holder"})
	lookups := onTargetLookup(t, env, func(lookup int, holder uuid.UUID, found bool) {
		require.True(t, found)
		_, err := env.manager.Update(env.ctx, txUpdateOp(ukIdem, holder, map[string]any{"idempotency_key": fmt.Sprintf("moved-%d", lookup)}))
		require.NoError(t, err)
		env.mustCreate(t, ukIdem, map[string]any{"instance_id": "i1", "idempotency_key": "k1", "status": "holder"})
	})

	_, err := env.upsertInTx(t, onIdempotency(idemCreate("i1", "k1", "mine", "h"), forma.UpsertDoNothing))
	require.ErrorIs(t, err, forma.ErrRetryableConflict)
	require.ErrorContains(t, err, "did not settle after 4 attempts")
	assert.Equal(t, maxUpsertAttempts, *lookups)
	assert.Equal(t, 1+maxUpsertAttempts, env.mainRowCount(t, ukIdem), "only the hook's holders exist")
	env.assertKeyTableMatchesRows(t)
}

// The row an upsert resolves against is locked as AssertRow locks it, the
// advisory lock and the entity_main row lock, until the Tx ends.
func TestUpsertLocksTheRowItResolves_PostgresOnly(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	holder := env.mustCreate(t, ukIdem, map[string]any{"instance_id": "i1", "idempotency_key": "k1", "status": "running", "counter": 0})
	for _, action := range []forma.UpsertAction{forma.UpsertDoNothing, forma.UpsertUpdate} {
		t.Run(string(action), func(t *testing.T) {
			req := onIdempotency(idemCreate("i1", "k1", "x", "h"), action)
			if action == forma.UpsertUpdate {
				req.UpdateOps = []forma.UpdateOp{forma.Increment("counter", 1)}
			}
			held, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- env.inTx(func(ctx context.Context, tx forma.Tx) error {
					if _, err := tx.Upsert(ctx, req); err != nil {
						return err
					}
					close(held)
					<-release
					return nil
				})
			}()
			select {
			case <-held:
			case err := <-done:
				t.Fatalf("the upsert failed: %v", err)
			}
			advisory, row := env.locksHeldElsewhere(t, ukIdem, holder)
			assert.True(t, advisory, "the advisory lock is held")
			assert.True(t, row, "the entity_main row lock is held")
			close(release)
			require.NoError(t, <-done)
		})
	}
}

// locksHeldElsewhere probes, from another transaction, whether rowID's
// advisory lock and its entity_main row lock are taken.
func (e *txEnv) locksHeldElsewhere(t *testing.T, schema string, rowID uuid.UUID) (advisory, row bool) {
	t.Helper()
	probe, err := e.postgresPool.Begin(e.ctx)
	require.NoError(t, err)
	defer func() { _ = probe.Rollback(e.ctx) }()
	var free bool
	require.NoError(t, probe.QueryRow(e.ctx, "SELECT pg_try_advisory_xact_lock($1)", rowVersionLockKey(e.schemaID(t, schema), rowID)).Scan(&free))
	_, err = probe.Exec(e.ctx, fmt.Sprintf("SELECT 1 FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2 FOR UPDATE NOWAIT",
		sanitizeIdentifier(e.tables.EntityMain)), e.schemaID(t, schema), rowID)
	var pgErr *pgconn.PgError
	if err != nil {
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "55P03", pgErr.Code, "the row probe fails only for the lock")
	}
	return !free, err != nil
}

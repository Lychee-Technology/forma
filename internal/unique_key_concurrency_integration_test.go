package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/uniquekey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Concurrency proofs of #651 on real PostgreSQL. Each worker runs its own
// write on its own pooled connection and transaction; a closed channel
// releases them together, and every wait on server state polls
// pg_stat_activity. Nothing sleeps for a fixed time. A loser's
// ErrUniqueConflict, rather than an OCC abort, holds because PostgreSQL makes
// an insert wait for a concurrent uncommitted entry, so the tests asserting it
// are _PostgresOnly (on DSQL a loser may instead see ErrRetryableConflict).

const ukWriters = 16

// race runs write once per worker, all released together, and answers each
// worker's error by worker index.
func race(workers int, write func(worker int) error) []error {
	errs := make([]error, workers)
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(workers)
	done.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			errs[w] = write(w)
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	return errs
}

// requireOneWinner asserts exactly one nil error and only ErrUniqueConflict
// besides, and answers the winner's index.
func requireOneWinner(t *testing.T, errs []error) int {
	t.Helper()
	winner := -1
	for w, err := range errs {
		if err == nil {
			require.Equal(t, -1, winner, "workers %d and %d both succeeded", winner, w)
			winner = w
			continue
		}
		require.ErrorIs(t, err, forma.ErrUniqueConflict, "worker %d", w)
	}
	require.NotEqual(t, -1, winner, "no worker succeeded")
	return winner
}

// waitForKeyTableWaiter polls until a session waits on another
// transaction's lock in a statement on keyTable: the second creator's insert
// waiting for the first creator's uncommitted entry. The table name is unique
// to the test, so another package's session cannot match.
func waitForKeyTableWaiter(t *testing.T, env *txEnv, keyTable string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var waiting bool
		err := env.postgresPool.QueryRow(env.ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND wait_event = 'transactionid' AND position($1 in query) > 0)`, keyTable).Scan(&waiting)
		return err == nil && waiting
	}, 10*time.Second, 10*time.Millisecond, "no session started waiting on an entry of %s", keyTable)
}

// Special scenario 4 of #593: concurrent creates of one composite key value
// commit exactly once, every time.
func TestUniqueKeyConcurrentCreatesOfOneValueHaveOneWinner_PostgresOnly(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	for iteration := 0; iteration < 50; iteration++ {
		rowIDs := make([]uuid.UUID, ukWriters)
		errs := race(ukWriters, func(w int) error {
			record, err := env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("race", "same@x", "")))
			if err == nil {
				rowIDs[w] = record.RowID
			}
			return err
		})
		winner := requireOneWinner(t, errs)
		require.Equal(t, 1, env.mainRowCount(t, ukAccount), "iteration %d", iteration)
		require.Len(t, env.keyEntries(t), 1, "iteration %d", iteration)
		env.assertKeyTableMatchesRows(t)
		require.NoError(t, env.manager.Delete(env.ctx, txDeleteOp(ukAccount, rowIDs[winner])))
	}
}

// The write paths pin READ COMMITTED, so a server or role whose default is
// a snapshot level still answers the losers ErrUniqueConflict, not 40001.
func TestUniqueKeyConcurrentCreatesUnderARepeatableReadDefault_PostgresOnly(t *testing.T) {
	env := setupUniqueKeyEnvWith(t, func(cfg *pgxpool.Config) {
		cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
	})
	var level string
	require.NoError(t, env.postgresPool.QueryRow(env.ctx, "SHOW default_transaction_isolation").Scan(&level))
	require.Equal(t, "repeatable read", level)
	for iteration := 0; iteration < 10; iteration++ {
		value := fmt.Sprintf("rr-%d", iteration)
		requireOneWinner(t, race(ukWriters, func(int) error {
			_, err := env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("", "", value)))
			return err
		}))
		requireOneWinner(t, race(ukWriters, func(int) error {
			_, err := env.manager.BatchCreate(env.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{
				*ukCreateOp(ukAccount, uuid.Nil, ukAccountData("", "", value+"-batch")),
			}})
			return err
		}))
	}
	env.assertKeyTableMatchesRows(t)
}

func TestConcurrentCreatesOfOneRowIDHaveOneWinner_PostgresOnly(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	for iteration := 0; iteration < 50; iteration++ {
		rowID := uuid.New()
		// One key value per iteration, so only the row ID is contested
		// across iterations; within one, every loser also reserves the
		// winner's value, which the winner's row holds.
		email := fmt.Sprintf("row-%d@x", iteration)
		errs := race(ukWriters, func(w int) error {
			_, err := env.manager.Create(env.ctx, ukCreateOp(ukAccount, rowID, ukAccountData("t", email, "")))
			return err
		})
		requireOneWinner(t, errs)
		require.True(t, env.mainRowExists(t, ukAccount, rowID))
		require.Len(t, env.keyEntriesOfRow(t, rowID), 1)
	}
	env.assertKeyTableMatchesRows(t)
}

// A creator that waited behind a first creator's uncommitted key entry finds
// the value free once the first creator rolls back.
func TestUniqueKeyCreatorAfterARolledBackCreatorSucceeds_PostgresOnly(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	value, err := uniquekey.Encode([]uniquekey.Component{{Type: forma.ValueTypeText, Value: "contested"}})
	require.NoError(t, err)
	created, release := make(chan struct{}), make(chan struct{})
	keyTable := uniquekey.KeyTable(env.tables.EntityMain)
	rollback := errors.New("first creator rolls back")

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- env.inTx(func(ctx context.Context, tx forma.Tx) error {
			if _, err := tx.Create(ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("", "", "contested"))); err != nil {
				close(created)
				return err
			}
			close(created)
			<-release
			return rollback
		})
	}()
	<-created

	var second *forma.DataRecord
	secondDone := make(chan error, 1)
	go func() {
		var err error
		second, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("", "", "contested")))
		secondDone <- err
	}()
	waitForKeyTableWaiter(t, env, keyTable)
	close(release)

	require.ErrorIs(t, <-firstDone, rollback)
	require.NoError(t, <-secondDone)
	assert.Equal(t, []keyEntry{{schemaID: ukSchemaIDs[ukAccount], keyID: ukKeyHandle, value: value, rowID: second.RowID}}, env.keyEntries(t))
	env.assertKeyTableMatchesRows(t)
}

// An update moving a row's key onto a value races a create of that value:
// exactly one of them commits.
func TestUniqueKeyMoveRacesCreate_PostgresOnly(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	for iteration := 0; iteration < 50; iteration++ {
		moved := env.mustCreateAccount(t, "", "", fmt.Sprintf("old-%d", iteration))
		target := fmt.Sprintf("v-%d", iteration)
		errs := race(2, func(w int) error {
			if w == 0 {
				_, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, moved, map[string]any{"handle": target}))
				return err
			}
			_, err := env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("", "", target)))
			return err
		})
		requireOneWinner(t, errs)
	}
	env.assertKeyTableMatchesRows(t)
}

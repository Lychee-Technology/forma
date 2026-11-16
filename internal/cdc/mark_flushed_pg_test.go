package cdc

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// The PostgreSQL flush mark against writers that hold change_log entries
// (#686), on the integration PostgreSQL. Each schedule is driven step by
// step: a session waiting on a lock is observed in pg_stat_activity, never
// assumed after a sleep. The timeouts only bound a failure.

const markTestSchema int16 = 7

// markTestEnv is a change_log table of the test's own with an entry per row
// at version 10, and the integration PostgreSQL it lives on. The rows are
// sorted and inserted in that order, so a mark of them reaches them in that
// order whether its plan follows the heap, the primary key or the list.
type markTestEnv struct {
	db    *sql.DB
	table string
	rows  []uuid.UUID
}

func newMarkTestEnv(t *testing.T, rows int) markTestEnv {
	t.Helper()
	db := pgTestDB(t, "")
	table := fmt.Sprintf("cdc_mark_pg_test_%d", time.Now().UnixNano())
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (schema_id SMALLINT NOT NULL, row_id UUID NOT NULL,
		flushed_at BIGINT NOT NULL DEFAULT 0, changed_at BIGINT NOT NULL, deleted_at BIGINT,
		PRIMARY KEY (schema_id, row_id, flushed_at))`, table))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })
	env := markTestEnv{db: db, table: table, rows: newIDs(rows)}
	slices.SortFunc(env.rows, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	for _, id := range env.rows {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (schema_id, row_id, changed_at) VALUES ($1, $2, 10)", table), markTestSchema, id)
		require.NoError(t, err)
	}
	return env
}

// writer opens a writer transaction that gives up on a lock after
// lockTimeout instead of waiting for good.
func (e markTestEnv) writer(t *testing.T, lockTimeout time.Duration) *sql.Tx {
	t.Helper()
	tx, err := e.db.Begin()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.Exec(fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", lockTimeout.Milliseconds()))
	require.NoError(t, err)
	return tx
}

// write writes a row's entry the way the repository does
// (upsertChangeLog), which holds the entry until tx ends.
func (e markTestEnv) write(tx *sql.Tx, id uuid.UUID, version int64) error {
	_, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s (schema_id, row_id, flushed_at, changed_at) VALUES ($1, $2, 0, $3)
		ON CONFLICT (schema_id, row_id, flushed_at) DO UPDATE SET changed_at = EXCLUDED.changed_at`, e.table),
		markTestSchema, id, version)
	return err
}

// mark runs the PostgreSQL engine's mark of ids, each listed at version 10.
func (e markTestEnv) mark(t *testing.T, ids ...uuid.UUID) ([]uuid.UUID, error) {
	return e.markWith(t, markFlushedFor(forma.EnginePostgreSQL), ids...)
}

func (e markTestEnv) markWith(t *testing.T, mark markFlushedFunc, ids ...uuid.UUID) ([]uuid.UUID, error) {
	t.Helper()
	versions := make(map[uuid.UUID]int64, len(ids))
	for _, id := range ids {
		versions[id] = 10
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return mark(ctx, e.db, e.table, markTestSchema, ids, versions, 99)
}

// dirtyVersion answers the version of row's unflushed entry, or 0 if it
// has none.
func (e markTestEnv) dirtyVersion(t *testing.T, id uuid.UUID) int64 {
	t.Helper()
	var version int64
	err := e.db.QueryRow(fmt.Sprintf("SELECT changed_at FROM %s WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0", e.table),
		markTestSchema, id).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return 0
	}
	require.NoError(t, err)
	return version
}

// requireLockWaiter returns once one of db's sessions waits on a lock, and
// fails the test if none does within ten seconds.
func requireLockWaiter(t *testing.T, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		err := db.QueryRow(`SELECT count(*) FROM pg_stat_activity
			WHERE application_name = current_setting('application_name') AND wait_event_type = 'Lock'`).Scan(&waiting)
		require.NoError(t, err)
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no session waited on a lock within ten seconds")
}

func requireSQLState(t *testing.T, err error, code string) {
	t.Helper()
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr) && pgErr.Code == code, "want SQLSTATE %s, got %v", code, err)
}

type markResult struct {
	ids []uuid.UUID
	err error
}

// TestMarkDoesNotHoldAWriterHostageToAnOpenTransaction is #686's first
// schedule: open Tx X wrote r1, a flush marks [r2, r1], and a writer of r2
// must not wait for X.
func TestMarkDoesNotHoldAWriterHostageToAnOpenTransaction(t *testing.T) {
	t.Run("the statement before #686 waits on X and holds r2", func(t *testing.T) {
		env := newMarkTestEnv(t, 2)
		r2, r1 := env.rows[0], env.rows[1] // the mark reaches r2 first
		x := env.writer(t, 10*time.Second)
		require.NoError(t, env.write(x, r1, 11))

		done := make(chan markResult, 1)
		go func() {
			ids, err := env.markWith(t, MarkFlushedVersions, r2, r1)
			done <- markResult{ids, err}
		}()
		requireLockWaiter(t, env.db)

		y := env.writer(t, 500*time.Millisecond)
		requireSQLState(t, env.write(y, r2, 12), "55P03") // lock_not_available: Y waited for X
		require.NoError(t, x.Rollback())
		res := <-done
		require.NoError(t, res.err)
		require.ElementsMatch(t, []uuid.UUID{r2, r1}, res.ids)
	})

	env := newMarkTestEnv(t, 2)
	r2, r1 := env.rows[0], env.rows[1] // the mark reaches r2 first
	x := env.writer(t, 10*time.Second)
	require.NoError(t, env.write(x, r1, 11))

	marked, err := env.mark(t, r2, r1)
	require.NoError(t, err, "the mark must not wait for X")
	require.Equal(t, []uuid.UUID{r2}, marked, "r2 is at its listed version, r1 is held by X")

	y := env.writer(t, 2*time.Second)
	require.NoError(t, env.write(y, r2, 12), "the writer of r2 must not wait for X")
	require.NoError(t, y.Commit())
	require.Equal(t, int64(10), env.dirtyVersion(t, r1), "r1 must stay dirty while X is open")
	require.Equal(t, int64(12), env.dirtyVersion(t, r2))

	require.NoError(t, x.Commit())
	require.Equal(t, int64(11), env.dirtyVersion(t, r1), "X's write must stay dirty for the next flush")
}

// TestMarkDoesNotDeadlockWithAMultiRowWriter is #686's second schedule: a
// writer wrote r1, a flush marks [r2, r1], and the writer then writes r2.
func TestMarkDoesNotDeadlockWithAMultiRowWriter(t *testing.T) {
	t.Run("the statement before #686 deadlocks", func(t *testing.T) {
		env := newMarkTestEnv(t, 2)
		r2, r1 := env.rows[0], env.rows[1] // the mark reaches r2 first
		w := env.writer(t, 10*time.Second)
		require.NoError(t, env.write(w, r1, 11))

		done := make(chan markResult, 1)
		go func() {
			ids, err := env.markWith(t, MarkFlushedVersions, r2, r1)
			done <- markResult{ids, err}
		}()
		requireLockWaiter(t, env.db)

		writeErr := env.write(w, r2, 12)
		if writeErr == nil {
			require.NoError(t, w.Commit())
		} else {
			require.NoError(t, w.Rollback())
		}
		res := <-done
		victim := writeErr
		if victim == nil {
			victim = res.err
		}
		requireSQLState(t, victim, "40P01")
	})

	env := newMarkTestEnv(t, 2)
	r2, r1 := env.rows[0], env.rows[1] // the mark reaches r2 first
	w := env.writer(t, 2*time.Second)
	require.NoError(t, env.write(w, r1, 11))

	marked, err := env.mark(t, r2, r1)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{r2}, marked)

	require.NoError(t, env.write(w, r2, 12), "the writer must not wait on the mark")
	require.NoError(t, w.Commit())
	require.Equal(t, int64(11), env.dirtyVersion(t, r1))
	require.Equal(t, int64(12), env.dirtyVersion(t, r2))
	var flushed int64
	require.NoError(t, env.db.QueryRow(fmt.Sprintf("SELECT changed_at FROM %s WHERE row_id = $1 AND flushed_at = 99", env.table), r2).Scan(&flushed))
	require.Equal(t, int64(10), flushed, "the mark flushed r2 at its listed version")
}

// TestMarkIsExactAndLeavesSkippedRowsDirty pins what the mark takes: an
// entry still at its listed version and held by nobody. A row rewritten in
// either direction, held by an open writer, or without an unflushed entry
// stays as it is, and a later flush that lists the rows again drains them.
func TestMarkIsExactAndLeavesSkippedRowsDirty(t *testing.T) {
	env := newMarkTestEnv(t, 5)
	current, advanced, regressed, held, absent := env.rows[0], env.rows[1], env.rows[2], env.rows[3], env.rows[4]
	for id, version := range map[uuid.UUID]int64{advanced: 11, regressed: 9} {
		_, err := env.db.Exec(fmt.Sprintf("UPDATE %s SET changed_at = $1 WHERE row_id = $2", env.table), version, id)
		require.NoError(t, err)
	}
	_, err := env.db.Exec(fmt.Sprintf("UPDATE %s SET flushed_at = 50 WHERE row_id = $1", env.table), absent)
	require.NoError(t, err)
	holder := env.writer(t, 2*time.Second)
	_, err = holder.Exec(fmt.Sprintf("SELECT 1 FROM %s WHERE row_id = $1 FOR UPDATE", env.table), held)
	require.NoError(t, err)

	marked, err := env.mark(t, advanced, regressed, held, absent)
	require.NoError(t, err, "a mark that takes nothing succeeds")
	require.Empty(t, marked)

	marked, err = env.mark(t, env.rows...)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{current}, marked)
	require.Equal(t, int64(11), env.dirtyVersion(t, advanced))
	require.Equal(t, int64(9), env.dirtyVersion(t, regressed))
	require.Equal(t, int64(10), env.dirtyVersion(t, held))
	require.Zero(t, env.dirtyVersion(t, current))

	require.NoError(t, holder.Rollback())
	ids, versions, _, err := SelectBatchRowIDs(context.Background(), env.db, env.table, markTestSchema, 100)
	require.NoError(t, err)
	require.ElementsMatch(t, []uuid.UUID{advanced, regressed, held}, ids)
	marked, err = markFlushedFor(forma.EnginePostgreSQL)(context.Background(), env.db, env.table, markTestSchema, ids, versions, 100)
	require.NoError(t, err)
	require.ElementsMatch(t, ids, marked, "once the writers are gone the next flush drains the rest")
	dirty, _, err := GetChangeLogStats(context.Background(), env.db, env.table, markTestSchema)
	require.NoError(t, err)
	require.Zero(t, dirty)
}

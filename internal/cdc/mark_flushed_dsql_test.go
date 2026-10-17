package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

func TestMarkFlushedForEngine(t *testing.T) {
	ids := make([]uuid.UUID, markRowsPerStatement+1)
	for i := range ids {
		ids[i] = uuid.New()
	}
	versions := make(map[uuid.UUID]int64, len(ids))
	for _, id := range ids {
		versions[id] = 1
	}
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec("CREATE TABLE change_log (schema_id SMALLINT, row_id UUID, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)
	for _, id := range ids {
		_, err = db.Exec("INSERT INTO change_log VALUES (7, ?, 1, 0)", id)
		require.NoError(t, err)
	}

	for _, engine := range []forma.DatabaseEngine{"", forma.EnginePostgreSQL, forma.EngineAuroraDSQL} {
		_, err = db.Exec("UPDATE change_log SET flushed_at = 0")
		require.NoError(t, err)
		marked, err := markFlushedFor(engine)(context.Background(), db, "change_log", 7, ids, versions, 99)
		require.NoError(t, err, "engine %q", engine)
		require.ElementsMatch(t, ids, marked, "engine %q must mark every listed row", engine)
	}
}

// recordingMark is a mark that records each statement's rows and answers
// with the errors queued in fail, then marks every row.
type recordingMark struct {
	statements [][]uuid.UUID
	fail       []error
}

func (m *recordingMark) mark(_ context.Context, _ *sql.DB, _ string, _ int16, ids []uuid.UUID, _ map[uuid.UUID]int64, _ int64) ([]uuid.UUID, error) {
	m.statements = append(m.statements, ids)
	if len(m.fail) > 0 {
		err := m.fail[0]
		m.fail = m.fail[1:]
		if err != nil {
			return nil, err
		}
	}
	return ids, nil
}

func newIDs(n int) []uuid.UUID {
	ids := make([]uuid.UUID, n)
	for i := range ids {
		ids[i] = uuid.New()
	}
	return ids
}

var errConflict = fmt.Errorf("mark flushed by listed versions: %w", &pgconn.PgError{Code: "40001", Message: "change conflicts with another transaction"})

// TestMarkInChunksStaysUnderDSQLRowLimit pins the reason for the chunked
// mark: DSQL limits a transaction to 3,000 modified rows, and the default
// flush batch is 10,000.
func TestMarkInChunksStaysUnderDSQLRowLimit(t *testing.T) {
	rec := &recordingMark{}
	ids := newIDs(2*markRowsPerStatement + 500)

	marked, err := markInChunks(rec.mark)(context.Background(), nil, "change_log", 7, ids, nil, 99)
	require.NoError(t, err)
	require.Equal(t, ids, marked)
	require.Len(t, rec.statements, 3)
	for _, stmt := range rec.statements {
		require.LessOrEqual(t, len(stmt), markRowsPerStatement)
	}
}

func TestMarkInChunksRetriesConflicts(t *testing.T) {
	rec := &recordingMark{fail: []error{nil, errConflict, errConflict}}
	ids := newIDs(markRowsPerStatement + 1)

	marked, err := markInChunks(rec.mark)(context.Background(), nil, "change_log", 7, ids, nil, 99)
	require.NoError(t, err)
	require.Equal(t, ids, marked)
	require.Len(t, rec.statements, 4, "the second chunk must be sent three times")
	require.Equal(t, ids[markRowsPerStatement:], rec.statements[3])
}

// TestMarkInChunksKeepsEarlierChunksOnFailure pins the failure contract: the
// error surfaces, and the rows earlier statements marked are reported marked.
func TestMarkInChunksKeepsEarlierChunksOnFailure(t *testing.T) {
	ids := newIDs(markRowsPerStatement + 1)
	conflicts := make([]error, 0, markConflictAttempts+1)
	conflicts = append(conflicts, nil)
	for range markConflictAttempts {
		conflicts = append(conflicts, errConflict)
	}
	rec := &recordingMark{fail: conflicts}

	marked, err := markInChunks(rec.mark)(context.Background(), nil, "change_log", 7, ids, nil, 99)
	require.Error(t, err)
	require.True(t, isSerializationFailure(err), "the conflict must stay matchable: %v", err)
	require.Contains(t, err.Error(), "mark 1 of 1001 rows flushed")
	require.Equal(t, ids[:markRowsPerStatement], marked)
	require.Len(t, rec.statements, 1+markConflictAttempts)
}

func TestMarkInChunksDoesNotRetryOtherErrors(t *testing.T) {
	boom := errors.New("connection reset")
	rec := &recordingMark{fail: []error{boom}}

	_, err := markInChunks(rec.mark)(context.Background(), nil, "change_log", 7, newIDs(3), nil, 99)
	require.ErrorIs(t, err, boom)
	require.Len(t, rec.statements, 1)
}

func TestMarkInChunksStopsRetryingWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rec := &recordingMark{fail: []error{errConflict, errConflict}}
	mark := func(ctx context.Context, db *sql.DB, table string, sid int16, ids []uuid.UUID, v map[uuid.UUID]int64, at int64) ([]uuid.UUID, error) {
		cancel()
		return rec.mark(ctx, db, table, sid, ids, v, at)
	}

	_, err := markInChunks(mark)(ctx, nil, "change_log", 7, newIDs(3), nil, 99)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, rec.statements, 1)
}

// TestMarkInChunksRetriesARealConflict runs the chunked mark on a REPEATABLE
// READ session, DSQL's level, against a writer holding one of its rows. The
// writer's commit fails the mark with 40001; the retry then marks the rows
// at their listed version, and leaves dirty the row the writer advanced.
func TestMarkInChunksRetriesARealConflict(t *testing.T) {
	db := pgTestDB(t, "repeatable read")
	table := fmt.Sprintf("cdc_mark_test_%d", time.Now().UnixNano())
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (schema_id SMALLINT, row_id UUID, changed_at BIGINT, flushed_at BIGINT,
		PRIMARY KEY (schema_id, row_id))`, table))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })
	ids := newIDs(3)
	versions := map[uuid.UUID]int64{}
	for _, id := range ids {
		versions[id] = 10
		_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (7, $1, 10, 0)", table), id)
		require.NoError(t, err)
	}

	writer, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = writer.Rollback() }()
	_, err = writer.Exec(fmt.Sprintf("UPDATE %s SET changed_at = 11 WHERE row_id = $1", table), ids[0])
	require.NoError(t, err)

	attempts := 0
	counting := func(ctx context.Context, db *sql.DB, tbl string, sid int16, chunk []uuid.UUID, v map[uuid.UUID]int64, at int64) ([]uuid.UUID, error) {
		attempts++
		return MarkFlushedVersions(ctx, db, tbl, sid, chunk, v, at)
	}
	go func() {
		waitForLockWaiter(t, db)
		_ = writer.Commit()
	}()
	marked, err := markInChunks(counting)(context.Background(), db, table, 7, ids, versions, 99)
	require.NoError(t, err)
	require.Equal(t, 2, attempts, "the conflict must fail the first statement and the retry succeed")
	require.ElementsMatch(t, ids[1:], marked, "the row the writer advanced must stay dirty")
}

// waitForLockWaiter returns once one of db's sessions, which pgTestDB names,
// waits on a lock, or after five seconds.
func waitForLockWaiter(t *testing.T, db *sql.DB) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		err := db.QueryRow(`SELECT count(*) FROM pg_stat_activity
			WHERE application_name = current_setting('application_name') AND wait_event_type = 'Lock'`).Scan(&waiting)
		if err == nil && waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Log("no session waited on a lock within five seconds")
}

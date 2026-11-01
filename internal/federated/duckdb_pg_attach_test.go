package federated

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

func newAttachTestExecutor(t *testing.T) *DuckDBClientQueryExecutor {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return &DuckDBClientQueryExecutor{client: &DuckDBClient{DB: db}}
}

// waitAttached runs a hot-leg query through exec and returns its error,
// closing the rows of a query that got through.
func waitAttached(ctx context.Context, exec *DuckDBClientQueryExecutor) error {
	rows, err := exec.queryWithPostgres(ctx, "host=127.0.0.1 port=1 dbname=none", "SELECT 1")
	if err == nil {
		_ = rows.Close()
	}
	return err
}

// A request waiting for the process-wide attach guard returns at its own
// deadline instead of waiting for the current holder, which may sit on a
// slow PostgreSQL attach (#752).
func TestAttachPostgresWaiterHonorsItsDeadline(t *testing.T) {
	exec := newAttachTestExecutor(t)
	require.NoError(t, acquirePGAttach(context.Background()), "the test holds the guard")
	t.Cleanup(releasePGAttach)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- waitAttached(ctx, exec) }()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("queryWithPostgres still blocked on the held guard after its 25ms deadline")
	}
}

func TestAttachPostgresWaiterHonorsCancellation(t *testing.T) {
	exec := newAttachTestExecutor(t)
	require.NoError(t, acquirePGAttach(context.Background()))
	t.Cleanup(releasePGAttach)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- waitAttached(ctx, exec) }()
	cancel()

	select {
	case err := <-done:
		require.True(t, errors.Is(err, context.Canceled), "got %v", err)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("queryWithPostgres still blocked on the held guard after cancellation")
	}
}

// The guard still admits one attach at a time: a waiter without a deadline
// gets it only after the holder releases it.
func TestAttachGuardSerializesAttaches(t *testing.T) {
	require.NoError(t, acquirePGAttach(context.Background()))

	acquired := make(chan struct{})
	go func() {
		if err := acquirePGAttach(context.Background()); err == nil {
			close(acquired)
		}
	}()

	select {
	case <-acquired:
		t.Fatal("a second attach took the guard while it was held")
	case <-time.After(50 * time.Millisecond):
	}
	releasePGAttach()
	select {
	case <-acquired:
		releasePGAttach()
	case <-time.After(2 * time.Second):
		t.Fatal("the waiter never took the released guard")
	}
}

// A request waiting for its client's exhausted pool does not hold the
// process-wide attach guard: a request to an unrelated, idle database still
// reaches its attach meanwhile, and the waiter still honors its own
// deadline (#752).
func TestAttachGuardNotHeldWhileWaitingForABusyPool(t *testing.T) {
	busy := newAttachTestExecutor(t)
	busy.client.DB.SetMaxOpenConns(1)
	held, err := busy.client.DB.Conn(context.Background())
	require.NoError(t, err, "the test holds the busy client's only connection")
	t.Cleanup(func() { _ = held.Close() })

	waitCtx, cancelWait := context.WithCancel(context.Background())
	defer cancelWait()
	waiting := make(chan error, 1)
	go func() { waiting <- waitAttached(waitCtx, busy) }()
	require.Eventually(t, func() bool { return busy.client.DB.Stats().WaitCount > 0 },
		5*time.Second, time.Millisecond, "the busy client's request waits for its pool")

	// The independent client cannot load the scanner, so its attach fails
	// at once: reaching that failure, not the guard's wait, is the point.
	idleDB, err := sql.Open("duckdb", unavailableExtensionsDSN(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = idleDB.Close() })
	idle := &DuckDBClientQueryExecutor{client: &DuckDBClient{DB: idleDB}}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = waitAttached(ctx, idle)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded, "the idle client waited on the busy client's pool")
	require.ErrorContains(t, err, "attach the hot leg's postgres catalog")

	cancelWait()
	select {
	case err := <-waiting:
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorContains(t, err, "take a duckdb connection")
	case <-time.After(2 * time.Second):
		t.Fatal("the busy client's request ignored its cancellation")
	}
}

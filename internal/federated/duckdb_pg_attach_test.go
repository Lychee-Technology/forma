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
// busy pool or a slow PostgreSQL attach (#752).
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

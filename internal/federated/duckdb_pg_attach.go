package federated

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/lychee-technology/forma/internal/redact"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// pgAttachSem serializes the hot leg's PostgreSQL attach across every DuckDB
// client of the process. ATTACH IF NOT EXISTS is not atomic in DuckDB 2.0:
// connections of one database that attach the same catalog at once fail all
// but one with "database with name ... already exists" (#752). One guard,
// not one per client, because two clients can share a file-backed database
// through duckdb-go's instance cache.
//
// It is a one-slot semaphore rather than a sync.Mutex so a waiter honors its
// own context: the holder keeps the guard across pool acquisition and the
// PostgreSQL attach, and a busy pool or a slow attach must not hold an
// unrelated request past its deadline.
var pgAttachSem = make(chan struct{}, 1)

// acquirePGAttach takes pgAttachSem, or returns the context's error once ctx
// is done first. The caller releases it with releasePGAttach.
func acquirePGAttach(ctx context.Context) error {
	select {
	case pgAttachSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait to attach the hot leg's postgres catalog: %w", ctx.Err())
	}
}

func releasePGAttach() { <-pgAttachSem }

// postgresQuerier is the optional half of DuckDBQueryExecutor that runs a
// hot-leg query on a connection it attached the query's PostgreSQL catalog
// on. Executors without it (the engine-seam fakes) get the ATTACH at the
// head of the query batch instead.
type postgresQuerier interface {
	queryWithPostgres(ctx context.Context, connStr, sqlStr string, args ...any) (duckDBRowsIterator, error)
}

// afterPostgresAttach, when a test sets it, runs between a query's attach
// and the query itself: the window in which DuckDB can invalidate the
// database and the recovering connector reopen it.
var afterPostgresAttach func()

// queryWithPostgres attaches the catalog the rendered query reads, under
// pgAttachSem, and runs the query on the very connection the attach ran on.
// Once the database holds the catalog the attach is a no-op statement; it
// runs before every query because a database DuckDB invalidated and the
// recovering connector reopened has lost it (#702).
//
// Pinning the connection is what keeps every first attach of a database
// under the guard (#752). Were the query free to take any pooled
// connection, a reopen between the attach and the query would hand it a
// connection of the new database, which lacks the catalog, and the
// requests caught in that window would each have to attach it unguarded,
// at once. On the pinned connection the query instead runs on the database
// the attach reached: if DuckDB has invalidated it meanwhile, the query
// fails like any statement on an invalidated database, and the next
// request's guarded attach is the first on the reopened one.
func (e *DuckDBClientQueryExecutor) queryWithPostgres(ctx context.Context, connStr, sqlStr string, args ...any) (duckDBRowsIterator, error) {
	if e == nil || e.client == nil || e.client.DB == nil {
		return nil, fmt.Errorf("duckdb client not available")
	}
	conn, err := e.attachedConn(ctx, connStr)
	if err != nil {
		return nil, err
	}
	if afterPostgresAttach != nil {
		afterPostgresAttach()
	}
	rows, err := conn.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &pinnedRows{Rows: rows, conn: conn}, nil
}

// attachedConn takes a pooled connection and attaches connStr on it, both
// under pgAttachSem.
func (e *DuckDBClientQueryExecutor) attachedConn(ctx context.Context, connStr string) (*sql.Conn, error) {
	if err := acquirePGAttach(ctx); err != nil {
		return nil, err
	}
	defer releasePGAttach()
	conn, err := e.client.DB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("take a duckdb connection for the hot leg: %w", err)
	}
	if _, err := conn.ExecContext(ctx, sqlgen.PostgresAttachSQL(connStr)); err != nil {
		_ = conn.Close()
		// DuckDB's attach error quotes the connection string, password
		// included (#306).
		return nil, fmt.Errorf("attach the hot leg's postgres catalog: %w", redact.Error(err))
	}
	return conn, nil
}

// pinnedRows are the rows of a query on a pinned connection. Close returns
// the connection to the pool along with the rows, so a caller that closes
// them early frees the pool's connection as before, and closing twice is
// harmless.
type pinnedRows struct {
	*sql.Rows
	conn     *sql.Conn
	once     sync.Once
	closeErr error
}

func (r *pinnedRows) Close() error {
	r.once.Do(func() {
		r.closeErr = errors.Join(r.Rows.Close(), r.conn.Close())
	})
	return r.closeErr
}

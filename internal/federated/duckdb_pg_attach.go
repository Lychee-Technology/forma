package federated

import (
	"context"
	"fmt"

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

// postgresAttacher is the optional half of DuckDBQueryExecutor that attaches
// the hot leg's PostgreSQL catalog before a query. Executors that cannot
// attach (the engine-seam fakes) leave it to the ATTACH at the head of the
// query batch.
type postgresAttacher interface {
	attachPostgres(ctx context.Context, connStr string) error
}

// attachPostgres attaches the catalog the rendered query reads, under
// pgAttachSem. Once the database holds it, this is a no-op statement; it runs
// before every query because a database DuckDB invalidated and the
// recovering connector reopened has lost it (#702).
func (e *DuckDBClientQueryExecutor) attachPostgres(ctx context.Context, connStr string) error {
	if e == nil || e.client == nil || e.client.DB == nil {
		return fmt.Errorf("duckdb client not available")
	}
	if err := acquirePGAttach(ctx); err != nil {
		return err
	}
	defer releasePGAttach()
	if _, err := e.client.DB.ExecContext(ctx, sqlgen.PostgresAttachSQL(connStr)); err != nil {
		// DuckDB's attach error quotes the connection string, password
		// included (#306).
		return fmt.Errorf("attach the hot leg's postgres catalog: %w", redact.Error(err))
	}
	return nil
}

// ensurePostgresAttached attaches the engine's PostgreSQL catalog when its
// executor can.
func (e *DBFederatedQueryEngine) ensurePostgresAttached(ctx context.Context) error {
	attacher, ok := e.duck.(postgresAttacher)
	if !ok || e.pgConnString == "" {
		return nil
	}
	return attacher.attachPostgres(ctx, e.pgConnString)
}

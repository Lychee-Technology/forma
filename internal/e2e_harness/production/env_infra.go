package production

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Infrastructure lifecycle verbs: restart/halt/resume of the per-test
// server-bound handles. Pure move from env.go (#249 review, 500-line limit).

// RestartPostgres restarts the cluster's Postgres container and rebinds
// every per-test handle that referenced the old server: the pgx pool, the
// CDC config (the host-mapped port can change), the DuckDB client (so no
// cached postgres attachment survives), and the lazily built EntityManager
// and federated engine. Registry and Metadata are load-once snapshots and
// need no rebuild. Only for tests owning a DedicatedCluster.
func (e *Env) RestartPostgres(ctx context.Context) error {
	if err := e.Cluster.RestartPostgres(ctx); err != nil {
		return fmt.Errorf("restart cluster postgres: %w", err)
	}
	return e.reconnectAfterRestart(ctx)
}

// reconnectAfterRestart rebuilds the Env's server-bound handles against the
// restarted Postgres.
func (e *Env) reconnectAfterRestart(ctx context.Context) error {
	if e.Pool != nil {
		e.Pool.Close()
	}
	pool, err := pgxpool.New(ctx, e.PGDSN())
	if err != nil {
		return fmt.Errorf("reconnect test database after restart: %w", err)
	}
	e.Pool = pool

	if e.Duck != nil {
		_ = e.Duck.Close()
	}
	if err := e.startDuckDB(); err != nil {
		return fmt.Errorf("rebuild duckdb client after restart: %w", err)
	}
	e.CDC = e.buildCDCConfig()
	e.manager = nil
	e.engine = nil
	return nil
}

// ReopenDuckDB replaces the Env's (possibly closed) DuckDB client with a
// fresh one and drops the lazily built engine and manager so the next use
// rebinds them. The cached circuit breaker deliberately survives: breaker
// state must span client rebuilds so #185 recovery scenarios can observe the
// open-to-closed transition against a healthy DuckDB.
func (e *Env) ReopenDuckDB() error {
	if e.Duck != nil {
		if err := e.Duck.Close(); err != nil {
			e.T.Logf("reopen duckdb: close old client: %v", err)
		}
	}
	if err := e.startDuckDB(); err != nil {
		return fmt.Errorf("reopen duckdb client: %w", err)
	}
	e.engine = nil
	e.manager = nil
	return nil
}

// ReopenDuckDBWithS3Creds replaces the DuckDB client with one whose httpfs
// session authenticates against S3 with the given credentials, leaving the
// Go S3 client and CDC config untouched. Wrong credentials make DuckDB's S3
// reads fail with a genuine signature-mismatch rejection from the object
// store — the closest fault to an object-permission error that can reach
// DuckDB reads on the single-credential test store (#187 scenario 3), since
// httpfs bypasses the Go S3 client and its decorators entirely. Restore by
// passing the cluster credentials.
func (e *Env) ReopenDuckDBWithS3Creds(access, secret string) error {
	e.duckS3Access, e.duckS3Secret = access, secret
	if err := e.ReopenDuckDB(); err != nil {
		return fmt.Errorf("reopen duckdb with s3 creds: %w", err)
	}
	return nil
}

// RestartS3 restarts the halted S3 container (pairs with Cluster.HaltS3) and
// rebinds every per-test handle that referenced the old endpoint: the
// cluster S3 client (rebuilt by Cluster.RestartS3 — the host-mapped port can
// change), the DuckDB client (the httpfs session carries the endpoint), the
// CDC config, and the lazily built engine and manager. The circuit breaker
// deliberately survives, mirroring ReopenDuckDB, so recovery scenarios can
// observe breaker state across the restoration. Only for tests owning a
// DedicatedCluster.
func (e *Env) RestartS3(ctx context.Context) error {
	if err := e.Cluster.RestartS3(ctx); err != nil {
		return fmt.Errorf("restart cluster s3: %w", err)
	}
	if e.Duck != nil {
		_ = e.Duck.Close()
	}
	if err := e.startDuckDB(); err != nil {
		return fmt.Errorf("rebuild duckdb client after s3 restart: %w", err)
	}
	e.CDC = e.buildCDCConfig()
	e.engine = nil
	e.manager = nil
	return nil
}

// HaltPostgres stops the Postgres container in place, deliberately leaving
// the Env's pool and DuckDB postgres attachment pointing at the dead server:
// queries must fail with genuine network errors (#187 scenario 9), not with
// a harness closed-pool artifact. Resume with ResumePostgres. Only for tests
// owning a DedicatedCluster.
func (e *Env) HaltPostgres(ctx context.Context) error {
	if err := e.Cluster.HaltPostgres(ctx); err != nil {
		return fmt.Errorf("halt cluster postgres: %w", err)
	}
	return nil
}

// ResumePostgres restarts the halted Postgres container (pairs with
// HaltPostgres) and rebuilds the Env's server-bound handles, exactly like
// RestartPostgres does after its atomic stop/start.
func (e *Env) ResumePostgres(ctx context.Context) error {
	if err := e.Cluster.ResumePostgres(ctx); err != nil {
		return fmt.Errorf("resume cluster postgres: %w", err)
	}
	return e.reconnectAfterRestart(ctx)
}

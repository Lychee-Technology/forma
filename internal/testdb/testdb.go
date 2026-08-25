// Package testdb centralizes how integration tests reach a real Postgres.
//
// Before #385 each venue hardcoded its own connection story: the internal
// suite pinned postgres:postgres@localhost:5432/forma and the factory tests
// required DATABASE_URL — so CI's provisioned service (test:test@…/forma_test,
// exported as DB_* env) was never reached and every Postgres-backed test
// silently skipped. This package resolves the DSN from the same env the rest
// of the repo reads and makes an unreachable database a hard failure in CI,
// where the service exists on purpose.
package testdb

import (
	"context"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lychee-technology/forma/internal/bootstrap"
)

// ResolveDSN returns the Postgres DSN for integration tests. DATABASE_URL
// wins when set (the factory suite's historical contract); otherwise the DSN
// is built from the DB_* variables CI's test job exports, defaulting to the
// scripts/local_server.sh Postgres (postgres:postgres@localhost:5432/forma).
func ResolveDSN() string {
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		return dsn
	}
	u := url.URL{
		Scheme: "postgres",
		User: url.UserPassword(
			bootstrap.Env("DB_USER", "postgres"),
			bootstrap.Env("DB_PASSWORD", "postgres"),
		),
		Host:     net.JoinHostPort(bootstrap.Env("DB_HOST", "localhost"), bootstrap.Env("DB_PORT", "5432")),
		Path:     "/" + bootstrap.Env("DB_NAME", "forma"),
		RawQuery: "sslmode=" + bootstrap.Env("DB_SSL_MODE", "disable"),
	}
	return u.String()
}

// FailOnUnreachable reports whether an unreachable database is a failure
// rather than a skip. GitHub Actions sets CI on every hosted runner, so this
// needs no workflow wiring and stays correct if more jobs provision Postgres
// (#385).
func FailOnUnreachable() bool {
	return os.Getenv("CI") != ""
}

// Connect opens and pings a pgxpool for the resolved DSN. When the database
// is unreachable it fails the test in CI (the service is provisioned on
// purpose — a skip would go dark, #385) and skips on developer machines
// without a local Postgres. The pool is closed via t.Cleanup.
func Connect(t testing.TB, ctx context.Context) *pgxpool.Pool {
	t.Helper()

	cfg, err := pgxpool.ParseConfig(ResolveDSN())
	if err != nil {
		t.Fatalf("failed to parse integration test DSN (check DATABASE_URL / DB_* env): %v", err)
	}
	cfg.ConnConfig.ConnectTimeout = 2 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err == nil {
		if err = pool.Ping(ctx); err != nil {
			pool.Close()
		}
	}
	if err != nil {
		if FailOnUnreachable() {
			t.Fatalf("integration database unreachable in CI — the workflow provisions Postgres on purpose, so this is a failure, not a skip (#385): %v", err)
		}
		t.Skipf("skipping: integration Postgres unreachable (%v)", err)
	}

	t.Cleanup(pool.Close)
	return pool
}

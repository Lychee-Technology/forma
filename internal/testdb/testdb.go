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
	"errors"
	"net"
	"net/url"
	"os"
	"syscall"
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
		RawQuery: "sslmode=" + url.QueryEscape(bootstrap.Env("DB_SSL_MODE", "disable")),
	}
	return u.String()
}

// InCI reports whether the tests run under CI, where a database is
// provisioned on purpose and therefore every connection failure — not only
// unreachability — is fatal rather than skippable. GitHub Actions sets CI on
// every hosted runner, so this needs no workflow wiring and stays correct if
// more jobs provision Postgres (#385).
func InCI() bool {
	return os.Getenv("CI") != ""
}

// unreachable reports whether err denotes network-level absence of the
// database for every connection attempt it carries. Absence means the
// failure originated at dial time: connection refused, unreachable
// host/network, DNS resolution, dial timeout. Anything after an accepted
// connection — server rejection, protocol/TLS failure, handshake or startup
// timeout — is misconfiguration and must fail loudly, even off-CI (#486
// review P2).
//
// The walk is manual rather than errors.Is/As because pgx joins per-address
// results (errors.Join inside pgconn.ConnectError), and errors.Is matches
// when ANY joined branch matches: a mixed outcome — one address refused,
// another answered and rejected us — would classify as absence and skip away
// the actionable server-side error. Every joined branch must be absence on
// its own (#486 review round 3).
func unreachable(err error) bool {
	for err != nil {
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			branches := joined.Unwrap()
			if len(branches) == 0 {
				return false
			}
			for _, branch := range branches {
				if !unreachable(branch) {
					return false
				}
			}
			return true
		}
		switch e := err.(type) {
		case *net.OpError:
			// The op tells where the failure happened: "dial" is absence
			// (refused, unreachable, DNS, dial timeout all surface here);
			// "read"/"write" mean the server accepted the connection first.
			// Deliberately errno-blind at dial time: nothing was accepted
			// yet, so any dial failure reads as absence (#486 review round
			// 4, recorded as intended breadth).
			return e.Op == "dial"
		case *net.DNSError:
			return true
		}
		// Leaf equality, not errors.Is: Is would descend into joined
		// subtrees below this node with any-branch semantics.
		if err == syscall.ECONNREFUSED || err == syscall.EHOSTUNREACH || err == syscall.ENETUNREACH {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

// Connect opens and pings a pgxpool for the resolved DSN. Connection failures
// split three ways: in CI every failure is fatal (the service is provisioned
// on purpose — a skip would go dark, #385); locally, network-level absence
// skips (a developer without Postgres), while a server that answered and
// rejected the connection is misconfiguration and fails loudly (#486 review
// P2). The pool is closed via t.Cleanup.
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
		if InCI() {
			t.Fatalf("integration database connection failed in CI — the workflow provisions Postgres on purpose, so this is a failure, not a skip (#385): %v", err)
		}
		if !unreachable(err) {
			t.Fatalf("integration database connection failed for a reason other than network absence — misconfiguration, not a missing Postgres; fix DATABASE_URL / DB_* env (#486 review): %v", err)
		}
		t.Skipf("skipping: integration Postgres unreachable (%v)", err)
	}

	t.Cleanup(pool.Close)
	return pool
}

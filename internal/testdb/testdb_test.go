package testdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// clearDBEnv blanks every variable ResolveDSN reads so ambient shell/CI env
// cannot leak into a test case. t.Setenv also registers restoration.
func clearDBEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"DATABASE_URL", "DB_HOST", "DB_PORT", "DB_NAME",
		"DB_USER", "DB_PASSWORD", "DB_SSL_MODE",
	} {
		t.Setenv(key, "")
	}
}

func TestResolveDSNDefaultsMatchLocalServer(t *testing.T) {
	clearDBEnv(t)
	got := ResolveDSN()
	want := "postgres://postgres:postgres@localhost:5432/forma?sslmode=disable"
	if got != want {
		t.Errorf("ResolveDSN() with no env = %q, want local_server.sh defaults %q", got, want)
	}
}

func TestResolveDSNReadsDBVars(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("DB_HOST", "db.example")
	t.Setenv("DB_PORT", "15432")
	t.Setenv("DB_NAME", "forma_test")
	t.Setenv("DB_USER", "test")
	t.Setenv("DB_PASSWORD", "test")
	t.Setenv("DB_SSL_MODE", "require")
	got := ResolveDSN()
	want := "postgres://test:test@db.example:15432/forma_test?sslmode=require"
	if got != want {
		t.Errorf("ResolveDSN() = %q, want %q", got, want)
	}
}

func TestResolveDSNEscapesPassword(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("DB_PASSWORD", "p@ss/word")
	got := ResolveDSN()
	if !strings.Contains(got, "p%40ss%2Fword") {
		t.Errorf("ResolveDSN() = %q, want URL-escaped password p%%40ss%%2Fword", got)
	}
}

func TestResolveDSNPrefersDatabaseURL(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("DB_HOST", "ignored.example")
	t.Setenv("DATABASE_URL", "postgres://u:p@override:5/db")
	if got := ResolveDSN(); got != "postgres://u:p@override:5/db" {
		t.Errorf("ResolveDSN() = %q, want DATABASE_URL to win over DB_*", got)
	}
}

// TestUnreachableClassifiesNetworkAbsenceOnly pins the local skip/fail split
// (#486 review P2): only network-level absence — nothing listening, unknown
// host, connect timeout — may skip; a server that answered and rejected us
// (bad credentials, missing database, protocol/TLS failure) is
// misconfiguration and must fail loudly even off-CI.
func TestUnreachableClassifiesNetworkAbsenceOnly(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	authFail := &pgconn.PgError{Code: "28P01", Message: "password authentication failed"}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"connection refused", fmt.Errorf("dial tcp: %w", syscall.ECONNREFUSED), true},
		{"host unreachable", fmt.Errorf("dial tcp: %w", syscall.EHOSTUNREACH), true},
		{"network unreachable", fmt.Errorf("dial tcp: %w", syscall.ENETUNREACH), true},
		{"unknown host", fmt.Errorf("lookup: %w", &net.DNSError{Err: "no such host", IsNotFound: true}), true},
		{"dial-level refusal in OpError", fmt.Errorf("connect: %w", refused), true},
		{"dial timeout", fmt.Errorf("connect: %w", &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}), true},
		{"auth failure", fmt.Errorf("connect: %w", authFail), false},
		{"missing database", fmt.Errorf("connect: %w", &pgconn.PgError{Code: "3D000", Message: "database does not exist"}), false},
		{"protocol junk from a non-postgres listener", fmt.Errorf("read: %w", io.EOF), false},
		// Handshake/startup timeouts happen after the server accepted the TCP
		// connection: the listener exists, so this is not absence (#486
		// review round 3, finding 2).
		{"handshake read timeout", fmt.Errorf("receive: %w", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}), false},
		{"bare expired context", fmt.Errorf("ping: %w", context.DeadlineExceeded), false},
		// pgx v5 joins per-address results inside pgconn.ConnectError
		// (pgconn.go builds errors.Join(allErrors...)). A mixed outcome —
		// one address refused, another answered and rejected us — must not
		// skip: the server-side error is the actionable one (#486 review
		// round 3, finding 1).
		{"joined mixed refusal and auth failure", fmt.Errorf("connect: %w", errors.Join(refused, fmt.Errorf("server: %w", authFail))), false},
		{"joined all-absence", fmt.Errorf("connect: %w", errors.Join(refused, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.EHOSTUNREACH})), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := unreachable(tt.err); got != tt.want {
				t.Errorf("unreachable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// outcomeTB records Connect's terminal action instead of ending the test.
// Embedding testing.TB satisfies the interface's unexported method; Fatalf
// and Skipf stop the goroutine the way the real implementations do.
type outcomeTB struct {
	testing.TB
	fatal string
	skip  string
}

func (o *outcomeTB) Helper() {}

func (o *outcomeTB) Fatalf(format string, args ...any) {
	o.fatal = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func (o *outcomeTB) Skipf(format string, args ...any) {
	o.skip = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

// runConnect drives Connect against the DATABASE_URL the caller staged and
// reports which terminal action it took. Connect ends its goroutine via
// Goexit (as real Fatalf/Skipf do), so it runs on a dedicated one.
func runConnect(t *testing.T) *outcomeTB {
	t.Helper()
	rec := &outcomeTB{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		Connect(rec, context.Background())
	}()
	<-done
	return rec
}

// A listener that accepts and then stays silent is a live server that never
// completes the Postgres startup exchange: pgx times out reading, and that
// must fail locally, not skip — the timeout happened after dial succeeded
// (#486 review round 3, finding 2). Exercises the real pgx error chain.
func TestConnectLocallyFailsOnSilentListener(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("CI", "")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("start silent listener: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()
	t.Setenv("DATABASE_URL", fmt.Sprintf("postgres://u:p@%s/db?sslmode=disable", ln.Addr()))

	rec := runConnect(t)
	if rec.fatal == "" || rec.skip != "" {
		t.Errorf("Connect on a silent listener: fatal=%q skip=%q, want a loud local failure", rec.fatal, rec.skip)
	}
}

// A closed port is genuine absence: the local skip must survive the
// narrowed classifier. Exercises the real pgx dial-refused chain, joined
// per-address by pgconn (#486 review round 3, finding 1's green half).
func TestConnectLocallySkipsOnClosedPort(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("CI", "")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close reserved port: %v", err)
	}
	t.Setenv("DATABASE_URL", fmt.Sprintf("postgres://u:p@%s/db?sslmode=disable", addr))

	rec := runConnect(t)
	if rec.skip == "" || rec.fatal != "" {
		t.Errorf("Connect on a closed port: fatal=%q skip=%q, want a local skip", rec.fatal, rec.skip)
	}
}

func TestResolveDSNEscapesSSLMode(t *testing.T) {
	clearDBEnv(t)
	t.Setenv("DB_SSL_MODE", "verify full&x=y")
	got := ResolveDSN()
	if !strings.Contains(got, "sslmode=verify+full%26x%3Dy") {
		t.Errorf("ResolveDSN() = %q, want query-escaped sslmode value", got)
	}
}

func TestInCIFollowsCIVar(t *testing.T) {
	t.Setenv("CI", "")
	if InCI() {
		t.Error("InCI() = true with CI unset, want false (developer machines skip)")
	}
	t.Setenv("CI", "true")
	if !InCI() {
		t.Error("InCI() = false with CI=true, want true (CI provisions Postgres on purpose)")
	}
}

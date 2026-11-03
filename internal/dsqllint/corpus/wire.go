package corpus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// WireConn answers a real pgx connection to an in-process stand-in for a
// PostgreSQL server that records every simple-protocol statement it
// receives in rec and acknowledges it. It exists for SQL that pgx itself
// writes, such as the BEGIN it sends for a pgx.TxOptions: a pgxmock
// connection never sees that statement, and spelling it in a test would be
// a second implementation that could drift from pgx's.
//
// pgx sends a statement without arguments over the simple protocol, so
// BeginTx, Commit, Rollback and argument-free Exec calls work; anything
// else answers an error.
func WireConn(tb testing.TB, rec *Recorder) *pgx.Conn {
	tb.Helper()
	cfg, err := pgx.ParseConfig("postgres://corpus@corpus.invalid:5432/corpus?sslmode=disable")
	if err != nil {
		tb.Fatalf("parse the corpus wire configuration: %v", err)
	}
	cfg.LookupFunc = func(_ context.Context, host string) ([]string, error) { return []string{host}, nil }
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go serveWire(server, rec)
		return client, nil
	}
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		tb.Fatalf("connect to the corpus wire: %v", err)
	}
	tb.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// serveWire answers one connection: the startup handshake, then every
// simple query, until the client terminates or hangs up.
func serveWire(c net.Conn, rec *Recorder) {
	defer func() { _ = c.Close() }()
	be := pgproto3.NewBackend(c, c)
	if _, err := be.ReceiveStartupMessage(); err != nil {
		return
	}
	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "16.0"})
	be.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	be.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: 1})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if be.Flush() != nil {
		return
	}
	for {
		msg, err := be.Receive()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
				rec.fail(fmt.Errorf("corpus wire: %w", err))
			}
			return
		}
		switch m := msg.(type) {
		case *pgproto3.Terminate:
			return
		case *pgproto3.Query:
			_ = rec.Match("", m.String)
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte(commandTag(m.String))})
		default:
			rec.fail(fmt.Errorf("corpus wire: unsupported %T: it records only argument-free statements", msg))
			be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "0A000", Message: "the corpus wire records only simple queries"})
		}
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if be.Flush() != nil {
			return
		}
	}
}

// commandTag is the tag PostgreSQL answers a statement with: its first
// keyword, upper-cased.
func commandTag(sql string) string {
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return "EMPTY"
	}
	return strings.ToUpper(strings.TrimSuffix(fields[0], ";"))
}

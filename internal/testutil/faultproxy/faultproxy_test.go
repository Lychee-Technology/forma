package faultproxy

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// These tests drive the proxy with hand-built protocol messages against a
// fake server, so the framing and the faults are pinned without PostgreSQL.

func TestIsCommit(t *testing.T) {
	for query, want := range map[string]bool{
		"commit\x00":          true,
		"COMMIT\x00":          true,
		"  Commit ;\x00":      true,
		"commit;\x00":         true,
		"begin\x00":           false,
		"select 1\x00":        false,
		"commit prepared\x00": false,
		"\x00":                false,
	} {
		if got := isCommit([]byte(query)); got != want {
			t.Errorf("isCommit(%q) = %v, want %v", query, got, want)
		}
	}
}

// fakeServer accepts one connection and hands it to the test.
func fakeServer(t *testing.T) (string, <-chan net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	conns := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		conns <- conn
	}()
	return ln.Addr().String(), conns
}

func sslRequest() []byte {
	return binary.BigEndian.AppendUint32(nil, sslRequestCode)
}

func query(text string) []byte { return append([]byte(text), 0) }

// connect dials the proxy, gets its SSL request declined, and completes a
// startup exchange with the fake server: the startup message is forwarded
// and the server's ReadyForQuery comes back.
func connect(t *testing.T, p *Proxy, conns <-chan net.Conn) (client, server net.Conn) {
	t.Helper()
	client, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))

	mustWrite(t, writeUntyped(client, sslRequest()))
	reply := make([]byte, 1)
	if _, err := io.ReadFull(client, reply); err != nil || reply[0] != 'N' {
		t.Fatalf("SSL request answer = %q, %v; want 'N'", reply, err)
	}
	startup := binary.BigEndian.AppendUint32(nil, 196608) // protocol 3.0
	mustWrite(t, writeUntyped(client, startup))

	server = <-conns
	_ = server.SetDeadline(time.Now().Add(5 * time.Second))
	got, err := readUntyped(server)
	if err != nil || binary.BigEndian.Uint32(got) != 196608 {
		t.Fatalf("server got startup %v, %v; want protocol 3.0 and no SSL request", got, err)
	}
	mustWrite(t, writeTyped(server, 'Z', []byte{'I'}))
	expectTyped(t, client, 'Z')
	return client, server
}

func mustWrite(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("write: %v", err)
	}
}

func expectTyped(t *testing.T, conn net.Conn, want byte) []byte {
	t.Helper()
	msgType, body, err := readTyped(conn)
	if err != nil || msgType != want {
		t.Fatalf("read message = %q, %v; want %q", msgType, err, want)
	}
	return body
}

// replyCommit sends what a server answers to COMMIT.
func replyCommit(t *testing.T, server net.Conn) {
	t.Helper()
	mustWrite(t, writeTyped(server, 'C', query("COMMIT")))
	mustWrite(t, writeTyped(server, 'Z', []byte{'I'}))
}

func expectClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	if _, _, err := readTyped(conn); err == nil {
		t.Fatal("connection still delivers messages; want it closed")
	}
}

func awaitFired(t *testing.T, fired <-chan struct{}) {
	t.Helper()
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("fault did not fire")
	}
}

func TestRelaysWithoutFault(t *testing.T) {
	upstream, conns := fakeServer(t)
	p := Start(t, upstream)
	client, server := connect(t, p, conns)

	mustWrite(t, writeTyped(client, 'Q', query("commit")))
	if body := expectTyped(t, server, 'Q'); !isCommit(body) {
		t.Fatalf("server got %q, want the COMMIT", body)
	}
	replyCommit(t, server)
	expectTyped(t, client, 'C')
	expectTyped(t, client, 'Z')
}

func TestDropCommitReply(t *testing.T) {
	upstream, conns := fakeServer(t)
	p := Start(t, upstream)
	client, server := connect(t, p, conns)
	fired := p.Arm(DropCommitReply)

	mustWrite(t, writeTyped(client, 'Q', query("COMMIT")))
	expectTyped(t, server, 'Q') // the COMMIT reaches the server
	replyCommit(t, server)
	awaitFired(t, fired)
	expectClosed(t, client) // and its reply never reaches the client
}

func TestSwallowCommit(t *testing.T) {
	upstream, conns := fakeServer(t)
	p := Start(t, upstream)
	client, server := connect(t, p, conns)
	fired := p.Arm(SwallowCommit)

	mustWrite(t, writeTyped(client, 'Q', query("COMMIT")))
	awaitFired(t, fired)
	if _, _, err := readTyped(server); !errors.Is(err, io.EOF) {
		t.Fatalf("server read = %v; want EOF, the COMMIT must not be forwarded", err)
	}
	expectClosed(t, client)
}

func TestHoldCommitReply(t *testing.T) {
	upstream, conns := fakeServer(t)
	p := Start(t, upstream)
	client, server := connect(t, p, conns)
	fired := p.Arm(HoldCommitReply)

	mustWrite(t, writeTyped(client, 'Q', query("COMMIT")))
	expectTyped(t, server, 'Q')
	replyCommit(t, server)
	awaitFired(t, fired)

	_ = client.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, _, err := readTyped(client)
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("client read = %v; want a timeout, the connection stays open and silent", err)
	}
}

func TestFaultFiresOnce(t *testing.T) {
	upstream, conns := fakeServer(t)
	p := Start(t, upstream)
	client, server := connect(t, p, conns)
	p.Arm(SwallowCommit)
	p.Arm(None) // re-arming replaces the pending fault

	mustWrite(t, writeTyped(client, 'Q', query("commit")))
	expectTyped(t, server, 'Q')
	replyCommit(t, server)
	expectTyped(t, client, 'C')
}

func TestSeverResetsTheClient(t *testing.T) {
	upstream, conns := fakeServer(t)
	p := Start(t, upstream)
	client, server := connect(t, p, conns)

	p.Sever()
	expectClosed(t, client)
	if _, _, err := readTyped(server); err == nil {
		t.Fatal("server connection survived Sever")
	}
}

// Package faultproxy is a test-only TCP proxy between a PostgreSQL client and
// server that injects a network fault at the client's COMMIT. It exists to
// test what a writer can know about a transaction whose COMMIT met a broken
// connection (#645), and is shared with the coordination contract suite
// (#654).
//
// The proxy reads the plaintext frontend/backend protocol message by message.
// It answers an SSL or GSS encryption request with 'N' itself, so a client in
// sslmode=prefer falls back to plaintext. A client that requires TLS cannot be
// proxied. COMMIT is recognised as a simple-protocol Query whose text is
// COMMIT, the form pgx's Tx.Commit sends.
package faultproxy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Fault is what the proxy does to the next COMMIT a client sends.
type Fault int

const (
	// None relays everything.
	None Fault = iota
	// DropCommitReply forwards COMMIT and reads the server's whole reply,
	// through ReadyForQuery. It relays none of it, and then closes the client
	// connection. The server committed and the client never learns it.
	DropCommitReply
	// HoldCommitReply forwards COMMIT and discards the server's reply, but
	// leaves the client connection open. The client waits until its own
	// context ends.
	HoldCommitReply
	// SwallowCommit closes both connections on seeing COMMIT, without
	// forwarding it. The server never commits. The client has already written
	// the COMMIT successfully, so it cannot tell this from DropCommitReply.
	SwallowCommit
)

// Startup-phase request codes (PostgreSQL protocol, "Message Formats").
const (
	sslRequestCode    = 80877103
	gssEncRequestCode = 80877104
)

// Proxy relays connections from Addr to an upstream PostgreSQL server.
type Proxy struct {
	upstream string
	ln       net.Listener

	mu       sync.Mutex
	armed    Fault
	fired    chan struct{}
	sessions map[*session]struct{}
	closed   bool

	wg sync.WaitGroup
}

// Start listens on a loopback port and proxies to upstream (host:port). The
// proxy closes, with every session it holds, when the test ends.
func Start(t testing.TB, upstream string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("faultproxy: listen: %v", err)
	}
	p := &Proxy{upstream: upstream, ln: ln, sessions: map[*session]struct{}{}}
	p.wg.Add(1)
	go p.accept()
	t.Cleanup(p.Close)
	return p
}

// Addr is the host:port clients dial instead of the upstream.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// Arm sets the fault applied to the next COMMIT any session sends; it fires
// once. The returned channel closes when the fault has been applied. For
// DropCommitReply and HoldCommitReply that is when the server's reply to the
// COMMIT has been read, so the server has finished committing.
func (p *Proxy) Arm(f Fault) <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed = f
	p.fired = make(chan struct{})
	return p.fired
}

// Sever resets every live connection now: the client side with a TCP RST,
// so the client's next read or write fails at once, and the server side with
// a close, so the server ends the session and rolls back its transaction.
func (p *Proxy) Sever() {
	p.mu.Lock()
	sessions := make([]*session, 0, len(p.sessions))
	for s := range p.sessions {
		sessions = append(sessions, s)
	}
	p.mu.Unlock()
	for _, s := range sessions {
		s.reset()
	}
}

// Close stops accepting, closes every session and waits for its goroutines.
func (p *Proxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	sessions := make([]*session, 0, len(p.sessions))
	for s := range p.sessions {
		sessions = append(sessions, s)
	}
	p.mu.Unlock()

	_ = p.ln.Close()
	for _, s := range sessions {
		s.close()
	}
	p.wg.Wait()
}

// takeFault claims the armed fault for one COMMIT, disarming the proxy.
func (p *Proxy) takeFault() (Fault, chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, fired := p.armed, p.fired
	p.armed, p.fired = None, nil
	return f, fired
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.upstream)
		if err != nil {
			_ = client.Close()
			continue
		}
		s := &session{proxy: p, client: client, server: server}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			s.close()
			return
		}
		p.sessions[s] = struct{}{}
		p.mu.Unlock()

		p.wg.Add(2)
		go s.relayFrontend()
		go s.relayBackend()
	}
}

// session is one client connection and its upstream connection.
type session struct {
	proxy  *Proxy
	client net.Conn
	server net.Conn

	// dropping makes relayBackend discard what the server sends from the
	// COMMIT on; dropFault and dropFired say what to do at its end.
	dropping  atomic.Bool
	dropFault Fault
	dropFired chan struct{}

	closeOnce sync.Once
}

func (s *session) close() {
	s.closeOnce.Do(func() {
		_ = s.client.Close()
		_ = s.server.Close()
		s.proxy.mu.Lock()
		delete(s.proxy.sessions, s)
		s.proxy.mu.Unlock()
	})
}

func (s *session) reset() {
	if tcp, ok := s.client.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	s.close()
}

// relayFrontend copies client messages to the server, applying the armed
// fault to the first COMMIT it sees.
func (s *session) relayFrontend() {
	defer s.proxy.wg.Done()
	defer s.close()

	if err := s.relayStartup(); err != nil {
		return
	}
	for {
		msgType, body, err := readTyped(s.client)
		if err != nil {
			return
		}
		if msgType == 'Q' && isCommit(body) {
			if done := s.applyFault(msgType, body); done {
				return
			}
			continue
		}
		if err := writeTyped(s.server, msgType, body); err != nil {
			return
		}
	}
}

// applyFault handles one COMMIT. done reports that the session is over.
func (s *session) applyFault(msgType byte, body []byte) (done bool) {
	fault, fired := s.proxy.takeFault()
	switch fault {
	case SwallowCommit:
		s.close()
		close(fired)
		return true
	case DropCommitReply, HoldCommitReply:
		// Set before the COMMIT goes out, so the reply cannot outrun it.
		s.dropFault, s.dropFired = fault, fired
		s.dropping.Store(true)
	}
	return writeTyped(s.server, msgType, body) != nil
}

// relayStartup forwards the untyped startup message, declining encryption
// requests on the server's behalf so both streams stay plaintext.
func (s *session) relayStartup() error {
	for {
		body, err := readUntyped(s.client)
		if err != nil {
			return err
		}
		if len(body) >= 4 {
			code := binary.BigEndian.Uint32(body[:4])
			if code == sslRequestCode || code == gssEncRequestCode {
				if _, err := s.client.Write([]byte{'N'}); err != nil {
					return fmt.Errorf("decline encryption: %w", err)
				}
				continue
			}
		}
		return writeUntyped(s.server, body)
	}
}

// relayBackend copies server messages to the client until a drop begins,
// then discards them and ends the COMMIT's fault at its ReadyForQuery.
func (s *session) relayBackend() {
	defer s.proxy.wg.Done()
	defer s.close()
	for {
		msgType, body, err := readTyped(s.server)
		if err != nil {
			return
		}
		if !s.dropping.Load() {
			if err := writeTyped(s.client, msgType, body); err != nil {
				return
			}
			continue
		}
		if msgType != 'Z' {
			continue
		}
		if s.dropFault == DropCommitReply {
			s.close()
		}
		close(s.dropFired)
		if s.dropFault == DropCommitReply {
			return
		}
		// HoldCommitReply: keep the client waiting and discard the rest.
		_, _ = io.Copy(io.Discard, s.server)
		return
	}
}

// isCommit reports whether a Query message body (a NUL-terminated string)
// is a COMMIT.
func isCommit(body []byte) bool {
	query := strings.TrimRight(string(body), "\x00")
	query = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	return strings.EqualFold(query, "commit")
}

// maxMessage bounds a message length read off the wire, so a corrupt length
// fails the session instead of allocating without limit.
const maxMessage = 64 << 20

func readUntyped(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	return readBody(r, binary.BigEndian.Uint32(header[:]))
}

func readTyped(r io.Reader) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}
	body, err := readBody(r, binary.BigEndian.Uint32(header[1:]))
	return header[0], body, err
}

// readBody reads the rest of a message whose length field, which counts
// itself, was length.
func readBody(r io.Reader, length uint32) ([]byte, error) {
	if length < 4 || length > maxMessage {
		return nil, errors.New("faultproxy: malformed message length")
	}
	body := make([]byte, length-4)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}

func writeUntyped(w io.Writer, body []byte) error {
	msg := binary.BigEndian.AppendUint32(make([]byte, 0, 4+len(body)), uint32(4+len(body)))
	_, err := w.Write(append(msg, body...))
	return err
}

func writeTyped(w io.Writer, msgType byte, body []byte) error {
	msg := append(make([]byte, 0, 5+len(body)), msgType)
	msg = binary.BigEndian.AppendUint32(msg, uint32(4+len(body)))
	_, err := w.Write(append(msg, body...))
	return err
}

package corpus

// EXPERIMENTAL (DSQL typed call-site spike, #740): runtime origin capture.
// Active only when SPIKE_ATTR_OUT names a directory; otherwise every hook
// is a no-op. Not part of the gate.

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

var spikeOut = os.Getenv("SPIKE_ATTR_OUT")

type spikeFrame struct {
	Func string `json:"func"`
	File string `json:"file"`
	Line int    `json:"line"`
}

type spikeRecord struct {
	Test      string       `json:"test"`
	Label     string       `json:"label"`
	SQL       string       `json:"sql"`
	Mechanism string       `json:"mechanism"`
	Frames    []spikeFrame `json:"frames"`
}

type spikeState struct {
	mu      sync.Mutex
	stacks  [][]spikeFrame
	mechs   []string
	pending [][]spikeFrame // client-side wire writes not yet paired
}

var spikeStates sync.Map // *Recorder -> *spikeState

func spikeStateOf(r *Recorder) *spikeState {
	s, _ := spikeStates.LoadOrStore(r, &spikeState{})
	return s.(*spikeState)
}

func spikeFrames(skip int) []spikeFrame {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(skip, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var out []spikeFrame
	for {
		f, more := frames.Next()
		out = append(out, spikeFrame{Func: f.Function, File: f.File, Line: f.Line})
		if !more {
			break
		}
	}
	return out
}

// spikeOnMatch runs inside Recorder.Match. For a mock (pgxmock, sqlmock)
// it is on the production caller's goroutine; for the wire it is on the
// server goroutine, so the stack captured on the client's write is used.
func spikeOnMatch(r *Recorder, wire bool) {
	if spikeOut == "" {
		return
	}
	s := spikeStateOf(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	if wire {
		var st []spikeFrame
		if len(s.pending) > 0 {
			st, s.pending = s.pending[0], s.pending[1:]
		}
		s.stacks = append(s.stacks, st)
		s.mechs = append(s.mechs, "wire-client-write")
		return
	}
	s.stacks = append(s.stacks, spikeFrames(4))
	s.mechs = append(s.mechs, "mock-matcher")
}

// spikeConn wraps the client side of the corpus wire: every Write captures
// the writing goroutine's stack, which is the production caller's.
type spikeConn struct {
	net.Conn
	r *Recorder
}

func (c spikeConn) Write(b []byte) (int, error) {
	if spikeOut != "" && len(b) > 0 && b[0] == 'Q' {
		s := spikeStateOf(c.r)
		s.mu.Lock()
		s.pending = append(s.pending, spikeFrames(3))
		s.mu.Unlock()
	}
	return c.Conn.Write(b)
}

func spikeWrapClient(c net.Conn, r *Recorder) net.Conn {
	if spikeOut == "" {
		return c
	}
	return spikeConn{Conn: c, r: r}
}

// spikeOnTake writes one record per taken statement, pairing the test's
// asserted origin label with the captured stack.
func spikeOnTake(tb testing.TB, r *Recorder, sql []string, origins []string) {
	if spikeOut == "" {
		return
	}
	s := spikeStateOf(r)
	s.mu.Lock()
	stacks, mechs := s.stacks, s.mechs
	s.stacks, s.mechs = nil, nil
	s.mu.Unlock()
	f, err := os.OpenFile(filepath.Join(spikeOut, "records.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		tb.Fatalf("spike capture: %v", err)
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	for i := range sql {
		rec := spikeRecord{Test: tb.Name(), Label: origins[i], SQL: sql[i]}
		if i < len(stacks) {
			rec.Frames, rec.Mechanism = stacks[i], mechs[i]
		} else {
			rec.Mechanism = fmt.Sprintf("missing-stack(%d of %d)", i, len(stacks))
		}
		if err := enc.Encode(rec); err != nil {
			tb.Fatalf("spike capture: %v", err)
		}
	}
}

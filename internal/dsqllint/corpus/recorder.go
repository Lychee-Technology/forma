package corpus

import (
	"errors"
	"sync"
	"testing"
)

// Recorder captures the SQL a connection receives, for code that builds its
// statement inline and sends it itself. Build a mock with the matcher of its
// driver, MatchPgx or MatchSQL: the mock then accepts every statement and
// records it in order, and the test still scripts the results the code under
// test reads back. A WireConn records into it too, so the statements pgx
// writes and those a mock receives interleave in the order they were sent.
type Recorder struct {
	mu         sync.Mutex
	statements []recordedStatement
	errs       []error
}

type recordedStatement struct {
	mechanism string
	sql       string
}

// MatchPgx is a pgxmock.QueryMatcherFunc: it records actual and accepts it.
func (r *Recorder) MatchPgx(_, actual string) error {
	r.record(MechanismPgxmock, actual)
	return nil
}

// MatchSQL is a sqlmock.QueryMatcherFunc: it records actual and accepts it.
func (r *Recorder) MatchSQL(_, actual string) error {
	r.record(MechanismSQLmock, actual)
	return nil
}

func (r *Recorder) record(mechanism, sql string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statements = append(r.statements, recordedStatement{mechanism: mechanism, sql: sql})
}

func (r *Recorder) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, err)
}

// Take answers the recorded statements as executed statements with labels,
// one label per statement in order, and clears the recorder. A count mismatch
// fails tb: the code under test sent more or fewer statements than the test
// names.
func (r *Recorder) Take(tb testing.TB, labels ...string) []Statement {
	tb.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.errs) > 0 {
		tb.Fatalf("recording failed: %v", errors.Join(r.errs...))
	}
	if len(r.statements) != len(labels) {
		tb.Fatalf("recorded %d statements, test names labels for %d: %q", len(r.statements), len(labels), r.statements)
	}
	out := make([]Statement, len(labels))
	for i, label := range labels {
		out[i] = Statement{Kind: KindExecuted, Mechanism: r.statements[i].mechanism, Label: label, SQL: r.statements[i].sql}
	}
	r.statements = nil
	return out
}

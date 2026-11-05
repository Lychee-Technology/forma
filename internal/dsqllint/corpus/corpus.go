// Package corpus is the export side of the Aurora DSQL lint gate (#740).
//
// The gate lints SQL that Forma's Go code produces, not a copy of it. Each
// package whose code can reach Aurora DSQL has TestDSQLCorpus tests (every
// test whose name starts with TestDSQLCorpus) that drive that code with
// representative arguments and hand the statements to Export. Run normally, a
// test only checks that its records are well formed; run by the gate with
// CorpusDirEnv set, it also writes them to that directory as one JSON Lines
// file per unit.
//
// A statement is a typed record, not SQL text with a comment beside it, so the
// gate never has to read SQL to learn where a statement starts, how many there
// are or what produced it.
package corpus

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

// CorpusDirEnv names the directory the gate asks TestDSQLCorpus tests to
// write their units into. Unset, Export writes nothing.
const CorpusDirEnv = "FORMA_DSQL_CORPUS_DIR"

// UnitFileSuffix ends the name of a unit's file in the corpus directory.
const UnitFileSuffix = ".jsonl"

// Kind says how a record's SQL was obtained.
type Kind string

const (
	// KindExecuted is SQL production code sent to a connection.
	KindExecuted Kind = "executed"
	// KindRendered is SQL a test obtained from a generator without sending it.
	// It says what the generator produces, not that anything sends it.
	KindRendered Kind = "rendered"
)

// The mechanisms a record's SQL can come through.
const (
	MechanismPgxmock    = "pgxmock"     // executed: received by a pgxmock connection
	MechanismSQLmock    = "sqlmock"     // executed: received by a sqlmock connection
	MechanismWireClient = "wire-client" // executed: written by real pgx to a WireConn
	MechanismGenerator  = "generator"   // rendered: returned by the generator
	// MechanismTestBoundary is a BEGIN or COMMIT a test spells around rendered
	// statements, to show which of them share a transaction.
	MechanismTestBoundary = "test-boundary"
)

// mechanismsOf is the mechanisms each kind can come through.
var mechanismsOf = map[Kind][]string{
	KindExecuted: {MechanismPgxmock, MechanismSQLmock, MechanismWireClient},
	KindRendered: {MechanismGenerator, MechanismTestBoundary},
}

// Statement is one statement of a unit, before Export stamps it.
type Statement struct {
	Kind      Kind
	Mechanism string
	// Label names what produced the statement, "path/to/file.go:Symbol"
	// relative to the repository root. The corpus test asserts it; nothing
	// verifies it yet (#806).
	Label string
	// SQL is the statement as Forma sends it.
	SQL string
}

// Rendered is a statement a test obtained from the generator label names.
func Rendered(label, sql string) Statement {
	return Statement{Kind: KindRendered, Mechanism: MechanismGenerator, Label: label, SQL: sql}
}

// Transaction wraps body in the BEGIN and COMMIT its caller sends around it,
// both labelled label, the code that opens the transaction. Spelling the
// boundary lets dsql-lint check DSQL's one-DDL-per-transaction rule.
func Transaction(label string, body ...Statement) []Statement {
	boundary := func(sql string) Statement {
		return Statement{Kind: KindRendered, Mechanism: MechanismTestBoundary, Label: label, SQL: sql}
	}
	out := make([]Statement, 0, len(body)+2)
	out = append(out, boundary("BEGIN"))
	out = append(out, body...)
	return append(out, boundary("COMMIT"))
}

// Unit is statements Forma sends together, in order.
type Unit struct {
	// Name is the file stem, unique across the whole corpus.
	Name string
	// Engine is the engine the unit's statements were built for. The gate
	// lints only forma.EngineAuroraDSQL.
	Engine     forma.DatabaseEngine
	Statements []Statement
}

// Record is one line of a unit's file: what the gate reads.
type Record struct {
	Kind      Kind                 `json:"kind"`
	SQL       string               `json:"sql"`
	Engine    forma.DatabaseEngine `json:"engine"`
	Mechanism string               `json:"mechanism"`
	Unit      string               `json:"unit"`
	Seq       int                  `json:"seq"`
	Label     string               `json:"label"`
}

var (
	unitName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	// LabelPattern is the shape of a record's label, and of a known finding's.
	LabelPattern = regexp.MustCompile(`^[A-Za-z0-9_./-]+\.go:[A-Za-z_][A-Za-z0-9_.]*$`)
	// fmtVerb catches a statement whose fmt.Sprintf template was not filled
	// in: a stray verb or a fmt error marker means the render went wrong.
	fmtVerb = regexp.MustCompile(`%(\[\d+\])?[sdvqx]|%!`)
)

// Validate answers why r cannot be linted, or nil. Whether its SQL holds a
// statement at all is the gate's check, which needs DuckDB.
func (r Record) Validate() error {
	where := fmt.Sprintf("corpus unit %s record %d", r.Unit, r.Seq)
	switch {
	case !unitName.MatchString(r.Unit):
		return fmt.Errorf("corpus unit name %q must be lower-case letters, digits and dashes", r.Unit)
	case r.Seq < 1:
		return fmt.Errorf("%s: seq must count from 1", where)
	case r.Engine != forma.EngineAuroraDSQL:
		return fmt.Errorf("%s: built for engine %q, the gate lints only %q", where, r.Engine, forma.EngineAuroraDSQL)
	case !LabelPattern.MatchString(r.Label):
		return fmt.Errorf("%s: label %q is not a path/file.go:Symbol location", where, r.Label)
	case !mechanismFits(r.Kind, r.Mechanism):
		return fmt.Errorf("%s: kind %q cannot come through mechanism %q", where, r.Kind, r.Mechanism)
	case fmtVerb.MatchString(r.SQL):
		return fmt.Errorf("%s (%s) holds an unrendered format verb: %s", where, r.Label, r.SQL)
	}
	return nil
}

func mechanismFits(kind Kind, mechanism string) bool {
	for _, m := range mechanismsOf[kind] {
		if m == mechanism {
			return true
		}
	}
	return false
}

// Records stamps u's statements with its name, its engine and their order, or
// answers why they cannot be linted.
func (u Unit) Records() ([]Record, error) {
	if len(u.Statements) == 0 {
		return nil, fmt.Errorf("corpus unit %s has no statements", u.Name)
	}
	records := make([]Record, len(u.Statements))
	for i, st := range u.Statements {
		records[i] = Record{Kind: st.Kind, SQL: st.SQL, Engine: u.Engine, Mechanism: st.Mechanism, Unit: u.Name, Seq: i + 1, Label: st.Label}
		if err := records[i].Validate(); err != nil {
			return nil, fmt.Errorf("invalid statement %d: %w", i+1, err)
		}
	}
	return records, nil
}

// Export checks u and, when CorpusDirEnv is set, writes it to that directory.
// Any failure fails tb: the gate must never lint a partial corpus. Two units
// with one name fail the second export.
func Export(tb testing.TB, u Unit) {
	tb.Helper()
	records, err := u.Records()
	if err != nil {
		tb.Fatalf("build DSQL lint corpus unit: %v", err)
	}
	dir := os.Getenv(CorpusDirEnv)
	if dir == "" {
		return
	}
	text, err := encodeRecords(records)
	if err != nil {
		tb.Fatalf("encode DSQL lint corpus unit %s: %v", u.Name, err)
	}
	if err := writeExclusive(filepath.Join(dir, u.Name+UnitFileSuffix), text); err != nil {
		tb.Fatalf("export DSQL lint corpus unit %s: %v", u.Name, err)
	}
}

func encodeRecords(records []Record) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	for _, r := range records {
		if err := enc.Encode(r); err != nil {
			return nil, fmt.Errorf("encode record %d: %w", r.Seq, err)
		}
	}
	return b.Bytes(), nil
}

func writeExclusive(path string, text []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if errors.Is(err, os.ErrExist) {
		return fmt.Errorf("%s already exists: another unit has this name", path)
	}
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	if _, err := f.Write(text); err != nil {
		_ = f.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}

// ReadUnit reads the file of one unit and holds it to what Export writes: at
// least one record, none with an unknown field, each valid, naming this unit
// and numbered in order. The file is the boundary between the corpus tests and
// the gate, so nothing a test wrote is taken on trust.
func ReadUnit(path string) ([]Record, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read corpus unit: %w", err)
	}
	unit := strings.TrimSuffix(filepath.Base(path), UnitFileSuffix)
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var records []Record
	for {
		var r Record
		err := dec.Decode(&r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("corpus unit %s record %d is not a typed record: %w", unit, len(records)+1, err)
		}
		if err := r.Validate(); err != nil {
			return nil, fmt.Errorf("invalid record in %s: %w", filepath.Base(path), err)
		}
		if r.Unit != unit || r.Seq != len(records)+1 {
			return nil, fmt.Errorf("%s line %d names unit %s record %d, want unit %s record %d", filepath.Base(path), len(records)+1, r.Unit, r.Seq, unit, len(records)+1)
		}
		records = append(records, r)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("corpus unit %s holds no record", unit)
	}
	return records, nil
}

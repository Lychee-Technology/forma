package corpus

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sizeguard"
)

// TestPackageFilesStayWithinFileSizeLimit holds coding-standard.md's 500-line
// cap for this package, sources and tests alike.
func TestPackageFilesStayWithinFileSizeLimit(t *testing.T) {
	sizeguard.Check(t, sizeguard.IncludeTests)
}

func TestRecordsStampEveryStatement(t *testing.T) {
	u := Unit{Name: "sample", Engine: forma.EngineAuroraDSQL, Statements: Transaction("cmd/tools/init_db.go:withTx",
		Rendered("internal/rowguard/ddl.go:tableDDL", "CREATE TABLE t (a INT)"),
	)}
	got, err := u.Records()
	if err != nil {
		t.Fatalf("Records: %v", err)
	}
	want := []Record{
		{Kind: KindRendered, SQL: "BEGIN", Engine: forma.EngineAuroraDSQL, Mechanism: MechanismTestBoundary, Unit: "sample", Seq: 1, Label: "cmd/tools/init_db.go:withTx"},
		{Kind: KindRendered, SQL: "CREATE TABLE t (a INT)", Engine: forma.EngineAuroraDSQL, Mechanism: MechanismGenerator, Unit: "sample", Seq: 2, Label: "internal/rowguard/ddl.go:tableDDL"},
		{Kind: KindRendered, SQL: "COMMIT", Engine: forma.EngineAuroraDSQL, Mechanism: MechanismTestBoundary, Unit: "sample", Seq: 3, Label: "cmd/tools/init_db.go:withTx"},
	}
	if len(got) != len(want) {
		t.Fatalf("Records = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestRecordsRefuseWhatCannotBeLinted(t *testing.T) {
	ok := Rendered("a/b.go:F", "SELECT 1")
	dsql := forma.EngineAuroraDSQL
	tests := []struct {
		name string
		unit Unit
		want string
	}{
		{"bad name", Unit{Name: "Bad Name", Engine: dsql, Statements: []Statement{ok}}, "lower-case"},
		{"no statements", Unit{Name: "empty", Engine: dsql}, "no statements"},
		{"bad label", Unit{Name: "u", Engine: dsql, Statements: []Statement{Rendered("init_db", "SELECT 1")}}, "not a path/file.go:Symbol"},
		{"wildcard label", Unit{Name: "u", Engine: dsql, Statements: []Statement{Rendered("a/*.go:F", "SELECT 1")}}, "not a path/file.go:Symbol"},
		{"unrendered verb", Unit{Name: "u", Engine: dsql, Statements: []Statement{Rendered("a/b.go:F", "SELECT * FROM %s")}}, "unrendered format verb"},
		{"fmt error marker", Unit{Name: "u", Engine: dsql, Statements: []Statement{Rendered("a/b.go:F", "SELECT %!d(string=x)")}}, "unrendered format verb"},
		{"unset engine", Unit{Name: "u", Statements: []Statement{ok}}, "the gate lints only"},
		{"postgres engine", Unit{Name: "u", Engine: forma.EnginePostgreSQL, Statements: []Statement{ok}}, "the gate lints only"},
		{"executed by a generator", Unit{Name: "u", Engine: dsql, Statements: []Statement{{Kind: KindExecuted, Mechanism: MechanismGenerator, Label: "a/b.go:F", SQL: "SELECT 1"}}}, "cannot come through"},
		{"rendered by a mock", Unit{Name: "u", Engine: dsql, Statements: []Statement{{Kind: KindRendered, Mechanism: MechanismPgxmock, Label: "a/b.go:F", SQL: "SELECT 1"}}}, "cannot come through"},
		{"no kind", Unit{Name: "u", Engine: dsql, Statements: []Statement{{Label: "a/b.go:F", SQL: "SELECT 1"}}}, "cannot come through"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.unit.Records()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Records error = %v, want one containing %q", err, tt.want)
			}
		})
	}
}

func TestExportWritesOnlyWhenAsked(t *testing.T) {
	u := Unit{Name: "one", Engine: forma.EngineAuroraDSQL, Statements: []Statement{Rendered("a/b.go:F", "SELECT a FROM t WHERE a > 1 -- x\n")}}

	t.Setenv(CorpusDirEnv, "")
	Export(t, u) // checks, writes nothing

	dir := t.TempDir()
	t.Setenv(CorpusDirEnv, dir)
	Export(t, u)
	path := filepath.Join(dir, "one"+UnitFileSuffix)
	got, err := ReadUnit(path)
	if err != nil {
		t.Fatalf("ReadUnit: %v", err)
	}
	if len(got) != 1 || got[0].SQL != u.Statements[0].SQL || got[0].Label != "a/b.go:F" || got[0].Seq != 1 {
		t.Fatalf("the record did not round-trip: %+v", got)
	}
	if err := writeExclusive(path, []byte("x")); err == nil || !strings.Contains(err.Error(), "another unit has this name") {
		t.Fatalf("a second unit named one must be refused, got %v", err)
	}
}

// TestReadUnitFailsClosed holds the file a corpus test leaves to what Export
// writes: the gate takes nothing in it on trust.
func TestReadUnitFailsClosed(t *testing.T) {
	const good = `{"kind":"rendered","sql":"SELECT 1","engine":"aurora-dsql","mechanism":"generator","unit":"u","seq":1,"label":"a/b.go:F"}`
	tests := []struct {
		name string
		text string
		want string
	}{
		{"no record", "", "holds no record"},
		{"sql text, not a record", "SELECT 1;\n", "not a typed record"},
		{"unknown field", strings.Replace(good, `"seq":1`, `"seq":1,"origin":"x"`, 1), "not a typed record"},
		{"truncated", good[:len(good)-5], "not a typed record"},
		{"postgres engine", strings.Replace(good, "aurora-dsql", "postgresql", 1), "the gate lints only"},
		{"missing engine", strings.Replace(good, `"engine":"aurora-dsql",`, "", 1), "the gate lints only"},
		{"missing kind", strings.Replace(good, `"kind":"rendered",`, "", 1), "cannot come through"},
		{"another unit", strings.Replace(good, `"unit":"u"`, `"unit":"v"`, 1), "names unit v"},
		{"seq from 2", strings.Replace(good, `"seq":1`, `"seq":2`, 1), "names unit u record 2"},
		{"duplicate seq", good + "\n" + good + "\n", "names unit u record 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "u"+UnitFileSuffix)
			if err := os.WriteFile(path, []byte(tt.text), 0o644); err != nil {
				t.Fatalf("write unit: %v", err)
			}
			_, err := ReadUnit(path)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ReadUnit error = %v, want one containing %q", err, tt.want)
			}
		})
	}
}

func TestRecorderTakesStatementsInOrderWithTheirMechanism(t *testing.T) {
	var r Recorder
	_ = r.MatchPgx("ignored", "SELECT 1")
	_ = r.MatchSQL("ignored", "SELECT 2")
	got := r.Take(t, "a.go:F", "a.go:G")
	if got[0].SQL != "SELECT 1" || got[0].Mechanism != MechanismPgxmock || got[0].Kind != KindExecuted {
		t.Errorf("first statement = %+v", got[0])
	}
	if got[1].SQL != "SELECT 2" || got[1].Mechanism != MechanismSQLmock || got[1].Label != "a.go:G" {
		t.Errorf("second statement = %+v", got[1])
	}
	if len(r.statements) != 0 {
		t.Errorf("Take left %+v in the recorder", r.statements)
	}
}

// TestWireConnRecordsWhatPgxWrites checks that a real pgx connection's BEGIN
// and COMMIT reach the record as executed statements of the wire mechanism.
func TestWireConnRecordsWhatPgxWrites(t *testing.T) {
	var r Recorder
	conn := WireConn(t, &r)
	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	got := r.Take(t, "a.go:F", "a.go:F")
	for i, want := range []string{"begin", "commit"} {
		if !strings.EqualFold(got[i].SQL, want) || got[i].Mechanism != MechanismWireClient || got[i].Kind != KindExecuted {
			t.Errorf("statement %d = %+v, want an executed %s from the wire", i, got[i], want)
		}
	}
}

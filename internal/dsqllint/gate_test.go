package dsqllint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

// fixtureTool is a stand-in dsql-lint for a fixture repository: it reports one
// index_async diagnostic per line holding CREATE INDEX, and nothing else.
const fixtureTool = `#!/bin/sh
if [ "$1" = --version ]; then echo 'dsql-lint ` + ToolVersion + `'; exit 0; fi
shift 2
code=0; sep=''
printf '{"schema_version": 1, "files": ['
for f in "$@"; do
  d=$(awk '/CREATE INDEX/ { printf "%s{\"rule\": \"index_async\", \"line\": %d, \"message\": \"m\", \"suggestion\": \"s\", \"statement_preview\": \"p\"}", sep, NR; sep="," }' "$f")
  if [ -n "$d" ]; then code=1; fi
  printf '%s{"file": "%s", "error": null, "diagnostics": [%s]}' "$sep" "$f" "$d"; sep=','
done
printf ']}\n'
exit $code
`

// fixtureCorpusTest copies the fixture's units into the corpus directory. The
// func keyword is kept off the start of a line here, so that this package holds
// no corpus test by corpusPackages' reading.
const fixtureCorpusTest = "package p\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n\t\"testing\"\n)\n\n" +
	"func TestDSQLCorpus(t *testing.T) {\n" + `
	dir := os.Getenv("FORMA_DSQL_CORPUS_DIR")
	if dir == "" {
		return
	}
	entries, err := os.ReadDir("units")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join("units", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, e.Name()), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
`

const (
	labelQ = "p/p.go:Q"
	labelI = "p/p.go:I"
)

// fixtureRecords answers a unit's file holding one rendered aurora-dsql record
// per statement, each labelled label.
func fixtureRecords(t *testing.T, unit, label string, statements ...string) string {
	t.Helper()
	var b strings.Builder
	for i, sql := range statements {
		line, err := json.Marshal(corpus.Record{Kind: corpus.KindRendered, SQL: sql, Engine: forma.EngineAuroraDSQL, Mechanism: corpus.MechanismGenerator, Unit: unit, Seq: i + 1, Label: label})
		if err != nil {
			t.Fatalf("marshal record: %v", err)
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// gateFixture is a one-package repository the gate can run on. By default it
// holds one unit of one clean statement, a corpus floor asking for that
// statement, and no known finding.
type gateFixture struct {
	root, tool string
}

func newGateFixture(t *testing.T) gateFixture {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the fixture dsql-lint is a shell script")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the gate runs go test to export the corpus")
	}
	f := gateFixture{root: t.TempDir()}
	f.tool = filepath.Join(f.root, "tools/dsql-lint")
	f.write(t, "go.mod", "module example.com/fixture\n\ngo 1.26\n")
	f.write(t, "p/p.go", "package p\n")
	f.write(t, "p/p_test.go", fixtureCorpusTest)
	f.write(t, "p/units/p.jsonl", fixtureRecords(t, "p", labelQ, "SELECT 1"))
	f.write(t, KnownPath, "# none\n")
	f.write(t, FloorPath, labelQ+" 1\n")
	f.write(t, RegressionDir+"/invalid.sql", "-- expect: index_async\nCREATE INDEX i ON t (a);\n")
	f.write(t, RegressionDir+"/valid.sql", "SELECT 1;\n")
	f.write(t, "tools/dsql-lint", fixtureTool)
	return f
}

func (f gateFixture) write(t *testing.T, rel, text string) {
	t.Helper()
	path := filepath.Join(f.root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(text), 0o755); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

func (f gateFixture) remove(t *testing.T, rel string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(f.root, rel)); err != nil {
		t.Fatalf("remove %s: %v", rel, err)
	}
}

func (f gateFixture) run() (string, error) {
	var out bytes.Buffer
	err := Run(context.Background(), Config{Root: f.root, Tool: f.tool, Out: &out})
	return out.String(), err
}

func TestRunPassesACleanCorpusAndSaysWhatItDoesNotCheck(t *testing.T) {
	t.Parallel()
	out, err := newGateFixture(t).run()
	if err != nil {
		t.Fatalf("Run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"Linted: 1 corpus units, 1 statements (0 executed, 1 rendered) from 1 origin labels",
		"no SQL inventory yet",
		"not proof of DSQL compatibility",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the report lacks %q:\n%s", want, out)
		}
	}
}

// TestRunFailsClosed walks the failures the gate must not pass, each through
// the whole gate over a real go test export. `want` is looked for in the error
// and the report together.
func TestRunFailsClosed(t *testing.T) {
	t.Parallel()
	postgres := strings.Replace(fixtureRecords(t, "p", labelQ, "SELECT 1"), "aurora-dsql", "postgresql", 1)
	tests := []struct {
		name   string
		mutate func(t *testing.T, f gateFixture)
		want   string
	}{
		{"zero units", func(t *testing.T, f gateFixture) {
			f.remove(t, "p/units/p.jsonl")
		}, "wrote no unit"},
		{"a stray file among the units", func(t *testing.T, f gateFixture) { f.write(t, "p/units/stray.sql", "SELECT 1;\n") }, "stray.sql, which is not a unit's records file"},
		{"no corpus test", func(t *testing.T, f gateFixture) { f.remove(t, "p/p_test.go") }, "no package has a TestDSQLCorpus test"},
		{"zero records", func(t *testing.T, f gateFixture) { f.write(t, "p/units/p.jsonl", "") }, "holds no record"},
		{"sql text instead of records", func(t *testing.T, f gateFixture) { f.write(t, "p/units/p.jsonl", "SELECT 1;\n") }, "not a typed record"},
		{"built for another engine", func(t *testing.T, f gateFixture) { f.write(t, "p/units/p.jsonl", postgres) }, `built for engine "postgresql"`},
		{"empty statement", func(t *testing.T, f gateFixture) { f.write(t, "p/units/p.jsonl", fixtureRecords(t, "p", labelQ, "")) }, "holds no statement"},
		{"whitespace statement", func(t *testing.T, f gateFixture) {
			f.write(t, "p/units/p.jsonl", fixtureRecords(t, "p", labelQ, " \n\v\f"))
		}, "holds no statement"},
		{"comment-only statement", func(t *testing.T, f gateFixture) {
			f.write(t, "p/units/p.jsonl", fixtureRecords(t, "p", labelQ, "/* a /* b */ c */"))
		}, "holds no statement"},
		{"export test failure", func(t *testing.T, f gateFixture) { f.remove(t, "p/units") }, "export the DSQL corpus"},
		{"a floor label lost", func(t *testing.T, f gateFixture) { f.write(t, FloorPath, labelQ+" 1\n"+labelI+" 1\n") }, "holds 0 record(s) of p/p.go:I"},
		{"a floor count lost", func(t *testing.T, f gateFixture) { f.write(t, FloorPath, labelQ+" 2\n") }, "holds 1 record(s) of p/p.go:Q, the floor is 2"},
		{"no floor", func(t *testing.T, f gateFixture) { f.remove(t, FloorPath) }, "open corpus floor"},
		{"no known findings file", func(t *testing.T, f gateFixture) { f.remove(t, KnownPath) }, "open known findings"},
		{"no fixtures", func(t *testing.T, f gateFixture) { f.remove(t, RegressionDir) }, "no regression fixtures"},
		{"positive fixture misses a rule", func(t *testing.T, f gateFixture) {
			f.write(t, RegressionDir+"/invalid.sql", "-- expect: index_async\n-- expect: truncate\nCREATE INDEX i ON t (a);\n")
		}, "the fixture expects [index_async,truncate]"},
		{"negative fixture has a diagnostic", func(t *testing.T, f gateFixture) {
			f.write(t, RegressionDir+"/valid.sql", "CREATE INDEX i ON t (a);\n")
		}, "valid.sql: dsql-lint reported rules [index_async], the fixture expects []"},
		{"no valid fixture", func(t *testing.T, f gateFixture) { f.remove(t, RegressionDir+"/valid.sql") }, "at least one fixture with expect comments and one without"},
		{"tool usage error", func(t *testing.T, f gateFixture) { f.write(t, "tools/dsql-lint", "#!/bin/sh\necho nope; exit 2\n") }, "exited 2"},
		{"tool emits bad JSON", func(t *testing.T, f gateFixture) { f.write(t, "tools/dsql-lint", "#!/bin/sh\necho nope; exit 1\n") }, "parse dsql-lint JSON"},
		{"nondeterministic corpus", func(t *testing.T, f gateFixture) {
			test := strings.Replace(fixtureCorpusTest, "os.ReadDir(\"units\")", "os.ReadDir(\"units\")\n\tif err == nil {\n\t\terr = os.WriteFile(filepath.Join(dir, \"odd.jsonl\"), []byte(dir), 0o644)\n\t}", 1)
			f.write(t, "p/p_test.go", test)
		}, "two exports differ in odd.jsonl"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newGateFixture(t)
			tt.mutate(t, f)
			out, err := f.run()
			if err == nil {
				t.Fatalf("Run passed:\n%s", out)
			}
			if !strings.Contains(err.Error()+"\n"+out, tt.want) {
				t.Fatalf("Run error = %v, want %q in it or the report:\n%s", err, tt.want, out)
			}
		})
	}
}

// corruptUnitResult wraps the fixture dsql-lint so that a sed script rewrites
// its output on the corpus run alone. The regression fixtures still pass and
// the exit code is kept, so that only the gate's reading of the corrupted result
// can fail the run.
func (f gateFixture) corruptUnitResult(t *testing.T, sedScript string) {
	t.Helper()
	wrapped := f.tool + "-wrapped"
	f.write(t, "tools/dsql-lint-wrapped", fixtureTool)
	f.write(t, "tools/dsql-lint", "#!/bin/sh\nout=$('"+wrapped+"' \"$@\")\ncode=$?\nprintf '%s\\n' \"$out\" | sed -e '"+sedScript+"'\nexit $code\n")
}

// TestRunRefusesAMalformedFileResult is the whole gate on a result that claims
// nothing wrong: a corpus unit's required field is gone, the regression fixtures
// are untouched, and the exit code still agrees with the other results.
func TestRunRefusesAMalformedFileResult(t *testing.T) {
	t.Parallel()
	const unitResult = `/\/sql\/p\.sql"/`
	tests := []struct {
		name      string
		sedScript string
		want      string
	}{
		{"diagnostics omitted", unitResult + `s/, "diagnostics": \[\]//`, "p.sql has no diagnostics array"},
		{"diagnostics null", unitResult + `s/"diagnostics": \[\]/"diagnostics": null/`, "p.sql has no diagnostics array"},
		{"error omitted", unitResult + `s/"error": null, //`, "p.sql has no error field"},
		{"file omitted", unitResult + `s/"file": "[^"]*", //`, "no file name"},
		{"files omitted", unitResult + `s/"files": .*/"summary": {}}/`, "no files array"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newGateFixture(t)
			f.corruptUnitResult(t, tt.sedScript)
			out, err := f.run()
			if err == nil {
				t.Fatalf("Run passed:\n%s", out)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Run error = %v, want %q in it:\n%s", err, tt.want, out)
			}
		})
	}
}

// TestRunRefusesAMalformedResultBesideKnownFindings is the failure as it was
// found: other units report the findings the known list accepts and the tool
// exits 1, so only the one unit's missing diagnostics can fail the run.
func TestRunRefusesAMalformedResultBesideKnownFindings(t *testing.T) {
	t.Parallel()
	f := newGateFixture(t)
	f.write(t, "p/units/p.jsonl", fixtureRecords(t, "p", labelI, "CREATE INDEX i ON t (a)"))
	f.write(t, "p/units/q.jsonl", fixtureRecords(t, "q", labelQ, "SELECT 1"))
	f.write(t, FloorPath, labelI+" 1\n"+labelQ+" 1\n")
	f.write(t, KnownPath, labelI+" index_async 1 #690 -- one index\n")
	if out, err := f.run(); err != nil {
		t.Fatalf("the uncorrupted gate must pass: %v\n%s", err, out)
	}
	f.corruptUnitResult(t, `s#\("file": "[^"]*/sql/q\.sql", "error": null\), "diagnostics": \[\]#\1#`)
	out, err := f.run()
	if err == nil {
		t.Fatalf("Run passed:\n%s", out)
	}
	if !strings.Contains(err.Error(), "q.sql has no diagnostics array") {
		t.Fatalf("Run error = %v\n%s", err, out)
	}
}

// TestRunMatchesKnownFindingsExactly is T-29 through the whole gate, on a unit
// whose lines dsql-lint's stand-in reports: the same list passes only the exact
// corpus.
func TestRunMatchesKnownFindingsExactly(t *testing.T) {
	t.Parallel()
	const index = "CREATE INDEX i ON t (a)"
	known := labelI + " index_async 2 #690 -- two indexes\n"
	tests := []struct {
		name       string
		statements []string
		label      string
		known      string
		want       string
	}{
		{"exact", []string{index, index}, labelI, known, "tracked by:   #690"},
		{"one more", []string{index, index, index}, labelI, known, "lists 2 index_async, the corpus has 3"},
		{"one fewer", []string{index}, labelI, known, "lists 2 index_async, the corpus has 1"},
		{"another origin", []string{index, index}, labelQ, known, "NEW findings (2)"},
		{"no entry", []string{index}, labelI, "# none\n", "NEW findings (1)"},
		{"stale entry", []string{"SELECT 1"}, labelI, known, "lists 2 index_async, the corpus has 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newGateFixture(t)
			f.write(t, "p/units/p.jsonl", fixtureRecords(t, "p", tt.label, tt.statements...))
			f.write(t, FloorPath, tt.label+" 1\n")
			f.write(t, KnownPath, tt.known)
			out, err := f.run()
			if !strings.Contains(out, tt.want) {
				t.Errorf("the report lacks %q:\n%s", tt.want, out)
			}
			if pass := tt.name == "exact"; pass != (err == nil) {
				t.Errorf("Run error = %v, want a pass: %v\n%s", err, pass, out)
			}
			if err != nil && !errors.Is(err, ErrGateFailed) {
				t.Errorf("Run error = %v, want ErrGateFailed", err)
			}
		})
	}
}

// TestRunReportsTheSameBytesEveryTime is T-37: two runs over one tree print the
// same report and export the same records, whatever directory they ran in.
func TestRunReportsTheSameBytesEveryTime(t *testing.T) {
	t.Parallel()
	f := newGateFixture(t)
	f.write(t, "p/units/p.jsonl", fixtureRecords(t, "p", labelI, "SELECT 1", "CREATE INDEX i ON t (a)", "CREATE INDEX j\nON t (a)"))
	f.write(t, FloorPath, labelI+" 1\n")
	f.write(t, KnownPath, labelI+" index_async 2 #690 -- two indexes\n")
	var reports [2]string
	var records [2][]byte
	for i := range reports {
		keep := filepath.Join(t.TempDir(), "corpus")
		var out bytes.Buffer
		if err := Run(context.Background(), Config{Root: f.root, Tool: f.tool, CorpusDir: keep, Out: &out}); err != nil {
			t.Fatalf("Run %d: %v\n%s", i, err, out.String())
		}
		reports[i] = out.String()
		data, err := os.ReadFile(filepath.Join(keep, "records", "p.jsonl"))
		if err != nil {
			t.Fatalf("read kept records: %v", err)
		}
		records[i] = data
		if _, err := os.Stat(filepath.Join(keep, "sql", "p.sql")); err != nil {
			t.Errorf("the kept corpus has no linted SQL: %v", err)
		}
	}
	if reports[0] != reports[1] {
		t.Errorf("two runs reported differently:\n%s\n---\n%s", reports[0], reports[1])
	}
	if !bytes.Equal(records[0], records[1]) {
		t.Errorf("two runs exported differently:\n%s\n---\n%s", records[0], records[1])
	}
}

func TestLintUnitsRefusesADiagnosticNoRecordHolds(t *testing.T) {
	dir := t.TempDir()
	u := &unit{name: "u", records: []corpus.Record{{Label: labelQ, SQL: "SELECT 1"}}}
	if err := u.writeSQL(dir); err != nil {
		t.Fatalf("writeSQL: %v", err)
	}
	output := `{"schema_version": 1, "files": [{"file": "` + u.sqlPath + `", "error": null, "diagnostics": [{"rule": "index_async", "line": 99, "message": "m"}]}]}`
	tool, _ := fakeTool(t, output, 1)
	_, err := lintUnits(context.Background(), tool, []*unit{u})
	if err == nil || !strings.Contains(err.Error(), "line 99 of corpus/u.sql, which holds no record") {
		t.Fatalf("lintUnits error = %v", err)
	}
}

// TestCorpusPackagesFindsCorpusTestsOnly holds the discovery to the test
// functions of this module: a nested module, testdata and a mention in a string
// are not corpus packages.
func TestCorpusPackagesFindsCorpusTestsOnly(t *testing.T) {
	f := gateFixture{root: t.TempDir()}
	corpusFile := "package x\n\nimport \"testing\"\n\nfunc TestDSQLCorpusThing(t *testing.T) {}\n"
	f.write(t, "root_test.go", corpusFile)
	f.write(t, "a/b/x_test.go", corpusFile)
	f.write(t, "mentions/x_test.go", "package x\n\nconst s = \"func TestDSQLCorpus(t)\"\n")
	f.write(t, "other/x_test.go", "package x\n\nimport \"testing\"\n\nfunc TestOther(t *testing.T) {}\n")
	f.write(t, "notest/x.go", corpusFile)
	f.write(t, "a/testdata/x_test.go", corpusFile)
	f.write(t, "nested/go.mod", "module nested\n")
	f.write(t, "nested/x_test.go", corpusFile)
	got, err := corpusPackages(f.root)
	if err != nil {
		t.Fatalf("corpusPackages: %v", err)
	}
	if want := []string{"./.", "./a/b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("corpusPackages = %v, want %v", got, want)
	}
}

// TestCorpusPackagesOfThisRepository pins the discovery against the repository
// itself: this package's fixtures must not make it a corpus package.
func TestCorpusPackagesOfThisRepository(t *testing.T) {
	got, err := corpusPackages(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("corpusPackages: %v", err)
	}
	want := []string{"./cmd/tools", "./internal", "./internal/cdc", "./internal/declaredindex", "./internal/federated", "./internal/rfc3339migrate", "./internal/widthaudit"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("corpusPackages = %v, want %v", got, want)
	}
}

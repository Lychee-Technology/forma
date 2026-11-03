package dsqllint

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// fixtureTool is a stand-in dsql-lint for a fixture repository: it reports
// one index_async diagnostic on any file named invalid.sql, so the gate's
// regression self-test passes, and nothing on any other file.
const fixtureTool = `#!/bin/sh
if [ "$1" = --version ]; then echo 'dsql-lint ` + ToolVersion + `'; exit 0; fi
shift 2
code=0; sep=''
printf '{"schema_version": 1, "files": ['
for f in "$@"; do
  d=''
  case "$f" in *invalid.sql) d='{"rule": "index_async", "line": 2, "message": "m", "suggestion": "s", "statement_preview": "p"}'; code=1;; esac
  printf '%s{"file": "%s", "error": null, "diagnostics": [%s]}' "$sep" "$f" "$d"; sep=','
done
printf ']}\n'
exit $code
`

// gateFixture writes a one-package repository whose TestDSQLCorpus writes
// unit as its corpus file, and answers its root and a fixture tool.
func gateFixture(t *testing.T, unit string) (root, tool string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the fixture dsql-lint is a shell script")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the gate runs go test to export the corpus")
	}
	root = t.TempDir()
	files := map[string]string{
		"go.mod": "module example.com/fixture\n\ngo 1.26\n",
		"p/p.go": "package p\n\nconst Q = \"SELECT 1\"\n",
		"p/p_test.go": "package p\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n\t\"testing\"\n)\n\n" +
			"func TestDSQLCorpus(t *testing.T) {\n\tdir := os.Getenv(\"FORMA_DSQL_CORPUS_DIR\")\n\tif dir == \"\" {\n\t\treturn\n\t}\n" +
			"\tif err := os.WriteFile(filepath.Join(dir, \"p.sql\"), []byte(" + quoteGo(unit) + "), 0o644); err != nil {\n\t\tt.Fatal(err)\n\t}\n}\n",
		InventoryPath:                  "rendered p/p.go:Q the fixture's one statement\n",
		KnownPath:                      "# no known findings\n",
		RegressionDir + "/invalid.sql": "-- expect: index_async\nCREATE INDEX i ON t (a);\n",
		RegressionDir + "/valid.sql":   "SELECT 1;\n",
		"tools/dsql-lint":              fixtureTool,
	}
	for rel, text := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(text), 0o755); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
	return root, filepath.Join(root, "tools/dsql-lint")
}

func quoteGo(s string) string {
	return "\"" + strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n").Replace(s) + "\""
}

// TestRunFailsClosedOnACommentOnlyCorpus is the end-to-end regression for
// a corpus that labels a statement but holds no SQL (#740 review R1-F3):
// Run, exporting through a real go test, refuses it, while the same
// fixture with a real statement passes.
func TestRunFailsClosedOnACommentOnlyCorpus(t *testing.T) {
	root, tool := gateFixture(t, "-- origin: p/p.go:Q\nSELECT 1;\n")
	var out bytes.Buffer
	if err := Run(context.Background(), Config{Root: root, Tool: tool, Out: &out}); err != nil {
		t.Fatalf("the fixture with one statement must pass: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "Linted: 1 corpus units, 1 statements from 1 generators") {
		t.Fatalf("the gate must count the one statement:\n%s", out.String())
	}

	for name, unit := range map[string]string{
		"comment only":         "-- origin: p/p.go:Q\n-- no statement\n",
		"delimiters only":      "-- origin: p/p.go:Q\n;\n /* nothing */ ;\n",
		"sql before any label": "SELECT 1;\n-- origin: p/p.go:Q\nSELECT 2;\n",
	} {
		t.Run(name, func(t *testing.T) {
			root, tool := gateFixture(t, unit)
			var out bytes.Buffer
			err := Run(context.Background(), Config{Root: root, Tool: tool, Out: &out})
			if err == nil {
				t.Fatalf("Run passed a corpus holding %q:\n%s", unit, out.String())
			}
			if !strings.Contains(err.Error(), "labels no executable SQL statement") && !strings.Contains(err.Error(), "before its first origin comment") {
				t.Fatalf("Run error = %v, want the empty or unlabelled statement named", err)
			}
		})
	}
}

// TestRunRefusesAnUnclassifiedSQLFileUntilItIsClassified is the end-to-end
// regression for #740 review R2-F1: a .sql file added to the repository
// without an inventory entry fails the gate before anything is linted, and
// classifying it as rendered makes the gate lint it.
func TestRunRefusesAnUnclassifiedSQLFileUntilItIsClassified(t *testing.T) {
	root, tool := gateFixture(t, "-- origin: p/p.go:Q\nSELECT 1;\n")
	if err := os.MkdirAll(filepath.Join(root, "db"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "db/new_table.sql"), []byte("CREATE TABLE t (id INT);\n"), 0o644); err != nil {
		t.Fatalf("write SQL file: %v", err)
	}
	var out bytes.Buffer
	err := Run(context.Background(), Config{Root: root, Tool: tool, Out: &out})
	if err == nil {
		t.Fatalf("Run passed with db/new_table.sql unclassified:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "SQL file db/new_table.sql is checked in and no inventory entry classifies it") {
		t.Fatalf("Run error = %v, want the unclassified file named:\n%s", err, out.String())
	}

	inventory := "rendered p/p.go:Q the fixture's one statement\nrendered db/new_table.sql a checked-in migration\n"
	if err := os.WriteFile(filepath.Join(root, InventoryPath), []byte(inventory), 0o644); err != nil {
		t.Fatalf("write inventory: %v", err)
	}
	out.Reset()
	if err := Run(context.Background(), Config{Root: root, Tool: tool, Out: &out}); err != nil {
		t.Fatalf("Run with db/new_table.sql classified: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "2 generators") {
		t.Fatalf("the gate must lint the classified file as a second origin:\n%s", out.String())
	}
}

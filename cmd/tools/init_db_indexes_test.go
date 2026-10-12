package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/declaredindex"
)

// The step is skipped, without touching the database, when the operator opts
// out or gives init-db no schema directory to read declarations from. A nil
// pool and conn would panic on any use.
func TestEnsureDeclaredIndexesSkips(t *testing.T) {
	cases := []struct {
		name string
		opts initDBOptions
		want string
	}{
		{"opt-out flag", initDBOptions{schemaDir: "schemas", skipDeclaredIndexes: true},
			"Skipped declared indexes (--skip-declared-indexes).\n"},
		{"no schema dir", initDBOptions{},
			"Skipped declared indexes: no --schema-dir to read x-forma-indexes from.\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := ensureDeclaredIndexes(context.Background(), nil, nil, tc.opts, &out); err != nil {
				t.Fatalf("ensureDeclaredIndexes: %v", err)
			}
			if out.String() != tc.want {
				t.Fatalf("output = %q, want %q", out.String(), tc.want)
			}
		})
	}
}

func TestPrintDeclaredIndexReportGolden(t *testing.T) {
	due := declaredindex.Index{
		Name:    "entity_main_cx_aaaaaaaaaaaaaaaa",
		Columns: []string{"ltbase_schema_id", "text_01", "bigint_01", "ltbase_row_id"},
		Sources: []declaredindex.Source{{Schema: "outbox_message", Index: "due"}, {Schema: "timer", Index: "due_at"}},
	}
	fifo := declaredindex.Index{
		Name:    "entity_main_cx_bbbbbbbbbbbbbbbb",
		Columns: []string{"ltbase_schema_id", "text_02", "bigint_03", "ltbase_row_id"},
		Sources: []declaredindex.Source{{Schema: "outbox_message", Index: "fifo"}},
	}
	report := declaredindex.Report{
		Created: []declaredindex.Index{due},
		Present: []declaredindex.Index{fifo},
		Unreferenced: []declaredindex.Existing{{
			Name: "entity_main_cx_cccccccccccccccc", Valid: true,
			Columns: []string{"ltbase_schema_id", "uuid_02", "ltbase_row_id"},
		}},
	}

	var out bytes.Buffer
	printDeclaredIndexReport(&out, "entity_main", report)
	want := strings.Join([]string{
		"Created declared index: entity_main_cx_aaaaaaaaaaaaaaaa on entity_main (ltbase_schema_id, text_01, bigint_01, ltbase_row_id) for outbox_message.due, timer.due_at",
		"Declared index already present: entity_main_cx_bbbbbbbbbbbbbbbb on entity_main (ltbase_schema_id, text_02, bigint_03, ltbase_row_id) for outbox_message.fifo",
		"Unreferenced declared index: entity_main_cx_cccccccccccccccc on entity_main (ltbase_schema_id, uuid_02, ltbase_row_id): no schema declares this key; init-db never drops indexes, so drop it yourself once nothing reads through it",
		"",
	}, "\n")
	if out.String() != want {
		t.Fatalf("report differs from golden:\n got: %q\nwant: %q", out.String(), want)
	}
}

func TestInitDBUsageListsSkipDeclaredIndexes(t *testing.T) {
	var runErr error
	stdout, _ := captureStdStreams(t, func() {
		runErr = runInitDB(context.Background(), []string{"--help"})
	})
	if runErr != nil {
		t.Fatalf("init-db --help: %v", runErr)
	}
	if !strings.Contains(stdout, "-skip-declared-indexes") {
		t.Fatalf("init-db usage does not list -skip-declared-indexes:\n%s", stdout)
	}
}

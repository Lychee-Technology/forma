package main

import (
	"strings"
	"testing"
)

func testInitDBOptions() initDBOptions {
	return initDBOptions{
		schemaTable: "schema_registry",
		eavTable:    "eav_data",
		entityMain:  "entity_main",
		changeLog:   "change_log",
	}
}

// TestCoreTableDDLCoversFourTables pins the four CREATE TABLE statements
// ensureTables issues, in order. Before #319 split them out, a dropped table
// would only have surfaced against a live database.
func TestCoreTableDDLCoversFourTables(t *testing.T) {
	ddl := coreTableDDL(testInitDBOptions())
	if len(ddl) != 4 {
		t.Fatalf("coreTableDDL returned %d statements, want 4", len(ddl))
	}
	wantTables := []string{"schema_registry", "entity_main", "eav_data", "change_log"}
	for i, want := range wantTables {
		if !strings.Contains(ddl[i].statement, want) {
			t.Errorf("statement %d does not mention %q: %s", i, want, ddl[i].statement)
		}
		if !strings.HasPrefix(ddl[i].statement, "CREATE TABLE IF NOT EXISTS") {
			t.Errorf("statement %d is not an idempotent CREATE: %s", i, ddl[i].statement)
		}
	}
}

// TestCoreTableDDLPreservesEntityMainAnnouncement guards a pre-existing quirk:
// the entity-main line is printed WITHOUT a trailing newline while the other
// three have one. #319 is a behaviour-preserving refactor, so the quirk stays;
// this test is what stops someone "tidying" it and changing tool output.
func TestCoreTableDDLPreservesEntityMainAnnouncement(t *testing.T) {
	ddl := coreTableDDL(testInitDBOptions())
	if got := ddl[1].announce; got != "Created entity main table: entity_main" {
		t.Errorf("entity main announcement = %q, want no trailing newline", got)
	}
	if got := ddl[0].announce; got != "Created schema registry table: schema_registry\n" {
		t.Errorf("schema registry announcement = %q, want a trailing newline", got)
	}
}

// TestMainColumnIndexDDLCoversEveryIndexedColumn pins the ten hot columns that
// get a covering index, and the per-column failure label ensureTables wraps.
func TestMainColumnIndexDDLCoversEveryIndexedColumn(t *testing.T) {
	ddl := mainColumnIndexDDL(testInitDBOptions())
	if len(ddl) != 10 {
		t.Fatalf("mainColumnIndexDDL returned %d statements, want 10", len(ddl))
	}
	wantCols := []string{
		"text_01", "text_02", "text_03",
		"smallint_01",
		"integer_01",
		"bigint_01", "bigint_02",
		"double_01", "double_02",
		"uuid_01",
	}
	for i, col := range wantCols {
		if !strings.Contains(ddl[i].statement, col) {
			t.Errorf("statement %d does not mention column %q: %s", i, col, ddl[i].statement)
		}
		if want := "create main index for " + col; ddl[i].failure != want {
			t.Errorf("failure label %d = %q, want %q", i, ddl[i].failure, want)
		}
	}
}

// TestEAVIndexDDLCoversNumericAndText pins the two partial EAV indexes and
// their WHERE guards — dropping a guard would silently index NULL rows.
func TestEAVIndexDDLCoversNumericAndText(t *testing.T) {
	ddl := eavIndexDDL(testInitDBOptions())
	if len(ddl) != 2 {
		t.Fatalf("eavIndexDDL returned %d statements, want 2", len(ddl))
	}
	if !strings.Contains(ddl[0].statement, "value_numeric IS NOT NULL") {
		t.Errorf("numeric index lost its partial guard: %s", ddl[0].statement)
	}
	if !strings.Contains(ddl[1].statement, "value_text IS NOT NULL") {
		t.Errorf("text index lost its partial guard: %s", ddl[1].statement)
	}
	if ddl[0].failure != "create numeric index" {
		t.Errorf("numeric failure label = %q", ddl[0].failure)
	}
	if ddl[1].failure != "create text index" {
		t.Errorf("text failure label = %q", ddl[1].failure)
	}
}

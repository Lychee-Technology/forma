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

// TestEntityMainDDLGolden pins the complete 37-line schema definition.
// This catches mutations to column types (e.g., bigint_01 TEXT), dropped
// constraints (e.g., NOT NULL, PRIMARY KEY), and missing columns (e.g., text_05).
func TestEntityMainDDLGolden(t *testing.T) {
	stmt := entityMainDDL(`"entity_main"`)

	// Critical columns and constraints that must not change
	criticalChecks := []string{
		"bigint_01          BIGINT",
		"ltbase_created_at  BIGINT NOT NULL",
		"PRIMARY KEY (ltbase_schema_id, ltbase_row_id)",
		"text_05            TEXT",
		"ltbase_schema_id   SMALLINT NOT NULL",
		"ltbase_row_id      UUID NOT NULL",
	}

	for _, check := range criticalChecks {
		if !strings.Contains(stmt, check) {
			t.Errorf("entityMainDDL missing critical definition: %q\nGot:\n%s", check, stmt)
		}
	}
}

// TestCoreTableDDLGolden pins all four table definitions and announcement strings
// to catch mutations in column definitions, constraints, and output text.
func TestCoreTableDDLGolden(t *testing.T) {
	opts := testInitDBOptions()
	ddl := coreTableDDL(opts)

	// Check schema registry table
	if !strings.Contains(ddl[0].statement, "schema_name TEXT PRIMARY KEY") {
		t.Errorf("schema registry table: missing schema_name PRIMARY KEY\n%s", ddl[0].statement)
	}
	if !strings.Contains(ddl[0].statement, "schema_id SMALLINT UNIQUE NOT NULL") {
		t.Errorf("schema registry table: missing schema_id UNIQUE NOT NULL\n%s", ddl[0].statement)
	}

	// Check entity main table (via entityMainDDL)
	if !strings.Contains(ddl[1].statement, "PRIMARY KEY (ltbase_schema_id, ltbase_row_id)") {
		t.Errorf("entity main table: missing composite PRIMARY KEY\n%s", ddl[1].statement)
	}
	if !strings.Contains(ddl[1].statement, "bigint_01          BIGINT") {
		t.Errorf("entity main table: bigint_01 must be BIGINT type\n%s", ddl[1].statement)
	}
	if !strings.Contains(ddl[1].statement, "ltbase_created_at  BIGINT NOT NULL") {
		t.Errorf("entity main table: ltbase_created_at must be NOT NULL\n%s", ddl[1].statement)
	}

	// Check EAV table
	if !strings.Contains(ddl[2].statement, "PRIMARY KEY (schema_id, row_id, attr_id, array_indices)") {
		t.Errorf("EAV table: primary key narrowed to fewer than 4 columns\n%s", ddl[2].statement)
	}
	if !strings.Contains(ddl[2].statement, "attr_id        SMALLINT NOT NULL") {
		t.Errorf("EAV table: missing attr_id column\n%s", ddl[2].statement)
	}

	// Check change log table
	if !strings.Contains(ddl[3].statement, "primary key (schema_id, row_id, flushed_at)") {
		t.Errorf("change log table: missing composite primary key\n%s", ddl[3].statement)
	}

	// Check announcements
	if ddl[0].announce != "Created schema registry table: schema_registry\n" {
		t.Errorf("schema registry announcement = %q", ddl[0].announce)
	}
	if ddl[1].announce != "Created entity main table: entity_main" {
		t.Errorf("entity main announcement = %q", ddl[1].announce)
	}
	if ddl[2].announce != "Created EAV table: eav_data\n" {
		t.Errorf("EAV announcement = %q, want 'Created EAV table: eav_data\\n'", ddl[2].announce)
	}
	if ddl[3].announce != "Created change log table: change_log\n" {
		t.Errorf("change log announcement = %q", ddl[3].announce)
	}
}

// TestMainColumnIndexDDLGolden pins the presence of ltbase_schema_id in all
// main indexes — dropping it would change query performance characteristics.
func TestMainColumnIndexDDLGolden(t *testing.T) {
	ddl := mainColumnIndexDDL(testInitDBOptions())
	for i, stmt := range ddl {
		if !strings.Contains(stmt.statement, "ltbase_schema_id") {
			t.Errorf("main index %d statement missing ltbase_schema_id: %s", i, stmt.statement)
		}
	}
}

// TestEAVIndexDDLGolden pins the exact key columns for both EAV indexes,
// particularly attr_id which is essential for attribute filtering.
func TestEAVIndexDDLGolden(t *testing.T) {
	ddl := eavIndexDDL(testInitDBOptions())

	// Numeric index must include attr_id in the key
	if !strings.Contains(ddl[0].statement, "schema_id, attr_id, value_numeric, row_id") {
		t.Errorf("numeric index key missing attr_id: %s", ddl[0].statement)
	}

	// Text index must include attr_id in the key
	if !strings.Contains(ddl[1].statement, "schema_id, attr_id, value_text, row_id") {
		t.Errorf("text index key missing attr_id: %s", ddl[1].statement)
	}
}

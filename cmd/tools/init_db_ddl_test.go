package main

import (
	"slices"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/rowguard"
	"github.com/lychee-technology/forma/internal/schemalease"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

func testInitDBOptions() initDBOptions {
	return initDBOptions{
		schemaTable: "schema_registry",
		eavTable:    "eav_data",
		entityMain:  "entity_main",
		changeLog:   "change_log",
	}
}

// allDDLStatements flattens the builder groups in the exact order
// ensureTables executes them: 8 core tables, 2 EAV indexes, 10 main-column
// indexes, and the unique key row index.
func allDDLStatements(opts initDBOptions) []tableDDL {
	var all []tableDDL
	_, uniqueKeyIndex := uniqueKeyDDL(opts)
	for _, group := range [][]tableDDL{coreTableDDL(opts), eavIndexDDL(opts), mainColumnIndexDDL(opts), {uniqueKeyIndex}} {
		all = append(all, group...)
	}
	return all
}

// wantDDLStatements is the golden text of every statement init-db issues,
// captured from the pre-#319 ensureTables and verified byte-for-byte against
// `git show 4b58a14:cmd/tools/init_db.go`.
//
// These are INDEPENDENT literals on purpose: never rebuild them by calling the
// builders, or the test asserts nothing.
//
// The whitespace inside these raw strings is data, not source formatting. The
// change_log literal genuinely mixes four-space and tab indentation; that is a
// pre-existing quirk #319 preserved deliberately. Do not "tidy" it — gofmt will
// not, and the DDL Postgres receives would change.
func wantDDLStatements() []string {
	statements := []string{
		// schema_registry table
		`CREATE TABLE IF NOT EXISTS "schema_registry" (
		schema_name TEXT PRIMARY KEY,
		schema_id SMALLINT UNIQUE NOT NULL
	)`,
		// entity_main table
		`CREATE TABLE IF NOT EXISTS "entity_main" (
		ltbase_schema_id   SMALLINT NOT NULL,
		ltbase_row_id      UUID NOT NULL,
		text_01            TEXT,
		text_02            TEXT,
		text_03            TEXT,
		text_04            TEXT,
		text_05            TEXT,
		text_06            TEXT,
		text_07            TEXT,
		text_08            TEXT,
		text_09            TEXT,
		text_10            TEXT,
		smallint_01        SMALLINT,
		smallint_02        SMALLINT,
		smallint_03        SMALLINT,
		integer_01         INTEGER,
		integer_02         INTEGER,
		integer_03         INTEGER,
		bigint_01          BIGINT,
		bigint_02          BIGINT,
		bigint_03          BIGINT,
		double_01          DOUBLE PRECISION,
		double_02          DOUBLE PRECISION,
		double_03          DOUBLE PRECISION,
		uuid_01            UUID,
		uuid_02            UUID,
		ltbase_created_at  BIGINT NOT NULL,
		ltbase_updated_at  BIGINT NOT NULL,
		ltbase_deleted_at  BIGINT,
		ltbase_created_by  TEXT,
		ltbase_updated_by  TEXT,
		ltbase_deleted_by  TEXT,
		PRIMARY KEY (ltbase_schema_id, ltbase_row_id)
	)`,
		// eav_data table
		`CREATE TABLE IF NOT EXISTS "eav_data" (
		schema_id      SMALLINT NOT NULL,
		row_id         UUID NOT NULL,
		attr_id        SMALLINT NOT NULL,
		array_indices  TEXT NOT NULL DEFAULT '',
		value_text     TEXT,
		value_numeric  NUMERIC,
		PRIMARY KEY (schema_id, row_id, attr_id, array_indices)
	)`,
		// change_log table
		`CREATE TABLE IF NOT EXISTS "change_log" (
		    schema_id  SMALLINT NOT NULL,
			row_id     UUID     NOT NULL,
			flushed_at BIGINT   NOT NULL DEFAULT 0,
			changed_at BIGINT   NOT NULL,
			deleted_at BIGINT,
			primary key (schema_id, row_id, flushed_at)
		);`,
	}
	statements = append(statements, wantUniqueKeyTableDDL()...)
	return append(statements,
		// eav numeric index
		"CREATE INDEX IF NOT EXISTS \"eav_data_numeric_idx\" ON \"eav_data\" (schema_id, attr_id, value_numeric, row_id) WHERE value_numeric IS NOT NULL",
		// eav text index
		"CREATE INDEX IF NOT EXISTS \"eav_data_text_idx\" ON \"eav_data\" (schema_id, attr_id, value_text, row_id) WHERE value_text IS NOT NULL",
		// main index on text_01
		"CREATE INDEX IF NOT EXISTS \"entity_main_text_01_idx\" ON \"entity_main\" (ltbase_schema_id, ltbase_row_id, \"text_01\")",
		// main index on text_02
		"CREATE INDEX IF NOT EXISTS \"entity_main_text_02_idx\" ON \"entity_main\" (ltbase_schema_id, ltbase_row_id, \"text_02\")",
		// main index on text_03
		"CREATE INDEX IF NOT EXISTS \"entity_main_text_03_idx\" ON \"entity_main\" (ltbase_schema_id, ltbase_row_id, \"text_03\")",
		// main index on smallint_01
		"CREATE INDEX IF NOT EXISTS \"entity_main_smallint_01_idx\" ON \"entity_main\" (ltbase_schema_id, ltbase_row_id, \"smallint_01\")",
		// main index on integer_01
		"CREATE INDEX IF NOT EXISTS \"entity_main_integer_01_idx\" ON \"entity_main\" (ltbase_schema_id, ltbase_row_id, \"integer_01\")",
		// main index on bigint_01
		"CREATE INDEX IF NOT EXISTS \"entity_main_bigint_01_idx\" ON \"entity_main\" (ltbase_schema_id, ltbase_row_id, \"bigint_01\")",
		// main index on bigint_02
		"CREATE INDEX IF NOT EXISTS \"entity_main_bigint_02_idx\" ON \"entity_main\" (ltbase_schema_id, ltbase_row_id, \"bigint_02\")",
		// main index on double_01
		"CREATE INDEX IF NOT EXISTS \"entity_main_double_01_idx\" ON \"entity_main\" (ltbase_schema_id, ltbase_row_id, \"double_01\")",
		// main index on double_02
		"CREATE INDEX IF NOT EXISTS \"entity_main_double_02_idx\" ON \"entity_main\" (ltbase_schema_id, ltbase_row_id, \"double_02\")",
		// main index on uuid_01
		"CREATE INDEX IF NOT EXISTS \"entity_main_uuid_01_idx\" ON \"entity_main\" (ltbase_schema_id, ltbase_row_id, \"uuid_01\")",
		// unique key row index (#651)
		"CREATE INDEX IF NOT EXISTS \"entity_main_unique_key_row_idx\" ON \"entity_main_unique_key\" (ltbase_schema_id, ltbase_row_id)",
	)
}

// wantUniqueKeyTableDDL is the golden text of the two unique key tables
// (#651), the row guard table and the schema lease table (#633), created
// after the four core tables.
func wantUniqueKeyTableDDL() []string {
	return []string{
		// unique key table (#651)
		`CREATE TABLE IF NOT EXISTS "entity_main_unique_key" (
		ltbase_schema_id SMALLINT NOT NULL,
		key_id           SMALLINT NOT NULL,
		key_value        TEXT     NOT NULL,
		ltbase_row_id    UUID     NOT NULL,
		PRIMARY KEY (ltbase_schema_id, key_id, key_value)
	)`,
		// unique key state table (#651)
		`CREATE TABLE IF NOT EXISTS "entity_main_unique_key_state" (
		ltbase_schema_id SMALLINT NOT NULL,
		key_id           SMALLINT NOT NULL,
		attributes_hash  TEXT     NOT NULL,
		storage_hash     TEXT     NOT NULL,
		built_at         BIGINT   NOT NULL,
		PRIMARY KEY (ltbase_schema_id, key_id)
	)`,
		// row guard table (#633)
		`CREATE TABLE IF NOT EXISTS "entity_main_row_guard" (
		ltbase_schema_id SMALLINT NOT NULL,
		ltbase_row_id    UUID     NOT NULL,
		generation       BIGINT   NOT NULL,
		PRIMARY KEY (ltbase_schema_id, ltbase_row_id)
	)`,
		// schema lease table (#633)
		`CREATE TABLE IF NOT EXISTS "entity_main_schema_lease" (
		schema_id  SMALLINT NOT NULL,
		holder     UUID     NOT NULL,
		expires_at BIGINT   NOT NULL,
		PRIMARY KEY (schema_id)
	)`,
	}
}

// wantCoreAnnouncements is the golden text of the eight lines init-db prints on
// success. Independent literals, captured the same way as the statements.
//
// Note the entity-main line has NO trailing newline while the other three do.
// That asymmetry is pre-existing tool output that #319 preserved verbatim.
func wantCoreAnnouncements() []string {
	return []string{
		"Created schema registry table: schema_registry\n",
		"Created entity main table: entity_main",
		"Created EAV table: eav_data\n",
		"Created change log table: change_log\n",
		"Created unique key table: entity_main_unique_key\n",
		"Created unique key state table: entity_main_unique_key_state\n",
		"Created row guard table: entity_main_row_guard\n",
		"Created schema lease table: entity_main_schema_lease\n",
	}
}

// TestDDLStatementsGolden compares every statement init-db issues against its
// full expected text. Spot-checking substrings lets whole columns and defaults
// be mutated undetected; this is the backstop that does not.
func TestDDLStatementsGolden(t *testing.T) {
	got := allDDLStatements(testInitDBOptions())
	want := wantDDLStatements()
	if len(got) != len(want) {
		t.Fatalf("init-db issues %d statements, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].statement != want[i] {
			t.Errorf("statement %d differs from golden:\n got: %q\nwant: %q", i, got[i].statement, want[i])
		}
	}
}

// TestCoreTableAnnouncementsGolden compares each success line against its full
// expected text, and pins that the index statements announce nothing.
func TestCoreTableAnnouncementsGolden(t *testing.T) {
	opts := testInitDBOptions()
	core := coreTableDDL(opts)
	want := wantCoreAnnouncements()
	if len(core) != len(want) {
		t.Fatalf("coreTableDDL returned %d statements, want %d", len(core), len(want))
	}
	for i := range want {
		if core[i].announce != want[i] {
			t.Errorf("announcement %d differs from golden:\n got: %q\nwant: %q", i, core[i].announce, want[i])
		}
	}
	_, uniqueKeyIndex := uniqueKeyDDL(opts)
	for i, ddl := range append(append(eavIndexDDL(opts), mainColumnIndexDDL(opts)...), uniqueKeyIndex) {
		if ddl.announce != "" {
			t.Errorf("index statement %d announces %q, want silence", i, ddl.announce)
		}
	}
}

// TestCoreTableDDLCoversEightTables pins the eight CREATE TABLE statements
// ensureTables issues, in order: the four core tables, the unique key and
// state tables (#651), then the row guard and schema lease tables (#633).
// Before #319 split them out, a dropped table would only have surfaced
// against a live database.
func TestCoreTableDDLCoversEightTables(t *testing.T) {
	ddl := coreTableDDL(testInitDBOptions())
	if len(ddl) != 8 {
		t.Fatalf("coreTableDDL returned %d statements, want 8", len(ddl))
	}
	wantTables := []string{"schema_registry", "entity_main", "eav_data", "change_log",
		`"entity_main_unique_key" (`, `"entity_main_unique_key_state" (`, `"entity_main_row_guard" (`,
		`"entity_main_schema_lease" (`}
	for i, want := range wantTables {
		if !strings.Contains(ddl[i].statement, want) {
			t.Errorf("statement %d does not mention %q: %s", i, want, ddl[i].statement)
		}
		if !strings.HasPrefix(ddl[i].statement, "CREATE TABLE IF NOT EXISTS") {
			t.Errorf("statement %d is not an idempotent CREATE: %s", i, ddl[i].statement)
		}
	}
	wantFailures := []string{
		"ensure schema registry table",
		"ensure entity main table",
		"ensure eav table",
		"ensure change log table",
		"ensure unique key table",
		"ensure unique key state table",
		"ensure row guard table",
		"ensure schema lease table",
	}
	for i, want := range wantFailures {
		if ddl[i].failure != want {
			t.Errorf("failure label %d = %q, want %q", i, ddl[i].failure, want)
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

// TestUniqueKeyDDLUsesSharedSource pins init-db's unique-key statements to
// uniquekey.Statements, the source both e2e harnesses use too (#651, #440).
func TestUniqueKeyDDLUsesSharedSource(t *testing.T) {
	opts := testInitDBOptions()
	tables, index := uniqueKeyDDL(opts)
	got := []string{tables[0].statement, index.statement, tables[1].statement}
	want := uniquekey.Statements(`"entity_main_unique_key"`, `"entity_main_unique_key_row_idx"`, `"entity_main_unique_key_state"`)
	if !slices.Equal(got, want) {
		t.Fatalf("init-db unique key DDL differs from uniquekey.Statements:\n got: %q\nwant: %q", got, want)
	}
	unquoted := uniquekey.StatementsFor("entity_main")
	for i := range unquoted {
		if strings.ReplaceAll(got[i], `"`, "") != unquoted[i] {
			t.Errorf("statement %d names other tables than the harnesses' copy:\n got: %q\nwant: %q", i, got[i], unquoted[i])
		}
	}
}

// TestRowGuardDDLUsesSharedSource pins init-db's row guard statement to
// rowguard.Statement, the source both e2e harnesses use too (#633).
func TestRowGuardDDLUsesSharedSource(t *testing.T) {
	got := rowGuardDDL(testInitDBOptions()).statement
	if want := rowguard.Statement(`"entity_main_row_guard"`); got != want {
		t.Fatalf("init-db row guard DDL differs from rowguard.Statement:\n got: %q\nwant: %q", got, want)
	}
	if unquoted := rowguard.StatementFor("entity_main"); strings.ReplaceAll(got, `"`, "") != unquoted {
		t.Errorf("init-db names another table than the harnesses' copy:\n got: %q\nwant: %q", got, unquoted)
	}
}

// TestSchemaLeaseDDLUsesSharedSource pins init-db's schema lease statement to
// schemalease.Statement, the source both e2e harnesses use too (#633).
func TestSchemaLeaseDDLUsesSharedSource(t *testing.T) {
	got := schemaLeaseDDL(testInitDBOptions()).statement
	if want := schemalease.Statement(`"entity_main_schema_lease"`); got != want {
		t.Fatalf("init-db schema lease DDL differs from schemalease.Statement:\n got: %q\nwant: %q", got, want)
	}
	if unquoted := schemalease.StatementFor("entity_main"); strings.ReplaceAll(got, `"`, "") != unquoted {
		t.Errorf("init-db names another table than the harnesses' copy:\n got: %q\nwant: %q", got, unquoted)
	}
}

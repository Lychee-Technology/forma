package textcollation

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
)

func TestDDLDrift_AcceptsEveryCollatableColumnUnderC(t *testing.T) {
	for name, ddl := range map[string]string{
		"a column per line": `CREATE TABLE IF NOT EXISTS "t" (
		schema_name TEXT COLLATE "C" PRIMARY KEY,
		array_indices  TEXT COLLATE "C" NOT NULL DEFAULT '',
		row_id UUID NOT NULL,
		label VARCHAR(20) COLLATE "C",
		PRIMARY KEY (schema_name, array_indices)
	)`,
		"one line":           `CREATE TABLE t (schema_name TEXT COLLATE "C" PRIMARY KEY, schema_id SMALLINT UNIQUE NOT NULL)`,
		"no collatable type": `CREATE TABLE t (schema_id SMALLINT NOT NULL, holder UUID NOT NULL, PRIMARY KEY (schema_id))`,
		"an added column":    `ALTER TABLE t ADD COLUMN IF NOT EXISTS note text COLLATE "C"`,
	} {
		t.Run(name, func(t *testing.T) { assert.Nil(t, DDLDrift(ddl)) })
	}
}

func TestDDLDrift_ReportsEachColumnOutsideC(t *testing.T) {
	ddl := `CREATE TABLE t (
		schema_name TEXT PRIMARY KEY,
		value_text text,
		label character varying(20) NOT NULL,
		code CHAR(2) COLLATE "POSIX",
		title TEXT COLLATE "en_US",
		ok TEXT COLLATE "C",
		id UUID)`
	assert.Equal(t, []string{
		`column code is declared COLLATE "POSIX", the contract is COLLATE "C"`,
		`column label is declared CHARACTER VARYING without COLLATE "C"`,
		`column schema_name is declared TEXT without COLLATE "C"`,
		`column title is declared COLLATE "en_US", the contract is COLLATE "C"`,
		`column value_text is declared TEXT without COLLATE "C"`,
	}, DDLDrift(ddl))
	assert.Equal(t, []string{`column note is declared TEXT without COLLATE "C"`},
		DDLDrift(`ALTER TABLE t ADD COLUMN note TEXT NOT NULL DEFAULT ''`))
}

func TestDriftError_NamesEachColumnAndTheMigration(t *testing.T) {
	err := &DriftError{Violations: []Violation{
		{Table: `"entity_main"`, Column: "text_01", Type: "text", Collation: `pg_catalog."default"`},
		{Table: `"eav_data"`, Column: "value_text", Type: "text", Collation: `public.letters_first`},
		{Table: `"entity_main"`, Column: "Label", Type: "character varying(20)", Collation: `pg_catalog."POSIX"`},
	}}
	assert.Equal(t, []string{
		`ALTER TABLE "entity_main" ALTER COLUMN "text_01" TYPE text COLLATE "C", ALTER COLUMN "Label" TYPE character varying(20) COLLATE "C";`,
		`ALTER TABLE "eav_data" ALTER COLUMN "value_text" TYPE text COLLATE "C";`,
	}, err.MigrationStatements())
	assert.Equal(t, `3 Forma-owned text column(s) are not under the C collation Forma's tables are created with (#816): `+
		`table "entity_main" column text_01 (text) has collation pg_catalog."default", expected pg_catalog."C"; `+
		`table "eav_data" column value_text (text) has collation public.letters_first, expected pg_catalog."C"; `+
		`table "entity_main" column Label (character varying(20)) has collation pg_catalog."POSIX", expected pg_catalog."C". `+
		`These tables were created before the contract; CREATE TABLE IF NOT EXISTS does not change an existing column, `+
		`and init-db never rewrites one. Each statement below changes no stored value but holds an ACCESS EXCLUSIVE lock `+
		`on its table while it rebuilds every index on those columns; read docs/text-collation.md before running them, then rerun: `+
		strings.Join(err.MigrationStatements(), " "), err.Error())
}

func TestForEngine_DropsTheClauseOnlyForDSQL(t *testing.T) {
	ddl := `CREATE TABLE t (schema_name TEXT COLLATE "C" PRIMARY KEY, note TEXT COLLATE "C" NOT NULL DEFAULT '', id UUID)`
	assert.Equal(t, ddl, ForEngine(forma.EnginePostgreSQL, ddl))
	assert.Equal(t, ddl, ForEngine("", ddl), "an unset engine is PostgreSQL")
	assert.Equal(t, `CREATE TABLE t (schema_name TEXT PRIMARY KEY, note TEXT NOT NULL DEFAULT '', id UUID)`,
		ForEngine(forma.EngineAuroraDSQL, ddl))
}

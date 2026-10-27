package schemaledger

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/sqlutil"
)

// PointerColumn is the schema registry column that names each schema's
// active generation. NULL means the schema has no baseline: no data-plane
// process starts on it until baseline-schema records one.
const PointerColumn = "active_generation"

// Table suffixes name the two ledger tables after the schema registry table,
// so a deployment that names its registry table names them too.
const (
	GenerationTableSuffix = "_generation"
	AttributeTableSuffix  = "_generation_attr"
)

// Tables names the schema registry table and the two ledger tables, each
// sanitized for a statement.
type Tables struct {
	Registry, Generation, Attribute string
}

// TablesFor names the ledger tables of registry, a schema registry table as
// configured (a schema-qualified name keeps its qualifier).
func TablesFor(registry string) Tables {
	return Tables{
		Registry:   sqlutil.SanitizeIdentifier(registry),
		Generation: sqlutil.SanitizeIdentifier(sqlutil.SuffixedIdentifier(registry, GenerationTableSuffix)),
		Attribute:  sqlutil.SanitizeIdentifier(sqlutil.SuffixedIdentifier(registry, AttributeTableSuffix)),
	}
}

// The ledger keeps plain rows: no JSONB, arrays, foreign keys or sequences,
// none of which Aurora DSQL supports. recorded_at is epoch milliseconds on
// the database clock. warnings is the apply's warnings, one per line, and
// empty when there were none.
const (
	pointerDDL = `ALTER TABLE %s ADD COLUMN IF NOT EXISTS ` + PointerColumn + ` BIGINT`

	generationDDL = `CREATE TABLE IF NOT EXISTS %s (
		schema_id    SMALLINT NOT NULL,
		generation   BIGINT   NOT NULL,
		content_hash TEXT     NOT NULL,
		recorded_by  TEXT     NOT NULL,
		recorded_at  BIGINT   NOT NULL,
		warnings     TEXT     NOT NULL DEFAULT '',
		PRIMARY KEY (schema_id, generation)
	)`

	attributeDDL = `CREATE TABLE IF NOT EXISTS %s (
		schema_id   SMALLINT NOT NULL,
		generation  BIGINT   NOT NULL,
		attr_id     SMALLINT NOT NULL,
		attr_name   TEXT     NOT NULL,
		value_type  TEXT     NOT NULL,
		items_type  TEXT     NOT NULL DEFAULT '',
		column_name TEXT     NOT NULL DEFAULT '',
		encoding    TEXT     NOT NULL DEFAULT '',
		retired     BOOLEAN  NOT NULL DEFAULT FALSE,
		PRIMARY KEY (schema_id, generation, attr_id)
	)`
)

// Statements answers the ledger's DDL for a schema registry table that
// already exists: the pointer column, which an upgrade adds to an existing
// registry, and the two ledger tables. It is the one source init-db and the
// e2e harnesses create the ledger from. Each is one statement with no
// secondary index.
func Statements(registry string) []string {
	t := TablesFor(registry)
	return []string{
		fmt.Sprintf(pointerDDL, t.Registry),
		fmt.Sprintf(generationDDL, t.Generation),
		fmt.Sprintf(attributeDDL, t.Attribute),
	}
}

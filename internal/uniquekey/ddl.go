package uniquekey

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// The two key tables take their names from the entity_main table they guard,
// so every deployment and every test that names its entity_main table names
// its key tables too.
const (
	KeyTableSuffix   = "_unique_key"
	StateTableSuffix = "_unique_key_state"
)

// KeyTable names the key table of entityMain: one entry per (live row,
// built key) whose attributes are all present. A schema-qualified name keeps
// its qualifier, and a quoted one its quotes.
func KeyTable(entityMain string) string { return suffixed(entityMain, KeyTableSuffix) }

// StateTable names the built-key registry of entityMain.
func StateTable(entityMain string) string { return suffixed(entityMain, StateTableSuffix) }

// suffixed appends suffix to the table name's last part, inside its closing
// quote when it is quoted: "entity_main" becomes "entity_main_unique_key".
func suffixed(table, suffix string) string {
	if quoted, ok := strings.CutSuffix(table, `"`); ok {
		return quoted + suffix + `"`
	}
	return table + suffix
}

// keyTableDDL is the key table. Uniqueness is the primary key on an ordinary
// table, a guard-row design that needs no partial index (#651), so it holds
// on Aurora DSQL too. The table is derived state: it is never in change_log,
// never flushed to S3 and never read by DuckDB.
const keyTableDDL = `CREATE TABLE IF NOT EXISTS %s (
		ltbase_schema_id SMALLINT NOT NULL,
		key_id           SMALLINT NOT NULL,
		key_value        TEXT     NOT NULL,
		ltbase_row_id    UUID     NOT NULL,
		PRIMARY KEY (ltbase_schema_id, key_id, key_value)
	)`

// keyRowIndexDDL serves a write's lookup of the entries one row holds.
const keyRowIndexDDL = `CREATE INDEX IF NOT EXISTS %s ON %s (ltbase_schema_id, ltbase_row_id)`

// stateTableDDL records each built key and the attribute list it was built
// for.
const stateTableDDL = `CREATE TABLE IF NOT EXISTS %s (
		ltbase_schema_id SMALLINT NOT NULL,
		key_id           SMALLINT NOT NULL,
		attributes_hash  TEXT     NOT NULL,
		built_at         BIGINT   NOT NULL,
		PRIMARY KEY (ltbase_schema_id, key_id)
	)`

// Statements answers the key tables' DDL, the one source init-db and both
// e2e harnesses create them from (#440). Each argument is an identifier as
// the statement should spell it: the caller quotes it if it needs quoting.
func Statements(keyTable, rowIndex, stateTable string) []string {
	return []string{
		fmt.Sprintf(keyTableDDL, keyTable),
		fmt.Sprintf(keyRowIndexDDL, rowIndex, keyTable),
		fmt.Sprintf(stateTableDDL, stateTable),
	}
}

// StatementsFor answers Statements for an entity_main name that needs no
// quoting, as both e2e harnesses name it: the row index takes the key table's
// name with a _row_idx suffix, which is what init-db names it too.
func StatementsFor(entityMain string) []string {
	keyTable := KeyTable(entityMain)
	return Statements(keyTable, keyTable+"_row_idx", StateTable(entityMain))
}

// Attribute is one attribute of a key as the attributes hash sees it.
type Attribute struct {
	Name      string
	ValueType forma.ValueType
}

// EncodingVersion names the canonical encoding Encode implements. It is part
// of every attributes hash, so a change to the encoding, which bumps it, can
// never leave entries built under the old one looking current.
const EncodingVersion = "1"

// AttributesHash identifies what a key is built for: the encoding version,
// and the attribute names in order with their value types, which together
// decide every key value. A key whose declaration or attribute types changed
// under its id has another hash, which the build and the startup check
// refuse.
func AttributesHash(attrs []Attribute) string {
	var b strings.Builder
	fmt.Fprintf(&b, "encoding %s;", EncodingVersion)
	for _, a := range attrs {
		fmt.Fprintf(&b, "%d:%s%d:%s", len(a.Name), a.Name, len(a.ValueType), a.ValueType)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

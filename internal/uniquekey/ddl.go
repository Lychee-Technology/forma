package uniquekey

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/lychee-technology/forma"
)

// The two key tables and the key table's row index take their names from the
// entity_main table they guard, so every deployment and every test that names
// its entity_main table names them too.
const (
	KeyTableSuffix   = "_unique_key"
	StateTableSuffix = "_unique_key_state"
	RowIndexSuffix   = "_unique_key_row_idx"
)

// maxIdentifierLen is PostgreSQL's NAMEDATALEN-1. The server truncates a
// longer identifier, quoted or not, so two derived names that differ only
// past it would name one relation.
const maxIdentifierLen = 63

// digestLen is the number of hex characters of the entity_main name's
// SHA-256 a shortened name carries: 64 bits, so long entity_main names that
// share their start do not collide in practice.
const digestLen = 16

// KeyTable names the key table of entityMain: one entry per (live row,
// built key) whose attributes are all present. A schema-qualified name keeps
// its qualifier, and a quoted one its quotes.
func KeyTable(entityMain string) string { return derivedTable(entityMain, KeyTableSuffix) }

// StateTable names the built-key registry of entityMain.
func StateTable(entityMain string) string { return derivedTable(entityMain, StateTableSuffix) }

// RowIndex names the key table's row index. An index lives in its table's
// schema, so the name carries no qualifier and no quotes: the caller quotes
// it if it needs quoting.
func RowIndex(entityMain string) string {
	_, name, _ := splitTableName(entityMain)
	return boundedName(name, RowIndexSuffix)
}

// derivedTable names a table after entityMain's last part, keeping its
// qualifier, and its quotes when it is quoted: "entity_main" becomes
// "entity_main_unique_key".
func derivedTable(entityMain, suffix string) string {
	qualifier, name, quoted := splitTableName(entityMain)
	name = boundedName(name, suffix)
	if quoted {
		name = `"` + name + `"`
	}
	return qualifier + name
}

// splitTableName splits a table name into its qualifier, final dot included,
// and its last part without quotes.
func splitTableName(table string) (qualifier, name string, quoted bool) {
	if i := strings.LastIndex(table, "."); i >= 0 {
		qualifier, table = table[:i+1], table[i+1:]
	}
	if len(table) >= 2 && strings.HasPrefix(table, `"`) && strings.HasSuffix(table, `"`) {
		return qualifier, table[1 : len(table)-1], true
	}
	return qualifier, table, false
}

// boundedName joins name and suffix when the result fits an identifier.
// Otherwise the server would truncate it, and the key table and state table
// of a long entity_main name, which differ only in their suffixes, would be
// one table. A longer join keeps the start of name, then "_" and a digest of
// all of name, then the whole suffix: the suffix keeps one entity_main's
// derived names apart, the digest keeps long entity_main names apart. The
// digest covers name alone, so every spelling of entityMain, qualified or
// quoted or neither, derives the same names.
func boundedName(name, suffix string) string {
	if len(name)+len(suffix) <= maxIdentifierLen {
		return name + suffix
	}
	sum := sha256.Sum256([]byte(name))
	digest := "_" + hex.EncodeToString(sum[:])[:digestLen]
	keep := maxIdentifierLen - len(digest) - len(suffix)
	for keep > 0 && !utf8.RuneStart(name[keep]) {
		keep--
	}
	return name[:keep] + digest + suffix
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
// quoting, as both e2e harnesses name it.
func StatementsFor(entityMain string) []string {
	return Statements(KeyTable(entityMain), RowIndex(entityMain), StateTable(entityMain))
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
